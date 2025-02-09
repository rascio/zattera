package io.r.raft.log.inmemory

import arrow.core.continuations.AtomicRef
import io.r.raft.log.RaftLog
import io.r.raft.log.RaftLog.Companion.AppendResult
import io.r.raft.protocol.Index
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.LogEntryMetadata
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.Term
import io.r.utils.logs.entry
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import java.util.NavigableMap
import java.util.TreeMap

class InMemoryRaftLog(
    log: NavigableMap<Index, LogEntry> = TreeMap(),
    term: Term = 0,
    votedFor: NodeId? = null
) : RaftLog {


    private val log: NavigableMap<Index, LogEntry> = TreeMap(log).apply {
        put(0, ROOT_ENTRY)
    }
    private val term = AtomicRef(term)
    private val votedFor = AtomicRef(votedFor)

    override suspend fun getLastIndex(): Index {
        return getLastIndexInternal()
    }

    override suspend fun getTerm(): Term {
        return term.get()
    }

    override suspend fun setTerm(term: Index) {
        this.term.set(term)
        votedFor.set(null)
    }

    override suspend fun getMetadata(index: Index): LogEntryMetadata? {
        return when {
            index == 0L -> LogEntryMetadata.ZERO
            else -> log[index]?.let { (term, _) -> LogEntryMetadata(index, term) }
                ?: run {
                    logger.warn(entry("LogEntry_Not_Found", "index" to index))
                    null
                }
        }
    }

    override suspend fun getEntries(from: Index, length: Int): List<LogEntry> {
        return log.subMap(from, true, from + length, false)
            .values
            .toList()
    }


    override suspend fun append(previous: LogEntryMetadata, entries: List<LogEntry>): AppendResult {
        val stored = log[previous.index]
        when {
            stored == null -> return AppendResult.IndexNotFound
            stored.term != previous.term -> return AppendResult.EntryMismatch
            else -> {
                entries.forEachIndexed { i, entry ->
                    log[previous.index + i + 1] = entry
                }
                if (previous.index + entries.size < getLastIndexInternal()) {
                    log.tailMap(previous.index + entries.size, false).clear()
                }
                return AppendResult.Appended(getLastIndexInternal())
            }
        }
    }

    override suspend fun setVotedFor(nodeId: NodeId) {
        votedFor.set(nodeId)
    }

    override suspend fun getVotedFor(): NodeId? {
        return votedFor.get()
    }

    private fun getLastIndexInternal() = log.size.toLong() - 1

    companion object {
        private val logger: Logger = LogManager.getLogger(InMemoryRaftLog::class.java)
        private val ROOT_ENTRY = LogEntry(
            term = 0,
            entry = LogEntry.NoOp,
            id = "root"
        )
    }
}