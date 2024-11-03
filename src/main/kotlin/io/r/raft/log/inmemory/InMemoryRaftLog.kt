package io.r.raft.log.inmemory

import arrow.core.continuations.AtomicRef
import io.r.raft.protocol.Index
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.LogEntryMetadata
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.Term
import io.r.raft.log.RaftLog
import io.r.raft.log.RaftLog.Companion.AppendResult
import io.r.utils.concurrency.ReadWriteLock
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

    private val lock = ReadWriteLock()
    private val log: NavigableMap<Index, LogEntry> = TreeMap(log).apply {
        put(0, LogEntry(0, ByteArray(0)))
    }
    private val term = AtomicRef(term)
    private val votedFor = AtomicRef(votedFor)

    override suspend fun getLastIndex(): Index = lock.withReadLock {
        return getLastIndexInternal()
    }

    override suspend fun getTerm(): Term {
        return term.get()
    }

    override suspend fun setTerm(index: Index) {
        term.set(index)
        votedFor.set(null)
    }

    override suspend fun getMetadata(index: Index): LogEntryMetadata? = lock.withReadLock {
        return when {
            index == 0L -> LogEntryMetadata.ZERO
            else -> log[index]?.let { (term, _) -> LogEntryMetadata(index, term) }
                ?: run {
                    logger.warn(entry("LogEntry_Not_Found", "index" to index))
                    null
                }
        }
    }

    override suspend fun getEntries(from: Index, length: Int): List<LogEntry> = lock.withReadLock {
        return log.subMap(from, true, from + length, false)
            .values
            .toList()
    }

    override suspend fun append(previous: LogEntryMetadata, entries: List<LogEntry>): AppendResult = lock.withWriteLock {
        val actual = log[previous.index]
        when {
            actual == null -> AppendResult.IndexNotFound
            actual.term != previous.term -> AppendResult.EntryMismatch
            else -> {
                entries.forEachIndexed { i, entry ->
                    log[previous.index + i + 1] = entry
                }
                if (previous.index + entries.size < getLastIndexInternal()) {
                    log.tailMap(previous.index + entries.size, false).clear()
                }
                AppendResult.Appended(getLastIndexInternal())
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
    }
}