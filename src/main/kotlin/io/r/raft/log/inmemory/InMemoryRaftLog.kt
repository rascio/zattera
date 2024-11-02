package io.r.raft.log.inmemory

import arrow.core.continuations.AtomicRef
import io.r.raft.protocol.Index
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.LogEntryMetadata
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.Term
import io.r.raft.log.RaftLog
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
    private val log: NavigableMap<Index, LogEntry> = TreeMap(log)
    private val term = AtomicRef(term)
    private val votedFor = AtomicRef(votedFor)

    init {
        logger.info(entry("InMemoryRaftLog_Initialized", "log" to log.size, "term" to term, "votedFor" to votedFor))
    }

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

    override suspend fun append(previous: Index, entries: List<LogEntry>): Index = lock.withWriteLock {
        entries.forEachIndexed { index, entry ->
            log[index + previous + 1] = entry
        }
        if (previous + entries.size < getLastIndexInternal()) {
            log.tailMap(previous + entries.size, false).clear()
        }
        return getLastIndexInternal()
    }

    override suspend fun setVotedFor(nodeId: NodeId) {
        votedFor.set(nodeId)
    }

    override suspend fun getVotedFor(): NodeId? {
        return votedFor.get()
    }
    // DAMN KOTLIN NOT REENTRANT MUTEX!
    private fun getLastIndexInternal() = when {
        log.isEmpty() -> 0
        else -> log.lastKey()
    }

    companion object {
        private val logger: Logger = LogManager.getLogger(InMemoryRaftLog::class.java)
    }
}