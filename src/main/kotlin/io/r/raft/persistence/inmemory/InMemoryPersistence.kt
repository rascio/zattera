package io.r.raft.persistence.inmemory

import io.r.raft.Index
import io.r.raft.LogEntry
import io.r.raft.LogEntryMetadata
import io.r.raft.NodeId
import io.r.raft.Persistence
import io.r.raft.ServerState
import io.r.raft.Term
import io.r.raft.encodeBase64
import io.r.utils.concurrency.ReadWriteLock
import io.r.utils.logs.entry
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import java.util.NavigableMap
import java.util.TreeMap
import kotlin.math.min

typealias LogEntriesHandler = (LogEntry) -> Unit
class InMemoryPersistence(
    private val logEntriesHandler: LogEntriesHandler = LOGGER_HANDLER,
) : Persistence {

    private val lock = ReadWriteLock()
    private var state = State()

    class State {
        var currentTerm: Term = 0L
        var votedFor: NodeId? = null
        var log: NavigableMap<Index, LogEntry> = TreeMap()
        var commitIndex: Index = 0L
        var lastApplied: Index = 0L

        fun copy(block: State.() -> Unit = { }): State {
            val copy = State()
            copy.currentTerm = currentTerm
            copy.votedFor = votedFor
            copy.log = TreeMap(log)
            copy.commitIndex = commitIndex
            copy.lastApplied = lastApplied
            copy.block()
            return copy
        }

        companion object {
            operator fun invoke(block: State.() -> Unit) = State().apply(block)
        }
    }

    companion object {
        val logger: Logger = LogManager.getLogger(InMemoryPersistence::class.java)

        private val LOGGER_HANDLER: LogEntriesHandler = {
            logger.warn(entry("apply_unhandled", "term" to it.term, "command" to it.command.encodeBase64()))
        }
    }

    suspend fun load(state: State) = lock.withWriteLock {
        this.state = state
    }

    override suspend fun getServerState(): ServerState = object : ServerState {
        override val commitIndex: Index get() = state.commitIndex
        override val lastApplied: Index get() = state.lastApplied
    }

    override suspend fun canVoteFor(nodeId: NodeId): Boolean =
        lock.withReadLock {
            state.votedFor == null || state.votedFor == nodeId
        }

    override suspend fun getCurrentTerm(): Term = lock.withReadLock { state.currentTerm }

    override suspend fun getLastEntryMetadata(): LogEntryMetadata = lock.withReadLock {
        val lastEntry = state.log[state.log.size.toLong()]
        LogEntryMetadata(state.log.size.toLong(), lastEntry?.term ?: 0)
    }

    override suspend fun getLogMetadata(index: Index): LogEntryMetadata? = lock.withReadLock {
        when {
            index == 0L -> LogEntryMetadata.ZERO
            else -> state.log[index]?.let { LogEntryMetadata(index, it.term) }
        }
    }

    override suspend fun getLogs(from: Index, size: Int): List<LogEntry> = lock.withReadLock {
        (from until min(state.log.size + 1L, from + size))
            .asSequence()
            .mapNotNull { state.log[it] }
            .toList()
    }


    override suspend fun incrementTermAndVote(id: NodeId) = lock.withWriteLock {
        state.currentTerm++
        state.votedFor = id
    }

    override suspend fun setTerm(term: Term) = lock.withWriteLock {
        if (term > state.currentTerm) {
            state.votedFor = null
        }
        state.currentTerm = term
    }


    override suspend fun commit(upTo: Index) = lock.withWriteLock {
        state.commitIndex = min(upTo, state.log.size.toLong())
    }

    override suspend fun apply(upTo: Index) = lock.withWriteLock {
        val toIndex = min(upTo, state.log.size.toLong())
        state.log
            .subMap(state.lastApplied, false, toIndex, true)
            .forEach { (_, v) ->
                logEntriesHandler(v)
            }
        state.lastApplied = toIndex
    }

    override suspend fun append(entries: List<LogEntry>): Index = lock.withWriteLock {
        val startFrom = state.log.size + 1L
        entries.forEachIndexed { index, entry ->
            state.log[startFrom + index] = entry
        }
        state.log.size.toLong()
    }

    /**
     * append should reject when previous entry does not match
     * append should replace uncommitted entry
     * append should update the term
     */
    override suspend fun append(term: Term, previous: LogEntryMetadata, entries: List<LogEntry>): Index? = when {
        term < state.currentTerm -> null
        previous.index > state.log.size.toLong() -> null
        previous.index < 0L -> null
        else -> lock.withWriteLock {
            val previousEntryMetadata = when {
                previous.index == 0L -> LogEntryMetadata.ZERO
                else -> state.log[previous.index]?.let { LogEntryMetadata(previous.index, it.term) }
            }
            takeUnless { previousEntryMetadata != previous }?.run {
                val startFrom = previous.index + 1
                state.log.apply { entries.forEachIndexed { index, e -> put(index + startFrom, e) } }
                    .size
                    .toLong()
            }
        }

    }

    override suspend fun voteFor(term: Term, nodeId: NodeId) = lock.withWriteLock {
        state.votedFor = nodeId
        state.currentTerm = term
    }

    private operator fun <V> TreeMap<Index, V>.get(range: LongRange): V? =
        tailMap(range.first)
            .headMap(range.last)
            .values
            .firstOrNull()
}