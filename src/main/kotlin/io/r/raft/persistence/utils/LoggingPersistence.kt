package io.r.raft.persistence.utils

import io.r.raft.Index
import io.r.raft.LogEntry
import io.r.raft.NodeId
import io.r.raft.Persistence
import io.r.raft.Term
import io.r.utils.logs.entry
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

class LoggingPersistence(
    nodeId: String,
    private val delegate: Persistence,
    private val logEntryContentFormatter: (ByteArray) -> String = { it.decodeToString() }
) : Persistence by delegate {

    val logger: Logger = LogManager.getLogger("LoggingPersistence.$nodeId")

    override suspend fun incrementTermAndVote(id: NodeId) {
        val current = getCurrentTerm()
        delegate.incrementTermAndVote(id).also {
            logger.info(entry("Incremented term from", "previous" to current, "current" to getCurrentTerm()))
        }
    }

    override suspend fun setTerm(term: Term) {
        val current = getCurrentTerm()
        delegate.setTerm(term).also {
            if (term != current)
                logger.info { entry("Set term to", "previous" to current, "current" to term) }
        }
    }

    override suspend fun commit(upTo: Index) {
        val current = getServerState().commitIndex
        delegate.commit(upTo).also {
            if (current != upTo)
                logger.info { entry("Commit index", "previous" to current, "current" to upTo) }
        }
    }

    override suspend fun append(entries: List<LogEntry>): Index? {
        return delegate.append(entries).also { index ->
            if (entries.isNotEmpty())
                logger.info(
                    entry(
                        "Appended entries",
                        "uncomitted_index" to index,
                        "committed_index" to getServerState().commitIndex,
                        "entries" to entries.joinToString { "[T${it.term}|${logEntryContentFormatter(it.command)}]" }
                    )
                )
        }
    }

    override suspend fun voteFor(term: Term, nodeId: NodeId) {
        delegate.voteFor(term, nodeId).also {
            logger.info { entry("Voted for", "candidate" to nodeId) }
        }
    }

    companion object {
    }
}