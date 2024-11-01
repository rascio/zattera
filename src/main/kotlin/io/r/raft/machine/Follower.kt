package io.r.raft.machine

import io.r.raft.Index
import io.r.raft.LogEntryMetadata
import io.r.raft.Persistence
import io.r.raft.RaftMessage
import io.r.raft.RaftProtocol
import io.r.raft.RaftRole
import io.r.raft.log.RaftLog
import io.r.raft.log.RaftLog.Companion.getLastMetadata
import io.r.raft.transport.RaftClusterNode
import io.r.utils.logs.entry
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

class Follower(
    override var commitIndex: Index,
    val configuration: RaftMachine.Configuration,
    override val log: RaftLog,
    override val clusterNode: RaftClusterNode,
    override val changeRole: suspend (RaftRole) -> Role
) : Role() {

    override suspend fun onReceivedMessage(message: RaftMessage) {
        when (message.protocol) {
            is RaftProtocol.RequestVote -> {
                val granted = message.protocol.term >= log.getTerm()
                    && log.getVotedFor().let { it == null || it == message.from }
                    && message.protocol.lastLog >= log.getLastMetadata()
                log.setVotedFor(message.from)
                clusterNode.send(
                    message.from,
                    RaftProtocol.RequestVoteResponse(
                        log.getTerm(),
                        granted
                    )
                )
            }
            is RaftProtocol.AppendEntries -> {
                val result = when {
                    message.protocol.term < log.getTerm() -> AppendResult.TermMismatch
                    message.protocol.prevLog.index > log.getLastIndex() -> AppendResult.PreviousIndexNotFound
                    message.protocol.prevLog.index < 0L -> AppendResult.BadInput("Previous index must be greater than 0")
                    else -> {
                        val metadata = log.getMetadata(message.protocol.prevLog.index)
                        checkNotNull(metadata) { "Metadata for previous index must always be available" }
                        when {
                            metadata != message.protocol.prevLog -> AppendResult.PreviousIndexMismatch(
                                expected = message.protocol.prevLog,
                                actual = metadata
                            )
                            else -> AppendResult.Success(
                                log.append(message.protocol.prevLog.index, message.protocol.entries)
                            )
                        }
                    }
                }
                if (result is AppendResult.Success) {
                    commitIndex = message.protocol.leaderCommit
                    message.from.send(
                        RaftProtocol.AppendEntriesResponse(
                            term = log.getTerm(),
                            matchIndex = result.index,
                            success = true,
                            entries = message.protocol.entries.size
                        )
                    )
                } else {
                    message.from.send(
                        RaftProtocol.AppendEntriesResponse(
                            term = log.getTerm(),
                            matchIndex = -1,
                            success = false,
                            entries = 0
                        )
                    )
                }
            }
            else -> {
                logger.debug(entry("Ignoring_Message", "message" to message.protocol, "from" to message.from))
            }
        }
    }

    override suspend fun onTimeout() {
        changeRole(RaftRole.CANDIDATE)
    }
    sealed class AppendResult {
        data class Success(val index: Index) : AppendResult()
        data object TermMismatch : AppendResult()
        data object PreviousIndexNotFound : AppendResult()
        data class PreviousIndexMismatch(val expected: LogEntryMetadata, val actual: LogEntryMetadata) : AppendResult()
        data class BadInput(val reason: String) : AppendResult()
    }
    companion object {
        private val logger: Logger = LogManager.getLogger(Follower::class.java)
    }
}