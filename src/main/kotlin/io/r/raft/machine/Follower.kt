package io.r.raft.machine

import io.r.raft.protocol.Index
import io.r.raft.protocol.LogEntryMetadata
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.protocol.RaftRole
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
    override val changeRole: RoleTransition
) : Role() {

    override suspend fun onReceivedMessage(message: RaftMessage) {
        when (message.rpc) {
            is RaftRpc.RequestVote -> {
                val granted = message.rpc.term >= log.getTerm()
                    && log.getVotedFor().let { it == null || it == message.from }
                    && message.rpc.lastLog >= log.getLastMetadata()
                log.setVotedFor(message.from)
                clusterNode.send(
                    message.from,
                    RaftRpc.RequestVoteResponse(
                        log.getTerm(),
                        granted
                    )
                )
            }
            is RaftRpc.AppendEntries -> {
                val result = when {
                    message.rpc.term < log.getTerm() -> AppendResult.TermMismatch
                    message.rpc.prevLog.index > log.getLastIndex() -> AppendResult.PreviousIndexNotFound
                    message.rpc.prevLog.index < 0L -> AppendResult.BadInput("Previous index must be greater than 0")
                    else -> {
                        val metadata = log.getMetadata(message.rpc.prevLog.index)
                        checkNotNull(metadata) { "Metadata for previous index must always be available" }
                        when {
                            metadata != message.rpc.prevLog -> AppendResult.PreviousIndexMismatch(
                                expected = message.rpc.prevLog,
                                actual = metadata
                            )
                            else -> AppendResult.Success(
                                log.append(message.rpc.prevLog.index, message.rpc.entries)
                            )
                        }
                    }
                }
                if (result !is AppendResult.Success) {
                    logger.warn(entry("Rejected_Append", "res" to result, "prev" to "I${message.rpc.prevLog.index},T${message.rpc.prevLog.term}"))
                }
                if (result is AppendResult.Success) {
                    commitIndex = message.rpc.leaderCommit
                    message.from.send(
                        RaftRpc.AppendEntriesResponse(
                            term = log.getTerm(),
                            matchIndex = result.index,
                            success = true,
                            entries = message.rpc.entries.size
                        )
                    )
                } else {
                    message.from.send(
                        RaftRpc.AppendEntriesResponse(
                            term = log.getTerm(),
                            matchIndex = message.rpc.prevLog.index,
                            success = false,
                            entries = 0
                        )
                    )
                }
            }
            else -> {
                logger.debug(entry("Ignoring_Message", "message" to message.rpc, "from" to message.from))
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