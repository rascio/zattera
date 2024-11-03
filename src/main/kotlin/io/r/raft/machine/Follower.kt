package io.r.raft.machine

import io.r.raft.log.RaftLog
import io.r.raft.log.RaftLog.Companion.getLastMetadata
import io.r.raft.protocol.Index
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRole
import io.r.raft.protocol.RaftRpc
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

    override suspend fun onReceivedMessage(message: RaftMessage) = when (message.rpc) {
        is RaftRpc.RequestVote -> {
            val granted = message.rpc.term >= log.getTerm()
                && log.getVotedFor().let { it == null || it == message.from }
                && message.rpc.lastLog >= log.getLastMetadata()
            log.setVotedFor(message.from)
            clusterNode.send(
                to = message.from,
                rpc = RaftRpc.RequestVoteResponse(
                    log.getTerm(),
                    granted
                )
            )
        }
        is RaftRpc.AppendEntries -> {
            val result = when {
                message.rpc.term < log.getTerm() -> "TermMismatch"
                message.rpc.prevLog.index < 0L -> "Previous index must be greater than 0"
                else -> when (val metadata = log.getMetadata(message.rpc.prevLog.index)) {
                    message.rpc.prevLog -> log.append(message.rpc.prevLog.index, message.rpc.entries)
                    null -> "PreviousIndexNotFound"
                    else -> "PreviousIndexMismatch expected=${message.rpc.prevLog} actual=$metadata"
                }
            }
            val rcp = when (result) {
                is Index -> {
                    commitIndex = message.rpc.leaderCommit
                    RaftRpc.AppendEntriesResponse(
                        term = log.getTerm(),
                        matchIndex = result,
                        success = true,
                        entries = message.rpc.entries.size
                    )
                }
                else -> {
                    logger.warn(entry("Rejected_Append", "reason" to result, "prev" to "I${message.rpc.prevLog.index},T${message.rpc.prevLog.term}"))
                    RaftRpc.AppendEntriesResponse(
                        term = log.getTerm(),
                        matchIndex = message.rpc.prevLog.index,
                        success = false,
                        entries = 0
                    )
                }
            }
            clusterNode.send(to = message.from, rpc = rcp)
        }
        else -> {
            logger.debug(entry("Ignoring_Message", "message" to message.rpc, "from" to message.from))
        }
    }

    override suspend fun onTimeout() {
        changeRole(RaftRole.CANDIDATE)
    }

    companion object {
        private val logger: Logger = LogManager.getLogger(Follower::class.java)
    }
}