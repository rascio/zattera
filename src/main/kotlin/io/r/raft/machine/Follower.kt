package io.r.raft.machine

import io.r.raft.log.RaftLog
import io.r.raft.log.RaftLog.Companion.AppendResult
import io.r.raft.log.RaftLog.Companion.getLastMetadata
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRole
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.RaftCluster
import io.r.utils.logs.entry
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import kotlin.random.Random

class Follower(
    override val serverState: ServerState,
    val configuration: RaftMachine.Configuration,
    override val log: RaftLog,
    override val cluster: RaftCluster,
    override val changeRole: RoleTransition
) : Role() {

    override val timeout: Long = configuration.leaderElectionTimeoutMs + Random.nextLong(configuration.leaderElectionTimeoutJitterMs)

    override suspend fun onReceivedMessage(message: RaftMessage) = when (message.rpc) {
        is RaftRpc.RequestVote -> {
            val granted = message.rpc.term >= log.getTerm()
                && log.getVotedFor().let { it == null || it == message.from }
                && message.rpc.lastLog >= log.getLastMetadata()
            log.setVotedFor(message.from)
            cluster.send(
                to = message.from,
                rpc = RaftRpc.RequestVoteResponse(
                    log.getTerm(),
                    granted
                )
            )
        }
        is RaftRpc.AppendEntries -> {
            val result = when {
                message.rpc.term < log.getTerm() -> null
                else -> {
                    serverState.leader = message.from
                    log.append(message.rpc.prevLog, message.rpc.entries)
                }
            }
            val rcp = when (result) {
                is AppendResult.Appended -> {
                    serverState.commitIndex = message.rpc.leaderCommit
                    RaftRpc.AppendEntriesResponse(
                        term = log.getTerm(),
                        matchIndex = result.index,
                        success = true,
                        entries = message.rpc.entries.size
                    )
                }
                else -> {
                    logger.warn {
                        entry(
                            "Rejected_Append",
                            "reason" to (result ?: "Leader is behind"),
                            "prev" to "I${message.rpc.prevLog.index},T${message.rpc.prevLog.term}"
                        )
                    }
                    RaftRpc.AppendEntriesResponse(
                        term = log.getTerm(),
                        matchIndex = message.rpc.prevLog.index,
                        success = false,
                        entries = 0
                    )
                }
            }
            cluster.send(to = message.from, rpc = rcp)
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