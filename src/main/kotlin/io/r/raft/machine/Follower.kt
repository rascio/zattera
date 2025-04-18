package io.r.raft.machine

import io.r.raft.persistence.RaftLog
import io.r.raft.persistence.RaftLog.Companion.AppendResult
import io.r.raft.machine.RaftMachine.Companion.DIAGNOSTIC_MARKER
import io.r.raft.persistence.RaftLog.Companion.getLastMetadata
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRole
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.RaftCluster
import io.r.utils.logs.entry
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import kotlin.math.min
import kotlin.random.Random

class Follower(
    override val serverState: ServerState,
    val configuration: RaftMachine.Configuration,
    override val log: RaftLog,
    override val cluster: RaftCluster,
    override val transitionTo: RoleTransition
) : Role() {

    override val timeout: Long = configuration.leaderElectionTimeoutMs + Random.nextLong(configuration.leaderElectionTimeoutJitterMs)

    override suspend fun onReceivedMessage(message: RaftMessage) = when (message.rpc) {
        is RaftRpc.RequestVote -> {
            val granted = message.rpc.term >= log.getTerm()
                && log.getVotedFor().let { it == null || it == message.from }
                && message.rpc.lastLog >= log.getLastMetadata()
            if (granted) {
                log.setVotedFor(message.from, message.rpc.term)
            }
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
                    serverState.currentLeader = message.from
                    log.append(message.rpc.prevLog, message.rpc.entries)
                }
            }
            val rpc = when (result) {
                is AppendResult.Appended -> {
                    if(message.rpc.leaderCommit > serverState.commitIndex) {
                        serverState.commitIndex = min(message.rpc.leaderCommit, result.index)
                    }
                    if (message.rpc.entries.isNotEmpty()) {
                        logger.debug {
                            entry(
                                "Committed",
                                "term" to message.rpc.term,
                                "from_index" to result.index,
                                "commitIndex" to serverState.commitIndex,
                                "leaderCommit" to message.rpc.leaderCommit,
                                "entries" to message.rpc.entries.joinToString(",") { it.id }
                            )
                        }
                    }
                    message.rpc
                        .entries
                        .filterIsInstance<LogEntry.ConfigurationChange>()
                        .forEach { cluster.changeConfiguration(it) }

                    RaftRpc.AppendEntriesResponse(
                        term = log.getTerm(),
                        matchIndex = result.index,
                        success = true,
                        entries = message.rpc.entries.size
                    )
                }
                else -> {
                    logger.warn(DIAGNOSTIC_MARKER) {
                        entry(
                            "Rejected_Append",
                            "reason" to (result ?: "Leader is behind"),
                            "prev_index" to message.rpc.prevLog.index,
                            "prev_term" to message.rpc.prevLog.term,
                            "term" to message.rpc.term,
                            "commitIndex" to serverState.commitIndex,
                            "leaderCommit" to message.rpc.leaderCommit,
                            "entries" to message.rpc.entries.joinToString(",") { it.id }
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
            cluster.send(to = message.from, rpc = rpc)
        }
        else -> {
            logger.debug(DIAGNOSTIC_MARKER) {
                entry("Ignoring_Message", "message" to message.rpc, "from" to message.from)
            }
        }
    }

    override suspend fun onTimeout() {
        transitionTo(RaftRole.CANDIDATE)
    }

    companion object {
        private val logger: Logger = LogManager.getLogger(Follower::class.java)
    }
}