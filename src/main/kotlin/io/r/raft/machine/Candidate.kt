package io.r.raft.machine

import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.protocol.RaftRole
import io.r.raft.log.RaftLog
import io.r.raft.log.RaftLog.Companion.getLastMetadata
import io.r.raft.transport.RaftCluster
import io.r.raft.transport.RaftCluster.Companion.quorum
import io.r.utils.logs.entry
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import kotlin.math.floor
import kotlin.random.Random

class Candidate(
    override val serverState: ServerState,
    val configuration: RaftMachine.Configuration,
    override val log: RaftLog,
    override val cluster: RaftCluster,
    override val transitionTo: suspend (RaftRole) -> Role
) : Role() {

    override val timeout: Long = configuration.leaderElectionTimeoutMs + Random.nextLong(configuration.leaderElectionTimeoutJitterMs)

    private val votesReceived = mutableSetOf<NodeId>()

    override suspend fun onEnter() {
        val nextTerm = log.getTerm() + 1
        log.setTerm(nextTerm)
        votesReceived += cluster.id
        logger.debug {
            entry("Starting_Election", "term" to nextTerm, "peers" to cluster.peers)
        }
        cluster.peers.forEach { peer ->
            cluster.send(
                to = peer,
                rpc = RaftRpc.RequestVote(
                    log.getTerm(),
                    cluster.id,
                    log.getLastMetadata()
                )
            )
        }
    }

    override suspend fun onReceivedMessage(message: RaftMessage) {
        require(message.rpc.term <= log.getTerm()) { "Candidate received message with higher term" }
        when (message.rpc) {
            is RaftRpc.RequestVoteResponse -> {
                if (message.rpc.voteGranted) {
                    votesReceived += message.from
                    logger.debug {
                        entry("Received_Vote", "from" to message.from, "votes" to votesReceived.size, "quorum" to floor(cluster.peers.size / 2.0))
                    }
                    if (votesReceived.size >= cluster.quorum) {
                        transitionTo(RaftRole.LEADER)
                    }
                }
            }
            is RaftRpc.AppendEntries -> {
                /*
                 * .onReceivedMessage(message) may (to check) not be needed
                 * as the append entries will be re-sent by the leader,
                 * but we can avoid a round trip by processing it here
                 */
                transitionTo(RaftRole.FOLLOWER)
                    .onReceivedMessage(message)
            }
            is RaftRpc.RequestVote -> {
                cluster.send(
                    message.from,
                    RaftRpc.RequestVoteResponse(
                        term = log.getTerm(),
                        voteGranted = false
                    )
                )
            }
            else -> {
                logger.debug(entry("Ignoring_Message", "message" to message.rpc, "from" to message.from))
            }
        }
    }

    override suspend fun onTimeout() {
        transitionTo(RaftRole.CANDIDATE)
    }

    companion object {
        private val logger: Logger = LogManager.getLogger(Candidate::class.java)
    }
}