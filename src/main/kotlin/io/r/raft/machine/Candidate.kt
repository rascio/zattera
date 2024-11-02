package io.r.raft.machine

import io.r.raft.protocol.Index
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.protocol.RaftRole
import io.r.raft.log.RaftLog
import io.r.raft.transport.RaftClusterNode
import io.r.raft.transport.RaftClusterNode.Companion.quorum
import io.r.utils.logs.entry
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import kotlin.math.floor

class Candidate(
    override var commitIndex: Index,
    val configuration: RaftMachine.Configuration,
    override val log: RaftLog,
    override val clusterNode: RaftClusterNode,
    override val changeRole: suspend (RaftRole) -> Role
) : Role() {

    private val votesReceived = mutableSetOf<NodeId>()

    override suspend fun onEnter() {
        val nextTerm = log.getTerm() + 1
        val lastIndex = log.getLastIndex()
        log.setTerm(nextTerm)
        votesReceived += clusterNode.id
        logger.debug(entry("Starting_Election", "term" to nextTerm))
        clusterNode.peers.forEach { peer ->
            clusterNode.send(
                peer,
                RaftRpc.RequestVote(
                    log.getTerm(),
                    clusterNode.id,
                    checkNotNull(log.getMetadata(lastIndex)) { "Metadata for lastIndex must always be available" }
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
                    logger.debug(entry("Received_Vote", "from" to message.from, "votes" to votesReceived.size, "quorum" to floor(clusterNode.peers.size / 2.0)))
                    if (votesReceived.size >= clusterNode.quorum) {
                        changeRole(RaftRole.LEADER)
                    }
                }
            }
            is RaftRpc.AppendEntries -> {
                changeRole(RaftRole.FOLLOWER)
                    .onReceivedMessage(message)
            }
            is RaftRpc.RequestVote -> {
                clusterNode.send(
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
        changeRole(RaftRole.CANDIDATE)
    }

    companion object {
        private val logger: Logger = LogManager.getLogger(Candidate::class.java)
    }
}