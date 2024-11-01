package io.r.raft.machine

import io.r.raft.Index
import io.r.raft.NodeId
import io.r.raft.RaftMessage
import io.r.raft.RaftProtocol
import io.r.raft.RaftRole
import io.r.raft.log.RaftLog
import io.r.raft.transport.RaftClusterNode
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
        logger.debug(entry("Starting_Election", "term" to nextTerm))
        clusterNode.peers.forEach { peer ->
            clusterNode.send(
                peer,
                RaftProtocol.RequestVote(
                    log.getTerm(),
                    clusterNode.id,
                    checkNotNull(log.getMetadata(lastIndex)) { "Metadata for lastIndex must always be available" }
                )
            )
        }
    }

    override suspend fun onReceivedMessage(message: RaftMessage) {
        require(message.protocol.term <= log.getTerm()) { "Candidate received message with higher term" }
        when (message.protocol) {
            is RaftProtocol.RequestVoteResponse -> {
                if (message.protocol.voteGranted) {
                    votesReceived += message.from
                    logger.debug(entry("Received_Vote", "from" to message.from, "votes" to votesReceived.size, "quorum" to floor(clusterNode.peers.size / 2.0)))
                    if (votesReceived.size >= floor(clusterNode.peers.size / 2.0)) {
                        changeRole(RaftRole.LEADER)
                    }
                }
            }
            is RaftProtocol.AppendEntries -> {
                changeRole(RaftRole.FOLLOWER)
                    .onReceivedMessage(message)
            }
            is RaftProtocol.RequestVote -> {
                clusterNode.send(
                    message.from,
                    RaftProtocol.RequestVoteResponse(
                        term = log.getTerm(),
                        voteGranted = false
                    )
                )
            }
            else -> {
                logger.debug(entry("Ignoring_Message", "message" to message.protocol, "from" to message.from))
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