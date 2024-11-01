package io.r.raft.machine

import io.r.raft.Index
import io.r.raft.LogEntryMetadata
import io.r.raft.NodeId
import io.r.raft.PeerState
import io.r.raft.RaftMessage
import io.r.raft.RaftProtocol
import io.r.raft.RaftRole
import io.r.raft.log.RaftLog
import io.r.raft.transport.RaftClusterNode
import io.r.utils.logs.entry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import java.util.UUID

class Leader(
    override var commitIndex: Index,
    override val log: RaftLog,
    override val clusterNode: RaftClusterNode,
    override val changeRole: suspend (RaftRole) -> Role,
    private val scope: CoroutineScope,
    private val configuration: RaftMachine.Configuration
) : Role() {

    private val peers: MutableMap<NodeId, PeerState> = mutableMapOf()
    private var heartbeat: Job? = null

    override suspend fun onEnter() {
        peers += clusterNode.peers
            .associateWith { PeerState(log.getLastIndex() + 1, 0) }
        val cont = CompletableDeferred<Unit>()
        heartbeat = scope.launch {
            clusterNode.peers.forEach { peer ->
                launch(CoroutineName("Heartbeat-$peer")) {
                    cont.complete(Unit)
                    while (isActive) {
                        val now = System.currentTimeMillis()
                        val lastPeerContactedTime = peers[peer]?.lastContactTime ?: Long.MIN_VALUE
                        val nextHeartbeatTime = lastPeerContactedTime + this@Leader.configuration.heartbeatTimeoutMs
                        if (now >= nextHeartbeatTime) {
                            peer.sendHeartbeat()
                        }
                        delay(this@Leader.configuration.heartbeatTimeoutMs)
                    }
                }
            }
        }
        cont.join()
    }

    override suspend fun onExit() {
        heartbeat?.cancel()
    }

    override suspend fun onReceivedMessage(message: RaftMessage) {
        require(message.protocol.term <= log.getTerm()) { "Leader received message with higher term" }

        when (message.protocol) {
            is RaftProtocol.AppendEntriesResponse -> {
                updatePeerMetadata(message.from, message.protocol)
                // Update commit index, this needs to be done after updating the peer metadata
                commitIndex = getQuorum() ?: commitIndex
                getNextBatch(message.from, message.protocol)?.let { (prev, entries) ->
                    clusterNode.send(
                        message.from,
                        RaftProtocol.AppendEntries(
                            term = log.getTerm(),
                            leaderId = clusterNode.id,
                            prevLog = prev,
                            entries = entries,
                            leaderCommit = commitIndex
                        )
                    )
                }
            }

            is RaftProtocol.AppendEntries -> {
                clusterNode.send(
                    message.from,
                    RaftProtocol.AppendEntriesResponse(
                        term = log.getTerm(),
                        matchIndex = -1,
                        success = false,
                        entries = 0
                    )
                )
            }

            is RaftProtocol.RequestVote -> {
                clusterNode.send(
                    message.from,
                    RaftProtocol.RequestVoteResponse(
                        term = message.protocol.term,
                        voteGranted = false
                    )
                )
            }

            else -> {
                logger.debug(entry("Ignoring_Message", "message" to message.protocol, "from" to message.from))
            }
        }
    }

    private suspend fun getQuorum(): Index? {
        val currentTerm = log.getTerm()

        for (index in (commitIndex + 1..log.getLastIndex())) {
            if (currentTerm != log.getMetadata(index)?.term) {
                continue
            }
            var count = 1
            for (peer in peers) {
                count++
                if (count >= clusterNode.peers.size / 2) {
                    return index
                }
            }
        }
        return null
    }

    private suspend fun getNextBatch(from: NodeId, message: RaftProtocol.AppendEntriesResponse) =
        takeIf { message.matchIndex < commitIndex }
            ?.let { (peers[from] ?: error("Peer not found $from")) }
            ?.let { (nextIndex) ->
                val previous = log.getMetadata(nextIndex - 1)
                check(previous != null) { "Out of sync node=${clusterNode.id} term=${log.getTerm()} peer=$from state=${peers[from]} nextIndex=$nextIndex" }

                val entries = when {
                    message.success -> log.getEntries(nextIndex, configuration.maxLogEntriesPerAppend)
                    else -> emptyList()
                }
                previous to entries
            }

    private fun updatePeerMetadata(from: NodeId, message: RaftProtocol.AppendEntriesResponse) {
        peers.computeIfPresent(from) { _, state ->
            when {
                message.success -> state.copy(
                    nextIndex = state.nextIndex + message.entries,
                    matchIndex = message.matchIndex,
                    lastContactTime = System.currentTimeMillis()
                )

                else -> state.copy(
                    nextIndex = state.nextIndex - 1,
                    lastContactTime = System.currentTimeMillis()
                )
            }
        }
    }

    private suspend fun NodeId.sendHeartbeat() {
        val (nextIndex, matchIndex, lastTimeContacted) = peers[this]!!
        val entries = log.getEntries(nextIndex, configuration.maxLogEntriesPerAppend)
        logger.info("${clusterNode.id} Heartbeat (last ${System.currentTimeMillis() - lastTimeContacted}ms ago) => ${this}[T${log.getTerm()},$nextIndex,$matchIndex] (${entries.size})")
        clusterNode.send(
            this,
            RaftProtocol.AppendEntries(
                term = log.getTerm(),
                leaderId = clusterNode.id,
                prevLog = log.getMetadata(nextIndex - 1)
                    ?: LogEntryMetadata.ZERO,
                entries = entries,
                leaderCommit = commitIndex
            )
        )
    }

    companion object {
        private val logger = LogManager.getLogger(Leader::class.java)
    }
}