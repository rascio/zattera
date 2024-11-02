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
import io.r.raft.transport.RaftClusterNode.Companion.quorum
import io.r.utils.logs.entry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager

class Leader(
    override var commitIndex: Index,
    override val log: RaftLog,
    override val clusterNode: RaftClusterNode,
    override val changeRole: suspend (RaftRole) -> Role,
    private val scope: CoroutineScope,
    private val configuration: RaftMachine.Configuration,
    private val peers: MutableMap<NodeId, PeerState> = mutableMapOf()
) : Role() {
    internal var heartbeat: Job? = null

    override suspend fun onEnter() {
        check(heartbeat == null) { "Heartbeat already started" }
        peers += clusterNode.peers
            .associateWith { PeerState(log.getLastIndex() + 1, 0) }
        val cont = peers.map { CompletableDeferred<Unit>() }
        heartbeat = scope.launch {
            clusterNode.peers.forEachIndexed { idx, peer ->
                launch(CoroutineName("Heartbeat-$peer")) {
                    cont[idx].complete(Unit)
                    logger.debug("${clusterNode.id} Starting heartbeat to $peer")
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
            logger.debug("${clusterNode.id} Heartbeat started")
        }
        cont.joinAll()
    }

    override suspend fun onExit() {
        logger.debug("${clusterNode.id} Stopping heartbeat")
        heartbeat?.cancel()
        heartbeat = null
    }

    override suspend fun onReceivedMessage(message: RaftMessage) {
        require(message.rpc.term <= log.getTerm()) { "Leader received message with higher term" }

        when (message.rpc) {
            is RaftProtocol.AppendEntriesResponse -> {
                updatePeerMetadata(message.from, message.rpc)
                // Update commit index, this needs to be done after updating the peer metadata
                commitIndex = getQuorum()
                getNextBatch(message.from, message.rpc)?.let { (prev, entries) ->
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
                        matchIndex = message.rpc.prevLog.index,
                        success = false,
                        entries = 0
                    )
                )
            }

            is RaftProtocol.RequestVote -> {
                clusterNode.send(
                    message.from,
                    RaftProtocol.RequestVoteResponse(
                        term = message.rpc.term,
                        voteGranted = false
                    )
                )
            }

            else -> {
                logger.debug(entry("Ignoring_Message", "message" to message.rpc, "from" to message.from))
            }
        }
    }

    private suspend fun getQuorum(): Index {
        val currentTerm = log.getTerm()

        for (index in (commitIndex + 1..peers.maxOf { (_, s) -> s.matchIndex }).reversed()) {
            if (currentTerm != log.getMetadata(index)?.term) {
                continue
            }
            var count = 1
            for ((id, peer) in peers) {
                if (peer.matchIndex >= index) {
                    count++
                    if (count >= clusterNode.quorum) {
                        return index
                    }
                }
            }
        }
        return commitIndex
    }

    private suspend fun getNextBatch(from: NodeId, message: RaftProtocol.AppendEntriesResponse) =
        checkNotNull(peers[from]) { "Peer not found $from" }
            // Only send the next batch if the matchIndex is the same as the one we are expecting
            // and the matchIndex is less than the commitIndex (follower is behind)
            .takeIf { message.matchIndex == it.matchIndex && message.matchIndex < commitIndex }
            ?.let { (nextIndex) ->
                val previous = log.getMetadata(nextIndex - 1)

                checkNotNull(previous) { "Out of sync node=${clusterNode.id} term=${log.getTerm()} peer=$from state=${peers[from]} nextIndex=$nextIndex" }

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
                    nextIndex = message.matchIndex + 1,
                    matchIndex = message.matchIndex,
                    lastContactTime = System.currentTimeMillis()
                )
                message.matchIndex == state.matchIndex -> state.copy(
                    nextIndex = state.nextIndex - 1,
                    lastContactTime = System.currentTimeMillis()
                )
                // This is a special case where the response is for a previous message
                // matchIndex is used to determine the last index follower and leader agree
                // are talking about
                else -> state.copy(
                    lastContactTime = System.currentTimeMillis()
                )
            }
        }
    }

    private suspend fun NodeId.sendHeartbeat() {
        val (nextIndex, matchIndex, lastTimeContacted) = peers[this]!!
        val entries = log.getEntries(nextIndex, configuration.maxLogEntriesPerAppend)
        logger.debug("${clusterNode.id} Heartbeat (last ${System.currentTimeMillis() - lastTimeContacted}ms ago) => ${this}[T${log.getTerm()},$nextIndex,$matchIndex] (${entries.size})")
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
