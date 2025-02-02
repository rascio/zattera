package io.r.raft.machine

import io.r.raft.log.RaftLog
import io.r.raft.log.RaftLog.Companion.getLastMetadata
import io.r.raft.protocol.Index
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.LogEntryMetadata
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRole
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.RaftCluster
import io.r.raft.transport.RaftCluster.Companion.quorum
import io.r.utils.logs.entry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager

class Leader(
    override val serverState: ServerState,
    override val log: RaftLog,
    override val cluster: RaftCluster,
    override val transitionTo: suspend (RaftRole) -> Role,
    private val scope: CoroutineScope,
    private val configuration: RaftMachine.Configuration,
    private val peers: MutableMap<NodeId, PeerState> = mutableMapOf()
) : Role() {

    private var heartbeat: Job? = null

    override val timeout: Long = Long.MAX_VALUE // forever

    override suspend fun onEnter() {
        check(heartbeat == null) { "Heartbeat already started" }
        serverState.leader = cluster.id
        peers += cluster.peers
            .associateWith { PeerState(log.getLastIndex() + 1, 0) }
        val cont = peers.map { CompletableDeferred<Unit>() }
        heartbeat = scope.launch {
            cluster.peers.forEachIndexed { idx, peer ->
                launch(CoroutineName("Heartbeat-$peer")) {
                    cont[idx].complete(Unit)
                    startHeartBeat(peer)
                }
            }
            launch {
                cluster.events.collect {
                    if (it is RaftCluster.Connected) {
                        val lastIndex = log.getLastIndex()
                        peers.computeIfAbsent(it.node) {
                            PeerState(lastIndex + 1, 0)
                        }
                        launch {
                            startHeartBeat(it.node)
                        }
                    }
                }
            }
        }
        cont.joinAll()
    }

    override suspend fun onExit() {
        logger.debug(entry("Stopping_Heartbeat"))
        heartbeat?.cancel()
        heartbeat = null
        serverState.leader = null
    }

    override suspend fun onReceivedMessage(message: RaftMessage) {
        require(message.rpc.term <= log.getTerm()) { "Leader received message with higher term" }

        when (message.rpc) {
            is RaftRpc.AppendEntriesResponse -> {
                updatePeerMetadata(message.from, message.rpc)
                // Update commit index, this needs to be done after updating the peer metadata
                serverState.commitIndex = getQuorum()
                getNextBatch(message.from, message.rpc)?.let { (prev, entries) ->
                    cluster.send(
                        message.from,
                        RaftRpc.AppendEntries(
                            term = log.getTerm(),
                            leaderId = cluster.id,
                            prevLog = prev,
                            entries = entries,
                            leaderCommit = serverState.commitIndex
                        )
                    )
                }
            }

            is RaftRpc.AppendEntries -> {
                cluster.send(
                    message.from,
                    RaftRpc.AppendEntriesResponse(
                        term = log.getTerm(),
                        matchIndex = message.rpc.prevLog.index,
                        success = false,
                        entries = 0
                    )
                )
            }

            is RaftRpc.RequestVote -> {
                cluster.send(
                    message.from,
                    RaftRpc.RequestVoteResponse(
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
        peers.maxOf { (_, s) -> s.matchIndex }
            .let { (serverState.commitIndex + 1..it) }
            .reversed()
            .forEach { index ->
                if (currentTerm != log.getMetadata(index)?.term) {
                    return@forEach
                }
                val count = peers.values.count { it.matchIndex >= index }
                if ((count + 1) >= cluster.quorum) {
                    return index
                }
            }
        return serverState.commitIndex
    }

    private suspend fun getNextBatch(from: NodeId, message: RaftRpc.AppendEntriesResponse) =
        checkNotNull(peers[from]) { "Peer not found $from" }
            // Only send the next batch if the matchIndex is the same as the one we are expecting
            // and the matchIndex is less than the commitIndex (follower is behind)
            .takeIf {
                (message.success && message.matchIndex < serverState.commitIndex)
                    || message.matchIndex == it.nextIndex
            }
            ?.let { (nextIndex) ->
                val previous = log.getMetadata(nextIndex - 1)

                checkNotNull(previous) { "Out of sync node=${cluster.id} term=${log.getTerm()} peer=$from state=${peers[from]} nextIndex=$nextIndex" }

                val entries = when {
                    message.success -> log.getEntries(nextIndex, configuration.maxLogEntriesPerAppend)
                    else -> emptyList()
                }
                previous to entries
            }

    private fun updatePeerMetadata(from: NodeId, message: RaftRpc.AppendEntriesResponse) {
        peers.computeIfPresent(from) { _, state ->
            when {
                message.success -> state.copy(
                    nextIndex = message.matchIndex + 1,
                    matchIndex = message.matchIndex,
                    lastContactTime = System.currentTimeMillis()
                )

                message.matchIndex == state.nextIndex - 1 -> state.copy(
                    nextIndex = state.nextIndex - 1,
                    lastContactTime = System.currentTimeMillis()
                )
                // This is a special case where the response is for a previous message
                // matchIndex is used to determine the last index follower and leader
                // are talking about
                else -> state.copy(
                    lastContactTime = System.currentTimeMillis()
                )
            }
        }
    }

    private suspend fun CoroutineScope.startHeartBeat(peer: NodeId) {
        logger.debug { entry("Starting_Heartbeat", "peer" to peer) }
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

    private suspend fun NodeId.sendHeartbeat() {
        val (nextIndex, matchIndex, lastTimeContacted) = peers[this]!!
        val entries = log.getEntries(nextIndex, configuration.maxLogEntriesPerAppend)
        val term = log.getTerm()
        logger.debug {
            entry(
                "Heartbeat",
                "from" to cluster.id,
                "to" to this,
                "lastContact" to System.currentTimeMillis() - lastTimeContacted,
                "term" to term,
                "nextIndex" to nextIndex,
                "matchIndex" to matchIndex,
                "entries" to entries.size
            )
        }
        cluster.send(
            this,
            RaftRpc.AppendEntries(
                term = term,
                leaderId = cluster.id,
                prevLog = log.getMetadata(nextIndex - 1) ?: LogEntryMetadata.ZERO,
                entries = entries,
                leaderCommit = serverState.commitIndex
            )
        )
    }

    companion object {
        private val logger = LogManager.getLogger(Leader::class.java)
    }
}
