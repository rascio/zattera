package io.r.raft.machine

import io.r.raft.log.RaftLog
import io.r.raft.log.RaftLog.Companion.getLastMetadata
import io.r.raft.machine.RaftMachine.Companion.DIAGNOSTIC_MARKER
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
import kotlinx.coroutines.isActive
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
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

    val heartBeatCompletion = HeartBeatCompletion()
    private var heartbeat: Job? = null

    override val timeout: Long = Long.MAX_VALUE // forever

    override suspend fun onEnter() {
        check(heartbeat == null) { "Heartbeat already started" }
        serverState.leader = cluster.id
        peers += cluster.peers
            .associateWith { PeerState(log.getLastIndex() + 1, 0) }
        val cont = peers.map { CompletableDeferred<Unit>() }

        log.append(
            log.getLastMetadata(),
            entries = listOf(
                LogEntry(
                    term = log.getTerm(),
                    entry = LogEntry.NoOp,
                    id = "${cluster.id}-NOOP"
                )
            )
        )

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
                heartBeatCompletion.increment()
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
        val (nextIndex, matchIndex, lastTimeContacted) = checkNotNull(peers[this]) { "$this is not registered" }
        val entries = log.getEntries(nextIndex, configuration.maxLogEntriesPerAppend)
        val term = log.getTerm()
        logger.debug(DIAGNOSTIC_MARKER) {
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

    inner class HeartBeatCompletion {
        private var deferred: CompletableDeferred<Unit>? = null
        private var count: Int = 0
        private val mutex = Mutex()

        suspend fun increment() {
            mutex.withLock {
                if (deferred != null) {
                    logger.debug(entry("Heartbeat_Incremented"))
                    count++
                    if (count >= cluster.quorum) {
                        deferred!!.complete(Unit)
                        deferred = null
                        count = 0
                    }
                }
            }
        }

        suspend fun await() {
            val res = mutex.withLock {
                deferred ?: run {
                    val res = CompletableDeferred<Unit>()
                    deferred = res
                    count = 1
                    cluster.peers.forEach { p -> p.sendHeartbeat() }
//                    val considerNewPeers = cluster.events
//                        .onEach {
//                            if (it is RaftCluster.Connected) it.node.sendHeartbeat()
//                        }
//                        .launchIn(scope)
//                    res.invokeOnCompletion { considerNewPeers.cancel() }
                    res
                }
            }
            logger.debug(entry("Waiting_Heartbeat_Completion"))
            res.await()
        }
    }

    companion object {
        private val logger = LogManager.getLogger(Leader::class.java)
    }
}
