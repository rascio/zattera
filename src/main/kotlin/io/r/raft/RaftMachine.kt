package io.r.raft

import arrow.core.continuations.AtomicRef
import io.netty.util.internal.MacAddressUtil
import io.r.raft.RaftMachine.RaftNode
import io.r.utils.timeout.Timeout
import io.r.utils.logs.entry
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import kotlin.math.ceil

class RaftMachine<NodeRef : RaftNode>(
    private val configuration: Configuration<NodeRef>,
    private val transport: Transport<NodeRef>,
    private val persistence: Persistence,
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)
) {
    private val logger: Logger = LogManager.getLogger("RaftMachine.${configuration.id}")

    private val roleState = MutableStateFlow(RaftRole.FOLLOWER)
    private val lastAppendTime = AtomicRef(Long.MIN_VALUE)
    private val waitingForCommit = ConcurrentLinkedQueue<WaitingCommit>()

    val role: StateFlow<RaftRole> get() = roleState.asStateFlow()
    val isLeader get() = roleState.value == RaftRole.LEADER
//    val isFollower get() = roleState.get() == RaftRole.FOLLOWER
//    val isCandidate get() = roleState.get() == RaftRole.CANDIDATE

    private var process: Job? = null

    private data class WaitingCommit(val index: Index, val deferred: CompletableDeferred<Unit>)

    suspend fun command(vararg command: ByteArray) = command(command.toList())
    suspend fun command(commands: List<ByteArray>): Deferred<Unit> {
        require(isLeader) { "Only leader can send commands" }
        val now = System.currentTimeMillis()
        val term = persistence.getCurrentTerm()
        val lastEntry = persistence.getLastEntryMetadata()
        val entries = commands.map { LogEntry(term, it) }
        val index = persistence.append(term, lastEntry, entries) ?: error("Failed to append lastEntryMetadata=$lastEntry")
        lastAppendTime.set(now)
        logger.info(entry("command_sent", "term" to term, "commands" to entries))

        val onCommit = CompletableDeferred<Unit>()
        waitingForCommit += WaitingCommit(index, onCommit)
        return onCommit
    }

    private fun releaseCommit(upTo: Index) {
        while ((waitingForCommit.peek()?.index ?: Long.MAX_VALUE) <= upTo) {
            runCatching {
                waitingForCommit.poll()
                    .deferred
                    .complete(Unit)
            }.onFailure { logger.error("complete_deferred_failed", it) }
        }
    }

    suspend fun start() {
        val onReady = CompletableDeferred<Unit>()
        require(process?.isCompleted ?: true) { "RaftMachine already started" }
        roleState.update { RaftRole.FOLLOWER }
        process = scope.launch(CoroutineName("RaftMachine[${configuration.id}]")) {

            onReady.complete(Unit)

            while (isActive) {
                val role: Role = when (roleState.value) {
                    RaftRole.LEADER -> Leader(this)
                    RaftRole.CANDIDATE -> Candidate()
                    RaftRole.FOLLOWER -> Follower()
                }
                logger.info(entry("Become $role", "term" to persistence.getCurrentTerm()))

                role.onEnter()

                try {
                    val newRole = role.loop()
                    roleState.update { newRole }

                } finally {
                    role.onExit()
                }
            }
        }
        onReady.await()
    }

    private abstract inner class Role {
        open suspend fun onEnter() {}
        open suspend fun onExit() {}

        suspend fun loop(): RaftRole {
            while (true) {
                val serverState = persistence.getServerState()
                if (serverState.commitIndex > serverState.lastApplied) {
                    persistence.apply(serverState.commitIndex)
                }
                val received = when (this) {
                    is Leader -> transport.receive()
                    else -> transport.receive(configuration.leaderElectionTimeout).getOrElse {
                        logger.info(entry("Timeout", "role" to this))
                        return RaftRole.CANDIDATE
                    }
                }
//            log("received", "message" to received.protocol::class.simpleName)
                when (val message = received.protocol) {

                    is RaftProtocol.AppendEntries -> {
                        if (message.term > persistence.getCurrentTerm()) {
                            persistence.setTerm(message.term)
                        }
                        val newIndex = persistence.append(message.term, message.prevLog, message.entries)
                        val accepted = newIndex != null
                        if (accepted) {
                            persistence.commit(message.leaderCommit)
                            releaseCommit(message.leaderCommit)
                        }
                        received.from.send(
                            RaftProtocol.AppendEntriesResponse(
                                term = persistence.getCurrentTerm(),
                                matchIndex = newIndex ?: -1,
                                success = accepted,
                                entries = if (accepted) message.entries.size else -1
                            )
                        )
                        if (accepted && this !is Follower) {
                            return RaftRole.FOLLOWER
                        }
                    }

                    is RaftProtocol.AppendEntriesResponse -> {
                        if (this is Leader) {
                            if (message.term > persistence.getCurrentTerm()) {
                                persistence.setTerm(message.term)
                                return RaftRole.FOLLOWER
                            }
                            updatePeerMetadata(received.from, message)
                            if (message.success && hasQuorum(message.matchIndex)) {
                                persistence.commit(message.matchIndex)
                                releaseCommit(message.matchIndex)
                            }
                            getNextBatch(received.from, message)?.let { (prev, entries) ->
                                received.from.send(
                                    RaftProtocol.AppendEntries(
                                        term = persistence.getCurrentTerm(),
                                        leaderId = configuration.id,
                                        prevLog = prev,
                                        entries = entries,
                                        leaderCommit = serverState.commitIndex
                                    )
                                )
                            }
                        }
                    }

                    is RaftProtocol.RequestVote -> {
                        val granted = when {
                            message.term <= persistence.getCurrentTerm() -> false
                            else -> {
                                persistence.setTerm(message.term)
                                persistence.canVoteFor(message.candidateId)
                                    && persistence.isUpToDate(message)
                            }
                        }
                        if (granted) {
                            persistence.voteFor(message.term, message.candidateId)
                        }
                        received.from.send(
                            RaftProtocol.RequestVoteResponse(
                                term = message.term,
                                voteGranted = granted
                            )
                        )
                        if (granted && this is Follower) {
                            return RaftRole.FOLLOWER
                        }
                    }

                    is RaftProtocol.RequestVoteResponse -> {
                        if (this is Candidate && voteReceived(received.from, message.voteGranted)) {
                            return RaftRole.LEADER
                        }
                    }
                }
            }
        }

        override fun toString(): String = this::class.simpleName ?: "Role"
    }

    private inner class Leader(val scope: CoroutineScope) : Role() {

        private val peers: MutableMap<NodeId, PeerState> = mutableMapOf()
        private var heartbeat: Job? = null

        override suspend fun onEnter() {
            peers += configuration.peers
                .entries
                .associate { (id, _) -> id to PeerState(persistence.getServerState().commitIndex + 1, -1) }
            val latch = CountDownLatch(peers.size)
            heartbeat = scope.launch {
                peers.keys.forEach { peer ->
                    launch {
                        latch.countDown()
                        while (isActive) {
                            val lastPeerContactedTime = peers[peer]?.lastContactTime ?: Long.MIN_VALUE
                            val nextHeartBeatTime = lastPeerContactedTime + (configuration.heartbeatTimeout.millis * 0.9)
                            val now = System.currentTimeMillis()
                            if (nextHeartBeatTime <= now) {
                                beat(peer)
                            }
                            delay(configuration.heartbeatTimeout.millis)
                        }
                    }
                }
            }
            withContext(Dispatchers.IO) {
                latch.await()
            }
        }

        override suspend fun onExit() {
            heartbeat?.cancel(CancellationException("Role changed, close heartbeat"))
            heartbeat = null
        }

        suspend fun getNextBatch(from: NodeId, message: RaftProtocol.AppendEntriesResponse) =
            takeIf { message.matchIndex < persistence.getServerState().commitIndex }
                ?.let { (peers[from] ?: error("Peer not found $from")) }
                ?.let { (nextIndex) ->
                    val previous = persistence.getLogMetadata(nextIndex - 1)
                        ?: error("Out of sync node=${configuration.id} term=${persistence.getCurrentTerm()} peer=$from state=${peers[from]} nextIndex=$nextIndex")
                    val entries = when {
                        message.success -> persistence.getLogs(nextIndex, configuration.maxLogEntriesPerAppend)
                        else -> emptyList()
                    }
                    previous to entries
                }

        fun updatePeerMetadata(from: NodeId, message: RaftProtocol.AppendEntriesResponse) {
            peers.compute(from) { _, state ->
                when {
                    message.success -> state?.copy(
                        nextIndex = state.nextIndex + message.entries,
                        matchIndex = message.matchIndex,
                        lastContactTime = System.currentTimeMillis()
                    )

                    else -> state?.copy(nextIndex = state.nextIndex - 1, lastContactTime = System.currentTimeMillis())
                }
            }
        }

        suspend fun hasQuorum(index: Index): Boolean {
//            log(
//                "Quorum check",
//                "matchIndex" to index,
//                "quorum" to configuration.quorum,
//                "peers" to peers.values.count { it.matchIndex >= index } + 1
//            )
            return index > persistence.getServerState().commitIndex
                && (configuration.quorum <= peers.values.count { it.matchIndex >= index } + 1)
        }

        private suspend fun beat(peer: NodeId) {
            val (nextIndex, matchIndex, lastTimeContacted) = peers[peer]!!
            val entries = persistence.getLogs(nextIndex, configuration.maxLogEntriesPerAppend)
            logger.info("Heartbeat (last ${System.currentTimeMillis() - lastTimeContacted}ms ago) => $peer[T${persistence.getCurrentTerm()},$nextIndex,$matchIndex] (${entries.size})")
            peer.send(
                RaftProtocol.AppendEntries(
                    term = persistence.getCurrentTerm(),
                    leaderId = configuration.id,
                    prevLog = persistence.getLogMetadata(nextIndex - 1) ?: error("Out of sync $peer"),
                    entries = entries,
                    leaderCommit = persistence.getServerState().commitIndex
                )
            )
        }
    }

    private inner class Candidate : Role() {
        private val receivedVotes = mutableSetOf(configuration.id)

        override suspend fun onEnter() {
            persistence.incrementTermAndVote(configuration.id)
            configuration.peers.keys.forEach { peer ->
                peer.send(
                    RaftProtocol.RequestVote(
                        term = persistence.getCurrentTerm(),
                        candidateId = configuration.id,
                        lastLog = persistence.getLastEntryMetadata()
                    )
                )
            }
        }

        fun voteReceived(from: NodeId, voteGranted: Boolean): Boolean {
            if (voteGranted) {
                receivedVotes += from
            }
            return receivedVotes.size >= configuration.quorum
        }
    }

    private inner class Follower : Role() {
        override suspend fun onEnter() {
            while (waitingForCommit.isNotEmpty()) {
                waitingForCommit.poll()
                    .deferred
                    .completeExceptionally(CancellationException("Role has changed"))
            }
        }
    }

    data class Configuration<NodeRef : RaftNode>(
        val id: NodeId = MacAddressUtil.bestAvailableMac().encodeBase64(),
        val peers: Map<NodeId, NodeRef>,
        val leaderElectionTimeout: Timeout = Timeout(150).jitter(50),
        val heartbeatTimeout: Timeout = Timeout(50),
        val maxLogEntriesPerAppend: Int = 10
    ) {
        val quorum: Int get() = ceil((peers.size + 1) / 2.0).toInt()
    }

    interface RaftNode

    /*
    data class InetRaftNode(
        val address: InetAddress,
        val port: Int
    ): RaftNode
    */
    suspend fun stop() {
        logger.info("Closing")
        runCatching { process?.cancelAndJoin() }
            .onFailure { logger.warn("process_close_failed", it) }
        waitingForCommit.forEach { c ->
            runCatching { c.deferred.cancelAndJoin() }
                .onFailure { logger.warn(entry("deferred_close_failed", "index" to c.index), it) }
        }
    }

    private suspend fun NodeId.send(rcp: RaftProtocol) = transport.send(
        configuration.peers[this]!!,
        RaftMessage(
            from = configuration.id,
            to = this,
            protocol = rcp
        )
    )
}

fun ByteArray.encodeBase64(): String = java.util.Base64.getEncoder().encodeToString(this)