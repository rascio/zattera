package io.r.raft.machine

import io.r.raft.log.RaftLog
import io.r.raft.log.RaftLog.Companion.getLastMetadata
import io.r.raft.log.StateMachine
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.RaftRole
import io.r.raft.transport.RaftClusterNode
import io.r.utils.encodeBase64
import io.r.utils.logs.entry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.select
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class RaftMachine(
    private val configuration: Configuration,
    private val cluster: RaftClusterNode,
    private val log: RaftLog,
    private val stateMachine: StateMachine,
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)
) {

    private class IncomingRequest(
        val entry: ByteArray,
        val response: CompletableDeferred<Any>,
        val id: String = UUID.randomUUID().toString()
    )
    private val logger: Logger = LogManager.getLogger("${this::class.java}.${cluster.id}")
    private val _role: MutableStateFlow<Role> = MutableStateFlow(
        createFollower(ServerState(0, 0))
    )
    private var job: Job? = null
    private val incomingRequests = Channel<IncomingRequest>(
        capacity = Channel.RENDEZVOUS,
        onUndeliveredElement = { it.response.completeExceptionally(IllegalStateException("RaftMachine is stopped")) },
    )
    private val waitingForCommit = ConcurrentHashMap<String, CompletableDeferred<Any>>()

    val commitIndex get() = _role.value.serverState.commitIndex
    val role
        get() = _role.map {
            when (it) {
                is Follower -> RaftRole.FOLLOWER
                is Candidate -> RaftRole.CANDIDATE
                is Leader -> RaftRole.LEADER
            }
        }

    @OptIn(ExperimentalCoroutinesApi::class)
    fun start() {
        logger.info(entry("Starting_RaftMachine", "job" to job))
        job = scope.launch {
            changeRole(RaftRole.FOLLOWER)
            try {
                while (isActive) {
                    applyCommittedEntries()

                    select {
                        cluster.input.onReceive { message ->
                            if (message.rpc.term > log.getTerm()) {
                                log.setTerm(message.rpc.term)
                                changeRole(RaftRole.FOLLOWER)
                            }
                            _role.value.onReceivedMessage(message)
                        }
                        onTimeout(_role.value.timeout) {
                            logger.debug(entry("Timeout", "role" to _role.value::class.simpleName))
                            _role.value.onTimeout()
                        }
                        incomingRequests.onReceive { command ->
                            dequeueRequest(command)
                        }
                    }
                }
            } finally {
                _role.value.onExit()
            }
            logger.info(entry("RaftMachine_Stopped", "role" to _role.value::class.simpleName, "term" to log.getTerm()))
        }
    }

    fun command(vararg command: ByteArray): Deferred<Unit> =
        command(command.toList())

    fun command(command: List<ByteArray>): Deferred<Unit> {
        return scope.async {
            command.map { command(it) }
                .awaitAll()
        }
    }

    suspend fun command(command: ByteArray): Deferred<Any> {
        val entry = IncomingRequest(command, CompletableDeferred())
        incomingRequests.send(entry)
        logger.info(
            entry(
                "command_sent",
                "id" to entry.id,
                "command" to command.encodeBase64()
            )
        )
        return entry.response
    }

    private suspend fun dequeueRequest(command: IncomingRequest) {
        when (_role.value) {
            is Leader -> {
                val term = log.getTerm()
                val entry = LogEntry(term = term, entry = command.entry, id = command.id)
                log.append(log.getLastMetadata(), listOf(entry))
                waitingForCommit[command.id] = command.response

                scope.launch {
                    delay(configuration.pendingCommandsTimeout)
                    waitingForCommit.remove(entry.id)
                        ?.cancel("Time out")
                }
            }
            else -> command.response
                .completeExceptionally(IllegalStateException("Only leader can accept commands"))
        }
    }

    private fun releaseCommit(entry: LogEntry, result: Any) {
        waitingForCommit.remove(entry.id)
            ?.complete(result)
    }

    private suspend fun applyCommittedEntries() {
        val serverState = _role.value.serverState
        val lastApplied = serverState.lastApplied
        if (serverState.commitIndex > lastApplied) {
            val entries = log.getEntries(
                from = lastApplied + 1,
                length = (serverState.commitIndex - lastApplied).toInt()
            )
            entries.forEach {

                logger.debug(
                    entry(
                        "Applying_Committed",
                        "entry" to it.id,
                        "index" to serverState.lastApplied + 1,
                        "client_handle" to waitingForCommit.contains(it.id)
                    )
                )
                val result = stateMachine.apply(it)
                releaseCommit(it, result)
                serverState.lastApplied++
            }
        }
    }

    suspend fun stop() {
        incomingRequests.cancel()
        waitingForCommit.forEach {
            it.value.cancel()
        }
        waitingForCommit.clear()
        job?.run {
            cancel("Stopping RaftMachine")
            join()
        }
    }

    private suspend fun changeRole(newRole: RaftRole): Role {
        _role.value.onExit()
        _role.value = when (newRole) {
            RaftRole.FOLLOWER -> createFollower()
            RaftRole.CANDIDATE -> createCandidate()
            RaftRole.LEADER -> createLeader()
        }
        logger.info(entry("role_change", "to" to newRole, "term" to log.getTerm()))
        _role.value.onEnter()
        return _role.value
    }

    data class Configuration(
        val leaderElectionTimeoutMs: Long = 150,
        val leaderElectionTimeoutJitterMs: Long = 50,
        val heartbeatTimeoutMs: Long = 50,
        val maxLogEntriesPerAppend: Int = 10,
        // very bad, commands rejection can be done deterministically
        // without the need of a timeout
        val pendingCommandsTimeout: Long = 3000
    )

    private fun createFollower(serverState: ServerState = _role.value.serverState) = Follower(
        serverState,
        configuration,
        log,
        cluster,
        ::changeRole
    )

    private fun createCandidate() = Candidate(
        _role.value.serverState,
        configuration,
        log,
        cluster,
        ::changeRole
    )

    private fun createLeader() = Leader(
        _role.value.serverState,
        log,
        cluster,
        ::changeRole,
        scope,
        configuration
    )

}