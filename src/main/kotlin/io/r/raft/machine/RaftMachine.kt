package io.r.raft.machine

import io.r.raft.log.RaftLog
import io.r.raft.log.RaftLog.Companion.getLastMetadata
import io.r.raft.log.StateMachine
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.RaftRole
import io.r.raft.transport.RaftClusterNode
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
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import java.util.concurrent.ConcurrentHashMap
import kotlin.random.Random

class RaftMachine(
    private val configuration: Configuration,
    private val cluster: RaftClusterNode,
    private val log: RaftLog,
    private val stateMachine: StateMachine,
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO)
) {

    private val logger: Logger = LogManager.getLogger("${this::class.java}.${cluster.id}")
    private val _role: MutableStateFlow<Role> = MutableStateFlow(
        createFollower(ServerState(0, 0))
    )
    private var job: Job? = null
    private val mutex = Mutex()
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

                    val currentRole = _role.value
                    select {
                        cluster.input.onReceive { message ->
                            if (message.rpc.term > log.getTerm()) {
                                log.setTerm(message.rpc.term)
                                changeRole(RaftRole.FOLLOWER)
                            }
                            currentRole.onReceivedMessage(message)
                        }
                        onTimeout(currentRole.timeout) {
                            logger.debug(entry("Timeout", "role" to currentRole::class.simpleName))
                            currentRole.onTimeout()
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

    // very bad implementation
    suspend fun command(command: ByteArray): Deferred<Any> {
        return mutex.withLock {
            check(_role.value is Leader) { "Only leader can accept commands" }
            val term = log.getTerm()
            val entry = LogEntry(term, command)
            val result = CompletableDeferred<Any>()
            log.append(log.getLastMetadata(), listOf(entry))
            logger.info(entry("command_sent", "term" to term, "command" to entry))
            waitingForCommit[entry.id] = result
            scope.launch {
                delay(configuration.pendingCommandsTimeout)
                waitingForCommit.remove(entry.id)?.cancel("Time out")
            }
            result
        }
    }

    // very bad implementation
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
            logger.debug(
                entry(
                    "Applying_Committed",
                    "entries" to entries.size,
                    "lastApplied" to lastApplied,
                    "commitIndex" to serverState.commitIndex
                )
            )
            entries.forEach {
                val result = stateMachine.apply(it)
                releaseCommit(it, result)
                serverState.lastApplied++
            }
        }
    }

    private fun getRoleTimeout() =
        configuration.leaderElectionTimeoutMs + Random.nextLong(configuration.leaderElectionTimeoutJitterMs)

    suspend fun stop() {
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
        return mutex.withLock {
            _role.value.onExit()
            _role.value = when (newRole) {
                RaftRole.FOLLOWER -> createFollower()
                RaftRole.CANDIDATE -> createCandidate()
                RaftRole.LEADER -> createLeader()
            }
            logger.info(entry("role_change", "to" to newRole, "term" to log.getTerm()))
            _role.value.onEnter()
            _role.value
        }
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