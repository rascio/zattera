package io.r.raft.machine

import io.r.raft.log.RaftLog
import io.r.raft.log.StateMachine
import io.r.raft.log.StateMachine.Companion.apply
import io.r.raft.protocol.Index
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRole
import io.r.raft.transport.RaftClusterNode
import io.r.utils.logs.entry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
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
    private val _role: MutableStateFlow<Role> = MutableStateFlow(createFollower(0))
    private var job: Job? = null
    private val mutex = Mutex()
    private val waitingForCommit = ConcurrentHashMap<String, CompletableDeferred<Unit>>()

    val commitIndex get() = _role.value.commitIndex
    val isLeader: Boolean get() = _role.value is Leader

    private sealed interface Step

    @JvmInline
    private value class MessageReceived(val message: RaftMessage) : Step
    data object Timeout : Step

    fun start() {
        logger.info(entry("Starting_RaftMachine", "job" to job))
        job = scope.launch {
            changeRole(RaftRole.FOLLOWER)
            try {
                while (isActive) {
                    applyCommittedEntries()

                    val step = merge(
                        getClusterCommand(),
                        getRoleTimeout()
                    ).first()

                    when (step) {
                        is MessageReceived -> {
                            if (step.message.rpc.term > log.getTerm()) {
                                log.setTerm(step.message.rpc.term)
                                changeRole(RaftRole.FOLLOWER)
                            }
                            _role.value.onReceivedMessage(step.message)
                        }

                        Timeout -> {
                            logger.info(entry("Timeout", "role" to _role.value::class.simpleName))
                            _role.value.onTimeout()
                        }
                    }
                }
            } finally {
                _role.value.onExit()
            }
            logger.info(entry("RaftMachine_Stopped", "role" to _role.value, "term" to log.getTerm()))
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
    suspend fun command(command: ByteArray): Deferred<Unit> {
        return mutex.withLock {
            check(_role.value is Leader) { "Only leader can accept commands" }
            val term = log.getTerm()
            val entry = LogEntry(term, command)
            val result = CompletableDeferred<Unit>()
            log.append(log.getLastIndex(), listOf(entry))
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
    private fun releaseCommit(entry: LogEntry) {
        waitingForCommit.remove(entry.id)
            ?.complete(Unit)
    }

    private suspend fun applyCommittedEntries() {
        val lastApplied = stateMachine.getLastApplied()
        if (_role.value.commitIndex > lastApplied) {
            val entries = log.getEntries(
                from = lastApplied + 1,
                length = (_role.value.commitIndex - lastApplied).toInt()
            )
            logger.debug(
                entry(
                    "Applying_Committed",
                    "entries" to entries.size,
                    "lastApplied" to lastApplied,
                    "commitIndex" to _role.value.commitIndex
                )
            )
            stateMachine.apply(entries)
            entries.forEach { releaseCommit(it) }
        }
    }

    private fun getRoleTimeout() = when (_role.value) {
        is Leader -> emptyFlow()
        else -> flow {
            val timeout =
                configuration.leaderElectionTimeoutMs + Random.nextLong(configuration.leaderElectionTimeoutJitterMs)
            delay(timeout)
            emit(Timeout)
        }
    }

    private fun getClusterCommand() = flow {
        val next = cluster.receive()
        emit(MessageReceived(next))
    }

    suspend fun stop() {
        waitingForCommit.forEach {
            runCatching { it.value.cancel() }
                .onFailure { e -> logger.warn(entry("cancel_error", "id" to it.key, "error" to it.value), e) }
        }
        waitingForCommit.clear()
        job?.run {
            logger.debug(entry("Stopping_RaftMachine", "job" to job))
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

    private fun createFollower(lastCommit: Index = _role.value.commitIndex) = Follower(
        lastCommit,
        configuration,
        log,
        cluster,
        ::changeRole
    )

    private fun createCandidate() = Candidate(
        _role.value.commitIndex,
        configuration,
        log,
        cluster,
        ::changeRole
    )

    private fun createLeader() = Leader(
        _role.value.commitIndex,
        log,
        cluster,
        ::changeRole,
        scope,
        configuration
    )

}