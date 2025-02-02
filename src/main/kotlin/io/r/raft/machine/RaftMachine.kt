package io.r.raft.machine

import io.r.raft.log.RaftLog
import io.r.raft.log.RaftLog.Companion.getLastMetadata
import io.r.raft.log.StateMachine
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRole
import io.r.raft.transport.RaftCluster
import io.r.utils.encodeBase64
import io.r.utils.logs.entry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.selects.whileSelect
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class RaftMachine(
    private val configuration: Configuration,
    private val cluster: RaftCluster,
    private val log: RaftLog,
    private val stateMachine: StateMachine,
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO),
    private val input: Channel<RaftMessage> = Channel(capacity = Channel.BUFFERED)
) {

    private class IncomingRequest(
        val entry: LogEntry.Entry,
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

    val id get() = cluster.id
    val serverState get() = _role.value.serverState
    val role: Flow<RaftRole>
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
        job = scope.launch(CoroutineName(cluster.id)) {
            transitionTo(RaftRole.FOLLOWER)
            try {
                while (isActive) {
                    applyCommittedEntries()

                    select {
                        input.onReceive { message ->
                            if (message.rpc.term > log.getTerm()) {
                                log.setTerm(message.rpc.term)
                                transitionTo(RaftRole.FOLLOWER)
                            }
                            _role.value.onReceivedMessage(message)
                        }
                        onTimeout(_role.value.timeout) {
                            logger.debug(entry("Timeout", "role" to _role.value::class.simpleName))
                            _role.value.onTimeout()
                        }
                        incomingRequests.onReceive { command ->
                            handleClientRequest(command)
                        }
                    }
                }
            } finally {
                _role.value.onExit()
            }
            logger.info(entry("RaftMachine_Stopped", "role" to _role.value::class.simpleName, "term" to log.getTerm()))
        }
    }

    suspend fun handle(message: RaftMessage) {
        input.send(message)
    }

    suspend fun request(e: LogEntry.Entry): Any =
        when (val leader = serverState.leader) {
            id -> {
                val entry = IncomingRequest(
                    entry = e,
                    response = CompletableDeferred()
                )
                incomingRequests.send(entry)
                logger.info {
                    entry(
                        "request_sent",
                        "id" to entry.id,
                        "request" to when (e) {
                            is LogEntry.ClientCommand -> e.bytes.encodeBase64()
                            is LogEntry.ConfigurationChange -> e
                        }
                    )
                }
                entry.response.await()
            }

            null -> error("Unknown leader [$id]")
            else -> cluster.forward(leader, e)
        }

    private suspend fun handleClientRequest(request: IncomingRequest) {
        when {
            _role.value is Leader -> {
                val term = log.getTerm()
                val entry = LogEntry(
                    term = term,
                    entry = request.entry,
                    id = request.id
                )
                log.append(log.getLastMetadata(), listOf(entry))

                if (request.entry is LogEntry.ConfigurationChange) {
                    cluster.changeConfiguration(request.entry)
                }

                // add to queue of requests to respond to
                waitingForCommit[request.id] = request.response

                scope.launch {
                    // cancel the timeout if the command is completed
                    request.response.invokeOnCompletion { cancel() }
                    delay(configuration.pendingCommandsTimeout)
                    // on timeout, remove and cancel from waiting queue
                    waitingForCommit.remove(entry.id)
                        ?.cancel("Time out")
                }

            }

            else -> request.response
                .completeExceptionally(IllegalStateException("Only leader can accept commands"))
        }
    }

    private fun releaseCommit(entry: LogEntry, result: Any) {
        logger.debug {
            entry(
                "Releasing_Entry",
                "entry" to entry.id,
                "result" to result
            )
        }
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
                logger.debug {
                    entry(
                        "Applying_Committed",
                        "entry" to it.id,
                        "index" to lastApplied + 1,
                        "commit_index" to serverState.commitIndex,
                        "client_handle" to waitingForCommit.contains(it.id)
                    )
                }
                val result = when (it.entry) {
                    is LogEntry.ClientCommand -> stateMachine.apply(it)
                    is LogEntry.ConfigurationChange -> "ConfigurationChanged"
                }
                serverState.lastApplied++
                releaseCommit(it, result)
            }
        }
    }

    suspend fun stop() {
        incomingRequests.cancel()
        waitingForCommit.forEach {
            it.value.cancel("Stopping RaftMachine")
        }
        waitingForCommit.clear()
        job?.run {
            cancel("Stopping RaftMachine")
            join()
        }
    }

    private suspend fun transitionTo(newRole: RaftRole): Role {
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
        ::transitionTo
    )

    private fun createCandidate() = Candidate(
        _role.value.serverState,
        configuration,
        log,
        cluster,
        ::transitionTo
    )

    private fun createLeader() = Leader(
        _role.value.serverState,
        log,
        cluster,
        ::transitionTo,
        scope,
        configuration
    )
}