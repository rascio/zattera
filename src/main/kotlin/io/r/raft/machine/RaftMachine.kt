package io.r.raft.machine

import io.r.raft.persistence.RaftLog
import io.r.raft.persistence.RaftLog.Companion.getLastMetadata
import io.r.raft.persistence.StateMachine
import io.r.raft.machine.StateMachineAdapter.Companion.isValidCommand
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRole
import io.r.raft.transport.RaftCluster
import io.r.utils.loggingCtx
import io.r.utils.logs.entry
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
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
import kotlinx.coroutines.withTimeout
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.Marker
import org.apache.logging.log4j.MarkerManager
import org.apache.logging.log4j.kotlin.additionalLoggingContext
import java.util.concurrent.ConcurrentHashMap

class RaftMachine<C : StateMachine.Contract<*, *, *>>(
    private val configuration: Configuration,
    private val cluster: RaftCluster,
    private val log: RaftLog,
    private val stateMachine: StateMachineAdapter<*, *, *, C>,
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.IO),
    private val input: Channel<RaftMessage> = Channel(capacity = Channel.BUFFERED)
) {

    /*
     * This class is used to keep track of the client requests
     */
    private class IncomingRequest<E>(
        val entry: E,
        val response: CompletableDeferred<Response>,
        val id: String
    )


    private val _role: MutableStateFlow<Role> = MutableStateFlow(
        createFollower(ServerState(0, 0))
    )

    /*
     * This job is used to run the main loop of the raft machine
     */
    private var job: Job? = null

    /*
     * This channel is used to receive client requests
     */
    private val incomingCommands = Channel<IncomingRequest<LogEntry.Entry>>(
        capacity = Channel.RENDEZVOUS,
        onUndeliveredElement = { it.response.completeExceptionally(IllegalStateException("RaftMachine is stopped")) },
    )

    /*
     * This map is used to keep track of the client requests that are waiting for commit
     */
    private val waitingForCommit = ConcurrentHashMap<String, CompletableDeferred<Response>>()

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
        job = scope.launch(additionalLoggingContext(map = mapOf("NodeId" to cluster.id))) {
            transitionTo(RaftRole.FOLLOWER)
            try {
                logger.debug("RaftMachine_Started")
                while (isActive) {
                    logger.debug(DIAGNOSTIC_MARKER) {
                        entry(
                            "RaftMachine_Loop",
                            "role" to _role.value::class.simpleName,
                            "state" to serverState
                        )
                    }
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
                        incomingCommands.onReceive { command ->
                            loggingCtx("cmd=${command.id}") {
                                handleClientCommand(command)
                            }
                        }
                    }
                }
            } catch (_: StopRaftMachine) {
                // stop() was called
            } catch (t: Throwable) {
                logger.error(entry("RaftMachine_Error", "error" to t.message), t)
            } finally {
                _role.value.onExit()
            }
            logger.info(entry("RaftMachine_Stopped", "role" to _role.value::class.simpleName, "term" to log.getTerm()))
        }
    }

    suspend fun handle(message: RaftMessage) {
        input.send(message)
    }

    /**
     * Execute a command in the state machine
     */
    suspend fun command(cmd: LogEntry.Entry): Response = loggingCtx(mapOf("NodeId" to cluster.id)) {
        if (cmd is LogEntry.ClientCommand) {
            require(stateMachine.contract.isValidCommand(cmd)) { "Invalid command $cmd" }
        }

        val request = IncomingRequest(
            id = cmd.id,
            entry = cmd,
            response = CompletableDeferred()
        )
        incomingCommands.send(request)
        logger.info {
            entry(
                "request_pending",
                "id" to request.id,
                "command" to cmd
            )
        }
        request.response.await()
    }

    /**
     * Query the state machine
     */
    suspend fun query(query: ByteArray): Response {
        return when (val role = _role.value) {
            is Leader -> {
                withTimeout(configuration.pendingCommandsTimeout) {
                    role.heartBeatCompletion.await()
                }
                Response.Success(stateMachine.read(query))
            }

            else -> {
                when (val leader = serverState.currentLeader) {
                    null -> Response.LeaderUnknown
                    else -> Response.NotALeader(cluster.getNode(leader).node)
                }
            }
        }
    }

    private suspend fun handleClientCommand(request: IncomingRequest<LogEntry.Entry>) {
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
                    logger.warn {
                        entry("Command_Timeout", "id" to request.id)
                    }
                    // on timeout, remove and cancel from waiting queue
                    waitingForCommit.remove(entry.id)
                        ?.cancel("Time out")
                }

            }

            else -> {
                val error = when (val leader = serverState.currentLeader) {
                    null -> Response.LeaderUnknown
                    else -> Response.NotALeader(cluster.getNode(leader).node)
                }
                request.response.complete(error)
            }
        }
    }

    private fun releaseCommit(entry: LogEntry, result: ByteArray) {
        waitingForCommit.remove(entry.id)
            ?.apply {
                logger.info {
                    entry(
                        "Releasing_Commit",
                        "entry" to entry.id,
                        "result" to result.decodeToString()
                    )
                }
                complete(Response.Success(result))
            }
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
                loggingCtx(it.id) {
                    logger.debug {
                        entry(
                            "Applying_Committed",
                            "index" to serverState.lastApplied + 1,
                            "commit_index" to serverState.commitIndex,
                            "client_handle" to waitingForCommit.contains(it.id)
                        )
                    }
                    val result = when (it.entry) {
                        is LogEntry.ClientCommand -> {
                            stateMachine.apply(it.entry)
                                .also {
                                    logger.info { entry("Applied", "result" to it.decodeToString()) }
                                }
                        }

                        is LogEntry.ConfigurationChange -> {
                            cluster.changeConfiguration(it.entry)
                            "true".encodeToByteArray()
                        }

                        is LogEntry.NoOp -> null
                    }

                    serverState.lastApplied++

                    if (result != null) releaseCommit(it, result)
                }
            }
            /*
             if (log.getSize() > config.snapshotThreshold) {
                val snapshot = stateMachine.snapshot()
                val metadata = log.getMetadata(serverState.lastApplied)
                log.compact(snapshot, metadata)
             }
             */
        }
    }

    private class StopRaftMachine : CancellationException("RaftMachine is stopped") {
        override fun fillInStackTrace(): Throwable = this
    }

    suspend fun stop() {
        val stop = StopRaftMachine()
        // stop accepting new commands
        incomingCommands.close(stop)

        // stop the main loop
        job?.run {
            cancel(stop)
            join()
        }
        job = null

        waitingForCommit.forEach {
            it.value.cancel()
        }
        waitingForCommit.clear()
    }

    private suspend fun transitionTo(newRole: RaftRole): Role {
        _role.value.onExit()
        _role.value = when (newRole) {
            RaftRole.FOLLOWER -> createFollower()
            RaftRole.CANDIDATE -> createCandidate()
            RaftRole.LEADER -> createLeader()
        }
        logger.info(entry("role_change", "to" to _role.value::class.simpleName, "term" to log.getTerm()))
        _role.value.onEnter()
        return _role.value
    }

    data class Configuration(
        val leaderElectionTimeoutMs: Long = 150,
        val leaderElectionTimeoutJitterMs: Long = 50,
        val heartbeatTimeoutMs: Long = 50,
        val maxLogEntriesPerAppend: Int = 10,
        /*
         * very bad, commands rejection can be done deterministically
         * without the need of a timeout
         * in order to do this deterministically, we need to keep track of the
         * index of the entry generated by the client command and the command id
         * if an entry with same index but different id is committed, we can reject the command
         */
        val pendingCommandsTimeout: Long = 3000,
        /**
         * Timeout for the queries that are waiting for the leader
         * to confirm it is still the leader
         */
        val pendingQueryTimeout: Long = 1000,
        val snapshotChunkSize: Int = 1024,
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

    companion object {
        val DIAGNOSTIC_MARKER: Marker = MarkerManager.getMarker("RAFT_DIAGNOSTIC")
        private val logger: Logger = LogManager.getLogger(RaftMachine::class.java)

    }
}