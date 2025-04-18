@file:JacocoExclusionNeedsGenerated

package io.r.raft.cli

import arrow.fx.coroutines.ResourceScope
import arrow.fx.coroutines.autoCloseable
import arrow.fx.coroutines.resourceScope
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.call
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.cors.routing.CORS
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import io.r.counter.SimpleCounter
import io.r.raft.machine.RaftMachine
import io.r.raft.machine.StateMachineAdapter
import io.r.raft.persistence.RaftLog
import io.r.raft.persistence.StateMachine
import io.r.raft.persistence.h2.MVStoreRaftLog
import io.r.raft.persistence.inmemory.InMemoryRaftLog
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.toClusterNode
import io.r.raft.transport.RaftCluster
import io.r.raft.transport.ktor.HttpRaftCluster
import io.r.raft.transport.ktor.HttpRaftController
import io.r.utils.JacocoExclusionNeedsGenerated
import io.r.utils.logs.entry
import io.r.utils.requireMatch
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import org.apache.logging.log4j.LogManager
import org.h2.mvstore.MVStore
import picocli.CommandLine.Command
import picocli.CommandLine.Model.CommandSpec
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters
import picocli.CommandLine.Spec
import java.io.File
import java.util.concurrent.Callable

@Command(
    name = "server",
    header = [
        "Raft server with a REST API",
    ],
    description = [
        """
        Start the HTTP server for the Raft consensus algorithm.
        Useful endpoints are:
            - POST /request used by the client to send commands
            - GET /entries to get all entries in the node log
        The StateMachine to execute can be configured with the --state-machine option.
        """
    ]
)
class RestRaftServer : Callable<String> {

    private val logger = LogManager.getLogger(RestRaftServer::class.java)

    @Spec
    private lateinit var spec: CommandSpec

    @Parameters(
        index = "0",
        description = ["The id of the node"]
    )
    private lateinit var id: String

    @Option(
        names = ["--port"],
        description = ["The port of the server"],
        required = true
    )
    private var port: Int = -1

    @Option(
        names = ["--peer"],
        description = ["The list of peers, example value: N1=localhost:8081"],
        required = true
    )
    lateinit var peers: List<String>

    @Option(
        names = ["--election-timeout"],
        description = ["The election timeout"]
    )
    private var leaderTimeout: Long = 500

    @Option(
        names = ["--heartbeat-timeout"],
        description = ["The heartbeat timeout"]
    )
    private var heartbeatTimeout: Long = 150

    @Option(
        names = ["--election-jitter"],
        description = ["The leader jitter"],
        required = false
    )
    private var leaderJitter: Long = 100

    @Option(
        names = ["--debug"],
        description = ["Enable debug logs, use -DlogLevel=DEBUG for all logs"],
        required = false
    )
    private var debugMessages: Boolean = false

    @Option(
        names = ["--state-machine"],
        description = ["The fully qualified name of the state machine"],
        defaultValue = "io.r.counter.SimpleCounter",
        required = false
    )
    private var stateMachine: String = SimpleCounter::class.qualifiedName!!

    @Option(
        names = ["--raft-log-file"],
        description = ["The file to store the raft log"],
        required = false
    )
    private var raftLogFile: File? = null

    override fun call(): String {
        peers.forEach { spec.requireMatch(it, PEER_PATTERN) }

        logger.info(entry("Starting server", "id" to id, "port" to port))
        runBlocking(Dispatchers.IO + CoroutineName("Server")) {
            resourceScope {
                execute()
            }
        }
        return "Started"
    }

    private suspend fun ResourceScope.execute() {
        val raftLog = installRaftLog()

        val httpRaftClusterPeers = autoCloseable { HttpRaftCluster() }
        val raftCluster = RaftCluster(id, httpRaftClusterPeers, debugMessages)

        val raftMachine = installRaftMachine(
            raftCluster = raftCluster,
            raftLog = raftLog,
            coroutineScope = CoroutineScope(Dispatchers.IO)
        )
        val http = installHttpServer(
            raftClusterNode = HttpRaftController(raftMachine, debugMessages = debugMessages),
            raftLog = raftLog
        )
        logger.info(entry("Server_started", "id" to id, "port" to port))
        if (peers.isNotEmpty()) {
            raftCluster.changeConfiguration(
                LogEntry.ConfigurationChange(
                    new = peers.map { it.toClusterNode() }
                )
            )
        }
        http.start(wait = true)
    }

    private suspend fun ResourceScope.installRaftLog() =
        when {
            raftLogFile == null -> {
                logger.info(entry("using_in_memory_log"))
                InMemoryRaftLog()
            }
            else -> {
                val store = installLogMVStore()
                logger.info(entry("using_mvstore_log", "file" to raftLogFile!!.absolutePath))
                MVStoreRaftLog(store, Json)
            }
        }

    private suspend fun ResourceScope.installLogMVStore() = install(
        acquire = { MVStore.open(raftLogFile!!.absolutePath) },
        release = { store, _ -> store.close() }
    )

    private suspend fun ResourceScope.installHttpServer(
        raftClusterNode: HttpRaftController,
        raftLog: RaftLog
    ) = install(
        acquire = {
            embeddedServer(Netty, port = port) {
                install(CORS) {
                    anyHost()
                }
                install(StatusPages) {
                    exception<Throwable> { call, cause ->
                        call.respondText(
                            text = "Error: ${cause.message}",
                            status = HttpStatusCode.InternalServerError
                        )
                    }
                }
                installRoutes(raftClusterNode, raftLog)
            }
        },
        release = { it, _ -> it.stop() }
    )

    private fun Application.installRoutes(
        raftClusterNode: HttpRaftController,
        raftLog: RaftLog
    ) {
        routing {
            get("/status") {
                call.respondText("OK")
            }
            route("/raft", raftClusterNode.endpoints)
            route("/entries") {
                get {
                    raftLog.getEntries(0, Int.MAX_VALUE)
                        .joinToString("\n") { it.entry.describe() }
                        .let { call.respondText(it) }
                }
            }
        }
    }

    private suspend fun ResourceScope.installRaftMachine(
        raftCluster: RaftCluster,
        raftLog: RaftLog,
        coroutineScope: CoroutineScope
    ) = install(
        acquire = {
            RaftMachine(
                configuration = RaftMachine.Configuration(
                    leaderElectionTimeoutMs = leaderTimeout,
                    leaderElectionTimeoutJitterMs = leaderJitter,
                    heartbeatTimeoutMs = heartbeatTimeout,
                ),
                cluster = raftCluster,
                log = raftLog,
                stateMachine = StateMachineAdapter(coroutineScope, newStateMachine()),
                scope = coroutineScope
            ).apply {
                start()
            }
        },
        release = { it, _ -> it.stop() }
    )

    private fun newStateMachine(): StateMachine<*, *, *> {
        val clazz = Class.forName(stateMachine)
        require(StateMachine::class.java.isAssignableFrom(clazz)) {
            "State machine must implement the StateMachine interface"
        }
        val constructor = clazz.getConstructor()
        return constructor.newInstance() as StateMachine<*, *, *>
    }
}

private fun LogEntry.Entry.describe() = when (this) {
    is LogEntry.ClientCommand -> bytes.decodeToString()
    else -> toString()
}