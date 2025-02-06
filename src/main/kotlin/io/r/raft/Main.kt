package io.r.raft

import arrow.atomic.AtomicLong
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
import io.ktor.server.request.receive
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import io.r.raft.log.StateMachine
import io.r.raft.log.inmemory.InMemoryRaftLog
import io.r.raft.machine.RaftMachine
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.toClusterNode
import io.r.raft.transport.RaftCluster
import io.r.raft.transport.ktor.HttpRaftCluster
import io.r.raft.transport.ktor.HttpRaftController
import io.r.utils.logs.entry
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.apache.logging.log4j.LogManager
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters
import java.util.concurrent.Callable
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    val exitCode = CommandLine(RestRaftServer()).execute(*args)
    exitProcess(exitCode)
}

@Command(
    name = "rest-raft-server",
    descriptionHeading = """
        RestRaftServer
        =============
        
        Start a Raft server with a REST API.
        Useful endpoints are:
        - POST /entries to append a new entry
        - GET /entries to get all entries
    """,
)
class RestRaftServer : Callable<String> {

    private val logger = LogManager.getLogger(RestRaftServer::class.java)

    @Parameters(
        index = "0",
        description = ["The id of the node"]
    )
    private lateinit var id: String

    @Option(
        names = ["--port"],
        description = ["The port of the server"]
    )
    private var port: Int = -1

    @Option(
        names = ["--peer"],
        description = ["The list of peers, example value: N1=localhost:8081"]
    )
    private var peers: List<String> = emptyList()

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
        names = ["--debug-messages"],
        description = ["Enable debug logs"],
        required = false
    )
    private var debugMessages: Boolean = false

    @Option(
        names = ["--state-machine"],
        description = ["The fully qualified name of the state machine"],
        defaultValue = "io.r.raft.SimpleCounter",
        required = false
    )
    private var stateMachine: String = SimpleCounter::class.qualifiedName!!

    override fun call(): String {
        logger.info(entry("Starting server", "id" to id, "port" to port))
        runBlocking(Dispatchers.IO + CoroutineName("Server")) {
            resourceScope {
                execute()
            }
        }
        return "Started"
    }

    private suspend fun ResourceScope.execute() {
        val raftLog = InMemoryRaftLog()

        val httpRaftClusterPeers = autoCloseable { HttpRaftCluster() }
        val raftCluster = RaftCluster(id, httpRaftClusterPeers, debugMessages)

        val raftMachine = installRaftMachine(
            raftCluster = raftCluster,
            raftLog = raftLog,
            coroutineScope = CoroutineScope(Dispatchers.IO)
        )
        val http = installHttpServer(
            raftClusterNode = HttpRaftController(raftMachine, debugMessages = debugMessages),
            raftMachine = raftMachine,
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

    private suspend fun ResourceScope.installHttpServer(
        raftClusterNode: HttpRaftController,
        raftMachine: RaftMachine,
        raftLog: InMemoryRaftLog
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
                installRoutes(raftClusterNode, raftMachine, raftLog)
            }
        },
        release = { it, _ -> it.stop() }
    )

    private fun Application.installRoutes(
        raftClusterNode: HttpRaftController,
        raft: RaftMachine,
        raftLog: InMemoryRaftLog
    ) {
        routing {
            get("/status") {
                call.respondText("OK")
            }
            route("/raft", raftClusterNode.endpoints)
            route("/entries") {
                post {
                    val entry = call.receive<ByteArray>()
                    val result = raft.command(LogEntry.ClientCommand(entry))
                    val response = Json.encodeToString(result)
                    call.respondText(response)
                }
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
        raftLog: InMemoryRaftLog,
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
                stateMachine = newStateMachine(),
                scope = coroutineScope
            ).apply {
                start()
            }
        },
        release = { it, _ -> it.stop() }
    )

    private fun newStateMachine() : StateMachine {
        val clazz = Class.forName(stateMachine)
        require(StateMachine::class.java.isAssignableFrom(clazz)) {
            "State machine must implement the StateMachine interface"
        }
        val constructor = clazz.getConstructor()
        return constructor.newInstance() as StateMachine
    }
}

class SimpleCounter : StateMachine{

    private val lastApplied = AtomicLong()

    override suspend fun apply(command: LogEntry): ByteArray {
        // Do nothing for now
        logger.info(entry("Applied", "command" to command.entry.describe()))
        return "ADDED_${lastApplied.incrementAndGet()}"
            .encodeToByteArray()
    }

    companion object {
        private val logger = LogManager.getLogger(SimpleCounter::class.java)
    }
}

private fun LogEntry.Entry.describe() = when (this) {
    is LogEntry.ClientCommand -> bytes.decodeToString()
    is LogEntry.ConfigurationChange -> toString()
}