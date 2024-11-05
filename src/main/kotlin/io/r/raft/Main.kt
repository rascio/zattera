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
import io.r.raft.transport.RaftClusterNode
import io.r.raft.transport.ktor.KtorRestRaftClusterNode
import io.r.raft.transport.ktor.KtorRestRaftClusterNode.RestNodeAddress
import io.r.raft.transport.utils.LoggingRaftClusterNode
import io.r.utils.logs.entry
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
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
        - POST /entries on the leader to append a new entry
        - GET /entries to get all entries
    """,
)
class RestRaftServer : Callable<String> {

    private val logger = LogManager.getLogger(RestRaftServer::class.java)

    @Parameters(index = "0", description = ["The id of the node"])
    private lateinit var id: String

    @Option(names = ["--port"], description = ["The port of the server"])
    private var port: Int = -1

    @Option(names = ["--peer"], description = ["The list of peers"])
    private lateinit var peers: List<String>

    @Option(names = ["--election-timeout"], description = ["The election timeout"])
    private var leaderTimeout: Long = -1

    @Option(names = ["--heartbeat-timeout"], description = ["The heartbeat timeout"])
    private var heartbeatTimeout: Long = -1

    @Option(names = ["--election-jitter"], description = ["The leader jitter"], required = false)
    private var leaderJitter: Long = 50

    @Option(names = ["--debug-messages"], description = ["Enable debug logs"], required = false)
    private var debugMessages: Boolean = false

    override fun call(): String {
        logger.info(entry("Starting server", "id" to id, "port" to port))
        runBlocking(Dispatchers.IO + CoroutineName("Server")) {
            resourceScope {
                execute()
            }
        }
        return "Started"
    }

    private suspend fun ResourceScope.execute() = coroutineScope {
        val raftLog = InMemoryRaftLog()
        val raftClusterNode = KtorRestRaftClusterNode(id, peers.map(RestNodeAddress::parse).toSet())

        val raftMachine = installRaftMachine(
            raftClusterNode = when {
                debugMessages -> autoCloseable { LoggingRaftClusterNode(raftClusterNode) }
                else -> raftClusterNode
            },
            raftLog = raftLog,
            coroutineScope = CoroutineScope(Dispatchers.IO)
        )
        val http = installHttpServer(
            raftClusterNode = raftClusterNode,
            raftMachine = raftMachine,
            raftLog = raftLog
        )
        logger.info(entry("Server_started", "id" to id, "port" to port))
        http.start(wait = true)
    }

    private suspend fun ResourceScope.installHttpServer(
        raftClusterNode: KtorRestRaftClusterNode,
        raftMachine: RaftMachine,
        raftLog: InMemoryRaftLog
    ) = install(
        acquire = {
            embeddedServer(Netty, port = port) {
                install(CORS) {
                    anyHost()
                }
                installRoutes(raftClusterNode, raftMachine, raftLog)
            }
        },
        release = { it, _ -> it.stop() }
    )

    private fun Application.installRoutes(
        raftClusterNode: KtorRestRaftClusterNode,
        raft: RaftMachine,
        raftLog: InMemoryRaftLog
    ) {
        routing {
            route("/raft", raftClusterNode.endpoints)
            route("/entries") {
                post {
                    val entry = call.receive<ByteArray>()
                    runCatching {
                        raft.command(entry).join()
                    }.onSuccess {
                        call.respondText("OK")
                    }.onFailure {
                        call.respondText(
                            text = "Error: ${it.message}",
                            status = HttpStatusCode.InternalServerError
                        )
                    }
                }
                get {
                    raftLog.getEntries(0, Int.MAX_VALUE)
                        .joinToString("\n") { it.command.decodeToString() }
                        .let { call.respondText(it) }
                }
            }
        }
    }

    private suspend fun ResourceScope.installRaftMachine(
        raftClusterNode: RaftClusterNode,
        raftLog: InMemoryRaftLog,
        coroutineScope: CoroutineScope
    ) = install(
        acquire = {
            RaftMachine(
                configuration = RaftMachine.Configuration(
                    leaderElectionTimeoutMs = leaderTimeout,
                    leaderElectionTimeoutJitterMs = leaderJitter,
                    heartbeatTimeoutMs = heartbeatTimeout
                ),
                cluster = raftClusterNode,
                log = raftLog,
                stateMachine = object : StateMachine {

                    val lastApplied = AtomicLong()

                    override suspend fun apply(command: LogEntry) {
                        // Do nothing for now
                        logger.info(entry("Applied", "command" to command.command.decodeToString()))
                        lastApplied.incrementAndGet()
                    }

                },
                scope = coroutineScope
            ).apply { start() }
        },
        release = { it, _ -> it.stop() }
    )
}