package io.r.raft.transport.ktor

import arrow.fx.coroutines.ResourceScope
import arrow.fx.coroutines.resourceScope
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.call
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.cors.routing.CORS
import io.ktor.server.request.receive
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.r.raft.RaftMachine
import io.r.raft.persistence.inmemory.InMemoryPersistence
import io.r.raft.persistence.utils.LoggingPersistence
import io.r.raft.transport.utils.LoggingTransport
import io.r.utils.logs.entry
import io.r.utils.timeout.Timeout
import kotlinx.coroutines.CoroutineName
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


@Command(name = "test-websocket-server")
class TestWebSocketServer : Callable<String> {

    private val logger = LogManager.getLogger(TestWebSocketServer::class.java)

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

    private fun String.toPeerId() = this.split("=").let { (id, address) ->
        id to WebRestAddress(address)
    }

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
        val transport = install(
            { KtorRestTransport(id) },
            { it, _ -> it.close() }
        )
        val persistence = InMemoryPersistence()
        val raft = install(
            {
                val peers1: Map<String, WebRestAddress> = peers.associate { it.toPeerId() }
                RaftMachine(
                    configuration = RaftMachine.Configuration<WebRestAddress>(
                        id = id,
                        peers = peers1,
                        leaderElectionTimeout = Timeout(leaderTimeout).jitter(leaderJitter),
                        heartbeatTimeout = Timeout(heartbeatTimeout)
                    ),
                    transport = LoggingTransport(id, transport),
                    persistence = LoggingPersistence(id, persistence),
                    this@coroutineScope
                ).apply { start() }
            },
            { it, _ -> it.stop() }
        )
        val http = install(
            {
                embeddedServer(Netty, port = port) {
                    install(CORS) {
                        anyHost()
                    }

                    routing {
                        get("/healthcheck") {
                            call.respondText("Tutt'apposto")
                        }
//                        webSocket(path = "/raft/{node}", handler = transport.webSocketHandler)
                        raftEndpoints(transport)
                        post("/append") {
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
                    }
                }
            },
            { it, _ -> it.stop() }
        )
        logger.info(entry("Server_started", "id" to id, "port" to port))
        http.start(wait = true)
    }
}

fun main(args: Array<String>) {
    val exitCode = CommandLine(TestWebSocketServer()).execute(*args)
    exitProcess(exitCode)
}