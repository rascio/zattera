package io.r.raft.transport.ktor

import arrow.fx.coroutines.ResourceScope
import arrow.fx.coroutines.resourceScope
import io.kotest.assertions.fail
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.websocket.DefaultClientWebSocketSession
import io.ktor.client.plugins.websocket.receiveDeserialized
import io.ktor.client.plugins.websocket.sendSerialized
import io.ktor.client.plugins.websocket.webSocket
import io.ktor.serialization.kotlinx.KotlinxWebsocketSerializationConverter
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.routing.Routing
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets
import io.ktor.server.websocket.webSocket
import io.ktor.utils.io.ByteReadChannel
import io.ktor.websocket.readBytes
import io.r.raft.LogEntryMetadata
import io.r.raft.RaftMessage
import io.r.raft.RaftProtocol
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.completeWith
import kotlinx.serialization.json.Json
import org.apache.logging.log4j.LogManager
import kotlin.time.Duration.Companion.seconds
import io.ktor.client.plugins.websocket.WebSockets as ClientWebSockets

class KtorWebsocketTransportTest : FunSpec({

    val logger = LogManager.getLogger(KtorWebsocketTransportTest::class.java)

    val aMessage = RaftMessage(
        from = "1",
        to = "1",
        protocol = RaftProtocol.RequestVote(
            term = 0,
            candidateId = "1",
            lastLog = LogEntryMetadata(0, 0)
        )
    )
    context("Single Server") {
        test("Given a server And a client connected to it When the client send a message Then the message is received from the transport")
            .config(timeout = 5.seconds) {
                resourceScope {
                    val transport = installTransport(1)
                    val server = installServer(1) {
                        webSocket(transport)
                    }
                    server.start()
                    val client = installClient()
                    logger.info("Connecting")
                    client.connectWebSocket(1) {
                        logger.info("Connected")
                        sendSerialized(aMessage)
                        logger.info("Sent")
                        val msg = transport.receive()
                        logger.info("Received")
                        msg shouldBe aMessage
                    }
                }
            }
        test("Given a server And a client connected to it When the transport sends a message Then the message is received by the client")
            .config(timeout = 5.seconds) {
                resourceScope {
                    val transport = installTransport(1)
                    val server = installServer(1) {
                        webSocket(transport)
                    }
                    server.start()
                    val client = installClient()
                    client.connectWebSocket(1) {
                        transport.send(WebSocketAddress("localhost", 8080), aMessage)
                        val msg = receiveDeserialized<RaftMessage>()
                        msg shouldBe aMessage
                    }
                }
            }
        test("Given a transport And a different server When the transport sends a message Then a connection is established with the other server And the message is sent").config(
            timeout = 5.seconds
        ) {
            resourceScope {
                val transport = installTransport(1)
                val received = CompletableDeferred<RaftMessage>()
                val server = installServer(2) {
                    webSocket(path = "/raft/1") {
                        received.completeWith(
                            runCatching {
                                val msg = incoming.receive()
                                Json.decodeFromString(RaftMessage.serializer(), msg.readBytes().decodeToString())
                            }
                        )
                    }
                }
                server.start()
                transport.send(WebSocketAddress("localhost", 8082), aMessage)
                val msg = received.await()
                msg shouldBe aMessage
            }
        }
        test("Given a transport And its server And a different server When the transport sends a message Then the message is received by the server")
            .config(timeout = 5.seconds) {
                resourceScope {
                    val transport = installTransport(1)
                    val received = CompletableDeferred<RaftMessage>()
                    val server = installServer(2) {
                        webSocket(path = "/raft/1") {
                            runCatching { incoming.receive() }
                                .map {
                                    Json.decodeFromString(
                                        RaftMessage.serializer(),
                                        it.readBytes().decodeToString()
                                    )
                                }
                                .let(received::completeWith)
                        }
                    }
                    server.start()
                    transport.send(WebSocketAddress("localhost", 8082), aMessage)
                    val msg = received.await()
                    msg shouldBe aMessage
                }
            }
    }

    context("Multiple Servers") {
        test("Given a transport And multiple servers When the transport sends a message Then the message is received by all servers")
            .config(timeout = 5.seconds) {
                resourceScope {
                    val message = aMessage.copy(from = "1", to = "2")
                    val transport1 = installTransport(1)
                    val server1 = installServer(1) {
                        webSocket(transport1)
                    }
                    val transport2 = installTransport(2)
                    val server2 = installServer(2) {
                        webSocket(transport2)
                    }
                    server1.start()
                    server2.start()
                    logger.info("Sending message")
                    transport1.send(WebSocketAddress("localhost", 8082), message)
                    val msg = transport2.receive()
                    msg shouldBe message
                }
            }
    }
})

suspend fun HttpClient.connectWebSocket(node: Int, block: suspend DefaultClientWebSocketSession.() -> Unit) =
    webSocket("ws://localhost:${8080 + node}/raft/$node") {
        block()
    }

suspend fun ResourceScope.installTransport(node: Int) = install(
    acquire = { KtorWebsocketTransport(node.toString()) },
    release = { it, _ -> it.close() }
)

suspend fun ResourceScope.installClient() = install(
    acquire = {
        HttpClient(CIO) {
            install(ClientWebSockets) {
                contentConverter = KotlinxWebsocketSerializationConverter(Json)
            }
        }
    },
    release = { it, _ -> it.close() }
)

suspend fun ResourceScope.installServer(node: Int, routing: Routing.() -> Unit) = install(
    acquire = {
        embeddedServer(Netty, port = 8080 + node) {
            install(WebSockets)
            routing {
                routing()
            }
        }
    },
    release = { it, _ -> it.stop() }
)
