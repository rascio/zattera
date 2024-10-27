package io.r.raft.transport.ktor

import io.ktor.client.HttpClient
import io.ktor.client.plugins.websocket.WebSockets
import io.ktor.client.plugins.websocket.webSocketSession
import io.ktor.server.routing.Routing
import io.ktor.server.websocket.DefaultWebSocketServerSession
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.CloseReason
import io.ktor.websocket.DefaultWebSocketSession
import io.ktor.websocket.close
import io.ktor.websocket.readBytes
import io.ktor.websocket.send
import io.r.raft.NodeId
import io.r.raft.RaftMachine
import io.r.raft.RaftMessage
import io.r.raft.Transport
import io.r.utils.logs.entry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.decodeFromStream
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import java.io.ByteArrayInputStream
import java.net.ConnectException
import kotlin.coroutines.EmptyCoroutineContext

data class WebSocketAddress(val host: String, val port: Int) : RaftMachine.RaftNode {
    companion object {
        operator fun invoke(value: String): WebSocketAddress {
            val (host, port) = value.split(":")
            return WebSocketAddress(host, port.toInt())
        }
    }
}
fun Routing.webSocket(transport: KtorWebsocketTransport) {
    webSocket(path = "/raft/{node}", handler = transport.webSocketHandler)
}
@OptIn(ExperimentalSerializationApi::class)
class KtorWebsocketTransport(val self: NodeId) : Transport<WebSocketAddress>, AutoCloseable {

    companion object {
        val logger: Logger = LogManager.getLogger(KtorWebsocketTransport::class.java)
    }
    private val json = Json
    private val raftGroup = RaftGroupConnection(self)
    internal val messagesFromNodes = Channel<RaftMessage>(capacity = Channel.UNLIMITED)

    override suspend fun receive(): RaftMessage = messagesFromNodes.receive()

    override suspend fun send(node: WebSocketAddress, message: RaftMessage) {
//        log("sending_message", "from" to message.from, "to" to message.to, "message" to message)
        try {
            val connection = raftGroup.get(message.to, node)
            connection.send(message)
        }
        catch (e: ConnectException) {
            logger.warn("connect_exception: ${e.message}")
        }
        catch (e: Exception) {
            logger.error(entry("unknown_error", "node" to message.to), e)
        }
    }

    val webSocketHandler: suspend DefaultWebSocketServerSession.() -> Unit = {
        val name = checkNotNull(call.parameters["node"])
        val connection = this

        val raftNodeConnection = raftGroup.addConnection(name, connection)
        when {
            raftNodeConnection == null -> {
                connection.send(json.encodeToString(InitConnectionProtocol.AlreadyConnected))
                connection.close(CloseReason(CloseReason.Codes.CANNOT_ACCEPT, "Already connected"))
            }
            else -> {
                connection.send(json.encodeToString(InitConnectionProtocol.Connected))
                logger.info(entry("connected", "self" to self, "node" to name))
                raftNodeConnection.job.collect { }
            }
        }
        logger.info(entry("closing_connection", "self" to self, "node" to name))
    }

    override fun close() {
        raftGroup.close()
    }

    private inner class RaftNodeConnection(
        private val connection: DefaultWebSocketSession,
    ) : AutoCloseable {

        val isActive get() = connection.isActive

        val job = connection.incoming
            .consumeAsFlow()
//            .onStart { log("start_receiving", "node" to self) }
//            .onEach { log("received", "msg" to it.readBytes().decodeToString()) }
            .map { ByteArrayInputStream(it.readBytes()) }
            .map { json.decodeFromStream<RaftMessage>(it) }
            .onEach { messagesFromNodes.send(it) }
            .catch { logger.error(entry("error_receiving", "node" to self), it) }

        suspend fun send(message: RaftMessage) {
            connection.send(json.encodeToString(message))
        }

        override fun close() {
            runCatching { connection.cancel("close raft connection") }
        }
    }

    private inner class RaftGroupConnection(
        private val self: NodeId,
        private val scope: CoroutineScope = CoroutineScope(EmptyCoroutineContext)
    ) : AutoCloseable {
        private val peersLock = Mutex()
        private val peers: MutableMap<NodeId, RaftNodeConnection> = mutableMapOf()
        private val http = HttpClient {
            install(WebSockets)
        }

        suspend fun get(node: NodeId, address: WebSocketAddress): RaftNodeConnection =
            try {
                peersLock.withLock {
                    val result = peers[node]
                    when {
                        result == null -> createClient(address).also {
                            peers[node] = it
                        }

                        !result.isActive -> {
                            runCatching { result.close() }
                                .onFailure { logger.warn("error_closing_stale_connection: ${it.message}") }
                            createClient(address).also {
                                peers[node] = it
                            }
                        }

                        else -> result
                    }
                }
            } catch (e: ClientAlreadyConnectedException) {
                logger.info("retry_connection already connected")
                get(node, address)
            } catch (e: ClientInitConnectionTimeoutException) {
                logger.info("retry_connection timeout")
                get(node, address)
            }
        suspend fun addConnection(node: NodeId, connection: DefaultWebSocketSession): RaftNodeConnection? =
            peersLock.withLock {
                if (node in peers) {
                    logger.info(entry("already_connected", "self" to self, "node" to node))
                    return null
                }
                val client = RaftNodeConnection(connection)
                peers[node] = client
                client
            }


        private suspend fun createClient(node: WebSocketAddress): RaftNodeConnection {
            logger.info(entry("connecting", "self" to self, "node" to node, "address" to "${node.host}:${node.port}"))
            val connection = http.webSocketSession("ws://${node.host}:${node.port}/raft/${self}")
            val response = withTimeoutOrNull(300){
                connection.incoming
                    .receive()
                    .let { ByteArrayInputStream(it.readBytes()) }
                    .let { json.decodeFromStream<InitConnectionProtocol>(it) }
            } ?: throw ClientInitConnectionTimeoutException()
            if (response == InitConnectionProtocol.AlreadyConnected) {
                connection.close(CloseReason(CloseReason.Codes.NORMAL, "Already connected"))
                throw ClientAlreadyConnectedException()
            }
            return RaftNodeConnection(connection)
                .also {
                    logger.info(entry("connected", "self" to self, "node" to node))
                    scope.launch { it.job.collect { } }
                }
        }

        override fun close() {
            logger.info(entry("closing", "self" to self))
            runCatching { peers.forEach { (_, client) -> client.close() } }
            runCatching { scope.cancel("close raft group") }
        }
    }

    private class ClientAlreadyConnectedException : Exception()
    private class ClientInitConnectionTimeoutException : Exception()
}
@Serializable
enum class InitConnectionProtocol {
    Connected, AlreadyConnected
}