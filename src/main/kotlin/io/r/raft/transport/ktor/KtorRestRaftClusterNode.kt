package io.r.raft.transport.ktor

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.application.call
import io.ktor.server.request.receiveText
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import io.ktor.util.collections.ConcurrentSet
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.RaftClusterNode
import io.r.utils.logs.entry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

class KtorRestRaftClusterNode(
    override val id: NodeId,
    peers: Set<RestNodeAddress>
) : RaftClusterNode {

    data class RestNodeAddress(val id: NodeId, val host: String) {
        companion object {
            operator fun invoke(value: String): RestNodeAddress {
                val (id, address) = value.split("=")
                return RestNodeAddress(id, address)
            }
        }
    }

    private val json = Json
    private val client = HttpClient(CIO)
    private val messagesFromNodes = Channel<RaftMessage>(capacity = Channel.UNLIMITED)
    private val peersMap = peers.associateBy { it.id } - id
    private val unavailableNodes = ConcurrentSet<NodeId>()

    override val peers: Set<NodeId> = peersMap.keys

    override suspend fun send(to: NodeId, rpc: RaftRpc) {
        client.launch(Dispatchers.IO) {
            val address = peersMap[to] ?: error("Unknown node $to")
            val message = RaftMessage(
                from = id,
                to = to,
                rpc = rpc
            )
            runCatching {
                val response = client.post("${address.host}/raft/${id}") {
                    contentType(ContentType.Application.Json)
                    setBody(json.encodeToString(message))
                }
                if (response.status != HttpStatusCode.Accepted) {
                    logger.warn(entry("error_sending", "to" to to, "message" to rpc.describe(), "status" to response.status))
                } else {
                    unavailableNodes -= to
                }
            }.onFailure { e ->
                if (unavailableNodes.add(to)) {
                    logger.warn(entry("error_sending", "to" to to, "message" to rpc.describe(), "error" to e.message))
                }
            }
        }
    }

    override suspend fun receive(): RaftMessage = messagesFromNodes.receive()

    val endpoints: Route.() -> Unit = {
        route("/{nodeId}") {
            post {
                checkNotNull(call.parameters["nodeId"])
                val string = call.receiveText()
                val message = json.decodeFromString<RaftMessage>(string)
                messagesFromNodes.send(message)
                call.respondText(text = "OK", status = HttpStatusCode.Accepted)
            }
        }
    }

    companion object {
        val logger: Logger = LogManager.getLogger(KtorRestRaftClusterNode::class.java)
    }
}
