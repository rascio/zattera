package io.r.raft.transport.ktor

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.call
import io.ktor.server.request.receiveText
import io.ktor.server.response.respondText
import io.ktor.server.routing.Routing
import io.ktor.server.routing.post
import io.ktor.util.pipeline.PipelineContext
import io.r.raft.NodeId
import io.r.raft.RaftMachine
import io.r.raft.RaftMessage
import io.r.raft.Transport
import io.r.utils.logs.entry
import kotlinx.coroutines.channels.Channel
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

data class WebRestAddress(val host: String, val port: Int) : RaftMachine.RaftNode {
    companion object {
        operator fun invoke(value: String): WebRestAddress {
            val (host, port) = value.split(":")
            return WebRestAddress(host, port.toInt())
        }
    }
}
fun Routing.raftEndpoints(transport: KtorRestTransport) {
    post("/raft/{node}", transport.restHandler)
}
class KtorRestTransport(val self: NodeId) : Transport<WebRestAddress>, AutoCloseable {

    companion object {
        val logger: Logger = LogManager.getLogger(KtorRestTransport::class.java)
    }
    private val json = Json
    private val client = HttpClient(CIO)
    private val messagesFromNodes = Channel<RaftMessage>(capacity = Channel.UNLIMITED)

    override suspend fun receive(): RaftMessage = messagesFromNodes.receive()

    override suspend fun send(node: WebRestAddress, message: RaftMessage) {
        val response = client.post("http://${node.host}:${node.port}/raft/${self}") {
            contentType(ContentType.Application.Json)
            setBody(json.encodeToString(message))
        }
        if (response.status != HttpStatusCode.Accepted) {
            logger.warn(entry("error_sending", "self" to self, "node" to node, "message" to message))
        }
    }

    val restHandler: suspend PipelineContext<Unit, ApplicationCall>.(Unit) -> Unit = {
        checkNotNull(call.parameters["node"])
        val string = call.receiveText()
        val message = json.decodeFromString<RaftMessage>(string)
        messagesFromNodes.send(message)
        call.respondText(text = "OK", status = HttpStatusCode.Accepted)
    }

    override fun close() {
        client.close()
    }

}
