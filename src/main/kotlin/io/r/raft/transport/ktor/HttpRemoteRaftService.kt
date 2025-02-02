package io.r.raft.transport.ktor

import io.ktor.client.HttpClient
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.readBytes
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.RaftService
import io.r.utils.logs.entry
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.apache.logging.log4j.LogManager

class HttpRemoteRaftService(
    private val node: RaftRpc.ClusterNode,
    private val client: HttpClient
) : RaftService, AutoCloseable {

    private val json = Json

    private var connected = true

    override suspend fun send(message: RaftMessage) {
        runCatching {
            val response = client.post("http://${node.host}:${node.port}/raft/${message.from}/rpc") {
                contentType(ContentType.Application.Json)
                setBody(json.encodeToString(message.rpc))
            }
            if (response.status != HttpStatusCode.Accepted) {
                logger.warn(
                    entry(
                        "error_sending",
                        "to" to node.id,
                        "message" to message.rpc.describe(),
                        "status" to response.status
                    )
                )
            } else {
                connected = true
            }
        }.onFailure { e ->
            if (connected) {
                logger.warn(
                    entry(
                        "error_sending",
                        "to" to node.id,
                        "message" to message.rpc.describe(),
                        "error" to e.message
                    ),
                    e
                )
                connected = false
            }
        }
    }

    override suspend fun forward(entry: LogEntry.Entry) {
        runCatching {
            val response = client.post("http://${node.host}:${node.port}/raft/request") {
                contentType(ContentType.Application.Json)
                setBody(json.encodeToString(entry))
            }
            if (response.status != HttpStatusCode.OK) {
                logger.warn(entry("error_forwarding", "to" to node.id, "entry" to entry, "status" to response.status))
            } else {
                connected = true
            }
            response.readBytes().decodeToString()
        }.onFailure { e ->
            if (connected) {
                logger.warn(entry("error_forwarding", "to" to node.id, "entry" to entry, "error" to e.message))
                connected = false
            }
        }
    }

    override fun close() {
        client.close()
    }

    companion object {
        private val logger = LogManager.getLogger(HttpRemoteRaftService::class.java)
    }
}