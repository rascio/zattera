package io.r.raft.transport.ktor

import io.ktor.client.HttpClient
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.readBytes
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.r.raft.machine.Response
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.Query
import io.r.raft.transport.RaftService
import io.r.utils.logs.entry
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.apache.logging.log4j.LogManager

class HttpRaftService(
    override val node: RaftRpc.ClusterNode,
    private val client: HttpClient
) : RaftService, AutoCloseable {

    private val json = Json

    private var connected = true

    override suspend fun send(message: RaftMessage) {
        runCatching {
            withTimeout(5000) {
                val response = client.post("http://${node.host}:${node.port}/raft/${message.from}/rpc") {
                    contentType(ContentType.Application.Json)
                    setBody(json.encodeToString(message.rpc))
                }
                if (response.status != HttpStatusCode.Accepted) {
                    logger.warn(
                        entry(
                            "server_error_response",
                            "to" to node.id,
                            "message" to message.rpc.describe(),
                            "status" to response.status
                        )
                    )
                } else {
                    connected = true
                }
            }
        }.onFailure { e ->
            if (connected) {
                logger.warn(
                    entry(
                        "error_sending",
                        "to" to node.id,
                        "message" to message.rpc.describe(),
                        "error" to e.message
                    )
                )
                connected = false
            }
        }
    }

    override suspend fun request(entry: LogEntry.Entry): Response {
        val response = client.post("http://${node.host}:${node.port}/raft/request") {
            contentType(ContentType.Application.Json)
            setBody(json.encodeToString(entry))
        }
        if (response.status != HttpStatusCode.OK) {
            logger.warn(entry("forwarding_failure", "to" to node.id, "entry" to entry, "status" to response.status))
        } else {
            connected = true
        }
        return response.readBytes()
            .decodeToString()
            .let { json.decodeFromString<Response>(it) }
    }

    override suspend fun query(query: Query): Response {
        val response = client.post("http://${node.host}:${node.port}/raft/query") {
            contentType(ContentType.Application.Json)
            setBody(query)
        }
        if (response.status != HttpStatusCode.OK) {
            logger.warn(entry("forwarding_failure", "to" to node.id, "query" to query, "status" to response.status))
        } else {
            connected = true
        }
        return response.readBytes()
            .decodeToString()
            .let { json.decodeFromString<Response>(it) }
    }

    override fun close() {
        client.close()
    }

    companion object {
        private val logger = LogManager.getLogger(HttpRaftService::class.java)
    }
}