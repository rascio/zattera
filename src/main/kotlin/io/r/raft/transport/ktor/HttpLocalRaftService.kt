package io.r.raft.transport.ktor

import io.ktor.http.HttpStatusCode
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.call
import io.ktor.server.request.receiveText
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.post
import io.ktor.util.pipeline.PipelineContext
import io.r.raft.machine.RaftMachine
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import org.apache.logging.log4j.LogManager

class HttpLocalRaftService(
    private val raftMachine: RaftMachine,
    private val debugMessages: Boolean = false
) {

    val endpoints: Route.() -> Unit = {
        val json = Json
        post("/{nodeId}/rpc") {
            runOrFail(HttpStatusCode.Accepted) {
                val nodeId = call.parameters["nodeId"]!!
                val body = call.receiveText()
                val payload = json.decodeFromString(RaftRpc.serializer(), body)
                if (debugMessages) {
                    logger.info("${raftMachine.id} <== ${payload.describe()} == $nodeId")
                }
                raftMachine.handle(
                    RaftMessage(
                        from = nodeId,
                        to = raftMachine.id,
                        rpc = payload
                    )
                )
                "OK"
            }
        }
        post("/request") {
            runOrFail(HttpStatusCode.OK) {
                val body = call.receiveText()
                val payload = json.decodeFromString<LogEntry.Entry>(body)
                if (debugMessages) {
                    logger.info("${raftMachine.id} <-- $payload -- ${raftMachine.id}")
                }
                raftMachine.request(payload)
                "OK2"
            }
        }
    }

    private suspend inline fun PipelineContext<*, ApplicationCall>.runOrFail(status: HttpStatusCode, block: () -> Any) {
        try {
            val res = block()
            call.respond(status, res)
        } catch (e: Exception) {
            e.printStackTrace()
            call.respond(HttpStatusCode.InternalServerError, e.message ?: "Internal server error")
        }
    }

    companion object {
        val logger = LogManager.getLogger(HttpLocalRaftService::class.java)
    }
}