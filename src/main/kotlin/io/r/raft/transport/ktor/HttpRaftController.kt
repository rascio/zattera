package io.r.raft.transport.ktor

import io.ktor.http.HttpStatusCode
import io.ktor.server.application.call
import io.ktor.server.request.receiveText
import io.ktor.server.response.respondText
import io.ktor.server.routing.Route
import io.ktor.server.routing.post
import io.r.raft.machine.RaftMachine
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

class HttpRaftController(
    private val raftMachine: RaftMachine<*>,
    private val debugMessages: Boolean = false
) {

    val endpoints: Route.() -> Unit = {
        val json = Json

        post("/{nodeId}/rpc") {
            val nodeId = call.parameters["nodeId"]!!
            val body = call.receiveText()
            val payload = json.decodeFromString(RaftRpc.serializer(), body)
            if (debugMessages) {
                httpMessagesLogger.info("${raftMachine.id} <== ${payload.describe()} == $nodeId")
            }
            raftMachine.handle(
                RaftMessage(
                    from = nodeId,
                    to = raftMachine.id,
                    rpc = payload
                )
            )
            call.respondText("OK", status = HttpStatusCode.Accepted)

        }
        post("/request") {
            val body = call.receiveText()
            val payload = json.decodeFromString<LogEntry.Entry>(body)
            if (debugMessages) {
                httpMessagesLogger.info("${raftMachine.id} <-- !$payload -- ${raftMachine.id}")
            }
            val res = raftMachine.command(payload)
            call.respondText(
                text = Json.encodeToString(res),
                status = HttpStatusCode.OK
            )
        }
        post("/query") {
            val payload = call.receiveText()
            if (debugMessages) {
                httpMessagesLogger.info("${raftMachine.id} <-- ?$payload -- ${raftMachine.id}")
            }
            val res = raftMachine.query(payload.encodeToByteArray())
            call.respondText(
                text = Json.encodeToString(res),
                status = HttpStatusCode.OK
            )
        }
    }

    companion object {
        private val httpMessagesLogger: Logger = LogManager.getLogger("HttpMessagesLogger.In")
    }
}