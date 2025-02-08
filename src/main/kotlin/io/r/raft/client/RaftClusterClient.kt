package io.r.raft.client

import io.r.raft.log.StateMachine
import io.r.raft.machine.RaftMachine.Companion.DIAGNOSTIC_MARKER
import io.r.raft.machine.Response
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.RaftCluster
import io.r.raft.transport.RaftService
import io.r.utils.logs.entry
import kotlinx.coroutines.delay
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import org.apache.logging.log4j.LogManager
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

class RaftClusterClient<Cmd : StateMachine.Command>(
    private val peers: RaftCluster.RaftPeers,
    private val commandSerializer: KSerializer<Cmd>,
    private val configuration: Configuration = Configuration()
) {
    private val clientId: String = UUID.randomUUID().toString()

    private val sequence = AtomicLong()

    data class Configuration(
        val retry: Int = 3,
        val delay: LongRange = 50L..100,
        val jitter: LongRange = 0L..30
    )

    private var leader: NodeId? = null

    fun connected(): RaftRpc.ClusterNode? = leader?.let { peers[it]?.node }

    suspend fun request(command: Cmd): Result<ByteArray> =
        command.toClientCommand()
            .let { entry ->
                send(configuration.retry) { it.request(entry) }
                    .map { it.value }
            }


    suspend fun query(query: ByteArray): Result<ByteArray> =
        send(configuration.retry) { it.query(query) }
            .map { it.value }

    class ClientExecutionException(message: String, cause: Throwable? = null) : Exception(message, cause) {
        override fun fillInStackTrace() = null
    }

    private suspend fun send(retry: Int, exec: suspend (RaftService) -> Response): Result<Response.Success> {
        var attempt = 0
        var result = Result.failure<Response.Success>(ClientExecutionException("Never started"))
        while (attempt < retry) {
            val node = getConnectedNode()
            try {
                when (val response = exec(node)) {
                    Response.LeaderUnknown -> {
                        logger.debug { entry("leader_unknown", "node" to node.id) }
                        delay(configuration.delay.random())
                    }

                    is Response.NotALeader -> {
                        logger.debug { entry("not_a_leader", "node" to node.id) }
                        if (response.node.id !in peers.ids) {
                            peers.connect(response.node)
                        }
                        leader = response.node.id
                    }

                    is Response.Success -> return Result.success(response)
                }
            } catch (e: Exception) {
                attempt++
                result = Result.failure(ClientExecutionException("Failed to execute command", e))
                changeNode()
                if (attempt < retry) {
                    logger.warn { entry("client_retry", "node" to node.id, "retry" to retry, "error" to e.message) }
                    delay(configuration.delay.random())
                }
            }
        }
        return result
    }

    private fun getConnectedNode() =
        leader?.let { peers[it] } ?: run {
            val l = checkNotNull(peers.ids.random()) {
                "No nodes available"
            }
            leader = l
            peers[l]!!
        }

    private fun changeNode() {
        val l = leader
        do {
            leader = peers.ids.random()
        } while (leader == l)
    }

    private fun Cmd.toClientCommand() =
        StateMachine.Message(clientId, sequence.incrementAndGet(), this)
            .let {
                val bytes = Json.encodeToString(
                    serializer = StateMachine.Message.serializer(commandSerializer),
                    value = it
                ).encodeToByteArray()
                logger.info(DIAGNOSTIC_MARKER) {
                    entry("track_messages", "cmd" to this, "id" to it.id)
                }
                LogEntry.ClientCommand(bytes, it.id)
            }

    companion object {
        private val logger = LogManager.getLogger("RaftClusterClient")
    }
}