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
import kotlinx.serialization.json.Json
import org.apache.logging.log4j.LogManager
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import kotlin.reflect.KSuspendFunction2

class RaftClusterClient<Cmd : StateMachine.Command, Query : StateMachine.Query, Resp: StateMachine.Response>(
    private val peers: RaftCluster.RaftPeers,
    private val contract: StateMachine.Contract<Cmd, Query, Resp>,
    private val configuration: Configuration = Configuration()
) {
    private val clientId: UUID = UUID.randomUUID()

    private val sequence = AtomicLong()

    data class Configuration(
        val retry: Int = 3,
        val delay: LongRange = 50L..100,
        val jitter: LongRange = 0L..30
    )

    private var leader: NodeId? = null

    fun connected(): RaftRpc.ClusterNode? = leader?.let { peers[it]?.node }

    suspend fun request(command: Cmd): Result<Resp> =
        send(configuration.retry, command.toClientCommand(), RaftService::request)
            .map { it.toClientResponse() }


    suspend fun query(query: Query): Result<Resp> =
        send(configuration.retry, query.toBytes(), RaftService::query)
            .map { it.toClientResponse() }

    class ClientExecutionException(message: String, cause: Throwable? = null) : Exception(message, cause) {
        override fun fillInStackTrace() = null
    }

    /*
     * Retry management for client requests.
     * The client will try to send the request to the connected node, if:
     * - the node is the leader, the request is sent
     * - the node is not the leader, the client will try to connect to the leader
     * - the node is unknown, the client will try to connect to a random node
     * If the request fails, the client will try to send the request to another node.
     */
    private suspend fun <T> send(retry: Int, value: T, op: KSuspendFunction2<RaftService, T, Response>): Result<Response.Success> {
        var attempt = 0
        var result = Result.failure<Response.Success>(ClientExecutionException("Never started"))
        while (attempt < retry) {
            val node = getConnectedNode()
            try {
                when (val response = op(node, value)) {
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
        Json.encodeToString(serializer = contract.commandKSerializer, value = this)
            .encodeToByteArray()
            .let { LogEntry.ClientCommand(it, clientId, sequence.incrementAndGet()) }
            .also {
                logger.info(DIAGNOSTIC_MARKER) {
                    entry("track_messages", "cmd" to this, "id" to it.id)
                }
            }
    private fun Query.toBytes() =
        Json.encodeToString(serializer = contract.queryKSerializer, value = this)
            .encodeToByteArray()

    private fun Response.Success.toClientResponse() =
        Json.decodeFromString(deserializer = contract.responseKSerializer, this.value.decodeToString())

    companion object {
        private val logger = LogManager.getLogger("RaftClusterClient")
    }
}