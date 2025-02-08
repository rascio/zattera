package io.r.raft.client

import io.r.raft.machine.Response
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.RaftCluster
import io.r.raft.transport.RaftService
import io.r.utils.logs.entry
import kotlinx.coroutines.delay
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.apache.logging.log4j.kotlin.logger
import kotlin.random.Random

class RaftClusterClient(
    private val peers: RaftCluster.RaftPeers,
    private val configuration: Configuration = Configuration()
) {

    data class Configuration(
        val retry: Int = 3,
        val delay: LongRange = 50L..100,
        val jitter: LongRange = 0L..30
    )

    private var leader: NodeId? = null

    fun connected(): RaftRpc.ClusterNode? = leader?.let { peers[it]?.node }

    suspend fun request(entry: LogEntry.ClientCommand): Result<ByteArray> =
        send(configuration.retry) { it.request(entry) }
            .map { it.value }

    suspend fun query(query: ByteArray): Result<ByteArray> =
        send(configuration.retry) { it.query(query) }
            .map { it.value }

    private suspend fun send(retry: Int, exec: suspend (RaftService) -> Response): Result<Response.Success> {
        val node = getConnectedNode()
        return try {
            when (val response = exec(node)) {
                Response.LeaderUnknown -> {
                    logger.debug { entry("leader_unknown", "node" to node.id) }
                    delay(configuration.delay.random())
                    send(retry - 1, exec)
                }

                is Response.NotALeader -> {
                    if (response.node.id !in peers.ids) {
                        peers.connect(response.node)
                    }
                    leader = response.node.id
                    logger.debug { entry("not_a_leader", "node" to node.id) }
                    send(retry - 1, exec)
                }

                is Response.Success -> Result.success(response)
            }
        } catch (e: Exception) {
            changeNode()
            if (retry > 0) {
                logger.debug(e) { entry("client_retry", "node" to node.id) }
                delay(configuration.delay.random())
                send(retry - 1, exec)
            } else {
                throw e
            }
        }
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
        do { leader = peers.ids.random() }
        while (leader == l)
    }
}