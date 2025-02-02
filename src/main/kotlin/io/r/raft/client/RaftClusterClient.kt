package io.r.raft.client

import arrow.core.flatMap
import io.r.raft.machine.Response
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.RaftCluster
import kotlinx.coroutines.delay
import kotlin.random.Random

class RaftClusterClient(
    private val peers: RaftCluster.RaftPeers
) {
    private var leader: NodeId? = null

    fun connected(): RaftRpc.ClusterNode? = leader?.let { peers[it]?.node }

    suspend fun request(entry: LogEntry.ClientCommand): Result<ByteArray> {
        val node = getConnectedNode()
        return runCatching { node.request(entry) }
            .onFailure { leader = null }
            .flatMap { response ->
                when (response) {
                    Response.LeaderUnknown -> {
                        delay(Random.nextLong(50, 100))
                        request(entry)
                    }

                    is Response.NotALeader -> {
                        if (response.node.id !in peers.ids) {
                            peers.connect(response.node)
                        }
                        leader = response.node.id
                        request(entry)
                    }

                    is Response.Success -> Result.success(response.value)
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
}