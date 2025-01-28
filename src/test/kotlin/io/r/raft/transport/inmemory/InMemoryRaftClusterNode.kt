package io.r.raft.transport.inmemory

import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.RaftCluster
import kotlinx.coroutines.channels.ReceiveChannel

class InMemoryRaftClusterNode(
    override val id: NodeId,
    private val cluster: RaftClusterInMemoryNetwork,
) : RaftCluster {
    override val peers: Set<NodeId> get() = cluster.nodes - id
    override val input: ReceiveChannel<RaftMessage> = cluster.createPeer(id)

    override suspend fun send(to: NodeId, rpc: RaftRpc) {
        val message = RaftMessage(
            from = id,
            to = to,
            rpc = rpc
        )
        cluster.send(message)
    }

    override fun addPeer(node: RaftRpc.ClusterNode) {
        cluster.createPeer(node.id)
    }
}