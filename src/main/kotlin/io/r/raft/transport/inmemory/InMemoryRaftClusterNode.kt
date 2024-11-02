package io.r.raft.transport.inmemory

import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.RaftClusterNode
import kotlinx.coroutines.channels.Channel

class InMemoryRaftClusterNode(
    override val id: NodeId,
    private val _peers: Map<NodeId, Channel<RaftMessage>>,
) : RaftClusterNode {
    override val peers: Set<NodeId> get() = _peers.keys - id

    override suspend fun send(node: NodeId, rpc: RaftRpc) {
        val message = RaftMessage(
            from = id,
            to = node,
            rpc = rpc
        )
        (_peers[node] ?: error("Node $node not found"))
            .send(message)
    }

    override suspend fun receive(): RaftMessage {
        return (_peers[id] ?: error("Self node not found in peers"))
            .receive()
    }
}