package io.r.raft.transport.inmemory

import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.RaftClusterNode
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel

class InMemoryRaftClusterNode(
    override val id: NodeId,
    private val _peers: Map<NodeId, Channel<RaftMessage>>,
) : RaftClusterNode {
    override val peers: Set<NodeId> get() = _peers.keys - id
    override val input: ReceiveChannel<RaftMessage>
        get() = _peers[id] ?: error("Self node not found in peers")

    override suspend fun send(to: NodeId, rpc: RaftRpc) {
        val message = RaftMessage(
            from = id,
            to = to,
            rpc = rpc
        )
        (_peers[to] ?: error("Node $to not found"))
            .send(message)
    }

}