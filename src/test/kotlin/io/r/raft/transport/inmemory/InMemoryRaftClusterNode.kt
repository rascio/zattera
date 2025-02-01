package io.r.raft.transport.inmemory

import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.RaftCluster
import kotlinx.coroutines.channels.ReceiveChannel

class InMemoryRaftClusterNode(
    override val id: NodeId,
    private val network: RaftClusterInMemoryNetwork,
) : RaftCluster {
    override val peers: Set<NodeId> get() = network.nodes - id

    override suspend fun send(to: NodeId, rpc: RaftRpc) {
        val message = RaftMessage(
            from = id,
            to = to,
            rpc = rpc
        )
        network.send(message)
    }

    override suspend fun forward(to: NodeId, entry: LogEntry.Entry): Any {
        TODO("Not yet implemented")
    }

    override fun changeConfiguration(entry: LogEntry.ConfigurationChange) {
        TODO("Not yet implemented")
    }
}