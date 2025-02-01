package io.r.raft.transport

import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.channels.ReceiveChannel

/**
 * Represents a node in the Raft cluster.
 * Offers a way to send messages to other nodes in the cluster and receive messages from them.
 */
interface RaftCluster {
    /**
     * The unique identifier of the node.
     */
    val id: NodeId

    /**
     * The set of nodes in the cluster, excluding the current node.
     */
    val peers: Set<NodeId>

    suspend fun send(to: NodeId, rpc: RaftRpc)
    suspend fun forward(to: NodeId, entry: LogEntry.Entry): Any

    fun changeConfiguration(entry: LogEntry.ConfigurationChange)

    companion object {
        val RaftCluster.quorum: Int
            get() = peers.size / 2 + 1
    }
}