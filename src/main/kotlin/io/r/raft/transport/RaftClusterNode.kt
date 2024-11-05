package io.r.raft.transport

import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import kotlinx.coroutines.channels.ReceiveChannel

/**
 * Represents a node in the Raft cluster.
 * Offers a way to send messages to other nodes in the cluster and receive messages from them.
 */
interface RaftClusterNode {
    /**
     * The unique identifier of the node.
     */
    val id: NodeId

    /**
     * The set of nodes in the cluster, excluding the current node.
     */
    val peers: Set<NodeId>
    val input: ReceiveChannel<RaftMessage>
    suspend fun send(to: NodeId, rpc: RaftRpc)
    //    val onReceive: SelectClause1<RaftMessage>
//    val input: Flow<RaftMessage>

    companion object {
        val RaftClusterNode.quorum: Int
            get() = peers.size / 2 + 1
    }
}