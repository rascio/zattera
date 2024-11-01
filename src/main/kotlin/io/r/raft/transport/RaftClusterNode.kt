package io.r.raft.transport

import io.r.raft.NodeId
import io.r.raft.RaftMessage
import io.r.raft.RaftProtocol

interface RaftClusterNode {
    val id: NodeId
    val peers: Set<NodeId>
//    val input: Channel<RaftMessage>
    suspend fun send(node: NodeId, rpc: RaftProtocol)
    suspend fun receive(): RaftMessage
//    val onReceive: SelectClause1<RaftMessage>
//    val input: Flow<RaftMessage>

}