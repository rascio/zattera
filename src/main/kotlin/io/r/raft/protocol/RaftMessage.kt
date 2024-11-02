package io.r.raft.protocol

import kotlinx.serialization.Serializable

@Serializable
data class RaftMessage(
    val from: NodeId,
    val to: NodeId,
    val rpc: RaftRpc
)