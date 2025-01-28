package io.r.raft.transport

import io.r.raft.protocol.RaftRpc
import kotlinx.coroutines.Deferred

interface RaftClient {

    suspend fun exec(value: ByteArray): Deferred<Any>
    suspend fun addPeer(node: RaftRpc.ClusterNode)
}
