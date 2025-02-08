package io.r.raft.transport

import io.r.raft.machine.Response
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc

typealias Query = ByteArray
interface RaftService {

    val node: RaftRpc.ClusterNode
    val id get() = node.id

    // RPC
    suspend fun send(message: RaftMessage)

    // Client
    suspend fun request(entry: LogEntry.Entry): Response

    suspend fun query(query: Query): Response
}
