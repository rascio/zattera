package io.r.raft.transport

import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.RaftMessage

interface RaftService {

    // RPC
    suspend fun send(message: RaftMessage)

    // Client
    suspend fun forward(entry: LogEntry.Entry): Any
}
