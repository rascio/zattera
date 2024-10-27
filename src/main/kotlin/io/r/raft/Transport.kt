package io.r.raft

import io.r.utils.timeout.Timeout
import io.r.utils.timeout.timeout

interface Transport<NodeRef> {

    suspend fun receive(): RaftMessage
    suspend fun send(node: NodeRef, message: RaftMessage)

    suspend fun receive(timeout: Timeout): Result<RaftMessage> = timeout(timeout) {
        receive()
    }
}

