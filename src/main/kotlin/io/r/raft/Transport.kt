package io.r.raft

import io.r.utils.arrow.Timeout
import io.r.utils.arrow.timeout

interface Transport<NodeRef> {

    suspend fun receive(): RaftMessage
    suspend fun send(node: NodeRef, message: RaftMessage)

    suspend fun receive(timeout: Timeout): Result<RaftMessage> = timeout(timeout) {
        receive()
    }
}

