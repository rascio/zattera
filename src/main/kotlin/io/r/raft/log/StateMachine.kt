package io.r.raft.log

import io.r.raft.protocol.LogEntry

interface StateMachine {
    /**
     * Apply a log entry to the state machine
     */
    suspend fun apply(command: LogEntry): ByteArray

    companion object {
        suspend fun StateMachine.apply(entries: List<LogEntry>) =
            entries.map { apply(it) }
    }
}