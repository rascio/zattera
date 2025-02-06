package io.r.raft.log

import io.r.raft.protocol.LogEntry

interface StateMachine {
    /**
     * Apply a log entry to the state machine
     */
    suspend fun apply(command: LogEntry): ByteArray
    suspend fun read(query: ByteArray): ByteArray = TODO()

    companion object {
        suspend fun StateMachine.apply(entries: List<LogEntry>) =
            entries.map { apply(it) }
    }

    interface Command
    interface Query
}