package io.r.raft.log

import io.r.raft.Index
import io.r.raft.LogEntry

interface StateMachine {
    /**
     * Apply a log entry to the state machine and update the last applied index
     */
    suspend fun apply(command: LogEntry)
    suspend fun getLastApplied(): Index

    companion object {
        suspend fun StateMachine.apply(entries: List<LogEntry>) {
            entries.forEach { apply(it) }
        }
    }
}