package io.r.raft.log

import kotlinx.serialization.KSerializer

interface StateMachine<Cmd: StateMachine.Command, Query: StateMachine.Query> {

    val contract: Contract<Cmd, Query>
    /**
     * Apply a log entry to the state machine
     */
    suspend fun apply(message: Cmd): ByteArray
    suspend fun read(query: Query): ByteArray = TODO()


    interface Contract<C: Command, Q: Query> {
        val commandKSerializer: KSerializer<C>
        val queryKSerializer: KSerializer<Q>
    }
    interface Command
    interface Query
}
