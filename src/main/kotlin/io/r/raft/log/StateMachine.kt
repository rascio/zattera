package io.r.raft.log

import kotlinx.serialization.KSerializer

interface StateMachine<Cmd: StateMachine.Command, Query: StateMachine.Query, Resp : StateMachine.Response> {

    val contract: Contract<Cmd, Query, Resp>
    /**
     * Apply a command to the state machine
     */
    suspend fun apply(message: Cmd): Resp

    /**
     * Read from the state machine
     */
    suspend fun read(query: Query): Resp


    /**
     * Contract for the state machine
     * Holds the serializers for the command, query and response
     */
    interface Contract<C: Command, Q: Query, Resp : Response> {
        val commandKSerializer: KSerializer<C>
        val queryKSerializer: KSerializer<Q>
        val responseKSerializer: KSerializer<Resp>
    }

    /**
     * Marker interface for commands
     * Commands are stored in the log and applied to the state machine
     */
    interface Command

    /**
     * Marker interface for queries
     * Queries are not stored in the log and are used to read the state machine
     */
    interface Query

    /**
     * Marker interface for responses
     * Responses are returned by the state machine
     */
    interface Response
}