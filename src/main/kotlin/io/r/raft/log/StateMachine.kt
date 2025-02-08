package io.r.raft.log

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable

interface StateMachine<Cmd: StateMachine.Command> {

    val commandSerializer: KSerializer<Cmd>

    /**
     * Apply a log entry to the state machine
     */
    suspend fun apply(message: Message<Cmd>): ByteArray
    suspend fun read(query: ByteArray): ByteArray = TODO()

    @Serializable
    class Message<Payload>(
        val clientId: String,
        val sequence: Long,
        val payload: Payload
    )
    interface Command

    companion object {
        fun <Cmd : Command> StateMachine<Cmd>.commandMessageDeserializer(): KSerializer<Message<Cmd>> {
            return Message.serializer(commandSerializer)
        }
    }
}
