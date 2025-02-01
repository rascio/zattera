package io.r.raft.protocol

import io.ktor.util.decodeBase64Bytes
import io.r.utils.encodeBase64
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encodeToString
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import java.util.UUID

@Serializable
data class LogEntry(
    val term: Term,
    val entry: Entry,
    val id: String = UUID.randomUUID().toString()
) {

    override fun toString(): String = "LogEntry(term=$term, c=${entry})"

    @Serializable
    sealed interface Entry
    @Serializable
    class ClientCommand(
        @Serializable(with = ByteArrayBase64Serializer::class)
        val bytes: ByteArray
    ) : Entry
    @Serializable
    data class ConfigurationChange(
        val new: List<RaftRpc.ClusterNode>,
        val old: List<RaftRpc.ClusterNode>? = null
    ) : Entry

    private object ByteArrayBase64Serializer : KSerializer<ByteArray> {
        override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor(
            "ClientCommandPayload", PrimitiveKind.STRING
        )

        override fun serialize(encoder: Encoder, value: ByteArray) {
            val base64String = value.encodeBase64()
            encoder.encodeString(base64String)
        }

        override fun deserialize(decoder: Decoder): ByteArray {
            val base64String = decoder.decodeString()
            return base64String.decodeBase64Bytes()
        }
    }
}

fun main() {
    val string = Json.encodeToString(
        LogEntry.ClientCommand("Hello, World!".toByteArray()) as LogEntry.Entry
    )
    println(
     string
    )
    println(Json.decodeFromString<LogEntry.Entry>(string))
}