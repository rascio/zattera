package io.r.raft.transport.serialization

import io.ktor.util.decodeBase64Bytes
import io.r.utils.encodeBase64
import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

object ByteArrayBase64Serializer : KSerializer<ByteArray> {
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

object UUIDSerializer : KSerializer<java.util.UUID> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor(
        "UUID", PrimitiveKind.STRING
    )

    override fun serialize(encoder: Encoder, value: java.util.UUID) {
        encoder.encodeString(value.toString())
    }

    override fun deserialize(decoder: Decoder): java.util.UUID {
        return java.util.UUID.fromString(decoder.decodeString())
    }
}