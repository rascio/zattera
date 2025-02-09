package io.r.raft.protocol

import io.r.raft.transport.serialization.ByteArrayBase64Serializer
import io.r.raft.transport.serialization.UUIDSerializer
import io.r.utils.murmur128
import io.r.utils.toHex
import kotlinx.serialization.Serializable
import java.nio.ByteBuffer
import java.util.UUID

@Serializable
data class LogEntry(
    val term: Term,
    val entry: Entry,
    val id: String
) {

    override fun toString(): String = "LogEntry(id=${id}, term=$term, c=${entry})"

    @Serializable
    sealed interface Entry {
        val id: String
    }
    @Serializable
    class ClientCommand(
        @Serializable(with = ByteArrayBase64Serializer::class)
        val bytes: ByteArray,
        @Serializable(with = UUIDSerializer::class)
        val clientId: UUID,
        val sequence: Long
    ) : Entry {

        override val id by lazy {
            ByteBuffer.allocate(bytes.size + (Long.SIZE_BYTES * 3))
                .putLong(sequence)
                .putLong(clientId.leastSignificantBits)
                .putLong(clientId.mostSignificantBits)
                .put(bytes)
                .array()
                .murmur128()
                .toHex()
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is ClientCommand) return false

            return bytes.contentEquals(other.bytes)
        }

        override fun toString(): String {
            return "ClientCommand($id)"
        }

        override fun hashCode(): Int {
            return bytes.contentHashCode()
        }
    }
    @Serializable
    data class ConfigurationChange(
        val new: List<RaftRpc.ClusterNode>,
        val old: List<RaftRpc.ClusterNode>? = null,
        override val id: String = UUID.randomUUID().toString()
    ) : Entry
    @Serializable
    data object NoOp : Entry {
        override val id: String = "NoOp"
    }

}
