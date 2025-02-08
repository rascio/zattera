package io.r.raft.protocol

import io.r.raft.transport.serialization.ByteArrayBase64Serializer
import io.r.toHex
import io.r.utils.encodeBase64
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.apache.commons.codec.digest.MurmurHash3
import java.util.UUID

@Serializable
data class LogEntry(
    val term: Term,
    val entry: Entry,
    val id: String
) {

    override fun toString(): String = "LogEntry(id=${id}, term=$term, c=${entry})"

    @Serializable
    sealed interface Entry
    @Serializable
    class ClientCommand(
        @Serializable(with = ByteArrayBase64Serializer::class)
        val bytes: ByteArray
    ) : Entry {

        private val hash by lazy { MurmurHash3.hash128x64(bytes).toHex() }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is ClientCommand) return false

            return bytes.contentEquals(other.bytes)
        }

        override fun toString(): String {
            return "ClientCommand($hash|${bytes.encodeBase64()})"
        }

        override fun hashCode(): Int {
            return bytes.contentHashCode()
        }
    }
    @Serializable
    data class ConfigurationChange(
        val new: List<RaftRpc.ClusterNode>,
        val old: List<RaftRpc.ClusterNode>? = null
    ) : Entry

}

fun main() {
    val clientCommand = LogEntry.ClientCommand("Hello, World!".toByteArray())
    val string = Json.encodeToString(
        clientCommand as LogEntry.Entry
    )
    println(string)
    val decoded = Json.decodeFromString<LogEntry.Entry>(string)
    println(decoded)
    check(clientCommand == decoded)
}