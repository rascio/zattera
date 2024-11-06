package io.r.raft.protocol

import io.r.utils.encodeBase64
import kotlinx.serialization.Serializable
import java.util.UUID

@Serializable
data class LogEntry(
    val term: Term,
    val entry: ByteArray,
    val id: String = UUID.randomUUID().toString()
) {

    override fun toString(): String = "LogEntry(term=$term, c=${entry.encodeBase64()})"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as LogEntry

        if (term != other.term) return false
        if (!entry.contentEquals(other.entry)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = term.hashCode()
        result = 31 * result + entry.contentHashCode()
        return result
    }
}