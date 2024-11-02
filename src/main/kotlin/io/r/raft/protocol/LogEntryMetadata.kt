package io.r.raft.protocol

import kotlinx.serialization.Serializable

@Serializable
data class LogEntryMetadata(val index: Index = 0, val term: Term = 0) {
    companion object {
        val ZERO = LogEntryMetadata()
    }
    operator fun compareTo(compare: LogEntryMetadata?): Int {
        val other = compare ?: LogEntryMetadata()
        return when {
            term == other.term -> index.compareTo(other.index)
            else -> term.compareTo(other.term)
        }
    }

    override fun toString(): String = "(index=$index, term=$term)"
}