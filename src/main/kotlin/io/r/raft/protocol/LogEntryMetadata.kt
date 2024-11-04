package io.r.raft.protocol

import kotlinx.serialization.Serializable

@Serializable
data class LogEntryMetadata(val index: Index = 0, val term: Term = 0) {
    init {
        require(index >= 0) { "Index must be greater than or equal to 0" }
        require(term >= 0) { "Term must be greater than or equal to 0" }
    }
    operator fun compareTo(other: LogEntryMetadata): Int {
        return when {
            term == other.term -> index.compareTo(other.index)
            else -> term.compareTo(other.term)
        }
    }

    override fun toString(): String = "(index=$index, term=$term)"

    companion object {
        val ZERO = LogEntryMetadata()
    }
}