package io.r.raft

import kotlinx.serialization.Serializable

interface Persistence {
    // Queries
    suspend fun getServerState(): ServerState
    suspend fun getCurrentTerm(): Term
    suspend fun getCommitIndex(): Index = getServerState().commitIndex
    suspend fun getLogMetadata(index: Index): LogEntryMetadata?
    suspend fun getLastEntryMetadata(): LogEntryMetadata
    suspend fun getLogs(from: Index, size: Int): List<LogEntry>
    suspend fun canVoteFor(nodeId: NodeId): Boolean
    // Commands
    suspend fun incrementTermAndVote(id: NodeId)
    suspend fun setTerm(term: Term)
    suspend fun voteFor(term: Term, nodeId: NodeId)

    sealed class AppendResult {
        data class Success(val index: Index) : AppendResult()
        object TermMismatch : AppendResult()
        object PreviousIndexNotFound : AppendResult()
        data class PreviousIndexMismatch(val expected: LogEntryMetadata, val actual: LogEntryMetadata) : AppendResult()
        data class BadInput(val reason: String) : AppendResult()
    }
    suspend fun append(term: Term, previous: LogEntryMetadata, entries: List<LogEntry>): AppendResult
    suspend fun commit(upTo: Index)
    suspend fun apply(upTo: Index)

    // Defaults
    suspend fun getLog(index: Index): LogEntry? =
        getLogs(index, 1).firstOrNull()
    suspend fun append(entries: List<LogEntry>): AppendResult =
        append(getCurrentTerm(), getLastEntryMetadata(), entries)
    suspend fun append(entry: LogEntry): AppendResult =
        append(listOf(entry))
    suspend fun isUpToDate(append: RaftProtocol.AppendEntries) =
        append.term >= getCurrentTerm()
            && append.prevLog >= getLastEntryMetadata()
    suspend fun isUpToDate(append: RaftProtocol.RequestVote) =
        append.lastLog >= getLastEntryMetadata()


}

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