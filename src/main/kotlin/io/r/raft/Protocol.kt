package io.r.raft

import kotlinx.serialization.Serializable

typealias Term = Long
typealias NodeId = String
typealias Index = Long

enum class RaftRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
}

interface ServerState {
    val commitIndex: Index
    val lastApplied: Index
}
data class PeerState(
    val nextIndex: Index,
    val matchIndex: Index,
    val lastContactTime: Long = System.currentTimeMillis()
)

@Serializable
sealed interface RaftProtocol {

    fun describe(): String

    @Serializable
    data class RequestVote(
        val term: Term,
        val candidateId: NodeId,
        val lastLog: LogEntryMetadata
    ) : RaftProtocol {
        override fun describe(): String = "RequestVote(term=$term, candidateId=$candidateId, lastLog=$lastLog)"
    }

    @Serializable
    data class RequestVoteResponse(
        val term: Term,
        val voteGranted: Boolean
    ) : RaftProtocol {
        override fun describe(): String = "RequestVoteResponse(term=$term, voteGranted=$voteGranted)"
    }

    @Serializable
    data class AppendEntries(
        val term: Term,
        val leaderId: NodeId,
        val prevLog: LogEntryMetadata,
        val entries: List<LogEntry>,
        val leaderCommit: Long
    ) : RaftProtocol {
        override fun describe(): String =
            "AppendEntries(term=$term, leaderId=$leaderId, prevLog=$prevLog, entries=${entries.size}, leaderCommit=$leaderCommit)"
    }

    @Serializable
    data class AppendEntriesResponse(
        val term: Term,
        val matchIndex: Index,
        val success: Boolean,
        val entries: Int
    ) : RaftProtocol {
        override fun describe(): String = "AppendEntriesResponse(term=$term, matchIndex=$matchIndex, success=$success, entries=$entries)"
    }
}

@Serializable
data class LogEntry(val term: Term, val command: ByteArray) {

    override fun toString(): String = "LogEntry(term=$term, command=${command.encodeBase64()})"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as LogEntry

        if (term != other.term) return false
        if (!command.contentEquals(other.command)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = term.hashCode()
        result = 31 * result + command.contentHashCode()
        return result
    }
}

@Serializable
data class RaftMessage(
    val from: NodeId,
    val to: NodeId,
    val protocol: RaftProtocol
)