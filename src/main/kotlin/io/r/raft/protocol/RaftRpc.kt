package io.r.raft.protocol

import kotlinx.serialization.Serializable

typealias Term = Long
typealias NodeId = String
typealias Index = Long

@Serializable
sealed interface RaftRpc {

    val term: Term

    fun describe(): String

    @Serializable
    data class RequestVote(
        override val term: Term,
        val candidateId: NodeId,
        val lastLog: LogEntryMetadata
    ) : RaftRpc {
        override fun describe(): String = "RequestVote(term=$term, candidateId=$candidateId, lastLog=${lastLog.index}:${lastLog.term})"
    }

    @Serializable
    data class RequestVoteResponse(
        override val term: Term,
        val voteGranted: Boolean
    ) : RaftRpc {
        override fun describe(): String = "RequestVoteResponse(term=$term, voteGranted=$voteGranted)"
    }

    @Serializable
    data class AppendEntries(
        override val term: Term,
        val leaderId: NodeId,
        val prevLog: LogEntryMetadata,
        val entries: List<LogEntry>,
        val leaderCommit: Long
    ) : RaftRpc {
        override fun describe(): String =
            "AppendEntries(term=$term, prev=${prevLog.index}:${prevLog.term}, e=${entries.size}, lc=$leaderCommit)"
    }

    @Serializable
    data class AppendEntriesResponse(
        override val term: Term,
        val matchIndex: Index,
        val success: Boolean,
        val entries: Int
    ) : RaftRpc {
        override fun describe(): String = "AppendEntriesResponse(term=$term, match=$matchIndex, s=$success, e=$entries)"
    }

    @Serializable
    data class ClusterNode(val id: NodeId, val host: String, val port: Int)
}
/**
 * Transform a string with the pattern "NodeId=localhost:8082" into a [RaftRpc.ClusterNode]
 */
fun String.toClusterNode(): RaftRpc.ClusterNode {
    val (id, address) = split("=")
    val (host, port) = address.split(":")
    return RaftRpc.ClusterNode(id, host, port.toInt())

}