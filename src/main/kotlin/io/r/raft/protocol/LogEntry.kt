package io.r.raft.protocol

import kotlinx.serialization.Serializable
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
    class ClientCommand(val bytes: ByteArray) : Entry
    @Serializable
    data class ConfigurationChange(val new: List<RaftRpc.ClusterNode>, val old: List<RaftRpc.ClusterNode>? = null) : Entry {

    }

}
