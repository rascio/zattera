package io.r.raft.machine

import io.r.raft.persistence.RaftLog
import io.r.raft.protocol.Index
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRole
import io.r.raft.transport.RaftCluster

typealias RoleTransition = suspend (RaftRole) -> Role

data class ServerState(
    var commitIndex: Index,
    var lastApplied: Index,
    var currentLeader: NodeId? = null
)

sealed class Role {
    abstract val serverState: ServerState
    abstract val timeout: Long

    protected abstract val log: RaftLog
    protected abstract val cluster: RaftCluster
    protected abstract val transitionTo: RoleTransition

    open suspend fun onEnter() { }
    open suspend fun onExit() { }
    open suspend fun onReceivedMessage(message: RaftMessage) { }
    open suspend fun onTimeout() { }
}
