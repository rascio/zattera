package io.r.raft.machine

import io.r.raft.protocol.Index
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRole
import io.r.raft.log.RaftLog
import io.r.raft.transport.RaftClusterNode

typealias RoleTransition = suspend (RaftRole) -> Role
data class ServerState(var commitIndex: Index, var lastApplied: Index)
sealed class Role {
    abstract val serverState: ServerState
    protected abstract val log: RaftLog
    protected abstract val clusterNode: RaftClusterNode
    protected abstract val changeRole: RoleTransition

    open suspend fun onEnter() { }
    open suspend fun onExit() { }
    open suspend fun onReceivedMessage(message: RaftMessage) { }
    open suspend fun onTimeout() { }
}
