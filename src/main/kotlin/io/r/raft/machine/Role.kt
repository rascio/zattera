package io.r.raft.machine

import io.r.raft.Index
import io.r.raft.NodeId
import io.r.raft.RaftMessage
import io.r.raft.RaftProtocol
import io.r.raft.RaftRole
import io.r.raft.log.RaftLog
import io.r.raft.transport.RaftClusterNode

typealias RoleTransition = suspend (RaftRole) -> Role
sealed class Role {
    abstract var commitIndex: Index
    protected abstract val log: RaftLog
    protected abstract val clusterNode: RaftClusterNode
    protected abstract val changeRole: RoleTransition

    open suspend fun onEnter() { }
    open suspend fun onExit() { }
    open suspend fun onReceivedMessage(message: RaftMessage) { }
    open suspend fun onTimeout() { }

    suspend fun NodeId.send(message: RaftProtocol) {
        clusterNode.send(this, message)
    }
}
