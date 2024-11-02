package io.r.raft.machine

import io.r.raft.protocol.Index
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.protocol.RaftRole
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

    suspend fun NodeId.send(message: RaftRpc) {
        clusterNode.send(this, message)
    }
}
