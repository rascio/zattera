package io.r.raft.machine

import io.mockk.mockk
import io.r.raft.RaftRole
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel

data class RoleTransitionMockContext(
    val changeRoleFn: RoleTransition,
    val probe: ReceiveChannel<RaftRole>,
    val mock: Role
)
fun mockRoleTransition(): RoleTransitionMockContext {
    val role = mockk<Role>()
    val raftRole = Channel<RaftRole>(capacity = Channel.UNLIMITED)
    val changeRole: RoleTransition = {
        raftRole.send(it)
        role
    }
    return RoleTransitionMockContext(changeRole, raftRole, role)
}