package io.r.raft.machine

import arrow.fx.coroutines.ResourceScope
import io.r.raft.log.RaftLog
import io.r.raft.log.StateMachine
import io.r.raft.log.inmemory.InMemoryRaftLog
import io.r.raft.protocol.LogEntryMetadata
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftRole
import io.r.raft.transport.RaftCluster
import io.r.raft.transport.inmemory.RaftClusterTestNetwork
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.first

suspend fun ResourceScope.installRaftTestNode(
    nodeId: NodeId,
    cluster: RaftClusterTestNetwork,
    configuration: RaftMachine.Configuration = RaftMachine.Configuration(),
    start: Boolean = true
): RaftTestNode<TestCmd, TestQuery> = installRaftTestNode(
    nodeId = nodeId,
    cluster = cluster,
    configuration = configuration,
    start = start,
    stateMachineFactory = { TestingStateMachine() }
)
suspend fun <C: StateMachine.Command, Q : StateMachine.Query> ResourceScope.installRaftTestNode(
    nodeId: NodeId,
    cluster: RaftClusterTestNetwork,
    configuration: RaftMachine.Configuration = RaftMachine.Configuration(),
    start: Boolean = true,
    stateMachineFactory: () -> StateMachine<C, Q>
): RaftTestNode<C, Q> = install(
    acquire = {
        RaftTestNode(
            raftClusterTestNetwork = cluster,
            nodeId = nodeId,
            configuration = configuration,
            stateMachine = stateMachineFactory()
        ).apply {
            cluster.createNode(nodeId, raftMachine)
            if (start) start()
        }
    },
    release = { n, _ -> n.stop() }
)
class RaftTestNode<C : StateMachine.Command, Q : StateMachine.Query> private constructor(
    private val raftClusterTestNetwork: RaftClusterTestNetwork,
    private val raftCluster: RaftCluster,
    val configuration: RaftMachine.Configuration,
    private var _log: RaftLog,
    private val scope: CoroutineScope,
    stateMachine: StateMachine<C, Q>
) {
    companion object {
        operator fun <C : StateMachine.Command, Q : StateMachine.Query> invoke(
            raftClusterTestNetwork: RaftClusterTestNetwork,
            nodeId: NodeId,
            configuration: RaftMachine.Configuration,
            scope: CoroutineScope? = null,
            stateMachine: StateMachine<C, Q>
        ) = RaftTestNode(
            raftClusterTestNetwork = raftClusterTestNetwork,
            raftCluster = RaftCluster(nodeId, raftClusterTestNetwork),
            configuration = configuration,
            _log = InMemoryRaftLog(),
            scope = scope ?: CoroutineScope(Dispatchers.IO),
            stateMachine = stateMachine
        )
        operator fun invoke(
            raftClusterTestNetwork: RaftClusterTestNetwork,
            nodeId: NodeId,
            configuration: RaftMachine.Configuration,
            scope: CoroutineScope? = null,
        ) = RaftTestNode(
            raftClusterTestNetwork = raftClusterTestNetwork,
            raftCluster = RaftCluster(nodeId, raftClusterTestNetwork),
            configuration = configuration,
            _log = InMemoryRaftLog(),
            scope = scope ?: CoroutineScope(Dispatchers.IO),
            stateMachine = TestingStateMachine()
        )
    }
    val log get() = _log
    val raftMachine = RaftMachine(
        input = raftClusterTestNetwork.channel(id),
        configuration = configuration,
        log = _log,
        cluster = raftCluster,
        stateMachine = StateMachineAdapter(scope, stateMachine),
        scope = scope
    )
    val commitIndex get() = raftMachine.serverState.commitIndex
    val id: NodeId get() = raftCluster.id
    val roleChanges get() = raftMachine.role

    var stateMachineApplyDelayMs = 0L

    suspend fun isLeader(): Boolean = raftMachine.role.first() == RaftRole.LEADER
    suspend fun getCurrentTerm() = _log.getTerm()

    suspend fun getLastEntryMetadata(): LogEntryMetadata {
        return _log.getMetadata(_log.getLastIndex())!!
    }

    fun start() {
        raftMachine.start()
    }

    suspend fun stop() {
        raftMachine.stop()
    }

    fun disconnect() {
        raftClusterTestNetwork.disconnect(id)
    }

    fun reconnect() {
        raftClusterTestNetwork.reconnect(id)
    }
}

