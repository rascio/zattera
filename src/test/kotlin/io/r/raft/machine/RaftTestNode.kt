package io.r.raft.machine

import arrow.fx.coroutines.ResourceScope
import io.r.raft.log.RaftLog
import io.r.raft.log.StateMachine
import io.r.raft.log.inmemory.InMemoryRaftLog
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.LogEntryMetadata
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRole
import io.r.raft.transport.RaftCluster
import io.r.raft.transport.inmemory.RaftClusterInMemoryNetwork
import io.r.utils.decodeToString
import io.r.utils.logs.entry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import java.util.concurrent.atomic.AtomicReference

suspend fun ResourceScope.installRaftTestNode(
    nodeId: NodeId,
    cluster: RaftClusterInMemoryNetwork,
    configuration: RaftMachine.Configuration = RaftMachine.Configuration(),
    start: Boolean = true
) = install(
    acquire = {
        RaftTestNode(
            raftClusterInMemoryNetwork = cluster,
            nodeId = nodeId,
            configuration = configuration
        ).apply {
            cluster.createNode(nodeId)
            if (start) start()
        }
    },
    release = { n, _ -> n.stop() }
)
class RaftTestNode private constructor(
    private val raftClusterInMemoryNetwork: RaftClusterInMemoryNetwork,
    private val raftCluster: RaftCluster,
    val configuration: RaftMachine.Configuration,
    private var _log: RaftLog,
    private val scope: CoroutineScope
) {
    companion object {
        private val logger: Logger = LogManager.getLogger(RaftTestNode::class.java)
        suspend operator fun invoke(
            raftClusterInMemoryNetwork: RaftClusterInMemoryNetwork,
            nodeId: NodeId,
            configuration: RaftMachine.Configuration,
            scope: CoroutineScope? = null
        ) = RaftTestNode(
            raftClusterInMemoryNetwork = raftClusterInMemoryNetwork,
            raftCluster = RaftCluster(nodeId, raftClusterInMemoryNetwork),
            configuration = configuration,
            _log = InMemoryRaftLog(),
            scope = scope ?: CoroutineScope(Dispatchers.IO)
        )
    }

    private val _raftMachine = AtomicReference(
        newRaftMachine()
    )
    val commitIndex get() = _raftMachine.get().commitIndex
    val raftMachine: RaftMachine get() = _raftMachine.get()
    val log get() = _log
    val id: NodeId get() = raftCluster.id
    val roleChanges get() = raftMachine.role

    var stateMachineApplyDelayMs = 0L

    suspend fun isLeader(): Boolean = raftMachine.role.first() == RaftRole.LEADER
    suspend fun getCurrentTerm() = _log.getTerm()

    suspend fun getLastEntryMetadata(): LogEntryMetadata {
        return _log.getMetadata(_log.getLastIndex())!!
    }

    suspend fun send(message: RaftMessage) {
        raftCluster.send(message.to, message.rpc)
    }

    fun start() {
        raftMachine.start()
    }

    suspend fun stop() {
        raftMachine.stop()
    }

    fun disconnect() {
        raftClusterInMemoryNetwork.disconnect(id)
    }

    fun reconnect() {
        raftClusterInMemoryNetwork.reconnect(id)
    }

    private fun newRaftMachine() = RaftMachine(
        input = raftClusterInMemoryNetwork.channel(id),
        configuration = configuration,
        log = _log,
        cluster = raftCluster,
        stateMachine = object : StateMachine {
            var applied = 0L
            override suspend fun apply(command: LogEntry) {
                logger.info(entry("apply", "command" to command.entry.decodeToString(), "_node" to id))
                delay(stateMachineApplyDelayMs)
                applied++
            }

        },
        scope = scope
    )
}