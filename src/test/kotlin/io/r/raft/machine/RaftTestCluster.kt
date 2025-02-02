package io.r.raft.machine

import arrow.fx.coroutines.ResourceScope
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.NodeId
import io.r.raft.transport.inmemory.RaftClusterTestNetwork
import io.r.utils.awaitility.atMost
import io.r.utils.awaitility.untilNotNull
import io.r.utils.logs.entry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class RaftTestCluster(nodes: List<RaftTestNode>) {

    val nodes = mutableListOf(*nodes.toTypedArray())

    private val logger: Logger = LogManager.getLogger(RaftTestCluster::class.java)

    suspend fun append(vararg commands: String) = append(commands.toList())
    suspend fun append(commands: List<String>): Unit = coroutineScope {
        awaitFindLeader()
            .let { leader ->
                try {
                    withTimeoutOrNull(1000) {
                        commands.map { LogEntry.ClientCommand(it.encodeToByteArray()) }
                            .forEach { leader.raftMachine.request(it) }
                    }
                } catch (e: Exception) {
                    when (e.message) {
                        "Role has changed" -> null
                        "Only leader can send commands" -> null
                        else -> throw e
                    }
                }
            }
            ?: run {
                delay(30)
                append(commands)
            }
    }

    suspend fun ResourceScope.addRaftTestNode(
        nodeId: NodeId,
        cluster: RaftClusterTestNetwork,
        configuration: RaftMachine.Configuration = RaftMachine.Configuration(),
        start: Boolean = true
    ) = installRaftTestNode(nodeId, cluster, configuration, start).also {
        nodes.add(it)
    }

    /**
     * Wait until the cluster has a consistent state, i.e., all nodes have the same commit index
     */
    suspend fun awaitLogConvergence(timeout: Duration = 3.seconds) {
        withContext(Dispatchers.IO) {
            logger.info("waiting_for_log_convergence")
            "await_log_convergence" atMost timeout untilNotNull {
                nodes.map { it.commitIndex }
                    .toSet()
                    .takeIf { it.size == 1 }
                    ?.first()
            }
            logger.info("log_converged")
        }
    }

    /**
     * Wait until a leader is elected
     */
    suspend fun awaitFindLeader(timeout: Duration = 3.seconds): RaftTestNode {
        logger.info("await_find_leader")
        val leader = "await_find_leader" atMost timeout untilNotNull {
            nodes.filter { it.isLeader() }
                .toSet()
                .takeIf { it.size == 1 }
                ?.first()
        }
        logger.info(entry("Leader_found", "leader" to leader.id))
        return leader
    }

    suspend fun awaitDifferentLeaderElected(initialLeader: NodeId, timeout: Duration = 3.seconds): RaftTestNode {
        logger.info("waiting_for_leader_change")
        val leader = "await_leader_change" atMost timeout untilNotNull {
            nodes.filter { it.id != initialLeader }
                .filter { it.isLeader() }
                .toSet()
                .takeIf { it.size == 1 }
                ?.first()
        }
        logger.info(entry("Leader_changed", "leader" to leader.id))
        return leader
    }
}

suspend fun ResourceScope.installRaftTestCluster(
    scope: CoroutineScope,
    network: RaftClusterTestNetwork,
    nodeIds: List<NodeId>,
    config: (NodeId) -> RaftMachine.Configuration
): RaftTestCluster {
    val nodes = nodeIds.map { id ->
        install({
            RaftTestNode(
                raftClusterTestNetwork = network,
                nodeId = id,
                configuration = config(id),
                scope = scope
            ).apply {
                network.createNode(id, raftMachine)
                start()
            }
        }) { node, _ -> node.stop() }
    }
    return RaftTestCluster(nodes)
}