package io.r.raft.machine

import arrow.fx.coroutines.ResourceScope
import io.r.raft.log.RaftLog.Companion.getLastMetadata
import io.r.raft.protocol.NodeId
import io.r.utils.awaitility.await
import io.r.utils.awaitility.coUntilNotNull
import io.r.utils.awaitility.timeout
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class RaftTestCluster(val nodes: List<RaftTestNode>) {

    private val logger: Logger = LogManager.getLogger(RaftTestCluster::class.java)

    suspend fun append(vararg commands: String) = append(commands.toList())
    suspend fun append(commands: List<String>): Unit = coroutineScope {
        awaitLeaderElected()
            .let { leader ->
                try {
                    withTimeoutOrNull(1000) {
                        leader.raftMachine.command(commands.map { it.encodeToByteArray() })
                            .join()
                    }
                } catch (e: Exception) {
                    when (e.message) {
                        "Role has changed" -> null
                        "Only leader can send commands" -> null
                        else -> throw e
                    }
                }
            } ?: run {
            delay(30)
            append(commands)
        }
    }

    suspend fun reboot() {
        nodes.forEach { it.reboot { } }
        logger.info("Cluster_rebooted")
    }

    /**
     * Wait until the cluster has a consistent state, i.e., all nodes have the same commit index
     */
    fun awaitLogConvergence(timeout: Duration = 3.seconds) =
        "await_log_convergence".await.timeout(timeout) coUntilNotNull  {
            nodes.map { it.commitIndex }
                .toSet()
                .takeIf { it.size == 1 }
                ?.first()
        }

    /**
     * Wait until a leader is elected
     */
    fun awaitLeaderElected(timeout: Duration = 3.seconds) =
        "await_leader_election".await.timeout(timeout) coUntilNotNull {
            nodes.filter { it.isLeader }
                .groupBy { it.log.getTerm() }
                .takeIf { it.size == 1 }
                ?.values
                ?.first()
                ?.takeIf { it.size == 1 }
                ?.first()
        }

    fun awaitDifferentLeaderElected(initialLeader: NodeId, timeout: Duration = 3.seconds) =
        "await_leader_change".await.timeout(timeout) coUntilNotNull {
            nodes.filter { it.id != initialLeader }
                .filter { it.isLeader }
                .groupBy { it.log.getTerm() }
                .takeIf { it.size == 1 }
                ?.values
                ?.first()
                ?.takeIf { it.size == 1 }
                ?.first()
        }
}

suspend fun ResourceScope.installRaftTestCluster(
    scope: CoroutineScope,
    network: RaftClusterInMemoryNetwork,
    nodeIds: List<NodeId>,
    config: (NodeId) -> RaftMachine.Configuration
): RaftTestCluster {
    val nodes = nodeIds.map { id ->
        install({
            RaftTestNode(
                raftClusterInMemoryNetwork = network,
                nodeId = id,
                configuration = config(id),
                scope = scope
            ).apply { start() }
        }) { node, _ -> node.stop() }
    }
    return RaftTestCluster(nodes)
}