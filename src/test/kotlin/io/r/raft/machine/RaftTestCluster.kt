package io.r.raft.machine

import arrow.fx.coroutines.ResourceScope
import io.r.raft.log.StateMachine
import io.r.raft.protocol.NodeId
import io.r.raft.test.installCoroutine
import io.r.raft.transport.inmemory.RaftClusterTestNetwork
import io.r.utils.awaitility.atMost
import io.r.utils.awaitility.untilNotNull
import io.r.utils.decodeToString
import io.r.utils.logs.entry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.apache.logging.log4j.kotlin.loggingContext
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

class RaftTestCluster<C : StateMachine.Command, Q : StateMachine.Query>(
    nodes: List<RaftTestNode<C, Q>>,
    private val stateMachineFactory: () -> StateMachine<C, Q>
) {

    val nodes: MutableList<RaftTestNode<C, Q>> = mutableListOf(*nodes.toTypedArray())

    private val logger: Logger = LogManager.getLogger(RaftTestCluster::class.java)

    suspend fun ResourceScope.addRaftTestNode(
        nodeId: NodeId,
        cluster: RaftClusterTestNetwork,
        configuration: RaftMachine.Configuration = RaftMachine.Configuration(),
        start: Boolean = true
    ) = installRaftTestNode(nodeId, cluster, configuration, start, stateMachineFactory)
        .also { nodes.add(it) }

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
    suspend fun awaitFindLeader(timeout: Duration = 3.seconds): RaftTestNode<C, Q> {
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

    suspend fun awaitDifferentLeaderElected(initialLeader: NodeId, timeout: Duration = 3.seconds): RaftTestNode<C, Q> {
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

    suspend fun dumpRaftLogs(decode: Boolean = false) {
        nodes.forEach { node ->
            node.log.getEntries(1, Int.MAX_VALUE)
                .mapIndexed { index, it -> if (decode) "${index + 1}|${it.term}|${it.id}|${it.entry.decodeToString()}" else it.toString() }
                .joinToString("\n")
                .let { logger.info("Dump_log node=${node.id}\n$it\nEnd_Dump") }
        }
    }

    companion object {

        suspend fun RaftTestCluster<TestCmd, TestQuery>.append(vararg commands: String) = append(commands.toList())
        suspend fun RaftTestCluster<TestCmd, TestQuery>.append(commands: List<String>): Unit = coroutineScope {
            val clientId = UUID.randomUUID()
            awaitFindLeader()
                .let { leader ->
                    try {
                        withTimeoutOrNull(1000) {
                            commands.forEach { leader.raftMachine.command(it.toTestCommand(clientId)) }
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
    }
}

suspend fun <C : StateMachine.Command, Q : StateMachine.Query> ResourceScope.installRaftTestCluster(
    network: RaftClusterTestNetwork,
    nodeIds: List<NodeId>,
    config: (NodeId) -> RaftMachine.Configuration,
    stateMachineFactory: () -> StateMachine<C, Q>
): RaftTestCluster<C, Q> {
    val nodes = nodeIds.map { id ->
        val scope = installCoroutine(loggingContext(mapOf("NodeId" to id)))
        install({
            RaftTestNode(
                raftClusterTestNetwork = network,
                nodeId = id,
                configuration = config(id),
                scope = scope,
                stateMachine = stateMachineFactory()
            ).apply {
                network.createNode(id, raftMachine)
                start()
            }
        }) { node, _ -> node.stop() }
    }
    return RaftTestCluster(nodes, stateMachineFactory)
}
suspend fun ResourceScope.installRaftTestCluster(
    network: RaftClusterTestNetwork,
    nodeIds: List<NodeId>,
    config: (NodeId) -> RaftMachine.Configuration,
) = installRaftTestCluster(network, nodeIds, config) { TestingStateMachine() }