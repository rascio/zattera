package io.r.raft.transport.inmemory

import arrow.fx.coroutines.ResourceScope
import arrow.fx.coroutines.autoCloseable
import io.r.raft.machine.RaftMachine
import io.r.raft.machine.Response
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.RaftCluster
import io.r.raft.transport.RaftService
import io.r.utils.logs.entry
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withTimeout
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

suspend fun ResourceScope.installRaftClusterNetwork(vararg nodeIds: NodeId) =
    autoCloseable { RaftClusterTestNetwork(*nodeIds) }

class RaftClusterTestNetwork(
    vararg nodeIds: NodeId
) : RaftCluster.RaftPeers, AutoCloseable {
    private val isolatedNodes = mutableSetOf<NodeId>()
    private val channels = mutableMapOf<NodeId, Channel<RaftMessage>>().apply {
        nodeIds.forEach { id -> put(id, Channel(Channel.UNLIMITED)) }
    }
    private val _peers = mutableMapOf<NodeId, InMemoryRaftClusterNode>()
    private val _raftMachines = mutableMapOf<NodeId, RaftMachine<*>>()

    // This method should be private, but it is used in InMemoryRaftClusterNode
    // Procrastination: I will do it later (read: never)
    fun channel(nodeId: NodeId): Channel<RaftMessage> {
        return channels.computeIfAbsent(nodeId) { Channel(Channel.UNLIMITED) }
    }

    suspend fun send(message: RaftMessage) {
        val node = requireNotNull(channels[message.to]) {
            "Node ${message.to} not found"
        }
        when {
            message.to in isolatedNodes -> {
                logger.debug { entry("isolated_node", "node" to message.to, "rpc" to message.rpc::class.simpleName) }
            }

            message.from in isolatedNodes -> {
                logger.debug { entry("isolated_node", "node" to message.from, "rpc" to message.rpc::class.simpleName) }
            }

            else -> {
                logger.info("${message.from} == ${message.rpc.describe()} ==> ${message.to}")
                node.send(message)
            }
        }
    }

    suspend fun command(id: NodeId, entry: LogEntry.Entry): Response {
        val raftMachine = requireNotNull(_raftMachines[id]) {
            "Node $id not found"
        }
        return when {
            id in isolatedNodes -> {
                logger.info(entry("isolated_node", "node" to id, "entry" to entry))
                error("Node $id is isolated")
            }

            else -> {
                logger.info("-- !$entry --> $id")
                raftMachine.command(entry)
            }
        }
    }

    suspend fun query(id: NodeId, query: ByteArray): Response {
        val raftMachine = requireNotNull(_raftMachines[id]) {
            "Node $id not found"
        }
        return when {
            id in isolatedNodes -> {
                logger.info(entry("isolated_node", "node" to id, "query" to query))
                error("Node $id is isolated")
            }

            else -> {
                logger.info("-- ?$query --> $id")
                raftMachine.query(query)
            }
        }
    }

    fun createNode(name: NodeId, raftMachine: RaftMachine<*>? = null): InMemoryRaftClusterNode {
        channels.computeIfAbsent(name) { Channel(Channel.UNLIMITED) }
        if (raftMachine != null) {
            _raftMachines[name] = raftMachine
        }
        val node = RaftRpc.ClusterNode(name, "localhost", 0)
        return _peers.computeIfAbsent(name) { InMemoryRaftClusterNode(node, this) }.also {
            logger.info(entry("create_node", "node" to name))
        }
    }

    fun disconnect(vararg nodeIds: NodeId) {
        logger.info(entry("disconnect", "nodes" to nodeIds.joinToString()))
        isolatedNodes.addAll(nodeIds)
    }

    fun reconnect(vararg nodeIds: NodeId) {
        logger.info(entry("reconnect", "nodes" to nodeIds.joinToString()))
        isolatedNodes.removeAll(nodeIds.toSet())
    }

    override fun close() {
        channels.values.forEach { it.close() }
    }

    override val ids: Set<NodeId> get() = channels.keys

    override suspend fun disconnect(node: RaftRpc.ClusterNode) {
        _peers.remove(node.id)
        channels.remove(node.id)?.close()
    }

    override fun contains(nodeId: NodeId): Boolean =
        nodeId in channels

    override fun get(nodeId: NodeId): RaftService? =
        _peers[nodeId]

    override suspend fun connect(node: RaftRpc.ClusterNode) {
        channel(node.id)
    }

    companion object {
        private val logger: Logger = LogManager.getLogger(RaftClusterTestNetwork::class.java)
    }
}