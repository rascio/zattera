package io.r.raft.transport.inmemory

import arrow.fx.coroutines.ResourceScope
import arrow.fx.coroutines.autoCloseable
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.RaftCluster
import io.r.raft.transport.utils.LoggingRaftClusterNode
import io.r.utils.logs.entry
import kotlinx.coroutines.channels.Channel
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

suspend fun ResourceScope.installRaftClusterNetwork(vararg nodeIds: NodeId) =
    autoCloseable { RaftClusterInMemoryNetwork(this@installRaftClusterNetwork, *nodeIds) }

class RaftClusterInMemoryNetwork(private val scope: ResourceScope, vararg nodeIds: NodeId) : AutoCloseable {
    private val isolatedNodes = mutableSetOf<NodeId>()
    private val _nodes = mutableMapOf<NodeId, Channel<RaftMessage>>().apply {
        nodeIds.forEach { id -> put(id, Channel(Channel.UNLIMITED)) }
    }
    val nodes get() = _nodes.keys

    fun createPeer(nodeId: NodeId): Channel<RaftMessage> {
        return _nodes.computeIfAbsent(nodeId) { Channel(Channel.UNLIMITED) }
    }

    suspend fun send(message: RaftMessage) {
        val node = requireNotNull(_nodes[message.to]) {
            "Node ${message.to} not found"
        }
        node.send(message)
    }

    suspend fun createNode(name: NodeId, logging: Boolean = true): RaftCluster {
        createPeer(name)
        var clusterNode: RaftCluster = InMemoryRaftClusterNode(name, this)

        if (logging) {
            clusterNode = scope.autoCloseable { LoggingRaftClusterNode(clusterNode) }
        }
        return object : RaftCluster by clusterNode {
            override suspend fun send(to: NodeId, rpc: RaftRpc) {
                when {
                    to in isolatedNodes -> {
                        logger.info(entry("isolated_node", "node" to to, "rpc" to rpc::class.simpleName))
                    }

                    clusterNode.id in isolatedNodes -> {
                        logger.info(entry("isolated_node", "node" to clusterNode.id, "rpc" to rpc::class.simpleName))
                    }

                    else -> {
                        clusterNode.send(to, rpc)
                    }
                }
            }
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
        _nodes.values.forEach { it.close() }
    }

    companion object {
        private val logger: Logger = LogManager.getLogger(RaftClusterInMemoryNetwork::class.java)
    }
}