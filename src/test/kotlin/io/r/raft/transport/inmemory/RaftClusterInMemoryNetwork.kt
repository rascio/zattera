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

    class MockedNode(
        val id: NodeId,
        val channel: Channel<RaftMessage>,
        val network: RaftClusterInMemoryNetwork
    ) {
        suspend fun send(to: NodeId, rpc: RaftRpc) {
            channel.send(RaftMessage(id, to, rpc))
        }
    }
    fun createCluster(name: NodeId, logging: Boolean = true): RaftCluster {
        createPeer(name)
        val clusterNode: RaftCluster = InMemoryRaftClusterNode(name, this)

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
                        if (logging) {
                            logger.info("$name == ${rpc.describe()} ==> $to")
                        }
                        clusterNode.send(to, rpc)
                    }
                }
            }
        }
    }
    fun createNode(name: NodeId): MockedNode {
        return MockedNode(name, createPeer(name), this)
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