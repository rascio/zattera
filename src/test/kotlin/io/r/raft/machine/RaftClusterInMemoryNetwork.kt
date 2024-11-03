package io.r.raft.machine

import arrow.fx.coroutines.ResourceScope
import arrow.fx.coroutines.autoCloseable
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.RaftClusterNode
import io.r.raft.transport.inmemory.InMemoryRaftClusterNode
import io.r.utils.logs.entry
import kotlinx.coroutines.channels.Channel
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

suspend fun ResourceScope.installRaftClusterNetwork(vararg nodeIds: NodeId) =
    autoCloseable { RaftClusterInMemoryNetwork(*nodeIds) }

class RaftClusterInMemoryNetwork(vararg nodeIds: NodeId) : AutoCloseable {
    private val isolatedNodes = mutableSetOf<NodeId>()
    private val nodes = mutableMapOf<NodeId, Channel<RaftMessage>>().apply {
        nodeIds.forEach { id -> put(id, Channel(Channel.UNLIMITED)) }
    }

    fun createNode(name: NodeId): RaftClusterNode {
        nodes.computeIfAbsent(name) { Channel(Channel.UNLIMITED) }
        val clusterNode = InMemoryRaftClusterNode(name, nodes)
        return object : RaftClusterNode by clusterNode {
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
        nodes.values.forEach { it.close() }
    }

    companion object {
        private val logger: Logger = LogManager.getLogger(RaftClusterInMemoryNetwork::class.java)
    }
}