package io.r.raft.transport.ktor

import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.RaftCluster
import io.r.raft.transport.RaftClusterImpl
import io.r.raft.transport.RaftService
import org.apache.logging.log4j.LogManager

class HttpRaftCluster(
    override val id: NodeId,
    private val debugMessages: Boolean = false
) : RaftCluster, RaftClusterImpl.RaftPeers, AutoCloseable {

    private val _peers = mutableMapOf<NodeId, HttpRemoteRaftService>()

    override val peers: Set<NodeId> get() = _peers.keys
    override val ids: List<NodeId> get() = _peers.keys.toList()

    override fun contains(nodeId: NodeId): Boolean =
        nodeId in _peers

    override fun get(nodeId: NodeId): RaftService? =
        _peers[nodeId]

    override suspend fun connect(node: RaftRpc.ClusterNode) {
        _peers.computeIfAbsent(node.id) { HttpRemoteRaftService(node) }
    }

    override suspend fun disconnect(node: RaftRpc.ClusterNode) {
        _peers.remove(node.id)?.close()
    }

    override suspend fun send(to: NodeId, rpc: RaftRpc) {
        require(to in _peers) { "Unknown node $to" }
        if (debugMessages) {
            logger.info("$id == ${rpc.describe()} ==> $to")
        }
        _peers[to]!!.send(RaftMessage(from = id, to = to, rpc = rpc))
    }
    override suspend fun forward(to: NodeId, entry: LogEntry.Entry): Any {
        require(to in _peers) { "Unknown node $to" }
        return _peers[to]!!.forward(entry)
    }

    override fun changeConfiguration(entry: LogEntry.ConfigurationChange) {
        entry.new
            .filter { it.id !in _peers && it.id != id }
            .forEach { node ->
                _peers[node.id] = HttpRemoteRaftService(node)
            }
        //entry.old?.forEach { cluster.removePeer(it.id) }
    }

    override fun close() {
        // improve multi exception handling
        _peers.values.forEach { it.close() }
    }

    companion object {
        val logger = LogManager.getLogger(HttpRaftCluster::class.java)
    }

}