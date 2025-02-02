package io.r.raft.transport.ktor

import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.HttpTimeout
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.RaftCluster
import io.r.raft.transport.RaftService
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

class HttpRaftCluster : RaftCluster.RaftPeers, AutoCloseable {

    private val client = HttpClient(CIO) {
        install(HttpTimeout) {
            connectTimeoutMillis = 1000
            socketTimeoutMillis = 500
            requestTimeoutMillis = 1000
        }
    }

    private val peers = mutableMapOf<NodeId, HttpRemoteRaftService>()

    override val ids: Set<NodeId> get() = peers.keys

    override fun contains(nodeId: NodeId): Boolean =
        nodeId in peers

    override fun get(nodeId: NodeId): RaftService? =
        peers[nodeId]

    override suspend fun connect(node: RaftRpc.ClusterNode) {
        peers.computeIfAbsent(node.id) { HttpRemoteRaftService(node, client) }
    }

    override suspend fun disconnect(node: RaftRpc.ClusterNode) {
        peers.remove(node.id)?.close()
    }

    override fun close() {
        // improve multi exception handling
        peers.values.forEach { it.close() }
    }

    companion object {
        private val logger: Logger = LogManager.getLogger(HttpRaftCluster::class.java)
    }

}