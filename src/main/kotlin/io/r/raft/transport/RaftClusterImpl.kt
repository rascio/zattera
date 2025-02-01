package io.r.raft.transport

import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.RaftCluster
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

class RaftClusterImpl(
    val id: NodeId,
    private val peers: RaftPeers,
    private val debugMessages: Boolean = false
): AutoCloseable {

    private val _events = MutableSharedFlow<Event>(
        replay = 0,
        extraBufferCapacity = 0, // buffer used when subscribers are slow
        onBufferOverflow = BufferOverflow.SUSPEND
    )
    val events = _events.asSharedFlow()



    suspend fun send(to: NodeId, rpc: RaftRpc) {
        val node = getNode(to)
        if (debugMessages) {
            logger.info("$id == ${rpc.describe()} ==> $to")
        }
        node.send(RaftMessage(from = id, to = to, rpc = rpc))
    }
    suspend fun forward(to: NodeId, entry: LogEntry.Entry): Any {
        val node = getNode(to)
        if (debugMessages) {
            logger.info("$id -- $entry --> $to")
        }
        return node.forward(entry)
    }

    private fun getNode(nodeId: NodeId): RaftService {
        return peers[nodeId] ?: error("Unknown node $nodeId")
    }

    suspend fun changeConfiguration(entry: LogEntry.ConfigurationChange) {
        entry.new
            .filter { it.id !in peers && it.id != id }
            .forEach { node ->
                peers.connect(node)
                _events.emit(Connected(node.id))
            }
        entry.old
            ?.filter { it.id in peers }
            ?.forEach { peers.disconnect(it) }
    }

    override fun close() {
        peers.close()
    }

    interface RaftPeers : AutoCloseable {
        val ids: List<NodeId>
        operator fun contains(nodeId: NodeId): Boolean
        operator fun get(nodeId: NodeId): RaftService?
        suspend fun connect(node: RaftRpc.ClusterNode)
        suspend fun disconnect(node: RaftRpc.ClusterNode)
    }

    sealed interface Event
    data class Connected(val node: NodeId): Event
    data class Disconnected(val node: NodeId): Event

    companion object {
        private val logger: Logger = LogManager.getLogger(RaftClusterImpl::class.java)
    }

}