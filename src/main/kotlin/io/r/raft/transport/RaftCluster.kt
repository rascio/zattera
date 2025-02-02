package io.r.raft.transport

import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.utils.logs.entry
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger

/**
 * Represents a node in the Raft cluster.
 * Offers a way to send messages to other nodes in the cluster and receive messages from them.
 */
class RaftCluster(
    val id: NodeId,
    private val _peers: RaftPeers,
    private val debugMessages: Boolean = false
) : AutoCloseable {

    private val _events = MutableSharedFlow<Event>(
        replay = 0,
        extraBufferCapacity = 0, // buffer used when subscribers are slow
        onBufferOverflow = BufferOverflow.SUSPEND
    )
    val events get() = _events.asSharedFlow()
    val peers get() = _peers.ids - id

    suspend fun send(to: NodeId, rpc: RaftRpc) {
        val node = getNode(to)
        if (debugMessages) {
            httpMessagesLogger.info("$id == ${rpc.describe()} ==> $to")
        }
        node.send(RaftMessage(from = id, to = to, rpc = rpc))
    }

    fun getNode(nodeId: NodeId): RaftService {
        return _peers[nodeId] ?: error("Unknown node $nodeId")
    }

    suspend fun changeConfiguration(entry: LogEntry.ConfigurationChange) {
        entry.new
            .filter { it.id !in _peers && it.id != id }
            .forEach { node ->
                logger.debug {
                    entry("Adding_Node", "node" to node.id)
                }
                _peers.connect(node)
                _events.emit(Connected(node.id))
            }
        entry.old
            ?.filter { it.id in _peers }
            ?.forEach { _peers.disconnect(it) }
    }

    override fun close() {
        _peers.close()
    }

    interface RaftPeers : AutoCloseable {
        val ids: Set<NodeId>
        operator fun contains(nodeId: NodeId): Boolean
        operator fun get(nodeId: NodeId): RaftService?
        suspend fun connect(node: RaftRpc.ClusterNode)
        suspend fun disconnect(node: RaftRpc.ClusterNode)
    }

    sealed interface Event
    data class Connected(val node: NodeId) : Event
    data class Disconnected(val node: NodeId) : Event

    companion object {
        private val httpMessagesLogger: Logger = LogManager.getLogger("HttpMessagesLogger.Out")
        private val logger: Logger = LogManager.getLogger(RaftCluster::class.java)

        val RaftCluster.quorum: Int
            get() = _peers.ids.size / 2 + 1
    }
}