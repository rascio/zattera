package io.r.raft.transport.inmemory

import arrow.core.continuations.AtomicRef
import arrow.core.continuations.update
import io.r.raft.NodeId
import io.r.raft.RaftMachine
import io.r.raft.RaftMessage
import io.r.raft.Transport
import io.r.raft.transport.inmemory.InProcessTransport.ChannelNodeRef
import io.r.utils.logs.entry
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import org.apache.logging.log4j.LogManager

class InProcessTransport(id: NodeId) : Transport<ChannelNodeRef> {

    companion object {
        private val isolatedNodes = AtomicRef(emptySet<NodeId>())
    }
    private val logger = LogManager.getLogger("InProcessTransport.${id}")

    val ref: ChannelNodeRef = ChannelNodeRef(id, Channel(capacity = Channel.UNLIMITED))

    val id = ref.id

    override suspend fun receive(): RaftMessage =
        ref.channel.receive()

    override suspend fun send(node: ChannelNodeRef, message: RaftMessage) {
        if (ref.id in isolatedNodes.get() || node.id in isolatedNodes.get()) {
            logger.info(entry("Dropping message", "from" to message.from, "to" to message.to, "message" to message))
            return
        }
        val latency = node.latency()
        if (latency > 0) {
            delay(latency)
        }
        node.channel.send(message)
    }

    fun disconnect() {
        logger.info("Disconnect ${ref.id}")
        isolatedNodes.update { it + ref.id }
    }

    fun fix() {
        logger.info("Fix ${isolatedNodes.get()}")
        isolatedNodes.update { it - ref.id }
        logger.info("Fixed ${isolatedNodes.get()}")
    }

    inner class ChannelNodeRef(
        val id: NodeId,
        val channel: Channel<RaftMessage>,
        val latency: () -> Long = { 0 }
    ) : RaftMachine.RaftNode {
        override fun toString(): String = "NodeRef($id)"
    }
}

