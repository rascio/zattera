package io.r.raft.transport.utils

import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.RaftCluster
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager

class LoggingRaftClusterNode(private val delegate: RaftCluster) : RaftCluster by delegate, AutoCloseable {

    private val logger = LogManager.getLogger("${LoggingRaftClusterNode::class.java.name}.${delegate.id}")
    private val copyOverChannel = Channel<RaftMessage>(Channel.UNLIMITED)

    private val copyOver = CoroutineScope(Dispatchers.IO).apply {
        launch {
            while (isActive) {
                val message = delegate.input.receive()
                logger.info("<== ${message.rpc.describe()} == ${message.from}")
                copyOverChannel.send(message)
            }
        }
    }

    override val input: ReceiveChannel<RaftMessage>
        get() = copyOverChannel

    override suspend fun send(to: NodeId, rpc: RaftRpc) {
        logger.info("== ${rpc.describe()}) ==> $to")
        delegate.send(to, rpc)
    }

    override fun close() {
        copyOver.cancel()
    }
}