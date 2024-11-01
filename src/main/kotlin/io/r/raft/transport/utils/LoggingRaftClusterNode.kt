package io.r.raft.transport.utils

import io.r.raft.NodeId
import io.r.raft.RaftMessage
import io.r.raft.RaftProtocol
import io.r.raft.transport.RaftClusterNode
import org.apache.logging.log4j.LogManager

class LoggingRaftClusterNode(private val delegate: RaftClusterNode) : RaftClusterNode by delegate {

    private val logger = LogManager.getLogger("${LoggingRaftClusterNode::class.java.name}.${delegate.id}")

    override suspend fun send(node: NodeId, rpc: RaftProtocol) {
        logger.info("== ${rpc.describe()}) ==> $node")
        delegate.send(node, rpc)
    }

    override suspend fun receive(): RaftMessage {
        val message = delegate.receive()
        logger.info("<== ${message.protocol.describe()} == ${message.from}")
        return message
    }
}