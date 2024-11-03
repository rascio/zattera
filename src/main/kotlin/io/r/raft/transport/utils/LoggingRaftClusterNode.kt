package io.r.raft.transport.utils

import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.RaftClusterNode
import org.apache.logging.log4j.LogManager

class LoggingRaftClusterNode(private val delegate: RaftClusterNode) : RaftClusterNode by delegate {

    private val logger = LogManager.getLogger("${LoggingRaftClusterNode::class.java.name}.${delegate.id}")

    override suspend fun send(to: NodeId, rpc: RaftRpc) {
        logger.info("== ${rpc.describe()}) ==> $to")
        delegate.send(to, rpc)
    }

    override suspend fun receive(): RaftMessage {
        val message = delegate.receive()
        logger.info("<== ${message.rpc.describe()} == ${message.from}")
        return message
    }
}