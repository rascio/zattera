package io.r.raft.transport.utils

import io.r.raft.RaftMachine.RaftNode
import io.r.raft.RaftMessage
import io.r.raft.Transport
import org.apache.logging.log4j.LogManager

class LoggingTransport<NodeRef: RaftNode>(logTag: String, private val delegate: Transport<NodeRef>) : Transport<NodeRef> {
    val logger = LogManager.getLogger("Transport.$logTag")
    override suspend fun receive(): RaftMessage {
//        log("Wait for message")
        val message = delegate.receive()
        logger.info("${message.to} <==${message.protocol::class.simpleName}== ${message.from}", "message" to message.protocol)
        return message
    }

    override suspend fun send(node: NodeRef, message: RaftMessage) {
        logger.info("${message.from} ==${message.protocol::class.simpleName}==> ${message.to}", "message" to message.protocol)
        delegate.send(node, message)
//        log("Sent ${message.protocol::class.simpleName}")
    }
}