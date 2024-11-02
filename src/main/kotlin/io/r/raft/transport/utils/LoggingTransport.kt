package io.r.raft.transport.utils

import io.r.raft.RaftMachine.RaftNode
import io.r.raft.RaftMessage
import io.r.raft.Transport
import org.apache.logging.log4j.LogManager

class LoggingTransport<NodeRef: RaftNode>(logTag: String, private val delegate: Transport<NodeRef>) : Transport<NodeRef> {
    private val logger = LogManager.getLogger("Transport.$logTag")
    override suspend fun receive(): RaftMessage {
//        log("Wait for message")
        val message = delegate.receive()
        logger.info("${message.to} <==${message.rpc::class.simpleName}== ${message.from}", "message" to message.rpc)
        return message
    }

    override suspend fun send(node: NodeRef, message: RaftMessage) {
        logger.info("${message.from} ==${message.rpc::class.simpleName}==> ${message.to}", "message" to message.rpc)
        delegate.send(node, message)
//        log("Sent ${message.protocol::class.simpleName}")
    }
}