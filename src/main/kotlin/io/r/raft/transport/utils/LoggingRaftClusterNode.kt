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

class LoggingRaftClusterNode(private val delegate: RaftCluster) : RaftCluster by delegate {

    private val logger = LogManager.getLogger("${LoggingRaftClusterNode::class.java.name}.${delegate.id}")


    override suspend fun send(to: NodeId, rpc: RaftRpc) {
        logger.info("== ${rpc.describe()}) ==> $to")
        delegate.send(to, rpc)
    }
}