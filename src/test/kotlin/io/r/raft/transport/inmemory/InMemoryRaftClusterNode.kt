package io.r.raft.transport.inmemory

import io.kotest.matchers.Matcher
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.r.raft.machine.RaftTestNode
import io.r.raft.machine.Response
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.RaftCluster
import io.r.raft.transport.RaftService
import kotlinx.coroutines.yield

class InMemoryRaftClusterNode(
    override val node: RaftRpc.ClusterNode,
    val network: RaftClusterTestNetwork
) : RaftService {

    val channel get() = network.channel(node.id)

    override suspend fun send(message: RaftMessage) {
        yield().let { network.send(message) } // suspend to simulate network I/O
    }

    override suspend fun request(entry: LogEntry.Entry): Response =
        yield().let { network.command(node.id, entry) } // suspend to simulate network I/O

    override suspend fun query(query: ByteArray): Response =
        yield().let { network.query(node.id, query) } // suspend to simulate network I/O

    companion object {

        /**
         * Make the [RaftTestNode] receive a message from the [RaftCluster]
         */
        suspend inline fun InMemoryRaftClusterNode.sendTo(to: RaftTestNode<*, *, *>, rpc: () -> RaftRpc) {
            network.send(RaftMessage(from = node.id, to = to.id, rpc = rpc()))
        }

        suspend infix fun InMemoryRaftClusterNode.shouldReceive(expected: RaftMessage) {
            network.channel(node.id).receive() shouldBe expected
        }
        suspend infix fun InMemoryRaftClusterNode.shouldReceive(matcher: Matcher<RaftMessage>) {
            network.channel(node.id).receive() should matcher
        }
    }
}