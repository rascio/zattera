package io.r.raft.machine

import arrow.fx.coroutines.autoCloseable
import arrow.fx.coroutines.resourceScope
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.comparables.shouldBeGreaterThan
import io.kotest.matchers.comparables.shouldBeLessThan
import io.kotest.matchers.shouldBe
import io.r.raft.Index
import io.r.raft.LogEntry
import io.r.raft.LogEntryMetadata
import io.r.raft.NodeId
import io.r.raft.RaftMessage
import io.r.raft.RaftProtocol
import io.r.raft.RaftProtocol.AppendEntries
import io.r.raft.RaftProtocol.AppendEntriesResponse
import io.r.raft.RaftRole
import io.r.raft.Term
import io.r.raft.log.RaftLog
import io.r.raft.log.StateMachine
import io.r.raft.log.inmemory.InMemoryRaftLog
import io.r.raft.transport.RaftClusterNode
import io.r.raft.transport.inmemory.InMemoryRaftClusterNode
import io.r.raft.transport.utils.LoggingRaftClusterNode
import io.r.utils.awaitility.await
import io.r.utils.awaitility.coUntil
import io.r.utils.awaitility.oneOf
import io.r.utils.entry
import io.r.utils.logs.entry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.awaitility.Awaitility
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import java.util.TreeMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.random.Random
import kotlin.test.assertNotNull
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class RaftMachineTest : FunSpec({

    val logger = LogManager.getLogger(RaftMachineTest::class.java)

    Awaitility.setDefaultTimeout(3.seconds.toJavaDuration())
    Awaitility.setDefaultPollDelay(30.milliseconds.toJavaDuration())

    context("Base RPC Protocol") {
        /*
         * Simulate a cluster with 3 nodes
         * Just one raft node is created and stopped at the end
         * N1 and N2 are the transport for two mocked nodes
         * They will not have a state machine attached,
         * but they will be used to send messages to the node under test
         */
        resourceScope {
            val cluster = autoCloseable {
                RaftClusterInMemoryNetwork("N1", "N2", "UnderTest")
            }
            val N1 = cluster.createNode("N1")
            val N2 = cluster.createNode("N2")
            // Create the node with the state machine
            // it is used in all tests and stopped at the end (through 'install')

            val underTest = install(
                acquire = {
                    RaftTestNode(
                        raftClusterInMemoryNetwork = cluster,
                        nodeId = "UnderTest",
                        configuration = RaftMachine.Configuration(),
                        scope = CoroutineScope(coroutineContext)
                    ).apply { raftMachine.start() }
                },
                release = { n, _ -> n.stop() }
            )

            test("RequestVote RPC - receives request vote in follower state, grant the vote").config(timeout = 1.seconds) {
                N1.sendTo(underTest) { RaftProtocol.RequestVote(1L, "N1", LogEntryMetadata.ZERO) }
                N1 shouldReceive RaftMessage(
                    from = underTest.id,
                    to = N1.id,
                    protocol = RaftProtocol.RequestVoteResponse(term = 1L, voteGranted = true)
                )
            }
            context("AppendEntries RPC") {
                test("should reply false if the term is lower").config(timeout = 15.seconds) {
                    underTest.reboot { term = 1L }
                    N1.sendTo(underTest) { AppendEntries(0L, N1.id, LogEntryMetadata.ZERO, emptyList(), 0L) }
                    N1 shouldReceive RaftMessage(underTest.id, N1.id, AppendEntriesResponse(1L, -1, false, 0))
                }
                test("should reply false if the previous log index is not found").config(timeout = 1.seconds) {
                    underTest.reboot { term = 1L }
                    N1.sendTo(underTest) {
                        AppendEntries(
                            term = 1L,
                            leaderId = N1.id,
                            prevLog = LogEntryMetadata(index = 1, term = 1),
                            entries = emptyList(),
                            leaderCommit = 0L
                        )
                    }
                    N1 shouldReceive RaftMessage(underTest.id, N1.id, AppendEntriesResponse(1L, -1, false, 0))
                }
                test("should reject append if the prevLog term does not match").config(timeout = 1.seconds) {
                    underTest.reboot {
                        term = 1L
                        +"Hello World"
                    }
                    N1.sendTo(underTest) {
                        AppendEntries(
                            term = 1L,
                            leaderId = N1.id,
                            prevLog = LogEntryMetadata(1, 2),
                            entries = listOf(entry(term = 3, command = "Hello World")),
                            leaderCommit = 0L
                        )
                    }
                    N1 shouldReceive RaftMessage(underTest.id, N1.id, AppendEntriesResponse(1L, -1, false, 0))
                }
            }
            context("RequestVote RPC") {
                test("should start election for next term after timeout").config(timeout = 3.seconds) {
                    underTest.reboot {
                        term = 0L
                    }
                    N1 shouldReceive RaftMessage(
                        underTest.id,
                        N1.id,
                        RaftProtocol.RequestVote(1L, underTest.id, LogEntryMetadata.ZERO)
                    )
                    N2 shouldReceive RaftMessage(
                        underTest.id,
                        N2.id,
                        RaftProtocol.RequestVote(1L, underTest.id, LogEntryMetadata.ZERO)
                    )
                }
                test("Given a node in candidate state When it receives a RequestVote for same term Then should reject it").config(timeout = 3.seconds) {
                    underTest.reboot {
                        term = 0L
                    }
                    N1 shouldReceive RaftMessage(
                        underTest.id,
                        N1.id,
                        RaftProtocol.RequestVote(1L, underTest.id, LogEntryMetadata.ZERO)
                    )
                    N1.sendTo(underTest) {
                        RaftProtocol.RequestVote(
                            1L,
                            N1.id,
                            LogEntryMetadata.ZERO
                        )
                    }
                    N1.receive() shouldBe RaftMessage(underTest.id, N1.id, RaftProtocol.RequestVoteResponse(1L, false))
                }
            }
        }
    }


    context("3 Nodes Cluster") {
        // 'refs' is a map with the id and transport for each node
        val refs = (1..3).map { "N$it" }

        resourceScope {
            val scope = CoroutineScope(Dispatchers.IO)
            val clusterNetwork = autoCloseable {
                RaftClusterInMemoryNetwork(*refs.toTypedArray())
            }
            val nodes = refs.map { ref ->
                install(
                    acquire = {
                        RaftTestNode(
                            nodeId = ref,
                            configuration = RaftMachine.Configuration(
                                maxLogEntriesPerAppend = 4,
                                leaderElectionTimeoutMs = 300L,
                                leaderElectionTimeoutJitterMs = 200,
                            ),
                            raftClusterInMemoryNetwork = clusterNetwork,
                            scope = scope
                        ).apply { raftMachine.start() }
                    },
                    release = { node, _ -> node.stop() }
                )
            }
            val cluster = RaftTestCluster(nodes)

            test("There should be at least one leader") {
                cluster.awaitLeaderElection()
            }
            cluster.awaitLogConvergence()
            test("If the leader is disconnected, a new leader should be elected") {
                val initialLeader = cluster.awaitLeaderElection()
                val initialTerm = initialLeader.getCurrentTerm()
                logger.info(entry("initial_leader", "term" to initialTerm, "id" to initialLeader.id))
                initialLeader.disconnect()
                try {
                    "await_leader_change".await.until(
                        { nodes.filter { n -> n.raftMachine.isLeader } },
                        { leaders -> leaders.any { it != initialLeader } }
                    )
                } finally {
                    initialLeader.reconnect()
                }

                await until { nodes.count { it.raftMachine.isLeader } == 1 }
                val newLeader = cluster.awaitLeaderElection()
                newLeader.getCurrentTerm() shouldBeGreaterThan initialTerm
                cluster.awaitLogConvergence()
            }
            test("When append is signaled committed, all nodes should have the entry") {
                val leader = cluster.awaitLeaderElection()
                withTimeout(3.seconds) {
                    leader.raftMachine.command("Hello World".encodeToByteArray())
                        .join()
                }
                nodes.forEach {
                    val logs = it.log.getEntries(0, 10)
                    logger.info(entry("check-log", "logs" to logs))
                    logs.size shouldBe 1
                    logs[0].command shouldBe "Hello World".encodeToByteArray()
                }
            }
            cluster.awaitLogConvergence()
            test("Append a batch of messages in the log") {
                val leader = cluster.awaitLeaderElection()
                val startIndex = leader.commitIndex + 1
                withTimeout(5.seconds) {
                    leader.raftMachine.command(
                        "Hey Jude".encodeToByteArray(),
                        "Don't make it bad".encodeToByteArray(),
                        "Take a sad song".encodeToByteArray(),
                        "And make it better".encodeToByteArray()
                    ).join()
                }
                nodes.forEach { n ->
                    launch {
                        val messages = n.log.getEntries(startIndex, 10)
                            .map { it.command.decodeToString() }
                        logger.info(entry("check-log", "from-index" to startIndex, "messages" to messages))

                        messages shouldBe listOf(
                            "Hey Jude",
                            "Don't make it bad",
                            "Take a sad song",
                            "And make it better"
                        )
                    }
                }
            }
            cluster.awaitLogConvergence()
            test("Node replace uncommitted context when receiving an append for a previous entry") {
                // Elect a leader
                val leader = cluster.awaitLeaderElection()
                val followers = nodes.filter { it != leader }
                val firstCmdIndex = leader.commitIndex + 1

                // Append a command to the leader
                val committed = leader.raftMachine.command("TV Series: Breaking Bad".encodeToByteArray())
                assertNotNull(committed).join()

                // Disconnect the leader from the cluster
                leader.disconnect()

                // A new leader should be elected
                val newLeader = "await_new_leader".await coUntil oneOf(followers) { it.raftMachine.isLeader }
                logger.info(
                    entry(
                        "new_leader",
                        "id" to newLeader.id,
                        "term" to newLeader.getCurrentTerm(),
                        "commit_index" to newLeader.commitIndex
                    )
                )

                // Append a command to the old leader
                @Suppress("UNUSED_VARIABLE")
                val oldLeaderCommitted = leader.raftMachine.command("TV Series: The Wire".encodeToByteArray())
                // And a list of commands to the new leader
                val newLeaderCommitted = newLeader.raftMachine
                    .command("TV Series: The Sopranos".encodeToByteArray(), "TV Series: Sherlock".encodeToByteArray())
                "await_old_leader_appended"
                    .await coUntil { leader.getLastEntryMetadata().index == firstCmdIndex + 1 }

                withTimeout(3.seconds) { newLeaderCommitted.join() }
                logger.info("message_replicated_2_out_of_3")

                logger.info("fix_old_leader")
                leader.reconnect()
                cluster.awaitLogConvergence()
                // The assertThrows is not working, for now...needs a fix :(
//                assertThrows<CancellationException>("The bad commit on the old leader should be rejected") {
//                    withTimeout(3.seconds) {
//                        oldLeaderCommitted.join()
//                    }
//                }
                // after the old leader re-join the cluster, it should have the same logs as the new leader
                val expectedLogs = listOf(
                    "TV Series: Breaking Bad",
                    "TV Series: The Sopranos",
                    "TV Series: Sherlock"
                )
                nodes.forEach { n ->
                    val logs = n.log.getEntries(firstCmdIndex, 4)
                    logger.info(
                        entry(
                            "check-log",
                            "node" to n.id,
                            "index" to n.commitIndex,
                            "logs" to logs.joinToString { "[T${it.term}|${it.command.decodeToString()}]" })
                    )
                    logs.map { it.command.decodeToString() } shouldBe expectedLogs
                }
            }
            test("Test: Sequential Consistency") {
                cluster.reboot()
                cluster.awaitLogConvergence()
                val messagePattern = Regex("([A-Z])-([0-9]+)")
                // C clients sending B batches of M messages each
                val C = 8
                val B = 15
                val M = 3

                val clients = 'A'..('A' + C)
                coroutineScope {
                    val clientsSendingEntriesJob = launch {
                        clients.forEach { client ->
                            launch(Dispatchers.IO) {
                                repeat(B) { b -> // batches
                                    val messages = (1..M).map { m ->
                                        "$client-${m + (b * M)}"
                                    }
                                    cluster.append(messages)
                                }
                            }
                        }
                    }
                    launch {
                        // Randomly disconnect a node and reboot it
                        do {
                            delay(Random.nextLong(100))
                            if (Random.nextBoolean()) {
                                nodes.random().apply {
                                    disconnect()
                                    delay(configuration.heartbeatTimeoutMs * Random.nextLong(5))
                                    reconnect()
                                }
                            }
                        } while (clientsSendingEntriesJob.isActive)
                    }
                }
                cluster.awaitLogConvergence()
                cluster.nodes.forEach { n ->
                    val logs = n.log.getEntries(1, Int.MAX_VALUE)
                    logger.info(entry("check_log", "node" to n.id, "logs" to logs.hashCode()))
                    // The logs can have a mixed order between clients,
                    // but the messages from the same client should be in order
                    val lastIndexByClient = mutableMapOf<String, Int>()
                    logs.forEach { entry ->
                        logger.info(
                            entry(
                                "check_entry",
                                "term" to entry.term,
                                "command" to entry.command.decodeToString()
                            )
                        )
                        val message = entry.command.decodeToString()
                        val (client, i) = messagePattern.matchEntire(message)!!
                            .destructured
                            .let { (c, i) -> c to i.toInt() }
                        val lastIndex = lastIndexByClient[client] ?: -1
                        lastIndex shouldBeLessThan i
                        lastIndexByClient[client] = i
                    }
                }
            }
        }
    }
})

class RaftTestCluster(val nodes: List<RaftTestNode>) {

    private val logger: Logger = LogManager.getLogger(RaftTestCluster::class.java)

    suspend fun append(vararg commands: String) = append(commands.toList())
    suspend fun append(commands: List<String>): Unit = coroutineScope {
        nodes.firstOrNull { r -> r.raftMachine.role.first() == RaftRole.LEADER }
            ?.let { leader ->
                try {
                    leader.raftMachine.command(commands.map { it.encodeToByteArray() })
                        .join()
                } catch (e: Exception) {
                    when (e.message) {
                        "Role has changed" -> null
                        "Only leader can send commands" -> null
                        else -> throw e
                    }
                }
            } ?: run {
                delay(30)
                append(commands)
            }
    }

    suspend fun reboot() {
        nodes.forEach { it.reboot { } }
    }

    /**
     * Wait until the cluster has a consistent state, i.e., all nodes have the same commit index
     */
    suspend fun awaitLogConvergence(timeout: Duration = 3.seconds) = withTimeoutOrNull(timeout) {
        var found = false
        while (!found) {
            found = nodes.map { it.getLastEntryMetadata() }.toSet().size == 1
        }
        val commitIndexes = nodes
            .map { it.id to it.getLastEntryMetadata() }
            .joinToString { (id, metadata) -> "${id}[IDX:${metadata.index},T:${metadata.term}]" }
        logger.info(
            entry("cluster_sync",
                "commit_indexes" to commitIndexes
            )
        )
    } ?: error("waitClusterSync timeout=$timeout")

    /**
     * Wait until a leader is elected
     */
    suspend fun awaitLeaderElection(timeout: Duration = 3.seconds) = withTimeoutOrNull(timeout) {
        val (_, node) = nodes.map { n -> n.raftMachine.role.map { it to n } }
            .merge()
            .first { (role, _) -> role == RaftRole.LEADER }
        logger.info(
            entry(
                "leader_found",
                "leader" to node.id,
                "term" to node.getCurrentTerm()
            )
        )
        node
    } ?: error("waitForLeader timeout=$timeout")
}

class RaftTestNode private constructor(
    private val raftClusterInMemoryNetwork: RaftClusterInMemoryNetwork,
    private val raftClusterNode: RaftClusterNode,
    val configuration: RaftMachine.Configuration,
    private var _log: RaftLog,
    private val scope: CoroutineScope
) {
    companion object {
        private val logger: Logger = LogManager.getLogger(RaftTestNode::class.java)
        operator fun invoke(
            raftClusterInMemoryNetwork: RaftClusterInMemoryNetwork,
            nodeId: NodeId,
            configuration: RaftMachine.Configuration,
            scope: CoroutineScope
        ) = RaftTestNode(
            raftClusterInMemoryNetwork = raftClusterInMemoryNetwork,
            raftClusterNode = raftClusterInMemoryNetwork.createNode(nodeId),
            configuration = configuration,
            _log = InMemoryRaftLog(),
            scope = scope
        )
    }

    val commitIndex get() = _raftMachine.get().commitIndex
    private val _raftMachine = AtomicReference(
        newRaftMachine()
    )
    val raftMachine: RaftMachine get() = _raftMachine.get()
    val log get() = _log
    val id: NodeId = raftClusterNode.id
    private var rebootCounter = AtomicLong()

    suspend fun getCurrentTerm() = _log.getTerm()

    suspend fun reboot(block: RaftLogBuilderScope.() -> Unit) {
        raftMachine.stop()
        _log = RaftLogBuilderScope.raftLog(block)
        _raftMachine.set(newRaftMachine())
        raftMachine.start()
    }


    suspend fun restart() {
        raftMachine.stop()
        raftMachine.start()
    }

    suspend fun getLastEntryMetadata(): LogEntryMetadata {
        return _log.getMetadata(_log.getLastIndex())!!
    }

    suspend fun start() {
        raftMachine.start()
    }

    suspend fun stop() {
        raftMachine.stop()
    }

    fun disconnect() {
        raftClusterInMemoryNetwork.disconnect(id)
    }

    fun reconnect() {
        raftClusterInMemoryNetwork.reconnect(id)
    }

    private fun newRaftMachine() = RaftMachine(
        configuration = configuration,
        log = _log,
        cluster = LoggingRaftClusterNode(raftClusterNode),
        stateMachine = object : StateMachine {
            var applied = 0L
            override suspend fun apply(command: LogEntry) {
                logger.info(entry("apply", "command" to command.command.decodeToString(), "_node" to id))
                applied++
            }

            override suspend fun getLastApplied(): Index =
                applied
        },
        scope = scope
    )
}


class RaftLogBuilderScope {
    var term: Term = 0L
    var votedFor: NodeId? = null
    private val log = TreeMap<Index, LogEntry>()

    operator fun String.unaryPlus() {
        log[log.size + 1L] = LogEntry(term, this.encodeToByteArray())
    }

    operator fun LogEntry.unaryPlus() {
        log[log.size + 1L] = this
    }

    companion object {
        fun raftLog(block: RaftLogBuilderScope.() -> Unit) =
            RaftLogBuilderScope()
                .apply(block)
                .let { InMemoryRaftLog(it.log, it.term, it.votedFor) }
    }
}

class RaftClusterInMemoryNetwork(vararg nodeIds: NodeId) : AutoCloseable {
    private val isolatedNodes = mutableSetOf<NodeId>()
    private val nodes = mutableMapOf<NodeId, Channel<RaftMessage>>().apply {
        nodeIds.forEach { id -> put(id, Channel(Channel.UNLIMITED)) }
    }

    fun createNode(name: NodeId): RaftClusterNode {
        val clusterNode = InMemoryRaftClusterNode(name, nodes)
        return object : RaftClusterNode by clusterNode {
            override suspend fun send(node: NodeId, rpc: RaftProtocol) {
                when {
                    node in isolatedNodes -> {
                        logger.info(entry("isolated_node", "node" to node, "rpc" to rpc::class.simpleName))
                    }
                    clusterNode.id in isolatedNodes -> {
                        logger.info(entry("isolated_node", "node" to clusterNode.id, "rpc" to rpc::class.simpleName))
                    }
                    else -> {
                        clusterNode.send(node, rpc)
                    }
                }
            }
        }
    }
    fun disconnect(vararg nodeIds: NodeId) {
        logger.info(entry("disconnect", "nodes" to nodeIds.joinToString()))
        isolatedNodes.addAll(nodeIds)
    }
    fun reconnect(vararg nodeIds: NodeId) {
        logger.info(entry("reconnect", "nodes" to nodeIds.joinToString()))
        isolatedNodes.removeAll(nodeIds.toSet())
    }

    override fun close() {
        nodes.values.forEach { it.close() }
    }

    companion object {
        private val logger: Logger = LogManager.getLogger(RaftClusterInMemoryNetwork::class.java)
    }
}


/**
 * Make the [RaftTestNode] receive a message from the [RaftClusterNode]
 */
suspend fun RaftClusterNode.sendTo(to: RaftTestNode, rpc: () -> RaftProtocol) {
    send(to.id, rpc())
}

private suspend infix fun RaftClusterNode.shouldReceive(expected: RaftMessage) {
    receive() shouldBe expected
}