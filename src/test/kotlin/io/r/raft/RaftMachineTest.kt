package io.r.raft

import arrow.fx.coroutines.resourceScope
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.ints.shouldBeLessThan
import io.kotest.matchers.longs.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import io.r.raft.LogBuilderScope.Companion.raftLog
import io.r.raft.RaftProtocol.AppendEntries
import io.r.raft.RaftProtocol.AppendEntriesResponse
import io.r.raft.persistence.inmemory.InMemoryPersistence
import io.r.raft.persistence.utils.LoggingPersistence
import io.r.raft.transport.inmemory.InProcessTransport
import io.r.raft.transport.inmemory.InProcessTransport.ChannelNodeRef
import io.r.raft.transport.utils.LoggingTransport
import io.r.utils.timeout.Timeout
import io.r.utils.awaitility.await
import io.r.utils.awaitility.coUntil
import io.r.utils.awaitility.oneOf
import io.r.utils.logs.entry
import kotlinx.coroutines.CancellationException
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
        val N1 = InProcessTransport("N1")
        val N2 = InProcessTransport("N2")
        resourceScope {
            // Create the node with the state machine
            // it is used in all tests and stopped at the end (through 'install')
            val underTest = install({
                RaftTestNode(
                    InProcessTransport("NTest"),
                    RaftMachine.Configuration("NTest", mapOf("N1" to N1.ref, "N2" to N2.ref))
                ).apply { stateMachine.start() }
            }) { n, _ -> n.stateMachine.stop() }

            test("RequestVote RPC - init") {
                N1.sendTo(underTest) { RaftProtocol.RequestVote(1L, "N1", LogEntryMetadata.ZERO) }
                N1 shouldReceive RaftMessage("NTest", "N1", RaftProtocol.RequestVoteResponse(1L, true))
            }
            context("AppendEntries RPC") {
                test("should reply false if the term is lower") {
                    underTest.reboot { currentTerm = 1L }
                    N1.sendTo(underTest) { AppendEntries(0L, N1.id, LogEntryMetadata.ZERO, emptyList(), 0L) }
                    N1 shouldReceive RaftMessage(underTest.id, N1.id, AppendEntriesResponse(1L, -1, false, -1))
                }
                test("should reply false if the previous log index is not found") {
                    underTest.reboot { currentTerm = 1L }
                    N1.sendTo(underTest) {
                        AppendEntries(
                            term = 1L,
                            leaderId = N1.id,
                            prevLog = meta(1, 1),
                            entries = emptyList(),
                            leaderCommit = 0L
                        )
                    }
                    N1 shouldReceive RaftMessage(underTest.id, N1.id, AppendEntriesResponse(1L, -1, false, -1))
                }
                test("should reject append if the prevLog term does not match") {
                    underTest.reboot {
                        currentTerm = 1L
                        log = raftLog {
                            +entry(term = 1, command = "Hello World")
                        }
                    }
                    N1.sendTo(underTest) {
                        AppendEntries(
                            term = 1L,
                            leaderId = N1.id,
                            prevLog = meta(1, 2),
                            entries = listOf(entry(3, "Hello World")),
                            leaderCommit = 0L
                        )
                    }
                    N1 shouldReceive RaftMessage(underTest.id, N1.id, AppendEntriesResponse(1L, -1, false, -1))
                }
            }
            context("RequestVote RPC") {
                test("should start election for next term after timeout").config(coroutineTestScope = true) {
                    underTest.reboot {
                        currentTerm = 0L
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
                test("Given a node in state candidate When receive RequestVote for same term Then should reject it") {
                    underTest.reboot {
                        currentTerm = 0L
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
        val refs = (1..3).associate {
            "N$it" to InProcessTransport("N$it")
        }

        resourceScope {
            val nodes = refs.map { (name, ref) ->
                install(
                    acquire = {
                        RaftTestNode(
                            transport = ref,
                            configuration = RaftMachine.Configuration(
                                id = name,
                                peers = (refs - name).mapValues { it.value.ref }, // all but itself
                                maxLogEntriesPerAppend = 4,
                                leaderElectionTimeout = Timeout(300).jitter(200),
                            )
                        ).apply { stateMachine.start() }
                    },
                    release = { node, _ -> node.stateMachine.stop() }
                )
            }
            val cluster = RaftTestCluster(nodes)

            test("There should be at least one leader") {
                cluster.awaitLeaderElection()
            }
            cluster.awaitLogConvergence()
            test("If the leader crash, a new leader should be elected") {
                val initialLeader = cluster.awaitLeaderElection()
                val initialTerm = initialLeader.persistence.getCurrentTerm()
                logger.info(entry("initial_leader", "term" to initialTerm, "id" to initialLeader.configuration.id))
                initialLeader.transport.disconnect()
                try {
                    "await_leader_change".await.until(
                        { nodes.filter { n -> n.stateMachine.isLeader } },
                        { leaders -> leaders.any { it != initialLeader } }
                    )
                } finally {
                    initialLeader.transport.fix()
                }

                await until { nodes.count { it.stateMachine.isLeader } == 1 }
                val newLeader = cluster.awaitLeaderElection()
                newLeader.persistence.getCurrentTerm() shouldBeGreaterThan initialTerm
            }
            cluster.awaitLogConvergence()
            test("When append is signaled committed, all nodes should have the entry") {
                val leader = cluster.awaitLeaderElection()
                leader.stateMachine.command("Hello World".encodeToByteArray())
                    .join()
                nodes.forEach {
                    val logs = it.persistence.getLogs(0, 10)
                    logger.info(entry("check-log", "logs" to logs))
                    logs.size shouldBe 1
                    logs[0].command shouldBe "Hello World".encodeToByteArray()
                }
            }
            cluster.awaitLogConvergence()
            test("Append a batch of messages in the log") {
                val leader = cluster.awaitLeaderElection()
                val startIndex = leader.commitIndex + 1
                leader.stateMachine.command(
                    "Hey Jude".encodeToByteArray(),
                    "Don't make it bad".encodeToByteArray(),
                    "Take a sad song".encodeToByteArray(),
                    "And make it better".encodeToByteArray()
                ).join()
                nodes.forEach { n ->
                    launch {
                        val messages = n.persistence.getLogs(startIndex, 10)
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
                val committed = leader.stateMachine.command("TV Series: Breaking Bad".encodeToByteArray())
                assertNotNull(committed).join()

                // Disconnect the leader from the cluster
                leader.transport.disconnect()

                // A new leader should be elected
                val newLeader = "await_new_leader".await coUntil oneOf(followers) { it.stateMachine.isLeader }
                logger.info(
                    entry(
                        "new_leader",
                        "id" to newLeader.configuration.id,
                        "term" to newLeader.persistence.getCurrentTerm(),
                        "commit_index" to newLeader.commitIndex
                    )
                )

                // Append a command to the old leader
                val oldLeaderCommitted = leader.stateMachine.command("TV Series: The Wire".encodeToByteArray())
                // And a list of commands to the new leader
                val newLeaderCommitted = newLeader.stateMachine
                    .command("TV Series: The Sopranos".encodeToByteArray(), "TV Series: Sherlock".encodeToByteArray())
                "await_old_leader_appended"
                    .await coUntil { leader.persistence.getLastEntryMetadata().index == firstCmdIndex + 1 }

                withTimeout(3.seconds) {
                    newLeaderCommitted.join()
                }
                logger.info("message_replicated_2_out_of_3")

                logger.info("fix_old_leader")
                leader.transport.fix()
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
                    val logs = n.persistence.getLogs(firstCmdIndex, 4)
                    logger.info(
                        entry(
                            "check-log",
                            "node" to n.configuration.id,
                            "index" to n.commitIndex,
                            "logs" to logs.joinToString { "[T${it.term}|${it.command.decodeToString()}]" })
                    )
                    logs.map { it.command.decodeToString() } shouldBe expectedLogs
                }
            }
            test("Test: Sequential Consistency") {
                val messagePattern = Regex("([A-Z])-([0-9]+)")
                cluster.reboot { }
                val clients = 'A'..'G'
                coroutineScope {
                    // 7 clients sending 3 batches of 4 messages each
                    val clientsSendingEntriesJob = launch {
                        clients.forEach { client ->
                            launch {
                                repeat(3) { b -> // batches
                                    val messages = (1..4).map { "$client-${it * (b + 1)}" }
                                    cluster.append(*messages.toTypedArray())
                                }
                            }
                        }
                    }
                    launch {
                        // Randomly disconnect a node and reboot it
                        do {
                            val cfg = nodes[0].configuration
                            delay(Random.nextLong(100))
                            if (Random.nextBoolean()) {
                                nodes.random().apply {
                                    transport.disconnect()
                                    stateMachine.stop()
                                    val term = persistence.getCurrentTerm()
                                    delay(cfg.heartbeatTimeout.millis * Random.nextLong(5))
                                    reboot { currentTerm = term }
                                    transport.fix()
                                }
                            }
                        } while (clientsSendingEntriesJob.isActive)
                    }
                }
                cluster.awaitLogConvergence()
                cluster.nodes.forEach { n ->
                    val logs = n.persistence.getLogs(1, Int.MAX_VALUE)
                    logger.info(entry("check_log", "node" to n.configuration.id, "logs" to logs.hashCode()))
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

    val logger: Logger = LogManager.getLogger(RaftTestCluster::class.java)

    suspend fun append(vararg commands: String) = append(commands.toList())
    suspend fun append(commands: List<String>): Unit = coroutineScope {
        nodes.asSequence()
            .filter { r -> r.stateMachine.isLeader }
            .forEach { r ->
                launch {
                    try {
                        r.stateMachine.command(commands.map { it.encodeToByteArray() }).await()
                    } catch (e: CancellationException) {
                        if (e.message == "Role has changed") {
                            delay(30)
                            append(commands)
                        }
                    }
                }
            }
    }

    suspend fun reboot(block: InMemoryPersistence.State.() -> Unit) {
        nodes.forEach { it.reboot(block) }
    }

    /**
     * Wait until the cluster has a consistent state, i.e., all nodes have the same commit index
     */
    suspend fun awaitLogConvergence(timeout: Duration = 3.seconds) = withTimeoutOrNull(timeout) {
        var found = false
        while (!found) {
            found = nodes.map { it.persistence.getLastEntryMetadata() }.toSet().size == 1
        }
        logger.info(
            entry("cluster_sync",
                "commit_indexes" to nodes.joinToString { "${it.configuration.id}:${it.commitIndex}" }
            )
        )
    } ?: error("waitClusterSync timeout=$timeout")

    /**
     * Wait until a leader is elected
     */
    suspend fun awaitLeaderElection(timeout: Duration = 3.seconds) = withTimeoutOrNull(timeout) {
        val (_, node) = nodes.map { n -> n.stateMachine.role.map { it to n } }
            .merge()
            .first { (role, _) -> role == RaftRole.LEADER }
        logger.info(
            entry(
                "leader_found",
                "leader" to node.configuration.id,
                "term" to node.persistence.getCurrentTerm()
            )
        )
        node
    } ?: error("waitForLeader timeout=$timeout")
}

class RaftTestNode private constructor(
    val transport: InProcessTransport,
    val configuration: RaftMachine.Configuration<ChannelNodeRef>,
    val persistence: InMemoryPersistence,
    serverState: ServerState
) : ServerState by serverState {
    companion object {
        suspend operator fun invoke(
            transport: InProcessTransport,
            configuration: RaftMachine.Configuration<ChannelNodeRef>
        ) = InMemoryPersistence().let { RaftTestNode(transport, configuration, it, it.getServerState()) }
    }

    val id: NodeId = configuration.id
    private val _transport = LoggingTransport("${configuration.id}-transport", transport)
    private val _persistence = LoggingPersistence("${configuration.id}-persistence", persistence)

    val stateMachine = RaftMachine(configuration, _transport, _persistence)

    suspend fun reboot(block: InMemoryPersistence.State.() -> Unit) {
        stateMachine.stop()
        persistence.load(InMemoryPersistence.State(block))
        stateMachine.start()
    }

    suspend fun restart() {
        stateMachine.stop()
        stateMachine.start()
    }
}

fun meta(index: Index, term: Term) = LogEntryMetadata(index, term)
fun entry(term: Term, command: String) = LogEntry(term, command.encodeToByteArray())
fun interface AppendLog {
    operator fun invoke(entry: Term)
}

class LogBuilderScope {
    private val entries = TreeMap<Index, LogEntry>()

    operator fun String.unaryPlus(): AppendLog =
        AppendLog { entries[entries.size.toLong()] = LogEntry(it, encodeToByteArray()) }

    operator fun LogEntry.unaryPlus() {
        entries[entries.size.toLong()] = this
    }

    companion object {
        fun raftLog(block: LogBuilderScope.() -> Unit) = LogBuilderScope().apply(block).entries
    }
}


/**
 * Make the [RaftTestNode] receive a message from the [InProcessTransport]
 */
suspend fun InProcessTransport.sendTo(to: RaftTestNode, rpc: () -> RaftProtocol) {
    send(to.transport.ref, RaftMessage(ref.id, to.configuration.id, rpc()))
}

private suspend infix fun Transport<*>.shouldReceive(expected: RaftMessage) {
    receive() shouldBe expected
}