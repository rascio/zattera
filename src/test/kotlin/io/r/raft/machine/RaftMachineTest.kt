package io.r.raft.machine

import arrow.fx.coroutines.autoCloseable
import arrow.fx.coroutines.resourceScope
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.comparables.shouldBeGreaterThan
import io.kotest.matchers.comparables.shouldBeLessThan
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.r.raft.protocol.LogEntryMetadata
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.protocol.RaftRpc.AppendEntries
import io.r.raft.protocol.RaftRpc.AppendEntriesResponse
import io.r.raft.test.installCoroutine
import io.r.raft.transport.RaftClusterNode
import io.r.utils.awaitility.await
import io.r.utils.awaitility.coUntil
import io.r.utils.entry
import io.r.utils.logs.entry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.logging.log4j.LogManager
import org.awaitility.Awaitility
import kotlin.random.Random
import kotlin.test.assertNotNull
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class RaftMachineTest : FunSpec({

    val logger = LogManager.getLogger(RaftMachineTest::class.java)

    Awaitility.setDefaultTimeout(3.seconds.toJavaDuration())
    Awaitility.setDefaultPollDelay(30.milliseconds.toJavaDuration())

    context("Single node tests") {
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
                        configuration = RaftMachine.Configuration()
                    ).apply { start() }
                },
                release = { n, _ -> n.stop() }
            )

            context("A node in Follower state") {
                test("should grant the vote when receiving an updated RequestVote") {
                    N1.sendTo(underTest) { RaftRpc.RequestVote(1L, "N1", LogEntryMetadata.ZERO) }
                    N1 shouldReceive RaftMessage(
                        from = underTest.id,
                        to = N1.id,
                        rpc = RaftRpc.RequestVoteResponse(term = 1L, voteGranted = true)
                    )
                }
                test("should reply false when the term is smaller") {
                    underTest.reboot { term = 1L }
                    N1.sendTo(underTest) { AppendEntries(0L, N1.id, LogEntryMetadata.ZERO, emptyList(), 0L) }
                    N1 shouldReceive RaftMessage(underTest.id, N1.id, AppendEntriesResponse(1L, 0, false, 0))
                }
                test("should reply reject AppendEntries when the prevLog index is not found") {
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
                    N1 shouldReceive RaftMessage(
                        from = underTest.id,
                        to = N1.id,
                        rpc = AppendEntriesResponse(
                            term = 1L,
                            matchIndex = 1,
                            success = false,
                            entries = 0
                        )
                    )
                }
                test("should reject AppendEntries when the prevLog term does not match") {
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
                    N1 shouldReceive RaftMessage(underTest.id, N1.id, AppendEntriesResponse(1L, 1, false, 0))
                }
                test("should start election for next term after the election timeout") {
                    underTest.reboot { term = 0L }
                    N1 shouldReceive RaftMessage(
                        underTest.id,
                        N1.id,
                        RaftRpc.RequestVote(1L, underTest.id, LogEntryMetadata.ZERO)
                    )
                    N2 shouldReceive RaftMessage(
                        underTest.id,
                        N2.id,
                        RaftRpc.RequestVote(1L, underTest.id, LogEntryMetadata.ZERO)
                    )
                }
            }
            context("RequestVote RPC") {

                test("Given a node in candidate state When it receives a RequestVote for same term Then should reject it") {
                    underTest.reboot {
                        term = 0L
                    }
                    N1 shouldReceive RaftMessage(
                        underTest.id,
                        N1.id,
                        RaftRpc.RequestVote(1L, underTest.id, LogEntryMetadata.ZERO)
                    )
                    N1.sendTo(underTest) {
                        RaftRpc.RequestVote(
                            1L,
                            N1.id,
                            LogEntryMetadata.ZERO
                        )
                    }
                    N1.receive() shouldBe RaftMessage(underTest.id, N1.id, RaftRpc.RequestVoteResponse(1L, false))
                }
            }
        }
    }


    context("3 Nodes Cluster") {
        // 'refs' is a map with the id and transport for each node
        val refs = (1..3).map { "N$it" }

        test("There should be at least one leader") {
            resourceScope {
                val scope = installCoroutine()
                val clusterNetwork = installRaftClusterNetwork()
                val cluster = installRaftTestCluster(
                    scope = scope,
                    network = clusterNetwork,
                    nodeIds = refs,
                    config = { _ ->
                        RaftMachine.Configuration(
                            maxLogEntriesPerAppend = 4,
                            leaderElectionTimeoutMs = 300L,
                            leaderElectionTimeoutJitterMs = 200,
                        )
                    }
                )
                cluster.awaitLeader()
            }
        }
        test("If the leader is disconnected, a new leader should be elected") {
            resourceScope {
                val scope = installCoroutine()
                val clusterNetwork = installRaftClusterNetwork()
                val cluster = installRaftTestCluster(
                    scope = scope,
                    network = clusterNetwork,
                    nodeIds = refs,
                    config = { _ ->
                        RaftMachine.Configuration(
                            maxLogEntriesPerAppend = 4,
                            leaderElectionTimeoutMs = 300L,
                            leaderElectionTimeoutJitterMs = 200,
                        )
                    }
                )
                val initialLeader = cluster.awaitLeader()
                val initialTerm = initialLeader.getCurrentTerm()
                logger.info(entry("initial_leader", "term" to initialTerm, "id" to initialLeader.id))
                initialLeader.disconnect()
                val newLeader = try {
                    cluster.awaitLeaderChangeFrom(initialLeader.id)
                } finally {
                    initialLeader.reconnect()
                }
                newLeader shouldNotBe initialLeader
                newLeader.getCurrentTerm() shouldBeGreaterThan initialTerm
                cluster.awaitLogConvergence()
            }
        }
        test("When append is signaled committed, it should be saved in majority of nodes") {
            resourceScope {
                val scope = installCoroutine()
                val clusterNetwork = installRaftClusterNetwork()
                val cluster = installRaftTestCluster(
                    scope = scope,
                    network = clusterNetwork,
                    nodeIds = refs,
                    config = { _ ->
                        RaftMachine.Configuration(
                            maxLogEntriesPerAppend = 4,
                            leaderElectionTimeoutMs = 300,
                            leaderElectionTimeoutJitterMs = 100,
                        )
                    }
                )
                val leader = cluster.awaitLeader()
                withTimeoutOrNull(100.milliseconds) {
                    leader.raftMachine.command("Hello World".encodeToByteArray())
                        .join()
                } ?: error("timeout waiting for command to be processed")
                val stored = cluster.nodes
                    .map { it.log.getLastIndex() }
                    .count { it == 1L }

                stored shouldBeGreaterThan cluster.nodes.size / 2
            }
        }
        test("Append a batch of messages in the log") {
            resourceScope {
                val scope = installCoroutine()
                val clusterNetwork = installRaftClusterNetwork()
                val cluster = installRaftTestCluster(
                    scope = scope,
                    network = clusterNetwork,
                    nodeIds = refs,
                    config = { _ ->
                        RaftMachine.Configuration(
                            maxLogEntriesPerAppend = 4,
                            leaderElectionTimeoutMs = 300L,
                            leaderElectionTimeoutJitterMs = 200,
                        )
                    }
                )
                val leader = cluster.awaitLeader()
                val startIndex = leader.commitIndex + 1
                withTimeoutOrNull(5.seconds) {
                    leader.raftMachine.command(
                        "Hey Jude".encodeToByteArray(),
                        "Don't make it bad".encodeToByteArray(),
                        "Take a sad song".encodeToByteArray(),
                        "And make it better".encodeToByteArray()
                    ).join()
                } ?: error("timeout waiting for commands to be processed")
                cluster.nodes.forEach { n ->
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
        }

        test("Node replace uncommitted content when receiving an append for a previous entry") {
            resourceScope {
                val scope = installCoroutine()
                val clusterNetwork = installRaftClusterNetwork()
                val cluster = installRaftTestCluster(
                    scope = scope,
                    network = clusterNetwork,
                    nodeIds = refs,
                    config = { _ ->
                        RaftMachine.Configuration(
                            maxLogEntriesPerAppend = 4,
                            leaderElectionTimeoutMs = 300L,
                            leaderElectionTimeoutJitterMs = 200,
                        )
                    }
                )
                // Elect a leader
                val leader = cluster.awaitLeader()
                val firstCmdIndex = leader.commitIndex + 1

                // Append a command to the leader
                val committed = leader.raftMachine.command("TV Series: Breaking Bad".encodeToByteArray())
                assertNotNull(committed).join()

                // Disconnect the leader from the cluster
                leader.disconnect()

                // A new leader should be elected
                val newLeader = cluster.awaitLeaderChangeFrom(leader.id)
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
                    .await coUntil { leader.getLastEntryMetadata().index > firstCmdIndex }

                withTimeout(3.seconds) { newLeaderCommitted.join() }
                logger.info("message_replicated_on_2_out_of_3_nodes")

                logger.info("fix_old_leader")
                leader.reconnect()
                cluster.awaitLogConvergence()
                // Rejection of client commands is not implemented yet, who knows if it will ever be
//                assertThrows<CancellationException>("The bad commit on the old leader should be rejected") {
//                    withTimeout(4.seconds) {
//                        oldLeaderCommitted.join()
//                    }
//                }
                // after the old leader re-join the cluster, it should have the same logs as the new leader
                val expectedLogs = listOf(
                    "TV Series: Breaking Bad",
                    "TV Series: The Sopranos",
                    "TV Series: Sherlock"
                )
                cluster.nodes.forEach { n ->
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
        }
        test("In case of network partition the order of messages should be preserved") {
            resourceScope {
                val scope = installCoroutine()
                val clusterNetwork = installRaftClusterNetwork()
                val cluster = installRaftTestCluster(
                    scope = scope,
                    network = clusterNetwork,
                    nodeIds = refs,
                    config = { _ ->
                        RaftMachine.Configuration(
                            maxLogEntriesPerAppend = 4,
                            leaderElectionTimeoutMs = 200L,
                            leaderElectionTimeoutJitterMs = 100,
                        )
                    }
                )

                val messagePattern = Regex("([A-Z])-([0-9]+)")
                // C clients sending B batches of M messages each
                val C = 8
                val B = 15
                val M = 3

                coroutineScope {
                    val clientsSendingEntriesJob = launch {
                        ('A'..('A' + C)).forEach { client ->
                            launch(Dispatchers.IO) {
                                val messages = generateSequence(1, Int::inc)
                                    .map { m -> "$client-$m" }
                                    .iterator()
                                repeat(B) { // batches
                                    val batch = messages.asSequence()
                                        .take(M)
                                        .map { it.encodeToByteArray() }
                                        .toList()
                                    @Suppress("DeferredResultUnused")
                                    cluster.awaitLeader(5.seconds).apply {
                                        raftMachine.command(batch)
                                        delay(40)
                                    }
                                }
                            }
                        }
                    }
                    launch {
                        // Randomly disconnect a node
                        do {
                            cluster.awaitLeader(5.seconds).apply {
                                disconnect()
                                cluster.awaitLeaderChangeFrom(id, timeout = 5.seconds)
                                delay(configuration.heartbeatTimeoutMs * Random.nextLong(3, 10))
                                reconnect()
                            }
                        } while (clientsSendingEntriesJob.isActive)
                    }
                }
                cluster.awaitLogConvergence(10.seconds)
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


/**
 * Make the [RaftTestNode] receive a message from the [RaftClusterNode]
 */
suspend fun RaftClusterNode.sendTo(to: RaftTestNode, rpc: () -> RaftRpc) {
    send(to.id, rpc())
}

private suspend infix fun RaftClusterNode.shouldReceive(expected: RaftMessage) {
    receive() shouldBe expected
}