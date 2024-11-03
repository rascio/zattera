package io.r.raft.machine

import arrow.fx.coroutines.resourceScope
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.comparables.shouldBeGreaterThan
import io.kotest.matchers.comparables.shouldBeLessThan
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.r.raft.protocol.LogEntryMetadata
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRole
import io.r.raft.protocol.RaftRpc
import io.r.raft.protocol.RaftRpc.AppendEntries
import io.r.raft.protocol.RaftRpc.AppendEntriesResponse
import io.r.raft.test.failOnTimeout
import io.r.raft.test.installCoroutine
import io.r.raft.transport.RaftClusterNode
import io.r.utils.awaitility.await
import io.r.utils.awaitility.coUntil
import io.r.utils.logs.entry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import org.apache.logging.log4j.LogManager
import org.awaitility.Awaitility
import kotlin.random.Random
import kotlin.test.assertNotNull
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

class RaftMachineTest : FunSpec({

    val logger = LogManager.getLogger(RaftMachineTest::class.java)

    Awaitility.setDefaultTimeout(5.seconds.toJavaDuration())
    Awaitility.setDefaultPollDelay(30.milliseconds.toJavaDuration())

    /*
     * Simulate a cluster with 3 nodes
     * Just one raft node is created and stopped at the end
     * N1 and N2 are the transport for two mocked nodes
     * They will not have a state machine attached,
     * but they will be used to send messages to the node under test
     */
    context("Single node tests") {
        context("A node in Leader state") {
            test("Should step down when receiving a RequestVote with a higher term") {
                resourceScope {
                    val network = installRaftClusterNetwork()
                    val N1 = network.createNode("N1")
                    val N2 = network.createNode("N2")

                    val underTest = installRaftTestNode("UnderTest", network)

                    // Elect underTest as leader
                    N1 shouldReceive RaftMessage(
                        from = underTest.id,
                        to = N1.id,
                        rpc = RaftRpc.RequestVote(1L, underTest.id, LogEntryMetadata.ZERO)
                    )
                    N1.sendTo(underTest) {
                        RaftRpc.RequestVoteResponse(1L, true)
                    }
                    N2 shouldReceive RaftMessage(
                        from = underTest.id,
                        to = N2.id,
                        rpc = RaftRpc.RequestVote(1L, underTest.id, LogEntryMetadata.ZERO)
                    )
                    N2.sendTo(underTest) {
                        RaftRpc.RequestVoteResponse(1L, true)
                    }
                    failOnTimeout("timeout waiting for leader", 1.seconds) {
                        underTest.roleChanges.first { it == RaftRole.LEADER }
                    }

                    // Send a RequestVote with a higher term
                    N1.sendTo(underTest) {
                        RaftRpc.RequestVote(underTest.getCurrentTerm() + 1, N1.id, LogEntryMetadata.ZERO)
                    }
                    N1 shouldReceive RaftMessage(
                        from = underTest.id,
                        to = N1.id,
                        rpc = RaftRpc.RequestVoteResponse(underTest.getCurrentTerm() + 1, true)
                    )
                    underTest.roleChanges.first() shouldBe RaftRole.FOLLOWER
                }
            }
            test("Should step down when receiving an AppendEntries with a higher term") {
                resourceScope {
                    val network = installRaftClusterNetwork()
                    val N1 = network.createNode("N1")
                    val N2 = network.createNode("N2")

                    val underTest = installRaftTestNode("UnderTest", network)

                    // Elect underTest as leader
                    N1 shouldReceive RaftMessage(
                        from = underTest.id,
                        to = N1.id,
                        rpc = RaftRpc.RequestVote(1L, underTest.id, LogEntryMetadata.ZERO)
                    )
                    N1.sendTo(underTest) {
                        RaftRpc.RequestVoteResponse(term = 1L, voteGranted = true)
                    }
                    N2 shouldReceive RaftMessage(
                        from = underTest.id,
                        to = N2.id,
                        rpc = RaftRpc.RequestVote(1L, underTest.id, LogEntryMetadata.ZERO)
                    )
                    N2.sendTo(underTest) {
                        RaftRpc.RequestVoteResponse(term = 1L, voteGranted = true)
                    }
                    failOnTimeout("timeout waiting for leader", 1.seconds) {
                        underTest.roleChanges.first { it == RaftRole.LEADER }
                    }
                    N1.receive() // heartbeat

                    // Send an AppendEntries with a higher term
                    N1.sendTo(underTest) {
                        AppendEntries(
                            term = underTest.getCurrentTerm() + 1,
                            leaderId = N1.id,
                            prevLog = LogEntryMetadata.ZERO,
                            entries = emptyList(),
                            leaderCommit = 0L
                        )
                    }
                    N1 shouldReceive RaftMessage(
                        from = underTest.id,
                        to = N1.id,
                        rpc = AppendEntriesResponse(underTest.getCurrentTerm() + 1, 0, true, 0)
                    )
                    underTest.roleChanges.first() shouldBe RaftRole.FOLLOWER
                }
            }
        }
    }


    context("3 Nodes Cluster") {
        // 'refs' is a map with the id and transport for each node
        val refs = (1..3).map { "N$it" }

        test("A leader should be elected") {
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
                assertNotNull(cluster.awaitLeaderElected())
            }
        }
        test("When the leader is disconnected, a new leader should be elected") {
            resourceScope {
                val clusterNetwork = installRaftClusterNetwork()
                val cluster = installRaftTestCluster(
                    scope = installCoroutine(),
                    network = clusterNetwork,
                    nodeIds = refs,
                    config = {
                        RaftMachine.Configuration(
                            maxLogEntriesPerAppend = 4,
                            leaderElectionTimeoutMs = 300L,
                            leaderElectionTimeoutJitterMs = 200,
                        )
                    }
                )
                val initialLeader = cluster.awaitLeaderElected()
                val initialTerm = initialLeader.getCurrentTerm()
                logger.info(entry("initial_leader", "term" to initialTerm, "id" to initialLeader.id))

                initialLeader.disconnect()
                val newLeader = cluster.awaitDifferentLeaderElected(initialLeader.id)
                initialLeader.reconnect()

                newLeader.id shouldNotBe initialLeader.id
                newLeader.getCurrentTerm() shouldBeGreaterThan initialTerm
                cluster.awaitLogConvergence()
                initialLeader.isLeader shouldBe false
            }
        }
        test("When append is signaled committed, it should be present in majority of nodes") {
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
                val leader = cluster.awaitLeaderElected()
                failOnTimeout("timeout waiting for command to be processed", 400.milliseconds) {
                    leader.raftMachine.command("Hello World".encodeToByteArray())
                        .join()
                }
                val stored = cluster.nodes
                    .map { it.log.getLastIndex() }
                    .count { it == 1L }

                stored shouldBeGreaterThan cluster.nodes.size / 2
            }
        }

        test("A disconnected leader should replace non-committed entries with the new leader's log") {
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
                val leader = cluster.awaitLeaderElected()
                val firstCmdIndex = leader.commitIndex + 1

                // Append a command to the leader
                val committed = leader.raftMachine.command("First_entry_for_everyone".encodeToByteArray())
                assertNotNull(committed).join()

                // Disconnect the leader from the cluster
                leader.disconnect()

                // A new leader should be elected
                val newLeader = cluster.awaitDifferentLeaderElected(leader.id)
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
                val oldLeaderCommitted = leader.raftMachine.command("Entry_to_be_replaced".encodeToByteArray())
                // And a list of commands to the new leader
                val newLeaderCommitted = newLeader.raftMachine
                    .command("New_entry_1".encodeToByteArray(), "New_entry_2".encodeToByteArray())
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
                    "First_entry_for_everyone",
                    "New_entry_1",
                    "New_entry_2"
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
                    config = {
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
                val M = 5

                coroutineScope {
                    val clientsSendingEntriesJob = launch {
                        startClientsSendingBatches(C, B, M, cluster)
                    }
                    launch {
                        // Randomly disconnect the leader
                        do {
                            cluster.awaitLeaderElected(timeout = 5.seconds).apply {
                                disconnect()
                                cluster.awaitDifferentLeaderElected(id, timeout = 5.seconds)
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

private fun CoroutineScope.startClientsSendingBatches(
    clients: Int,
    batches: Int,
    messagesPerBatch: Int,
    cluster: RaftTestCluster
) {
    ('A'..('A' + clients)).forEach { client ->
        launch(Dispatchers.IO) {
            val messages = generateSequence(1, Int::inc)
                .map { m -> "$client-$m" }
                .iterator()
            repeat(batches) { // batches
                val batch = messages.asSequence()
                    .take(messagesPerBatch)
                    .map { it.encodeToByteArray() }
                    .toList()
                @Suppress("DeferredResultUnused")
                cluster.awaitLeaderElected(5.seconds).apply {
                    raftMachine.command(batch)
                    delay(40)
                }
            }
        }
    }
}


/**
 * Make the [RaftTestNode] receive a message from the [RaftClusterNode]
 */
suspend inline fun RaftClusterNode.sendTo(to: RaftTestNode, rpc: () -> RaftRpc) {
    send(to.id, rpc())
}

private suspend infix fun RaftClusterNode.shouldReceive(expected: RaftMessage) {
    receive() shouldBe expected
}