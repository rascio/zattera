package io.r.raft.machine

import arrow.fx.coroutines.resourceScope
import io.kotest.assertions.withClue
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.comparables.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.r.kv.StringsKeyValueStore
import io.r.raft.client.RaftClusterClient
import io.r.raft.log.StateMachine
import io.r.raft.machine.RaftTestCluster.Companion.append
import io.r.raft.protocol.LogEntry.ClientCommand
import io.r.raft.protocol.LogEntryMetadata
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRole
import io.r.raft.protocol.RaftRpc
import io.r.raft.protocol.RaftRpc.AppendEntries
import io.r.raft.protocol.RaftRpc.AppendEntriesResponse
import io.r.raft.test.failOnTimeout
import io.r.raft.transport.inmemory.InMemoryRaftClusterNode.Companion.sendTo
import io.r.raft.transport.inmemory.InMemoryRaftClusterNode.Companion.shouldReceive
import io.r.raft.transport.inmemory.installRaftClusterNetwork
import io.r.utils.awaitility.atMost
import io.r.utils.awaitility.until
import io.r.utils.decodeToString
import io.r.utils.loggingCtx
import io.r.utils.logs.entry
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.kotlin.logger
import org.awaitility.Awaitility
import kotlin.random.Random
import kotlin.test.assertIs
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

                    val underTest = installRaftTestNode(
                        nodeId = "UnderTest",
                        cluster = network,
                        configuration = RaftMachine.Configuration(
                            leaderElectionTimeoutJitterMs = 100
                        )
                    )

                    val n1Voted = launch {
                        // Elect underTest as leader
                        N1 shouldReceive RaftMessage(
                            from = underTest.id,
                            to = N1.id,
                            rpc = RaftRpc.RequestVote(1L, underTest.id, LogEntryMetadata.ZERO)
                        )
                        N1.sendTo(underTest) {
                            RaftRpc.RequestVoteResponse(1L, true)
                        }
                    }
                    val n2Voted = launch {
                        N2 shouldReceive RaftMessage(
                            from = underTest.id,
                            to = N2.id,
                            rpc = RaftRpc.RequestVote(1L, underTest.id, LogEntryMetadata.ZERO)
                        )
                        N2.sendTo(underTest) {
                            RaftRpc.RequestVoteResponse(1L, true)
                        }
                    }
                    joinAll(n1Voted, n2Voted)
                    failOnTimeout("timeout waiting for leader", 50.milliseconds) {
                        underTest.roleChanges.first { it == RaftRole.LEADER }
                    }
                    N1.channel.receive().rpc.shouldBeInstanceOf<AppendEntries>() // discard heartbeat
                    N2.channel.receive().rpc.shouldBeInstanceOf<AppendEntries>() // discard heartbeat

                    // Send a RequestVote with a higher term
                    val newTerm = underTest.getCurrentTerm() + 1
                    N1.sendTo(underTest) {
                        RaftRpc.RequestVote(newTerm, N1.id, LogEntryMetadata.ZERO)
                    }
                    val waitChangeToFollower = launch {
                        failOnTimeout("timeout waiting for follower", 50.milliseconds) {
                            underTest.roleChanges.first { it == RaftRole.FOLLOWER }
                        }
                    }
                    N1 shouldReceive RaftMessage(
                        from = underTest.id,
                        to = N1.id,
                        rpc = RaftRpc.RequestVoteResponse(newTerm, true)
                    )
                    waitChangeToFollower.join()
                }
            }
            test("Should step down when receiving an AppendEntries with a higher term") {
                resourceScope {
                    logger.info("Should step down when receiving an AppendEntries with a higher term")
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
                    N1.channel.receive() // heartbeat

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
                val clusterNetwork = installRaftClusterNetwork()
                val cluster = installRaftTestCluster(
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
                assertNotNull(cluster.awaitFindLeader())
            }
        }
        test("When the leader is disconnected, a new leader should be elected") {
            resourceScope {
                val clusterNetwork = installRaftClusterNetwork()
                val cluster = installRaftTestCluster(
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
                val initialLeader = cluster.awaitFindLeader()
                val initialTerm = initialLeader.getCurrentTerm()
                logger.info(entry("initial_leader", "term" to initialTerm, "id" to initialLeader.id))

                initialLeader.disconnect()
                val newLeader = cluster.awaitDifferentLeaderElected(initialLeader.id)
                initialLeader.reconnect()

                newLeader.id shouldNotBe initialLeader.id
                newLeader.getCurrentTerm() shouldBeGreaterThan initialTerm
                val becomeFollower = launch {
                    initialLeader.roleChanges
                        .first { it == RaftRole.FOLLOWER }
                }
                cluster.awaitLogConvergence()
                failOnTimeout("timeout waiting for initial leader to step down", 100.milliseconds) {
                    becomeFollower.join()
                }
            }
        }
        test("When append is signaled committed, it should be present in majority of nodes") {
            resourceScope {
                val clusterNetwork = installRaftClusterNetwork()
                val cluster = installRaftTestCluster(
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
                val leader = cluster.awaitFindLeader()
                failOnTimeout("timeout waiting for command to be processed", 400.milliseconds) {
                    leader.raftMachine.command("Hello World".toTestCommand())
                }
                val stored = cluster.nodes
                    .map { it.log.getLastIndex() }
                    .count { it == leader.commitIndex }

                stored shouldBeGreaterThan cluster.nodes.size / 2
            }
        }

        test("A disconnected leader should replace non-committed entries with the new leader's log") {
            resourceScope {
                val clusterNetwork = installRaftClusterNetwork()
                val cluster = installRaftTestCluster(
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
                logger.info("Elect_a_leader")
                val leader = cluster.awaitFindLeader()
                val firstCmdIndex = leader.commitIndex + 1

                logger.info("Append_a_command_to_the_leader")
                leader.raftMachine
                    .command("First_entry_for_everyone".toTestCommand())

                logger.info("Disconnect_the_leader_from_the_cluster")
                leader.disconnect()

                logger.info("A_new_leader_should_be_elected")
                val newLeader = cluster.awaitDifferentLeaderElected(leader.id)
                logger.info(
                    entry(
                        "new_leader",
                        "id" to newLeader.id,
                        "term" to newLeader.getCurrentTerm(),
                        "commit_index" to newLeader.commitIndex
                    )
                )


                launch {
                    logger.info("Append_a_command_to_the_old_leader [${leader.id}]")
                    leader.raftMachine
                        .command("Entry_to_be_replaced".toTestCommand())
                }
                val newLeaderCommitted = launch {
                    logger.info("Append a list of commands to the new leader [${newLeader.id}")
                    newLeader.raftMachine.apply {
                        command("New_entry_1".toTestCommand())
                        command("New_entry_2".toTestCommand())
                    }
                }

                logger.info("Reconnect_the_old_leader")
                "await_old_leader[${leader.id}]_appended" atMost 3.seconds until {
                    leader.getLastEntryMetadata().index > firstCmdIndex
                }

                failOnTimeout("new_leader[${newLeader.id}]_commit", 3.seconds) {
                    newLeaderCommitted.join()
                }
                logger.info("message_replicated_on_2_out_of_3_nodes")

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
                            "logs" to logs.joinToString { "[T${it.term}|${it.entry.decodeToString()}]" })
                    )
                    val actualCmds = logs
                        .filter { it.entry is ClientCommand }
                        .map { it.entry.decodeToString() }
                        .map { Json.decodeFromString<TestCmd>(it) }
                        .map { it.value }
                    actualCmds shouldBe expectedLogs
                }
            }
        }
        test("Test linearaizability of client commands").config(timeout = 90.seconds) {
            resourceScope {
                // C clients sending B batches of M messages each
                val C = 8
                val M = 15

                val clusterNetwork = installRaftClusterNetwork()
                val cluster = installRaftTestCluster(
                    network = clusterNetwork,
                    nodeIds = (1..3).map { "T$it" },
                    config = {
                        RaftMachine.Configuration(
                            maxLogEntriesPerAppend = 4,
                            leaderElectionTimeoutMs = 100L,
                            leaderElectionTimeoutJitterMs = 60,
                            heartbeatTimeoutMs = 30L,
                            pendingCommandsTimeout = 5000L
                        )
                    },
                    stateMachineFactory = { StringsKeyValueStore() }
                )
                val clientConfiguration = RaftClusterClient.Configuration(
                    retry = 30,
                    delay = 200..300L,
                    jitter = 100..200L
                )
                val client = RaftClusterClient(
                    peers = clusterNetwork,
                    configuration = clientConfiguration,
                    contract = StringsKeyValueStore
                )
                logger.info("----- started -----")

                coroutineScope {
                    val clientsSendingEntriesJob = launch {
                        startClientsSendingBatches(C, M) {
                            RaftClusterClient(
                                peers = clusterNetwork,
                                configuration = clientConfiguration.copy(),
                                contract = StringsKeyValueStore
                            )
                        }.joinAll()
                    }
                    launch(CoroutineName("Chaos")) {
                        // Disconnect the leader
                        do {
                            cluster.nodes.forEach { node ->
                                if (node.isLeader()) {
                                    node.disconnect()
                                    delay(node.configuration.heartbeatTimeoutMs * Random.nextLong(5, 10))
                                    node.reconnect()
                                    delay(node.configuration.heartbeatTimeoutMs * Random.nextLong(8, 15))
                                }
                            }
                        } while (clientsSendingEntriesJob.isActive)
                    }.invokeOnCompletion {
                        cluster.nodes.forEach { it.reconnect() }
                    }
                    clientsSendingEntriesJob.join()
                }
                cluster.awaitLogConvergence(10.seconds)
                cluster.dumpRaftLogs(decode = true)
                ('A'..('A' + C)).forEach { key ->
                    val string = StringsKeyValueStore.Get("$key")
                        .toJson()
                        .let { client.query(it) }
                    val response = string.getOrThrow()
                        .toKvResponse()
                    assertIs<StringsKeyValueStore.Value>(response)
                    val list = response
                        .value
                        .split(",")
                        .drop(1) // messages start with a comma

                    withClue("Key $key") {
                        list shouldBe (0 until M).map { "$it" }
                    }
                }
            }
        }
    }
    context("Dynamic cluster") {
        // 'refs' is a map with the id and transport for each node
        val refs = (1..2).map { "N$it" }

        resourceScope {
            val network = installRaftClusterNetwork()
            val cluster = installRaftTestCluster(
                network = network,
                nodeIds = refs,
                config = { _ ->
                    RaftMachine.Configuration(
                        maxLogEntriesPerAppend = 4,
                        leaderElectionTimeoutMs = 300L,
                        leaderElectionTimeoutJitterMs = 200,
                        pendingCommandsTimeout = 30000
                    )
                }
            )
            assertNotNull(cluster.awaitFindLeader())

            cluster.append("First")

            logger.info("first message appended")

            cluster.awaitLogConvergence()

            test("Adding a new node to the cluster should replicate the logs") {

                val N4 = with(cluster) {
                    addRaftTestNode(
                        nodeId = "N4",
                        cluster = network,
                        configuration = RaftMachine.Configuration(
                            maxLogEntriesPerAppend = 4,
                            leaderElectionTimeoutMs = 300L,
                            leaderElectionTimeoutJitterMs = 200,
                        ),
                        start = false
                    )
                }
                N4.start()
                withTimeout(5000) {
                    cluster.awaitLogConvergence()
                }
                N4.log.getEntries(0, Int.MAX_VALUE) shouldBe cluster.nodes[0].log.getEntries(0, Int.MAX_VALUE)
            }
            test("The new node should allow the cluster to elect a new leader and commit entries") {
                val initialLeader = cluster.awaitFindLeader()
                val initialCommit = initialLeader.commitIndex
                initialLeader.disconnect()
                val leader = cluster.awaitDifferentLeaderElected(initialLeader.id)

                leader.raftMachine.command("Second".toTestCommand())
                leader.commitIndex shouldBeGreaterThan initialCommit
                initialLeader.reconnect()
            }
        }

    }
})

private fun CoroutineScope.startClientsSendingBatches(
    clients: Int,
    messages: Int,
    raftClusterClientFactory: () -> RaftClusterClient<StringsKeyValueStore.KVCommand>
) =
    ('A'..('A' + clients)).map { client ->
        launch(Dispatchers.IO) {
            val raftClusterClient = raftClusterClientFactory()
            loggingCtx("client:$client") {
                repeat(messages) { n -> // batches
                    loggingCtx("message-$client$n") {
                        StringsKeyValueStore.Set("$client", "${'$'}{$client},$n")
                            .also { logger.info { entry("Client_Send") } }
                            .let { cmd ->
                                raftClusterClient.request(cmd)
                                    .map { Json.decodeFromString<StringsKeyValueStore.Response>(it.decodeToString()) }
                                    .onFailure {
                                        logger.error {
                                            entry(
                                                "Client_Send_Failure",
                                                "result" to it.message
                                            )
                                        }
                                    }.onSuccess {
                                        logger.info {
                                            entry("Client_Send_Success", "result" to it, "entry" to "$client$n")
                                        }
                                        // Keep for debug if any weird behavior happen again
                                        if (it is StringsKeyValueStore.Value) {
                                            require(it.value.startsWith("key=$client")) { "expected=message-$client$n found=${it.value}" }
                                        }
                                    }
                                    .getOrThrow()
                            }
                    }
                }
            }
        }
    }

private fun ByteArray.toKvResponse() =
    this.decodeToString()
        .let { Json.decodeFromString<StringsKeyValueStore.Response>(it) }

private fun StringsKeyValueStore.Request.toJson() =
    Json.encodeToString(this)
        .encodeToByteArray()
