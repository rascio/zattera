package io.r.raft.machine

import arrow.fx.coroutines.ResourceScope
import arrow.fx.coroutines.resourceScope
import io.kotest.common.ExperimentalKotest
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.r.raft.persistence.RaftLog.Companion.getLastMetadata
import io.r.raft.persistence.inmemory.InMemoryRaftLog
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.LogEntryMetadata
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.test.RaftLogBuilderScope.Companion.raftLog
import io.r.raft.test.failOnTimeout
import io.r.raft.test.installCoroutine
import io.r.raft.transport.RaftCluster
import io.r.raft.transport.inmemory.InMemoryRaftClusterNode.Companion.shouldReceive
import io.r.raft.transport.inmemory.installRaftClusterNetwork
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.ChannelResult
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.logging.log4j.LogManager
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.fail
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalKotest::class, ExperimentalCoroutinesApi::class)
class LeaderTest : FunSpec({

    val logger = LogManager.getLogger(LeaderTest::class.java)

    context("A node in Leader state") {
        context("Upon election").config(timeout = 10.seconds) {
            resourceScope {
                val network = installRaftClusterNetwork()
                val N1 = network.createNode("N1")
                val N2 = network.createNode("N2")
                val (changeRoleFn) = mockRoleTransition()
                val log = raftLog {
                    term = 2
                    +"First Command"
                }
                val leader = installLeader(
                    log = log,
                    changeRoleFn = changeRoleFn,
                    scope = installCoroutine(Dispatchers.IO),
                    raftCluster = RaftCluster("UnderTest", network)
                )

                leader.onEnter()

                val term2NoOpEntry = LogEntry(
                    term = 2,
                    entry = LogEntry.NoOp,
                    id = "UnderTest-NOOP"
                )
                val lastCommittedEntry = log.getMetadata(log.getLastIndex() - 1)!!
                test("Append a NoOp operation to the log to ensure read linearaizability") {
                    log.getEntries(
                        from = log.getLastIndex(),
                        length = 1
                    ) shouldBe listOf(term2NoOpEntry)
                }
                test("send initial empty AppendEntries RPCs (heartbeat) to each server").config(timeout = 1.seconds) {
                    N1 shouldReceive RaftMessage(
                        from = "UnderTest",
                        to = "N1",
                        rpc = RaftRpc.AppendEntries(
                            term = 2,
                            leaderId = "UnderTest",
                            prevLog = lastCommittedEntry,
                            entries = listOf(term2NoOpEntry),
                            leaderCommit = lastCommittedEntry.index
                        )
                    )
                    N2 shouldReceive RaftMessage(
                        from = "UnderTest",
                        to = "N2",
                        rpc = RaftRpc.AppendEntries(
                            term = 2,
                            leaderId = "UnderTest",
                            prevLog = lastCommittedEntry,
                            entries = listOf(term2NoOpEntry),
                            leaderCommit = lastCommittedEntry.index
                        )
                    )
                }
                test("repeat (heartbeat) during idle periods to prevent election timeouts").config(timeout = 3.seconds) {
                    logger.info("repeat (heartbeat) during idle periods to prevent election timeouts")
                    repeat(2) {
                        N1 shouldReceive RaftMessage(
                            from = "UnderTest",
                            to = "N1",
                            rpc = RaftRpc.AppendEntries(
                                term = 2,
                                leaderId = "UnderTest",
                                prevLog = lastCommittedEntry,
                                entries = listOf(term2NoOpEntry),
                                leaderCommit = lastCommittedEntry.index
                            )
                        )
                        N2 shouldReceive RaftMessage(
                            from = "UnderTest",
                            to = "N2",
                            rpc = RaftRpc.AppendEntries(
                                term = 2,
                                leaderId = "UnderTest",
                                prevLog = lastCommittedEntry,
                                entries = listOf(term2NoOpEntry),
                                leaderCommit = lastCommittedEntry.index
                            )
                        )
                    }
                }
                test("send empty entries in heartbeat after receiving positive response") {
                    repeat(2) {
                        leader.onReceivedMessage(
                            RaftMessage(
                                from = "N${it + 1}",
                                to = "UnderTest",
                                rpc = RaftRpc.AppendEntriesResponse(
                                    term = 2,
                                    matchIndex = log.getLastIndex(),
                                    success = true,
                                    entries = 1
                                )
                            )
                        )
                    }
                    N1 shouldReceive RaftMessage(
                        from = "UnderTest",
                        to = "N1",
                        rpc = RaftRpc.AppendEntries(
                            term = 2,
                            leaderId = "UnderTest",
                            prevLog = log.getLastMetadata(),
                            entries = emptyList(),
                            leaderCommit = log.getLastIndex()
                        )
                    )
                    N2 shouldReceive RaftMessage(
                        from = "UnderTest",
                        to = "N2",
                        rpc = RaftRpc.AppendEntries(
                            term = 2,
                            leaderId = "UnderTest",
                            prevLog = log.getLastMetadata(),
                            entries = emptyList(),
                            leaderCommit = log.getLastIndex()
                        )
                    )

                }
            }
        }
        context("If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex") scope@{
            test("If AppendEntries fails because of log inconsistency: decrement nextIndex and retry").config(timeout = 1.seconds) {
                resourceScope {
                    val network = installRaftClusterNetwork()
                    val N1 = network.createNode("N1")
                    val (changeRoleFn) = mockRoleTransition()
                    val log = raftLog {
                        term = 2
                        +"Command1"
                        +"Command2"
                    }
                    val peersState = mutableMapOf(
                        "N1" to PeerState(nextIndex = 2, matchIndex = 1, lastContactTime = 0)
                    )
                    val leader = installLeader(
                        log = log,
                        changeRoleFn = changeRoleFn,
                        scope = installCoroutine(Dispatchers.IO),
                        raftCluster = RaftCluster("UnderTest", network),
                        peersState = peersState
                    )
                    // responding to the initial AppendEntries appending log at index 2, prevLog = 1
                    leader.onReceivedMessage(
                        RaftMessage(
                            from = "N1",
                            to = "UnderTest",
                            // means the log at index 1 (prevLog) is not matching
                            rpc = RaftRpc.AppendEntriesResponse(
                                term = 2,
                                matchIndex = 1,
                                success = false,
                                entries = 0
                            )
                        )
                    )
                    peersState["N1"]!!.nextIndex shouldBe 1
                    peersState["N1"]!!.matchIndex shouldBe 1
                    // so it should try to send the log at index 1, prevLog = 0
                    N1 shouldReceive RaftMessage(
                        from = "UnderTest",
                        to = "N1",
                        rpc = RaftRpc.AppendEntries(
                            term = 2,
                            leaderId = "UnderTest",
                            prevLog = LogEntryMetadata.ZERO,
                            entries = emptyList(),
                            leaderCommit = log.getLastIndex()
                        )
                    )
                }
            }
            test("If successful: update nextIndex and matchIndex for follower").config(timeout = 1.seconds) {
                resourceScope {
                    val network = installRaftClusterNetwork()
                    val N1 = network.createNode("N1")
                    val (changeRoleFn) = mockRoleTransition()
                    val log = raftLog {
                        term = 2
                        +"Command1"
                        +"Command2"
                    }
                    val peersState = mutableMapOf(
                        "N1" to PeerState(nextIndex = 2, matchIndex = 1, lastContactTime = 0)
                    )
                    val leader = installLeader(
                        log = log,
                        changeRoleFn = changeRoleFn,
                        scope = installCoroutine(Dispatchers.IO),
                        raftCluster = RaftCluster("UnderTest", network),
                        peersState = peersState
                    )
                    // responding to the initial AppendEntries appending log at index 2, prevLog = 1
                    leader.onReceivedMessage(
                        RaftMessage(
                            from = "N1",
                            to = "UnderTest",
                            rpc = RaftRpc.AppendEntriesResponse(
                                term = 2,
                                matchIndex = 1,
                                success = true,
                                entries = 0
                            )
                        )
                    )
                    peersState["N1"]!!.nextIndex shouldBe 2
                    peersState["N1"]!!.matchIndex shouldBe 1
                    failOnTimeout("receive response", 1.seconds) {
                        N1 shouldReceive RaftMessage(
                            from = "UnderTest",
                            to = "N1",
                            rpc = RaftRpc.AppendEntries(
                                term = 2,
                                leaderId = "UnderTest",
                                prevLog = log.getMetadata(1)!!,
                                entries = log.getEntries(from = 2, length = 1),
                                leaderCommit = log.getLastIndex()
                            )
                        )
                    }
                }
            }
        }
        test("If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N") {
            resourceScope {
                val commitIndex = 2L
                val N = commitIndex + 3
                // followers are not aligned between each other
                // 5 nodes, N1 confirming up to N will create a quorum for 'N' to be committed
                val peersState = mutableMapOf(
                    "N1" to PeerState(nextIndex = 1, matchIndex = 0, lastContactTime = 0),
                    "N2" to PeerState(nextIndex = N + 2, matchIndex = N + 1, lastContactTime = 0),
                    "N3" to PeerState(nextIndex = 1, matchIndex = 0, lastContactTime = 0),
                    "N4" to PeerState(nextIndex = 1, matchIndex = 0, lastContactTime = 0),
                )

                val (changeRoleFn) = mockRoleTransition()
                val log = raftLog {
                    term = 2
                    // The cluster is not totally consistent
                    // The leader has more logs than the followers
                    repeat(N.toInt() + 3) {
                        +"Command-$it"
                    }
                }
                val network = installRaftClusterNetwork().apply {
                    peersState.keys.forEach { createNode(it) }
                }
                val leader = installLeader(
                    log = log,
                    changeRoleFn = changeRoleFn,
                    scope = installCoroutine(Dispatchers.IO),
                    raftCluster = RaftCluster("UnderTest", network),
                    peersState = peersState,
                    serverState = ServerState(commitIndex, 2L)
                )
                leader.onReceivedMessage(
                    RaftMessage(
                        from = "N1",
                        to = "UnderTest",
                        // Confirmation of N1 is now consistent up to N
                        rpc = RaftRpc.AppendEntriesResponse(
                            term = 2,
                            matchIndex = N,
                            success = true,
                            entries = N.toInt()
                        )
                    )
                )
                leader.serverState.commitIndex shouldBe N
            }
        }
    }

    context("Configuration change") {
        test("A Leader should start the heartbeat for new connected nodes") {
            resourceScope {
                val network = installRaftClusterNetwork()
                val (changeRoleFn) = mockRoleTransition()
                val cluster = RaftCluster("UnderTest", network)
                val leader = installLeader(
                    log = raftLog {  },
                    changeRoleFn = changeRoleFn,
                    scope = installCoroutine(Dispatchers.IO),
                    raftCluster = cluster
                )

                logger.info("Starting leader")

                leader.onEnter()
                // force suspension to be sure the heartbeat is started
                // (yield() also should work, but it depends on the implementation of the Dispatchers)
                delay(5)

                logger.info("Changing configuration")

                cluster.changeConfiguration(
                    LogEntry.ConfigurationChange(
                        new = listOf(
                            RaftRpc.ClusterNode("N1", "localhost", 1234)
                        )
                    )
                )

                val lastCommittedEntry = LogEntryMetadata(index = 1, term = 0)
                val N1 = network.createNode("N1")

                logger.info("Waiting for heartbeat")
                withTimeout(1000) {
                    N1 shouldReceive RaftMessage(
                        from = "UnderTest",
                        to = "N1",
                        rpc = RaftRpc.AppendEntries(
                            term = 0,
                            leaderId = "UnderTest",
                            prevLog = lastCommittedEntry,
                            entries = emptyList(),
                            leaderCommit = 0
                        )
                    )
                }

            }
        }
    }

    context("Edge cases") {
        val heartbeatTimeoutMs: Long = 500
        test("Should stop the heartbeat on exit").config(timeout = (heartbeatTimeoutMs * 5).milliseconds) {
            resourceScope {
                val network = installRaftClusterNetwork()
                val N1 = network.createNode("N1")
                val log = raftLog {
                    term = 5
                    +"First Command"
                    +"Second Command"
                }
                val (changeRoleFn) = mockRoleTransition()
                val scope = installCoroutine(Dispatchers.IO)
                val leader = installLeader(
                    log = log,
                    changeRoleFn = changeRoleFn,
                    scope = scope,
                    raftCluster = RaftCluster("UnderTest", network),
                    heartbeatTimeoutMs = heartbeatTimeoutMs
                )
                leader.onEnter()
                N1.channel.receive()
                leader.onExit()
                var message: ChannelResult<RaftMessage>
                do {
                    message = N1.channel.tryReceive()
                } while (message.isSuccess)
                delay(heartbeatTimeoutMs + 100)
                val newMessages = withTimeoutOrNull(heartbeatTimeoutMs * 3) {
                    N1.channel.receive()
                }
                assertNull(newMessages)
            }
        }
        test("Should ignore a rejected AppendEntriesResponse when it is not matching matchIndex") {
            resourceScope {
                val network = installRaftClusterNetwork()
                val N1 = network.createNode("N1")
                val log = raftLog {
                    term = 5
                    +"First Command"
                    +"Second Command"
                }
                val (changeRoleFn) = mockRoleTransition()
                val leader = installLeader(
                    log = log,
                    changeRoleFn = changeRoleFn,
                    scope = installCoroutine(Dispatchers.IO),
                    raftCluster = RaftCluster("UnderTest", network),
                )
                logger.info("Starting test")
                leader.onEnter()

                val response = withTimeout(1000) {
                    N1.channel.receive().rpc
                }
                assertIs<RaftRpc.AppendEntries>(response)
                assertEquals(2, response.prevLog.index)
                assertEquals(5, response.prevLog.term)
                assertEquals(1, response.entries.size)
                assertEquals(2, response.leaderCommit)
                logger.info("Received initial AppendEntries, responding...")
                leader.onReceivedMessage(
                    RaftMessage(
                        from = "N1",
                        to = "UnderTest",
                        rpc = RaftRpc.AppendEntriesResponse(
                            term = 5,
                            matchIndex = -1,
                            success = false,
                            entries = 0
                        )
                    )
                )
                N1.channel.receive()
                    .let { assertIs<RaftRpc.AppendEntries>(it.rpc) }
                    .run {
                        assertEquals(5, term)
                        assertEquals(2, prevLog.index)
                        assertEquals(5, prevLog.term)
                        assertEquals(1, entries.size)
                        assertEquals(2, leaderCommit)
                    }
            }
        }
        test("A crashed follower restart and starts to sync").config(timeout = 3.seconds) {
            resourceScope {
                val network = installRaftClusterNetwork()
                val N1 = network.createNode("N1")
                val (changeRoleFn) = mockRoleTransition()
                val log = raftLog {
                    term = 12
                    +"Command-1"
                    +"Command-2"
                    +"Command-3"
                    +"Command-4"
                    +"Command-5"
                    term = 17
                }
                val initialCommittedIndex = 5L
                val peersState = mutableMapOf(
                    "N1" to PeerState(
                        nextIndex = initialCommittedIndex,
                        matchIndex = 0,
                        lastContactTime = System.currentTimeMillis() - 200
                    )
                )
                val leader = installLeader(
                    log = log,
                    changeRoleFn = changeRoleFn,
                    scope = installCoroutine(Dispatchers.IO),
                    raftCluster = RaftCluster("UnderTest", network),
                    peersState = peersState,
                    serverState = ServerState(initialCommittedIndex, initialCommittedIndex)
                )
                leader.onEnter()
                val msg = N1.channel.receive().rpc as RaftRpc.AppendEntries
                leader.onExit() // stop the heartbeat
                logger.info("Received message: $msg")
                // responding to the initial AppendEntries appending log at index 2, prevLog = 1
                leader.onReceivedMessage(
                    RaftMessage(
                        from = "N1",
                        to = "UnderTest",
                        // means the log at index 1 (prevLog) is not matching
                        rpc = RaftRpc.AppendEntriesResponse(
                            term = msg.term,
                            matchIndex = msg.prevLog.index,
                            success = false,
                            entries = 0
                        )
                    )
                )
                // server has added the noop, so the peer nextIndex shouldBe (initialCommittedIndex + 1 - 1)
                peersState["N1"]!!.nextIndex shouldBe initialCommittedIndex
                peersState["N1"]!!.matchIndex shouldBe 0
                // so it should try to send the log at index 1, prevLog = 0
                N1 shouldReceive RaftMessage(
                    from = "UnderTest",
                    to = "N1",
                    rpc = RaftRpc.AppendEntries(
                        term = log.getTerm(),
                        leaderId = "UnderTest",
                        prevLog = log.getMetadata(msg.prevLog.index - 1)!!,
                        entries = emptyList(),
                        leaderCommit = log.getLastIndex() - 1 // noop is not committed yet
                    )
                )
            }
        }
    }
    context("HeartbeatCompletion") {
        test("HeartbeatCompletion.await() send heartbeat and notify after receiving majority of responses").config(timeout = 1.seconds) {
            resourceScope {
                val network = installRaftClusterNetwork()
                val nodes = listOf("N1", "N2").map {
                    network.createNode(it)
                }
                val (changeRoleFn) = mockRoleTransition()
                val log = raftLog {
                    term = 1
                }
                val leader = installLeader(
                    log = log,
                    changeRoleFn = changeRoleFn,
                    scope = installCoroutine(Dispatchers.IO),
                    raftCluster = RaftCluster("UnderTest", network),
                    peersState = nodes
                        .associate { it.node.id to PeerState(nextIndex = 1, matchIndex = 0, lastContactTime = 0) }
                        .toMutableMap()
                )
                // await for the quorum to accept the heartbeat
                val notification = launch {
                    leader.heartBeatCompletion.await()
                }
                withTimeout(300) {
                    nodes.forEach { node ->
                        node shouldReceive RaftMessage(
                            from = "UnderTest",
                            to = node.node.id,
                            rpc = RaftRpc.AppendEntries(
                                term = 1,
                                leaderId = "UnderTest",
                                prevLog = LogEntryMetadata.ZERO,
                                entries = emptyList(),
                                leaderCommit = log.getLastIndex()
                            )
                        )
                    }
                }
                leader.onReceivedMessage(
                    RaftMessage(
                        from = "N1",
                        to = "UnderTest",
                        rpc = RaftRpc.AppendEntriesResponse(
                            term = 1,
                            matchIndex = 0,
                            success = true,
                            entries = 0
                        )
                    )
                )
                withTimeout(100) {
                    notification.join()
                }
            }
        }
        test("HeartbeatCompletion.await() send heartbeat and don't notify if majority of cluster didn't responded").config(timeout = 5.seconds) {
            resourceScope {
                val network = installRaftClusterNetwork()
                val nodes = listOf("N1", "N2", "N3", "N4").map {
                    network.createNode(it)
                }
                val (changeRoleFn) = mockRoleTransition()
                val log = raftLog {
                    term = 1
                }
                val leader = installLeader(
                    log = log,
                    changeRoleFn = changeRoleFn,
                    scope = installCoroutine(Dispatchers.IO),
                    raftCluster = RaftCluster("UnderTest", network),
                    peersState = nodes
                        .associate { it.node.id to PeerState(nextIndex = 1, matchIndex = 0, lastContactTime = 0) }
                        .toMutableMap()
                )
                // await for the quorum to accept the heartbeat
                val notification = launch {
                    leader.heartBeatCompletion.await()
                }
                withTimeout(300) {
                    nodes.forEach { node ->
                        node shouldReceive RaftMessage(
                            from = "UnderTest",
                            to = node.node.id,
                            rpc = RaftRpc.AppendEntries(
                                term = 1,
                                leaderId = "UnderTest",
                                prevLog = LogEntryMetadata.ZERO,
                                entries = emptyList(),
                                leaderCommit = log.getLastIndex()
                            )
                        )
                    }
                }
                leader.onReceivedMessage(
                    RaftMessage(
                        from = "N1",
                        to = "UnderTest",
                        rpc = RaftRpc.AppendEntriesResponse(
                            term = 1,
                            matchIndex = 0,
                            success = true,
                            entries = 0
                        )
                    )
                )
                logger.debug("Waiting_for_timeout")
                select {
                    notification.onJoin { fail("Should not notify") }
                    onTimeout(300) { notification.cancelAndJoin() }
                }
            }
        }
        test("await() multiple times before receiving response do not send multiple heartbeats").config(timeout = 5.seconds) {
            resourceScope {
                val network = installRaftClusterNetwork()
                val nodes = listOf("N1", "N2", "N3", "N4", "N5", "N6").map {
                    network.createNode(it)
                }
                val (changeRoleFn) = mockRoleTransition()
                val log = raftLog {
                    term = 1
                }
                val leader = installLeader(
                    log = log,
                    changeRoleFn = changeRoleFn,
                    scope = installCoroutine(Dispatchers.IO),
                    raftCluster = RaftCluster("UnderTest", network),
                    peersState = nodes
                        .associate { it.node.id to PeerState(nextIndex = 1, matchIndex = 0, lastContactTime = 0) }
                        .toMutableMap()
                )
                // make things more complicated
                // some nodes will respond before the second await() and some after
                val (respondBeforeSecondAwait, respondAfterSecondAwait) = nodes.split(2)

                // await for the quorum to accept the heartbeat
                val notification1 = launch {
                    leader.heartBeatCompletion.await()
                }
                withTimeout(300) {
                    respondBeforeSecondAwait.forEach { node ->
                        node shouldReceive RaftMessage(
                            from = "UnderTest",
                            to = node.node.id,
                            rpc = RaftRpc.AppendEntries(
                                term = 1,
                                leaderId = "UnderTest",
                                prevLog = LogEntryMetadata.ZERO,
                                entries = emptyList(),
                                leaderCommit = log.getLastIndex()
                            )
                        )
                    }
                }

                val notification2 = launch {
                    leader.heartBeatCompletion.await()
                }
                withTimeout(300) {
                    respondAfterSecondAwait.forEach { node ->
                        node shouldReceive RaftMessage(
                            from = "UnderTest",
                            to = node.node.id,
                            rpc = RaftRpc.AppendEntries(
                                term = 1,
                                leaderId = "UnderTest",
                                prevLog = LogEntryMetadata.ZERO,
                                entries = emptyList(),
                                leaderCommit = log.getLastIndex()
                            )
                        )
                    }
                }

                nodes.forEach {
                    leader.onReceivedMessage(
                        RaftMessage(
                            from = it.node.id,
                            to = "UnderTest",
                            rpc = RaftRpc.AppendEntriesResponse(
                                term = 1,
                                matchIndex = 0,
                                success = true,
                                entries = 0
                            )
                        )
                    )
                }
                logger.debug("Waiting_for_timeout")
                withTimeout(100) {
                    notification1.join()
                    notification2.join()
                }
                select {
                    nodes.forEach { node ->
                        node.channel.onReceive { fail("no more messages should be sent. message=$it") }
                    }
                    onTimeout(300) { /*everything is fine*/ }
                }
            }
        }

    }

}) {
    companion object {
        private suspend fun ResourceScope.installLeader(
            log: InMemoryRaftLog,
            changeRoleFn: RoleTransition,
            scope: CoroutineScope,
            raftCluster: RaftCluster,
            peersState: MutableMap<NodeId, PeerState> = mutableMapOf(),
            heartbeatTimeoutMs: Long = 700,
            serverState: ServerState? = null
        ) = install(
            acquire = {
                Leader(
                    serverState = serverState ?: ServerState(commitIndex = log.getLastIndex(), lastApplied = 0),
                    log = log,
                    cluster = raftCluster,
                    transitionTo = changeRoleFn,
                    scope = scope,
                    configuration = RaftMachine.Configuration(
                        heartbeatTimeoutMs = heartbeatTimeoutMs
                    ),
                    peers = peersState
                )
            },
            release = { l, _ -> l.onExit() }
        )

        private fun <T> List<T>.split(index: Int) =
            subList(0, index) to subList(index, size)
    }
}
