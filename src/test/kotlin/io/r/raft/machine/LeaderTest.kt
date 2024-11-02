package io.r.raft.machine

import arrow.fx.coroutines.ResourceScope
import arrow.fx.coroutines.resourceScope
import io.kotest.common.ExperimentalKotest
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.test.testCoroutineScheduler
import io.kotest.matchers.shouldBe
import io.r.raft.Index
import io.r.raft.LogEntryMetadata
import io.r.raft.NodeId
import io.r.raft.PeerState
import io.r.raft.RaftMessage
import io.r.raft.RaftProtocol
import io.r.raft.log.RaftLog.Companion.getLastMetadata
import io.r.raft.log.inmemory.InMemoryRaftLog
import io.r.raft.test.RaftLogBuilderScope.Companion.raftLog
import io.r.raft.test.installChannel
import io.r.raft.test.installCoroutine
import io.r.raft.test.shouldReceive
import io.r.raft.transport.inmemory.InMemoryRaftClusterNode
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ChannelResult
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.yield
import org.apache.logging.log4j.LogManager
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalKotest::class)
class LeaderTest : FunSpec({

    val logger = LogManager.getLogger(LeaderTest::class.java)

    context("A node in Leader state") {
        context("Upon election").config(timeout = 10.seconds) {
            resourceScope {
                val N1 = installChannel<RaftMessage>()
                val N2 = installChannel<RaftMessage>()
                val (changeRoleFn) = mockRoleTransition()
                val log = raftLog {
                    term = 2
                    +"First Command"
                }
                val leader = installLeader(
                    log = log,
                    changeRoleFn = changeRoleFn,
                    scope = installCoroutine(Dispatchers.IO),
                    peers = mapOf("N1" to N1, "N2" to N2),
                    commitIndex = log.getLastIndex()
                )
                leader.onEnter()
                test("send initial empty AppendEntries RPCs (heartbeat) to each server").config(timeout = 1.seconds) {
                    N1 shouldReceive RaftMessage(
                        from = "UnderTest",
                        to = "N1",
                        rpc = RaftProtocol.AppendEntries(
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
                        rpc = RaftProtocol.AppendEntries(
                            term = 2,
                            leaderId = "UnderTest",
                            prevLog = log.getLastMetadata(),
                            entries = emptyList(),
                            leaderCommit = log.getLastIndex()
                        )
                    )
                }
                test("repeat (heartbeat) during idle periods to prevent election timeouts").config(timeout = 1.seconds) {
                    N1 shouldReceive RaftMessage(
                        from = "UnderTest",
                        to = "N1",
                        rpc = RaftProtocol.AppendEntries(
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
                        rpc = RaftProtocol.AppendEntries(
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
                    val N1 = installChannel<RaftMessage>()
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
                        peers = mapOf("N1" to N1),
                        peersState = peersState,
                        commitIndex = log.getLastIndex()
                    )
                    // responding to the initial AppendEntries appending log at index 2, prevLog = 1
                    leader.onReceivedMessage(
                        RaftMessage(
                            from = "N1",
                            to = "UnderTest",
                            // means the log at index 1 (prevLog) is not matching
                            rpc = RaftProtocol.AppendEntriesResponse(
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
                        rpc = RaftProtocol.AppendEntries(
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
                    val N1 = installChannel<RaftMessage>()
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
                        peers = mapOf("N1" to N1),
                        peersState = peersState,
                        commitIndex = log.getLastIndex()
                    )
                    // responding to the initial AppendEntries appending log at index 2, prevLog = 1
                    leader.onReceivedMessage(
                        RaftMessage(
                            from = "N1",
                            to = "UnderTest",
                            rpc = RaftProtocol.AppendEntriesResponse(
                                term = 2,
                                matchIndex = 1,
                                success = true,
                                entries = 0
                            )
                        )
                    )
                    peersState["N1"]!!.nextIndex shouldBe 2
                    peersState["N1"]!!.matchIndex shouldBe 1
                    N1 shouldReceive RaftMessage(
                        from = "UnderTest",
                        to = "N1",
                        rpc = RaftProtocol.AppendEntries(
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
                val leader = installLeader(
                    log = log,
                    changeRoleFn = changeRoleFn,
                    scope = installCoroutine(Dispatchers.IO),
                    peers = peersState.mapValues { installChannel() },
                    peersState = peersState,
                    commitIndex = commitIndex
                )
                leader.onReceivedMessage(
                    RaftMessage(
                        from = "N1",
                        to = "UnderTest",
                        // Confirmation of N1 is now consistent up to N
                        rpc = RaftProtocol.AppendEntriesResponse(
                            term = 2,
                            matchIndex = N,
                            success = true,
                            entries = N.toInt()
                        )
                    )
                )
                leader.commitIndex shouldBe N
            }
        }

    }
    val heartbeatTimeoutMs: Long = 500
    test("Should stop the heartbeat on exit").config(timeout = (heartbeatTimeoutMs * 5).milliseconds) {
        resourceScope {
            val N1 = installChannel<RaftMessage>()
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
                peers = mapOf("N1" to N1),
                heartbeatTimeoutMs = heartbeatTimeoutMs
            )
            leader.onEnter()
            N1.receive()
            leader.onExit()
            var message: ChannelResult<RaftMessage>
            do {
                message = N1.tryReceive()
            } while (message.isSuccess)
            delay(heartbeatTimeoutMs + 100)
            val newMessages = withTimeoutOrNull(heartbeatTimeoutMs * 3) {
                N1.receive()
            }
            assertNull(newMessages)
        }
    }
    test("Should ignore a rejected AppendEntriesResponse when it is not matching matchIndex") {
        resourceScope {
            val N1 = installChannel<RaftMessage>()
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
                peers = mapOf("N1" to N1)
            )
            logger.info("Starting test")
            leader.onEnter()

            val response = withTimeout(1000) {
                N1.receive().rpc
            }
            assertIs<RaftProtocol.AppendEntries>(response)
            assertEquals(2, response.prevLog.index)
            assertEquals(5, response.prevLog.term)
            assertEquals(0, response.entries.size)
            assertEquals(2, response.leaderCommit)
            logger.info("Received initial AppendEntries, responding...")
            leader.onReceivedMessage(
                RaftMessage(
                    from = "N1",
                    to = "UnderTest",
                    rpc = RaftProtocol.AppendEntriesResponse(
                        term = 5,
                        matchIndex = -1,
                        success = false,
                        entries = 0
                    )
                )
            )
            N1.receive()
                .let { assertIs<RaftProtocol.AppendEntries>(it.rpc) }
                .run {
                    assertEquals(5, term)
                    assertEquals(2, prevLog.index)
                    assertEquals(5, prevLog.term)
                    assertEquals(0, entries.size)
                    assertEquals(2, leaderCommit)
                }
        }
    }
}) {
    companion object {
        private suspend fun ResourceScope.installLeader(
            log: InMemoryRaftLog,
            changeRoleFn: RoleTransition,
            scope: CoroutineScope,
            peers: Map<NodeId, Channel<RaftMessage>>,
            peersState: MutableMap<NodeId, PeerState> = mutableMapOf(),
            commitIndex: Index? = null,
            heartbeatTimeoutMs: Long = 700
        ) = install(
            acquire = {
                Leader(
                    commitIndex = commitIndex ?: log.getLastIndex(),
                    log = log,
                    clusterNode = InMemoryRaftClusterNode("UnderTest", peers),
                    changeRole = changeRoleFn,
                    scope = scope,
                    configuration = RaftMachine.Configuration(
                        heartbeatTimeoutMs = heartbeatTimeoutMs
                    ),
                    peers = peersState
                )
            },
            release = { l, _ -> l.onExit() }
        )
    }
}
