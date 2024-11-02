package io.r.raft.machine

import arrow.fx.coroutines.resourceScope
import io.kotest.core.spec.style.FunSpec
import io.r.raft.protocol.LogEntryMetadata
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import io.r.raft.protocol.RaftRpc.AppendEntries
import io.r.raft.protocol.RaftRpc.AppendEntriesResponse
import io.r.raft.protocol.RaftRole
import io.r.raft.log.RaftLog.Companion.getLastMetadata
import io.r.raft.test.RaftLogBuilderScope.Companion.raftLog
import io.r.raft.test.shouldReceive
import io.r.raft.transport.inmemory.InMemoryRaftClusterNode
import kotlinx.coroutines.channels.Channel
import kotlin.time.Duration.Companion.seconds

class FollowerTest : FunSpec({

    timeout = 10.seconds.inWholeMilliseconds

    context("A node in Follower state") {
        resourceScope {
            val N1 = install(
                acquire = { Channel<RaftMessage>(capacity = Channel.UNLIMITED) },
                release = { c, _ -> c.close() }
            )
            val N2 = install(
                acquire = { Channel<RaftMessage>(capacity = Channel.UNLIMITED) },
                release = { c, _ -> c.close() }
            )
            val clusterNode = InMemoryRaftClusterNode("UnderTest", mapOf("N1" to N1, "N2" to N2))
            val log = raftLog {
                term = 0L
                +"Hello World"
                term = 1L
            }
            val (changeRoleFn, probe) = mockRoleTransition()
            val underTest = Follower(
                commitIndex = 0,
                log = log,
                clusterNode = clusterNode,
                changeRole = changeRoleFn,
                configuration = RaftMachine.Configuration(
                    leaderElectionTimeoutMs = 1000
                )
            ).apply { onEnter() }

            context("respond to RPCs from candidates and leaders") {

                test("should grant the vote when receiving an updated RequestVote") {
                    val electionTerm = log.getTerm()
                    underTest.onReceivedMessage(
                        RaftMessage(
                            from = "N1",
                            to = "UnderTest",
                            rpc = RaftRpc.RequestVote(
                                term = electionTerm,
                                candidateId = "N1",
                                lastLog = log.getLastMetadata()
                            )
                        )
                    )
                    N1 shouldReceive RaftMessage(
                        from = "UnderTest",
                        to = "N1",
                        rpc = RaftRpc.RequestVoteResponse(
                            term = electionTerm,
                            voteGranted = true
                        )
                    )
                }
                test("should not grant the vote for another node when receiving a RequestVote if already voted") {
                    log.setVotedFor("N1")
                    val electionTerm = log.getTerm()
                    underTest.onReceivedMessage(
                        RaftMessage(
                            from = "N2",
                            to = "UnderTest",
                            rpc = RaftRpc.RequestVote(
                                term = electionTerm,
                                candidateId = "N2",
                                lastLog = log.getLastMetadata()
                            )
                        )
                    )
                    N2 shouldReceive RaftMessage(
                        from = "UnderTest",
                        to = "N2",
                        rpc = RaftRpc.RequestVoteResponse(
                            term = electionTerm,
                            voteGranted = false
                        )
                    )
                    log.setTerm(log.getTerm())
                }

                test("should reply false when the term is smaller") {
                    val prevLog = log.getLastMetadata()
                    val smallerTerm = log.getTerm() - 1
                    underTest.onReceivedMessage(
                        RaftMessage(
                            from = "N1",
                            to = "UnderTest",
                            rpc = AppendEntries(
                                term = smallerTerm,
                                leaderId = "N1",
                                prevLog = prevLog,
                                entries = emptyList(),
                                leaderCommit = 0L
                            )
                        )
                    )
                    N1 shouldReceive RaftMessage(
                        from = "UnderTest",
                        to = "N1",
                        rpc = AppendEntriesResponse(
                            term = log.getTerm(),
                            matchIndex = prevLog.index,
                            success = false,
                            entries = 0
                        )
                    )
                }
                test("should reply reject AppendEntries when the prevLog index is not found") {
                    val missingLog = LogEntryMetadata(index = log.getLastIndex() + 1, term = 1)
                    val term = log.getTerm()
                    underTest.onReceivedMessage(
                        RaftMessage(
                            from = "N1",
                            to = "UnderTest",
                            rpc = AppendEntries(
                                term = term,
                                leaderId = "N1",
                                prevLog = missingLog,
                                entries = emptyList(),
                                leaderCommit = 0L
                            )
                        )
                    )
                    N1 shouldReceive RaftMessage(
                        from = "UnderTest",
                        to = "N1",
                        rpc = AppendEntriesResponse(
                            term = term,
                            matchIndex = missingLog.index,
                            success = false,
                            entries = 0
                        )
                    )
                }
                test("should reject AppendEntries when the prevLog term does not match") {
                    val currentTerm = log.getTerm()
                    val prevLogWithWrongTerm = log.getLastMetadata().run {
                        copy(term = term + 1)
                    }
                    underTest.onReceivedMessage(
                        RaftMessage(
                            from = "N1",
                            to = "UnderTest",
                            rpc = AppendEntries(
                                term = currentTerm,
                                leaderId = "N1",
                                prevLog = prevLogWithWrongTerm,
                                entries = emptyList(),
                                leaderCommit = 0L
                            )
                        )
                    )

                    N1 shouldReceive RaftMessage(
                        from = "UnderTest",
                        to = "N1",
                        rpc = AppendEntriesResponse(
                            term = currentTerm,
                            matchIndex = prevLogWithWrongTerm.index,
                            success = false,
                            entries = 0
                        )
                    )
                }
            }

            test("If election timeout elapses, should transition to candidate") {

                underTest.onTimeout()

                probe shouldReceive RaftRole.CANDIDATE

//                N1 shouldReceive RaftMessage(
//                    from = "UnderTest",
//                    to = "N1",
//                    rpc = RaftProtocol.RequestVote(term = 1L, candidateId = "UnderTest", lastLog = log.getLastMetadata())
//                )
//                N2 shouldReceive RaftMessage(
//                    from = "UnderTest",
//                    to = "N1",
//                    rpc = RaftProtocol.RequestVote(term = 1L, candidateId = "UnderTest", lastLog = log.getLastMetadata())
//                )
            }
        }
    }
})
