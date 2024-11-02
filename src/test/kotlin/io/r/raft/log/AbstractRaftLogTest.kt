package io.r.raft.log

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.LogEntryMetadata
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.Term
import io.r.utils.entry
import kotlin.random.Random

@Suppress("LeakingThis")
abstract class AbstractRaftLogTest : FunSpec() {
    init {
        context("getLastIndex") {
            test("should return 0 when the log is empty") {
                val log = createLogFromState()
                log.getLastIndex() shouldBe 0L
            }
            test("should return N when the log has N entries") {
                val N = Random.nextLong(100)
                val log = createLogFromState(entries = List(N.toInt()) { entry(1L, "Test_$it") })
                log.getLastIndex() shouldBe N
            }
        }
        context("Term") {
            test("should return 0 when the log is empty") {
                val log = createLogFromState()
                log.getTerm() shouldBe 0L
            }
            test("should return the term previously set") {
                val log = createLogFromState()
                log.setTerm(1L)
                log.getTerm() shouldBe 1L
            }
            test("should return the term previously set after multiple changes") {
                val log = createLogFromState()
                log.setTerm(1L)
                log.setTerm(2L)
                log.setTerm(3L)
                log.getTerm() shouldBe 3L
            }
            test("should reset votedFor after setting the term") {
                val log = createLogFromState(votedFor = "Test")
                check(log.getVotedFor() == "Test") { "Precondition failed" }
                log.setTerm(1L)
                log.getVotedFor() shouldBe null
            }
        }
        context("getMetadata") {
            test("should return ZERO for index 0") {
                val log = createLogFromState()
                log.getMetadata(0L) shouldBe LogEntryMetadata.ZERO
            }
            test("should return null for index greater than the last index") {
                val log = createLogFromState()
                log.getMetadata(1L) shouldBe null
            }
            test("should return the metadata with correct term for the index") {
                val term = Random.nextLong(10, 50)
                val log = createLogFromState(entries = listOf(entry(term, "Test")))
                log.getMetadata(1L) shouldBe LogEntryMetadata(1L, term)
            }
        }
        context("getEntries") {
            test("should return empty list when the log is empty") {
                val log = createLogFromState()
                log.getEntries(1L, 1) shouldBe emptyList()
            }
            test("should return empty list when the from index is greater than the last index") {
                val log = createLogFromState(entries = listOf(entry(1L, "Test")))
                log.getEntries(2L, 1) shouldBe emptyList()
            }
            test("should return empty list when the length is 0") {
                val log = createLogFromState(entries = listOf(entry(1L, "Test")))
                log.getEntries(1L, 0) shouldBe emptyList()
            }
            test("should return the entries from the given index") {
                val entries = List(10) { entry(1L, "Test_$it") }
                val log = createLogFromState(entries = entries)
                log.getEntries(1L, 10) shouldBe entries
            }
            test("should return the entries from the given index with the given length") {
                val entries = List(10) { entry(it.toLong(), "Test_$it") }
                val log = createLogFromState(entries = entries)
                log.getEntries(8L, 2) shouldBe listOf(entries[7], entries[8])
            }
            test("should return the sublit of entries from the given index") {
                val N = 10
                val F = 3
                val size = 4
                val entries = List(N) { entry(it.toLong(), "Test_$it") }
                val log = createLogFromState(entries = entries)
                log.getEntries(F.toLong(), size) shouldBe entries[F..<F + size]
            }
        }
        context("Append") {
            test("should return 1 when append 1 entry to an empty log") {
                val log = createLogFromState()
                log.append(0L, listOf(entry(1L, "Test"))) shouldBe 1L
            }
            test("should return N when append N entries to an empty log") {
                val N = Random.nextLong(10, 50)
                val log = createLogFromState()
                log.append(0L, List(N.toInt()) { entry(1L, "Test_$it") }) shouldBe N
            }
            test("should return N+M when append N entries to a log with M entries") {
                val M = Random.nextLong(10, 50)
                val N = Random.nextLong(10, 50)
                val log = createLogFromState(entries = List(M.toInt()) { entry(it.toLong(), "Test_$it") })
                check(log.getLastIndex() == M) { "Precondition failed" }
                log.append(log.getLastIndex(), List(N.toInt()) { entry(1L, "Test_${it + M}") }) shouldBe N + M
            }
            test("should replace the entries and clean all the logs after it when the previous index is smaller than the last index") {
                val entries = List(10) { entry(1L, "Test_$it") }
                val log = createLogFromState(entries = entries)
                check(log.getLastIndex() == 10L) { "Precondition failed" }
                val newEntries = List(3) { entry(2L, "New_$it") }
                log.append(4L, newEntries) shouldBe 7
                log.getLastIndex() shouldBe 7
                log.getEntries(1, 10) shouldBe entries[1..4] + newEntries
            }
        }
        context("votedFor") {
            test("should return null when no node was voted for") {
                val log = createLogFromState()
                log.getVotedFor() shouldBe null
            }
            test("should return the node that was voted for") {
                val log = createLogFromState(votedFor = "Test")
                log.getVotedFor() shouldBe "Test"
            }
            test("should return the node that was voted for after multiple changes") {
                val log = createLogFromState(votedFor = "Test")
                log.setVotedFor("Test2")
                log.getVotedFor() shouldBe "Test2"
            }
        }
    }

    abstract fun createLogFromState(term: Term = 0L, votedFor: NodeId? = null, entries: List<LogEntry> = emptyList()): RaftLog

    private operator fun List<LogEntry>.get(range: IntRange) = subList(range.first - 1, range.last)
}