package io.r.raft.persistence

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.r.raft.Index
import io.r.raft.LogEntry
import io.r.raft.LogEntryMetadata
import io.r.raft.Persistence

interface PersistedStateTestAdapter {
    suspend fun createPersistedState(): Persistence
}
abstract class AbstractPersistedStateTest(
    persistedStateTestAdapter: PersistedStateTestAdapter
) : FunSpec({

    context("PersistedState") {
        val persistedState = persistedStateTestAdapter.createPersistedState()

        val firstEntry = LogEntry(3, "First".encodeToByteArray())
        val secondEntry = LogEntry(3, "Second".encodeToByteArray())
        val thirdEntry = LogEntry(3, "Third".encodeToByteArray())
        val fourthEntry = LogEntry(3, "Fourth".encodeToByteArray())

        val firstEntryMetadata = LogEntryMetadata(1, 3)
        val secondEntryMetadata = LogEntryMetadata(2, 3)
        val thirdEntryMetadata = LogEntryMetadata(3, 3)
        val fourthEntryMetadata = LogEntryMetadata(4, 3)

        val expectedLog = sequence {
            yield(firstEntryMetadata to firstEntry)
            yield(secondEntryMetadata to secondEntry)
            yield(thirdEntryMetadata to thirdEntry)
            yield(fourthEntryMetadata to fourthEntry)
        }.toList()
        suspend fun assertLogIsConsistent(upToIndex: Index) {
            println("assertLogIsConsistent $upToIndex")
            expectedLog
                .take(upToIndex.toInt())
                .forEach { (metadata, entry) ->
                    persistedState.getLogMetadata(metadata.index) shouldBe metadata
                    println("${metadata.index} $metadata == ${persistedState.getLogMetadata(metadata.index)}")
                    persistedState.getLogs(metadata.index, 1) shouldBe listOf(entry)
                }
            expectedLog
                .drop(upToIndex.toInt())
                .forEach { (metadata, _) ->
                    val index = metadata.index + upToIndex
                    println("$index null == ${persistedState.getLogMetadata(index)}")
                    persistedState.getLogMetadata(index) shouldBe null
                    persistedState.getLogs(index, 1) shouldBe emptyList()
                }
        }
        test("Initial state is correct") {
            persistedState.getCurrentTerm() shouldBe 0
            persistedState.getServerState().commitIndex shouldBe 0
            persistedState.getServerState().lastApplied shouldBe 0
            persistedState.getLastEntryMetadata() shouldBe LogEntryMetadata.ZERO
            assertLogIsConsistent(0)
            persistedState.getLogMetadata(0) shouldBe LogEntryMetadata.ZERO
            persistedState.getLogMetadata(1) shouldBe null
            persistedState.getLogs(0, 1) shouldBe emptyList()
            persistedState.canVoteFor("N1") shouldBe true
            persistedState.canVoteFor("N2") shouldBe true
        }
        test("Vote and increment term") {
            persistedState.incrementTermAndVote("N1")
            persistedState.getCurrentTerm() shouldBe 1
            persistedState.getServerState().commitIndex shouldBe 0
            persistedState.getServerState().lastApplied shouldBe 0
            assertLogIsConsistent(0)
            persistedState.canVoteFor("N1") shouldBe true
            persistedState.canVoteFor("N2") shouldBe false
        }
        test("Set term") {
            persistedState.setTerm(2)
            persistedState.getCurrentTerm() shouldBe 2
            persistedState.getServerState().commitIndex shouldBe 0
            persistedState.getServerState().lastApplied shouldBe 0
            assertLogIsConsistent(0)
            persistedState.canVoteFor("N1") shouldBe true
            persistedState.canVoteFor("N2") shouldBe true
        }
        test("Vote for") {
            persistedState.voteFor(3, "N1")
            persistedState.getCurrentTerm() shouldBe 3
            persistedState.getServerState().commitIndex shouldBe 0
            persistedState.getServerState().lastApplied shouldBe 0
            assertLogIsConsistent(0)
            persistedState.canVoteFor("N1") shouldBe true
            persistedState.canVoteFor("N2") shouldBe false
        }

        test("Append") {
            val index = persistedState.append(firstEntry)
            index shouldBe 1
            persistedState.getLogs(0,1) shouldBe emptyList()
            persistedState.getLogs(1,1) shouldBe listOf(firstEntry)
            persistedState.getLogs(0,2) shouldBe listOf(firstEntry)
            persistedState.getLogs(2, 1) shouldBe emptyList()
            persistedState.getLastEntryMetadata() shouldBe firstEntryMetadata
            persistedState.getLogMetadata(1) shouldBe firstEntryMetadata
            persistedState.getLogMetadata(2) shouldBe null
            assertLogIsConsistent(1)
            persistedState.getServerState().commitIndex shouldBe 0
            persistedState.getServerState().lastApplied shouldBe 0
        }
        test("Commit") {
            persistedState.commit(1)
            persistedState.getServerState().commitIndex shouldBe 1
            persistedState.getServerState().lastApplied shouldBe 0
            persistedState.getLastEntryMetadata() shouldBe firstEntryMetadata
            assertLogIsConsistent(1)
            persistedState.getLogMetadata(1) shouldBe firstEntryMetadata
            persistedState.getLogs(0, 0) shouldBe emptyList()
            persistedState.getLogs(0, 1) shouldBe emptyList()
            persistedState.getLogs(1, 2) shouldBe listOf(firstEntry)
        }
        test("Apply") {
            persistedState.apply(1)
            persistedState.getServerState().commitIndex shouldBe 1
            persistedState.getServerState().lastApplied shouldBe 1
            persistedState.getLastEntryMetadata() shouldBe firstEntryMetadata
            assertLogIsConsistent(1)
            persistedState.getLogMetadata(1) shouldBe firstEntryMetadata
            persistedState.getLogs(0, 0) shouldBe emptyList()
            persistedState.getLogs(0, 1) shouldBe emptyList()
            persistedState.getLogs(1, 2) shouldBe listOf(firstEntry)
        }
        test("Append second entry") {
            val index = persistedState.append(secondEntry)
            index shouldBe 2
            persistedState.getLogs(0, 1) shouldBe emptyList()
            persistedState.getLogs(0, 2) shouldBe listOf(firstEntry)
            persistedState.getLogs(1, 2) shouldBe listOf(firstEntry, secondEntry)
            persistedState.getLogs(2, 1) shouldBe listOf(secondEntry)
            assertLogIsConsistent(2)
            persistedState.getLastEntryMetadata() shouldBe secondEntryMetadata
            persistedState.getLogMetadata(1) shouldBe firstEntryMetadata
            persistedState.getLogMetadata(2) shouldBe secondEntryMetadata
            persistedState.getLogMetadata(3) shouldBe null
            persistedState.getServerState().commitIndex shouldBe 1
            persistedState.getServerState().lastApplied shouldBe 1
        }
        test("Commit second entry") {
            persistedState.commit(2)
            persistedState.getServerState().commitIndex shouldBe 2
            persistedState.getServerState().lastApplied shouldBe 1
            assertLogIsConsistent(2)
            persistedState.getLastEntryMetadata() shouldBe secondEntryMetadata
            persistedState.getLogMetadata(1) shouldBe firstEntryMetadata
            persistedState.getLogMetadata(2) shouldBe secondEntryMetadata
            persistedState.getLogs(0, 1) shouldBe emptyList()
            persistedState.getLogs(0,2) shouldBe listOf(firstEntry)
            persistedState.getLogs(0,3) shouldBe listOf(firstEntry, secondEntry)
            persistedState.getLogs(1, 2) shouldBe listOf(firstEntry, secondEntry)

        }
        test("Apply second entry") {
            persistedState.apply(2)
            persistedState.getServerState().commitIndex shouldBe 2
            persistedState.getServerState().lastApplied shouldBe 2
            assertLogIsConsistent(2)
            persistedState.getLastEntryMetadata() shouldBe secondEntryMetadata
            persistedState.getLogMetadata(1) shouldBe firstEntryMetadata
            persistedState.getLogMetadata(2) shouldBe secondEntryMetadata
            persistedState.getLogs(0, 1) shouldBe emptyList()
            persistedState.getLogs(0,2) shouldBe listOf(firstEntry)
            persistedState.getLogs(0,3) shouldBe listOf(firstEntry, secondEntry)
            persistedState.getLogs(1, 2) shouldBe listOf(firstEntry, secondEntry)
        }
        test("Append multiple entries") {
            val index = persistedState.append(listOf(thirdEntry, fourthEntry))
            index shouldBe 4
            persistedState.getLogs(0, 4) shouldBe listOf(firstEntry, secondEntry, thirdEntry)
            assertLogIsConsistent(4)
            persistedState.getLogs(2, 10) shouldBe listOf(secondEntry, thirdEntry, fourthEntry)
//            persistedState.getLogs(1..2L) shouldBe listOf(secondEntry)
//            persistedState.getLogs(2..3L) shouldBe listOf(thirdEntry)
//            persistedState.getLogs(3..4L) shouldBe listOf(fourthEntry)
            persistedState.getLastEntryMetadata() shouldBe fourthEntryMetadata
//            persistedState.getLogMetadata(1) shouldBe firstEntryMetadata
//            persistedState.getLogMetadata(2) shouldBe secondEntryMetadata
//            persistedState.getLogMetadata(3) shouldBe thirdEntryMetadata
//            persistedState.getLogMetadata(4) shouldBe fourthEntryMetadata
            persistedState.getLogMetadata(5) shouldBe null
            persistedState.getServerState().commitIndex shouldBe 2
            persistedState.getServerState().lastApplied shouldBe 2
        }
        test("Commit multiple entries") {
            persistedState.commit(4)
            persistedState.getServerState().commitIndex shouldBe 4
            persistedState.getServerState().lastApplied shouldBe 2
            persistedState.getLastEntryMetadata() shouldBe fourthEntryMetadata
            assertLogIsConsistent(4)
            persistedState.getLogMetadata(1) shouldBe firstEntryMetadata
            persistedState.getLogMetadata(2) shouldBe secondEntryMetadata
            persistedState.getLogMetadata(3) shouldBe thirdEntryMetadata
            persistedState.getLogMetadata(4) shouldBe fourthEntryMetadata
            persistedState.getLogs(0, 4) shouldBe listOf(firstEntry, secondEntry, thirdEntry)
            persistedState.getLogs(0, 5) shouldBe listOf(firstEntry, secondEntry, thirdEntry, fourthEntry)
            persistedState.getLogs(0, 1) shouldBe emptyList()
            persistedState.getLogs(1, 2) shouldBe listOf(firstEntry, secondEntry)
            persistedState.getLogs(2, 3) shouldBe listOf(secondEntry, thirdEntry, fourthEntry)
            persistedState.getLogs(4, 1) shouldBe listOf(fourthEntry)
            persistedState.getLogs(5, 1) shouldBe emptyList()
        }
        test("Apply multiple entries") {
            persistedState.apply(4)
            persistedState.getServerState().commitIndex shouldBe 4
            persistedState.getServerState().lastApplied shouldBe 4
            persistedState.getLastEntryMetadata() shouldBe fourthEntryMetadata
            assertLogIsConsistent(4)
            persistedState.getLogMetadata(1) shouldBe firstEntryMetadata
            persistedState.getLogMetadata(2) shouldBe secondEntryMetadata
            persistedState.getLogMetadata(3) shouldBe thirdEntryMetadata
            persistedState.getLogMetadata(4) shouldBe fourthEntryMetadata
            persistedState.getLogs(0, 4) shouldBe listOf(firstEntry, secondEntry, thirdEntry)
            persistedState.getLogs(0, 5) shouldBe listOf(firstEntry, secondEntry, thirdEntry, fourthEntry)
            persistedState.getLogs(2, 2) shouldBe listOf(secondEntry, thirdEntry)
            persistedState.getLogs(0, 1) shouldBe emptyList()
            persistedState.getLogs(1, 2) shouldBe listOf(firstEntry, secondEntry)
            persistedState.getLogs(2, 3) shouldBe listOf(secondEntry, thirdEntry, fourthEntry)
            persistedState.getLogs(4, 1) shouldBe listOf(fourthEntry)
            persistedState.getLogs(5, 1) shouldBe emptyList()
        }

        val entryToBeReplaced = LogEntry(3, "Replaced".encodeToByteArray())
        val fifthEntry = LogEntry(3, "Fifth".encodeToByteArray())

        val fifthEntryMetadata = LogEntryMetadata(5, 3)

        test("append should reject when term is lower") {
            persistedState.append(2, fifthEntryMetadata, listOf(fifthEntry)) shouldBe null
            persistedState.getLogs(0, 5) shouldBe listOf(firstEntry, secondEntry, thirdEntry, fourthEntry)
        }

        test("append should reject when previous entry does not match") {
            // entry: index is larger than log size
            persistedState.append(3, fifthEntryMetadata, listOf(entryToBeReplaced)) shouldBe null
            // entry: term for index not matching
            persistedState.append(3, fourthEntryMetadata.copy(term = fourthEntryMetadata.term - 1), listOf(entryToBeReplaced)) shouldBe null
            persistedState.getLogs(1, 5) shouldBe listOf(firstEntry, secondEntry, thirdEntry, fourthEntry)
        }

        test("append should replace uncommitted entry") {
            persistedState.append(3, fourthEntryMetadata, listOf(entryToBeReplaced)) shouldBe 5
            persistedState.getLogs(1, 5) shouldBe listOf(firstEntry, secondEntry, thirdEntry, fourthEntry, entryToBeReplaced)

            persistedState.append(3, fourthEntryMetadata, listOf(fifthEntry)) shouldBe 5
            persistedState.getLogs(1, 5) shouldBe listOf(firstEntry, secondEntry, thirdEntry, fourthEntry, fifthEntry)
        }
    }
})