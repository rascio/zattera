package io.r.raft

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.r.raft.protocol.LogEntryMetadata
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

class ProtocolTest : FunSpec({

    val json = Json

    context("Serialization tests") {
        test("serialize RequestVote") {
            val message = RaftMessage(
                from = "1",
                to = "1",
                rpc = RaftRpc.RequestVote(
                    term = 0,
                    candidateId = "1",
                    lastLog = LogEntryMetadata(0, 0)
                )
            )
            val serialized = json.encodeToString(message)
            val deserialized = json.decodeFromString<RaftMessage>(serialized)
            deserialized shouldBe message
        }
        test("serialize RequestVoteResponse") {
            val message = RaftMessage(
                from = "1",
                to = "1",
                rpc = RaftRpc.RequestVoteResponse(
                    term = 0,
                    voteGranted = true
                )
            )
            val serialized = json.encodeToString(message)
            val deserialized = json.decodeFromString<RaftMessage>(serialized)
            deserialized shouldBe message
        }
        test("serialize AppendEntries") {
            val message = RaftMessage(
                from = "1",
                to = "1",
                rpc = RaftRpc.AppendEntries(
                    term = 0,
                    leaderId = "1",
                    prevLog = LogEntryMetadata(0, 0),
                    entries = emptyList(),
                    leaderCommit = 0
                )
            )
            val serialized = json.encodeToString(message)
            val deserialized = json.decodeFromString<RaftMessage>(serialized)
            deserialized shouldBe message
        }
        test("serialize AppendEntriesResponse") {
            val message = RaftMessage(
                from = "1",
                to = "1",
                rpc = RaftRpc.AppendEntriesResponse(
                    term = 0,
                    success = true,
                    matchIndex = 0,
                    entries = 23
                )
            )
            val serialized = json.encodeToString(message)
            val deserialized = json.decodeFromString<RaftMessage>(serialized)
            deserialized shouldBe message
        }
    }
})