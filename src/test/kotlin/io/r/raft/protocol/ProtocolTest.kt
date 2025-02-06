package io.r.raft.protocol

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

class ProtocolTest : FunSpec({

    val json = Json

    context("Serialization tests") {
        val cases = AtomicInteger()
        generateMessages().forEach { message ->
            val serialized = json.encodeToString(message)
            val deserialized = json.decodeFromString<RaftMessage>(serialized)
            deserialized shouldBe message
            cases.incrementAndGet()
        }
        println("Cases: ${cases.get()}")
    }
})


private val logEntryMetadata = LogEntryMetadata(
    index = Random.nextLong(from = 0, until = 100),
    term = Random.nextLong(from = 0, until = 100)
)

private val clusterNode = RaftRpc.ClusterNode(
    id = randomAlphabetic(),
    host = randomAlphabetic(),
    port = Random.nextInt(0, 10000)
)

private val logEntries = LogEntry.Entry::class
    .sealedSubclasses
    .flatMap { type ->
        when (type) {
            LogEntry.ClientCommand::class -> listOf(
                    LogEntry.ClientCommand(
                        bytes = randomAlphabetic().encodeToByteArray()
                    )
            )

            LogEntry.ConfigurationChange::class -> listOf(
                    LogEntry.ConfigurationChange(
                        new = listOf(clusterNode),
                        old = listOf(clusterNode)
                    )
            )

            else -> error("Unknown type")
        }
    }
    .map {
        LogEntry(
            term = Random.nextLong(0, 100),
            entry = it
        )
    }

private fun generateMessages() = RaftRpc::class
    .sealedSubclasses
    .flatMap { type ->
        when (type) {
            RaftRpc.RequestVote::class -> listOf(
                RaftRpc.RequestVote(
                    term = Random.nextLong(0, 100),
                    candidateId = randomAlphabetic(),
                    lastLog = logEntryMetadata
                )
            )

            RaftRpc.RequestVoteResponse::class -> listOf(
                RaftRpc.RequestVoteResponse(
                    term = Random.nextLong(),
                    voteGranted = Random.nextBoolean()
                )
            )

            RaftRpc.AppendEntries::class -> logEntries.map { logEntry ->
                RaftRpc.AppendEntries(
                    term = Random.nextLong(0, 10000),
                    leaderId = randomAlphabetic(),
                    prevLog = logEntryMetadata,
                    entries = listOf(logEntry),
                    leaderCommit = Random.nextLong(0, 100)
                )
            }


            RaftRpc.AppendEntriesResponse::class -> listOf(
                RaftRpc.AppendEntriesResponse(
                    term = Random.nextLong(0, 10000),
                    matchIndex = Random.nextLong(0, 100),
                    success = Random.nextBoolean(),
                    entries = Random.nextInt(0, 100)
                )
            )

            else -> error("Unknown type")
        }
    }
    .map { rpc ->
        RaftMessage(
            from = randomAlphabetic(),
            to = randomAlphabetic(),
            rpc = rpc
        )
    }

private fun randomAlphabetic() = (0..Random.nextInt(until = 16))
    .map { Random.nextInt(from = 'A'.code, until = 'Z'.code).toChar() }
    .joinToString("")