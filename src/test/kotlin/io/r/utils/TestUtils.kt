package io.r.utils

import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.Term
import io.r.raft.protocol.randomAlphabetic
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

private val client = UUID.randomUUID()
private val sequence = AtomicLong()

fun entry(term: Term, command: String) =
    LogEntry(
        term = term,
        entry = LogEntry.ClientCommand(
            bytes = command.encodeToByteArray(),
            clientId = client,
            sequence = sequence.incrementAndGet()
        ),
        id = randomAlphabetic()
    )