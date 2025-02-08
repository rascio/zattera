package io.r.utils

import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.Term
import io.r.raft.protocol.randomAlphabetic

fun entry(term: Term, command: String) =
    LogEntry(term, LogEntry.ClientCommand(command.encodeToByteArray()), randomAlphabetic())