package io.r.utils

import io.r.raft.protocol.Index
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.LogEntryMetadata
import io.r.raft.protocol.Term


fun meta(index: Index, term: Term) = LogEntryMetadata(index, term)
fun entry(term: Term, command: String) = LogEntry(term, command.encodeToByteArray())