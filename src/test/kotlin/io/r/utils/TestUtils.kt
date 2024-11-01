package io.r.utils

import io.r.raft.Index
import io.r.raft.LogEntry
import io.r.raft.LogEntryMetadata
import io.r.raft.Term


fun meta(index: Index, term: Term) = LogEntryMetadata(index, term)
fun entry(term: Term, command: String) = LogEntry(term, command.encodeToByteArray())