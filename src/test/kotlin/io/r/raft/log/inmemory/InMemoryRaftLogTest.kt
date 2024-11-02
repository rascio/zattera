package io.r.raft.log.inmemory

import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.Term
import io.r.raft.log.AbstractRaftLogTest
import io.r.raft.log.RaftLog
import java.util.TreeMap

class InMemoryRaftLogTest : AbstractRaftLogTest() {
    override fun createLogFromState(term: Term, votedFor: NodeId?, entries: List<LogEntry>): RaftLog {
        val log = entries.withIndex()
            .associate { (idx, entry) -> idx.toLong() + 1 to entry }
            .let(::TreeMap)
        return InMemoryRaftLog(term = term, votedFor = votedFor, log = log)
    }
}
