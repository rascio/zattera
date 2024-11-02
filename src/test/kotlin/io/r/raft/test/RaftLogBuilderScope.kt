package io.r.raft.test

import io.r.raft.Index
import io.r.raft.LogEntry
import io.r.raft.NodeId
import io.r.raft.Term
import io.r.raft.log.inmemory.InMemoryRaftLog
import java.util.TreeMap

class RaftLogBuilderScope {
    var term: Term = 0L
    var votedFor: NodeId? = null
    private val log = TreeMap<Index, LogEntry>()

    operator fun String.unaryPlus() {
        log[log.size + 1L] = LogEntry(term, this.encodeToByteArray())
    }

    operator fun LogEntry.unaryPlus() {
        log[log.size + 1L] = this
    }

    companion object {
        fun raftLog(block: RaftLogBuilderScope.() -> Unit) =
            RaftLogBuilderScope()
                .apply(block)
                .let { InMemoryRaftLog(it.log, it.term, it.votedFor) }
    }
}