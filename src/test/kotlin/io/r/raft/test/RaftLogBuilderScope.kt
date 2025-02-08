package io.r.raft.test

import io.r.raft.protocol.Index
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.Term
import io.r.raft.log.inmemory.InMemoryRaftLog
import io.r.raft.protocol.randomAlphabetic
import java.util.TreeMap

class RaftLogBuilderScope {
    var term: Term = 0L
    var votedFor: NodeId? = null
    private val log = TreeMap<Index, LogEntry>()

    operator fun String.unaryPlus() {
        log[log.size + 1L] = LogEntry(term, LogEntry.ClientCommand(this.encodeToByteArray(), randomAlphabetic()), randomAlphabetic())
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