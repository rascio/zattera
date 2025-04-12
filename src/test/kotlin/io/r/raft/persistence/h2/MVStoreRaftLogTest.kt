package io.r.raft.persistence.h2

import io.r.raft.persistence.AbstractRaftLogTest
import io.r.raft.persistence.RaftLog
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.Term
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.h2.mvstore.MVStore

class MVStoreRaftLogTest : AbstractRaftLogTest() {

    override fun createLogFromState(term: Term, votedFor: NodeId?, entries: List<LogEntry>): RaftLog {
        val store = MVStore.Builder()
            .open()

        return MVStoreRaftLog(store, Json).also {
            store.openMap<Long, ByteArray>(MVStoreRaftLog.LOG_KEY).run {
                entries.withIndex()
                    .associate { (idx, entry) -> idx.toLong() + 1 to entry }
                    .forEach { (idx, entry) -> put(idx, Json.encodeToString(entry).toByteArray()) }
            }
            store.openMap<String, Any>(MVStoreRaftLog.METADATA_KEY).run {
                put("term", term)
                when (votedFor) {
                    null -> remove("votedFor")
                    else -> put("votedFor", votedFor)
                }
            }
            store.commit()
        }
    }
}
