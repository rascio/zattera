package io.r.raft.persistence.h2

import io.r.raft.persistence.RaftLog
import io.r.raft.persistence.RaftLog.Companion.AppendResult
import io.r.raft.protocol.Index
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.LogEntryMetadata
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.Term
import io.r.utils.logs.entry
import io.r.utils.murmur128
import io.r.utils.toHex
import kotlinx.serialization.StringFormat
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.h2.mvstore.MVStore

class MVStoreRaftLog(
    private val store: MVStore,
    private val format: StringFormat
) : RaftLog {

    private val log = store.openMap<Index, ByteArray>(LOG_KEY)
    private val metadata = store.openMap<String, Any>(METADATA_KEY)

    private var currentTerm: Term
        get() = metadata["currentTerm"] as? Term ?: 0L
        set(value) { metadata["currentTerm"] = value }
    private var votedFor: NodeId?
        get() = metadata["votedFor"] as? NodeId
        set(value) { metadata["votedFor"] = value }

    init {
        if (log.isEmpty()) {
            log[0] = ROOT_ENTRY.serialize()
            currentTerm = 0
            store.commit()
        }
    }

    override suspend fun getLastIndex(): Index = log.lastKey() ?: 0L

    override suspend fun getTerm(): Term =
        currentTerm

    override suspend fun setTerm(term: Term) {
        currentTerm = term
        metadata.remove("votedFor")
        store.commit()

    }

    override suspend fun getMetadata(index: Index): LogEntryMetadata? {
        return when {
            index == 0L -> LogEntryMetadata.ZERO
            else -> when (val bytes = log[index]) {
                null -> {
                    logger.warn(entry("LogEntry_Not_Found", "index" to index))
                    null
                }

                else -> LogEntryMetadata(index, bytes.deserializeLogEntry().term)
            }
        }
    }

    override suspend fun getEntries(from: Index, length: Int): List<LogEntry> {
        return log.cursor(from).let { cursor ->
            cursor.asSequence()
                .take(length)
                .map { cursor.value.deserializeLogEntry() }
                .toList()
        }
    }

    override suspend fun append(previous: LogEntryMetadata, entries: List<LogEntry>): AppendResult {
        val stored = log[previous.index]?.deserializeLogEntry()
        when {
            stored == null -> return AppendResult.IndexNotFound
            stored.term != previous.term -> return AppendResult.EntryMismatch
            else -> {
                entries.forEachIndexed { i, entry ->
                    log[previous.index + i + 1] = entry.serialize()
                }
                if (previous.index + entries.size < getLastIndex()) {
                    log.cursor(previous.index + entries.size)
                        .asSequence()
                        .drop(1)
                        .toList()
                        .forEach { idx -> log.remove(idx) }
                }
                store.commit()
                return AppendResult.Appended(getLastIndex())
            }
        }
    }

    override suspend fun setVotedFor(nodeId: NodeId, term: Term) {
        votedFor = nodeId
        currentTerm = term
        store.commit()
    }

    override suspend fun getVotedFor(): NodeId? =
        votedFor

    private fun LogEntry.serialize(): ByteArray =
        format.encodeToString(LogEntry.serializer(), this)
            .toByteArray()

    private fun ByteArray.deserializeLogEntry(): LogEntry =
        format.decodeFromString(LogEntry.serializer(), this.decodeToString())

    companion object {
        private val logger: Logger = LogManager.getLogger(MVStoreRaftLog::class.java)
        private val ROOT_ENTRY = LogEntry(
            term = 0,
            entry = LogEntry.NoOp,
            id = "root"
        )
        val LOG_KEY = "log"
        val METADATA_KEY = "metadata"
    }
}