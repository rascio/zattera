package io.r.raft.log

import io.r.raft.protocol.Index
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.LogEntryMetadata
import io.r.raft.protocol.NodeId
import io.r.raft.protocol.Term

interface RaftLog {

    /**
     * Get the size of the log
     */
    suspend fun getLastIndex(): Index

    /**
     * Get the current term
     */
    suspend fun getTerm(): Term

    /**
     * Set the current term
     * Reset the voted for node.
     */
    suspend fun setTerm(index: Index)

    /**
     * Get the term at a specific index
     */
    suspend fun getMetadata(index: Index): LogEntryMetadata?

    /**
     * Get the entries from a range.
     * If the [from] index is greater than the last index, an empty list is returned.
     * @param from inclusive, greater than 0
     * @param length number of entries to get
     */
    suspend fun getEntries(from: Index, length: Int): List<LogEntry>

    /**
     * Append entries to the log starting from the [previous] index.
     * If the [previous] index is smaller than the last index, the entries will be replaced.
     * When `previous + entries.size < getLastIndex()`, the entries set after this batch will be removed.
     * @param previous the index of the last entry in the list
     * @param entries the entries to append
     * @return the index of the last entry in the log
     */
    suspend fun append(previous: Index, entries: List<LogEntry>): Index

    /**
     * Append a single entry to the log
     */
    suspend fun setVotedFor(nodeId: NodeId)

    /**
     * Get the node that was voted for
     */
    suspend fun getVotedFor(): NodeId?

    companion object {
        suspend fun RaftLog.getEntry(index: Index): LogEntry? =
            getEntries(index, 1).firstOrNull()
        suspend fun RaftLog.getLastMetadata(): LogEntryMetadata =
            checkNotNull(getMetadata(getLastIndex())) { "Metadata of last index must not be null" }
    }
}