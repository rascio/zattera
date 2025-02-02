package io.r.utils

import io.r.raft.protocol.LogEntry

fun ByteArray.encodeBase64(): String = java.util.Base64.getEncoder().encodeToString(this)

fun LogEntry.Entry.decodeToString() = when (this) {
    is LogEntry.ClientCommand -> bytes.decodeToString()
    is LogEntry.ConfigurationChange -> toString()
    else -> error("Not managed: $this")
}

