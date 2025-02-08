package io.r.utils

import io.r.raft.protocol.LogEntry
import kotlinx.coroutines.withContext
import org.apache.commons.codec.digest.MurmurHash3
import org.apache.logging.log4j.kotlin.additionalLoggingContext

fun ByteArray.encodeBase64(): String = java.util.Base64.getEncoder().encodeToString(this)
fun ByteArray.murmur128(): LongArray = MurmurHash3.hash128x64(this, 0, this.size, 0)

fun LogEntry.Entry.decodeToString() = when (this) {
    is LogEntry.ClientCommand -> bytes.decodeToString()
    else -> toString()
}

suspend inline fun <T> loggingCtx(
    data: Map<String, String> = emptyMap(),
    vararg stack: String,
    crossinline block: suspend () -> T
) = withContext(additionalLoggingContext(map = data, stack = stack.toList())) {
    block()
}
suspend inline fun <T> loggingCtx(
    vararg stack: String,
    crossinline block: suspend () -> T
) = withContext(additionalLoggingContext(map = emptyMap(), stack = stack.toList())) {
    block()
}

fun LongArray.toHex() =
    joinToString("") { "%02x".format(it) }