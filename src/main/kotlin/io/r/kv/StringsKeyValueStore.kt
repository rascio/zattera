package io.r.kv

import io.r.raft.log.StateMachine
import io.r.raft.protocol.LogEntry
import io.r.utils.decodeToString
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.apache.logging.log4j.LogManager

class StringsKeyValueStore : StateMachine {

    private val store = mutableMapOf<String, String>()

    override suspend fun apply(command: LogEntry): ByteArray {
        val request = Json.decodeFromString<Request>(command.entry.decodeToString())
        return Json.encodeToString(handle(request))
            .encodeToByteArray()
    }

    override suspend fun read(query: ByteArray): ByteArray {
        val request = Json.decodeFromString<Request>(query.decodeToString())
        return Json.encodeToString(handle(request))
            .encodeToByteArray()
    }

    private fun handle(request: Request): Response = when (request) {
        is Get -> {
            val value = store[request.key]
            if (value != null) Value(value)
            else NotFound
        }
        is Set -> {
            store[request.key] = request.value.resolvePlaceholders().also {
                logger.info("Set key=${request.key} new=$it old=${store[request.key]} value=${request.value}")
            }
            Ok
        }
        is Delete -> {
            store.remove(request.key)
            Ok
        }
    }

    /**
     * Resolves placeholders in the given string using the current store.
     */
    private fun String.resolvePlaceholders(): String {
        return PLACEHOLDER_REGEX.replace(this) {
            store[it.groupValues[1]] ?: ""
        }
    }

    @Serializable
    sealed interface Request
    @Serializable
    data class Get(val key: String) : Request
    @Serializable
    data class Set(val key: String, val value: String) : Request
    @Serializable
    data class Delete(val key: String) : Request

    @Serializable
    sealed interface Response
    @Serializable
    data object Ok : Response
    @Serializable
    data class Value(val value: String) : Response
    @Serializable
    data object NotFound : Response

    companion object {
        val PLACEHOLDER_REGEX = Regex("\\$\\{([^}]+)}")
        private val logger = LogManager.getLogger(StringsKeyValueStore::class.java)
    }
}
