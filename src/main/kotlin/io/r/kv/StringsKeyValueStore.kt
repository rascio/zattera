package io.r.kv

import io.r.kv.StringsKeyValueStore.KVCommand
import io.r.kv.StringsKeyValueStore.KVQuery
import io.r.kv.StringsKeyValueStore.KVResponse
import io.r.raft.persistence.StateMachine
import io.r.utils.logs.entry
import kotlinx.serialization.Serializable
import org.apache.logging.log4j.LogManager

/**
 * A simple key-value store that stores strings.
 * It supports setting, getting, and deleting keys.
 * It also supports placeholders in the values, which are resolved using the current store, eg:
 * ```
 * set key1 value1
 * set key2 {{key1}}-value2
 * get key2
 * ```
 * will return `value1-value2`.
 *
 */
class StringsKeyValueStore : StateMachine<KVCommand, KVQuery, KVResponse> {

    private val store = mutableMapOf<String, String>()

    override val contract = Companion

    override suspend fun apply(message: KVCommand): KVResponse {
        return handle(message)
    }

    override suspend fun read(query: KVQuery): KVResponse {
        return handle(query)

    }

    private fun handle(request: KVRequest): KVResponse = when (request) {
        is Get -> {
            val value = store[request.key]
            if (value != null) Value(value)
            else NotFound
        }

        is Set -> {
            val value = request.value.resolvePlaceholders().also {
                logger.debug {
                    entry("Set-${request.key}", "new" to it, "old" to store[request.key], "value" to request.value)
                }
            }
            store[request.key] = value
            Ok
//            Value("key=${request.key}, value=$value, id=$id")
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
    sealed interface KVRequest


    @Serializable
    sealed interface KVCommand : StateMachine.Command, KVRequest
    @Serializable
    sealed interface KVQuery : StateMachine.Query, KVRequest
    @Serializable
    sealed interface KVResponse : StateMachine.Response


    @Serializable
    data class Set(val key: String, val value: String) : KVCommand
    @Serializable
    data class Delete(val key: String) : KVCommand


    @Serializable
    data class Get(val key: String) : KVQuery


    @Serializable
    data object Ok : KVResponse
    @Serializable
    data class Value(val value: String) : KVResponse
    @Serializable
    data object NotFound : KVResponse


    companion object : StateMachine.Contract<KVCommand, KVQuery, KVResponse> {
        override val commandKSerializer = KVCommand.serializer()
        override val queryKSerializer = KVQuery.serializer()
        override val responseKSerializer = KVResponse.serializer()

        val PLACEHOLDER_REGEX = Regex("\\{\\{([^}]+)}}")
        private val logger = LogManager.getLogger(StringsKeyValueStore::class.java)
    }
}
