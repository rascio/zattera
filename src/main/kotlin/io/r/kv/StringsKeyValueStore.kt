package io.r.kv

import io.r.kv.StringsKeyValueStore.KVCommand
import io.r.kv.StringsKeyValueStore.KVQuery
import io.r.raft.log.StateMachine
import io.r.utils.logs.entry
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.apache.logging.log4j.LogManager
import java.util.concurrent.ConcurrentHashMap

class StringsKeyValueStore : StateMachine<KVCommand, KVQuery> {

    private val store = ConcurrentHashMap<String, String>()

    override val contract = Companion

    override suspend fun apply(message: KVCommand): ByteArray {
        return Json.encodeToString(handle(message))
            .encodeToByteArray()
    }

    override suspend fun read(query: KVQuery): ByteArray {
        return Json.encodeToString(handle(query))
            .encodeToByteArray()

    }

    private fun handle(request: Request): Response = when (request) {
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
    sealed interface Request

    @Serializable
    sealed interface KVCommand : StateMachine.Command, Request

    @Serializable
    sealed interface KVQuery : StateMachine.Query, Request

    @Serializable
    data class Get(val key: String) : KVQuery

    @Serializable
    data class Set(val key: String, val value: String) : KVCommand

    @Serializable
    data class Delete(val key: String) : KVCommand

    @Serializable
    sealed interface Response

    @Serializable
    data object Ok : Response

    @Serializable
    data class Value(val value: String) : Response

    @Serializable
    data object NotFound : Response

    companion object : StateMachine.Contract<KVCommand, KVQuery> {
        override val commandKSerializer = KVCommand.serializer()
        override val queryKSerializer = KVQuery.serializer()

        val PLACEHOLDER_REGEX = Regex("\\$\\{([^}]+)}")
        private val logger = LogManager.getLogger(StringsKeyValueStore::class.java)
    }
}
