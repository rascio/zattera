package io.r.raft.machine

import io.r.raft.log.StateMachine
import io.r.raft.protocol.LogEntry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class StateMachineAdapter<
    C : StateMachine.Command,
    Q : StateMachine.Query,
    Contract : StateMachine.Contract<C, Q>
    >(
    parent: CoroutineScope,
    private val stateMachine: StateMachine<C, Q>
) {

    val contract = stateMachine.contract

    private class Entry(val sequence: Long, val result: ByteArray, val expiration: Long)

    private val cache = ConcurrentHashMap<UUID, Entry>()

    init {
        parent.launch {
            while (isActive) {
                cache.entries.removeIf { (_, entry) -> entry.expiration < System.currentTimeMillis() }
                delay(60000)
            }
        }
    }

    suspend fun apply(entry: LogEntry.ClientCommand): ByteArray {
        val cached = cache[entry.clientId]

        return when {
            cached == null || cached.sequence != entry.sequence -> {
                entry.parse(stateMachine.contract.commandKSerializer)
                    .map { stateMachine.apply(it) }
                    .onSuccess {
                        cache[entry.clientId] = Entry(entry.sequence, it, System.currentTimeMillis() + 60000)
                    }
                    .getOrThrow()
            }

            else -> cached.result
        }
    }

    suspend fun read(entry: ByteArray): ByteArray {
        val query = Json.decodeFromString(stateMachine.contract.queryKSerializer, entry.decodeToString())
        return stateMachine.read(query)
    }

    private fun <T> LogEntry.ClientCommand.parse(deserializer: KSerializer<T>): Result<T> = runCatching {
        Json.decodeFromString(deserializer, bytes.decodeToString())
    }

    companion object {
        fun StateMachine.Contract<*, *>.isValidCommand(entry: LogEntry.ClientCommand): Boolean =
            runCatching {
                Json.decodeFromString(commandKSerializer, entry.bytes.decodeToString())
                true
            }.getOrDefault(false)
    }
}