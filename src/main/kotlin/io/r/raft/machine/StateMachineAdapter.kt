package io.r.raft.machine

import io.r.raft.persistence.StateMachine
import io.r.raft.protocol.LogEntry
import io.r.utils.concurrency.ReadWriteLock
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import java.util.UUID

class StateMachineAdapter<
    C : StateMachine.Command,
    Q : StateMachine.Query,
    R : StateMachine.Response,
    Contract : StateMachine.Contract<C, Q, R>
    >(
    parent: CoroutineScope,
    private val stateMachine: StateMachine<C, Q, R>
) {

    val contract = stateMachine.contract

    private class Entry(val sequence: Long, val result: ByteArray, val expiration: Long)

    private val cache = mutableMapOf<UUID, Entry>()
    private val lock = ReadWriteLock()

    init {
        parent.launch {
            while (isActive) {
                lock.withWriteLock {
                    cache.entries.removeIf { (_, entry) -> entry.expiration < System.currentTimeMillis() }
                }
                delay(60000)
            }
        }
    }

    suspend fun apply(entry: LogEntry.ClientCommand): ByteArray = lock.withWriteLock {
        val cached = cache[entry.clientId]

        return when {
            cached == null || cached.sequence != entry.sequence -> {
                entry.parse(stateMachine.contract.commandKSerializer)
                    .map { stateMachine.apply(it) }
                    .map { encode(contract.responseKSerializer, it) }
                    .onSuccess {
                        cache[entry.clientId] = Entry(entry.sequence, it, System.currentTimeMillis() + 60000)
                    }
                    .getOrThrow()
            }

            else -> cached.result
        }
    }

    suspend fun read(entry: ByteArray): ByteArray = lock.withReadLock {
        val query = parse(stateMachine.contract.queryKSerializer, entry)
        return Json.encodeToString(stateMachine.contract.responseKSerializer, stateMachine.read(query))
            .encodeToByteArray()
    }


    private fun <T> LogEntry.ClientCommand.parse(deserializer: KSerializer<T>): Result<T> = runCatching {
        parse(deserializer, bytes)
    }
    private fun <T> parse(kSerializer: KSerializer<T>, entry: ByteArray) =
        Json.decodeFromString(kSerializer, entry.decodeToString())

    private fun <T> encode(kSerializer: KSerializer<T>, entry: T) =
        Json.encodeToString(kSerializer, entry)
            .encodeToByteArray()

    companion object {
        fun StateMachine.Contract<*, *, *>.isValidCommand(entry: LogEntry.ClientCommand): Boolean =
            runCatching {
                Json.decodeFromString(commandKSerializer, entry.bytes.decodeToString())
                true
            }.getOrDefault(false)
    }
}