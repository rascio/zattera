package io.r.raft.machine.linearizability

import io.r.raft.log.StateMachine
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.util.concurrent.ConcurrentHashMap

class IdempotentStateMachine<Cmd: StateMachine.Command>(
    parent: CoroutineScope,
    private val stateMachine: StateMachine<Cmd>
) : StateMachine<Cmd> by stateMachine {

    private class Entry(val sequence: Long, val result: ByteArray, val expiration: Long)
    private val cache = ConcurrentHashMap<String, Entry>()

    init {
        parent.launch {
            while (isActive) {
                cache.entries.removeIf { (_, entry) -> entry.expiration < System.currentTimeMillis() }
                delay(60000)
            }
        }
    }

    override suspend fun apply(message: StateMachine.Message<Cmd>): ByteArray =
        when (val cached = cache[message.clientId]) {
            null -> {
                val result = stateMachine.apply(message)
                cache[message.clientId] = Entry(message.sequence, result, System.currentTimeMillis() + 60000)
                result
            }
            else -> {
                if (cached.sequence == message.sequence) {
                    cached.result
                } else {
                    val result = stateMachine.apply(message)
                    cache[message.clientId] = Entry(message.sequence, result, System.currentTimeMillis() + 60000)
                    result
                }
            }

    }
}