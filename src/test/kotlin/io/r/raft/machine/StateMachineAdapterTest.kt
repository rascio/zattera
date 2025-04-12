package io.r.raft.machine

import arrow.fx.coroutines.CyclicBarrier
import io.kotest.assertions.withClue
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.ints.shouldBeGreaterThan
import io.kotest.matchers.shouldBe
import io.r.raft.persistence.StateMachine
import io.r.raft.protocol.LogEntry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration.Companion.seconds

class StateMachineAdapterTest : FunSpec({

    val clientId = java.util.UUID.randomUUID()
    val sequence = AtomicLong()

    fun UnsafeAdd.toClientCommand() = LogEntry.ClientCommand(
        Json.encodeToString(this).encodeToByteArray(),
        clientId,
        sequence.incrementAndGet()
    )

    fun Get.toQuery() = Json.encodeToString(this).encodeToByteArray()
    fun ByteArray.toResponse() = Json.decodeFromString(Current.serializer(), decodeToString())

    context("Testing concurrent access") {


        test("Read, Sum 10 and then Set, 100 times in parallel should end up having 1000").config(timeout = 3.seconds) {
            val s = UnsafeStateMachine()
            val adapter = StateMachineAdapter(CoroutineScope(Dispatchers.IO), s)

            withContext(Dispatchers.IO) {
                repeat(100) {
                    launch { adapter.apply(UnsafeAdd(10).toClientCommand()) }
                }
            }

            val current = s.state

            current shouldBe 1000
        }
        test("Reads should run in parallel").config(timeout = 5.seconds) {
            val parallelReads = 10
            val s = UnsafeStateMachine(expectedParallelReads = parallelReads)
            val adapter = StateMachineAdapter(CoroutineScope(Dispatchers.IO), s)

            val j = launch(Dispatchers.IO) {
                repeat(10) {
                    adapter.apply(UnsafeAdd(10).toClientCommand())
                }
            }

            var reads = 0
            do {
                val batch = List(parallelReads) {
                    async {
                        delay(it * 10L)
                        adapter.read(Get.toQuery())
                    }
                }
                val differentValues = batch.awaitAll()
                    .map { it.toResponse().value }
                    .toSet()

                withClue("Expected all reads to return the same value") {
                    differentValues.size shouldBe 1
                }
                reads++
            } while (j.isActive)
            reads shouldBeGreaterThan 1
        }
    }

})

@Serializable
private data class UnsafeAdd(val n: Int) : StateMachine.Command

@Serializable
private data object Get : StateMachine.Query

@Serializable
private data class Current(val value: Int) : StateMachine.Response
private class UnsafeStateMachine(expectedParallelReads: Int = 1) : StateMachine<UnsafeAdd, Get, Current> {

    override val contract = Companion

    var barrier = CyclicBarrier(expectedParallelReads)

    var state = 0

    override suspend fun apply(message: UnsafeAdd): Current {
        val sum = message.n + state
        delay((1..10L).random())
        state = sum
        delay((1..10L).random())
        return Current(sum)
    }

    override suspend fun read(query: Get): Current {
        barrier.await()
        return Current(state)
    }

    companion object : StateMachine.Contract<UnsafeAdd, Get, Current> {
        override val commandKSerializer = UnsafeAdd.serializer()
        override val queryKSerializer = Get.serializer()
        override val responseKSerializer = Current.serializer()
    }
}
