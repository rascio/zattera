package io.r.counter

import arrow.atomic.AtomicLong
import io.r.raft.persistence.StateMachine
import io.r.utils.JacocoExclusionNeedsGenerated
import io.r.utils.logs.entry
import kotlinx.serialization.Serializable
import org.apache.logging.log4j.LogManager

@JacocoExclusionNeedsGenerated
class SimpleCounter : StateMachine<SimpleCounter.Inc, SimpleCounter.Get, SimpleCounter.Value> {

    @Serializable
    data object Inc : StateMachine.Command
    @Serializable
    data object Get : StateMachine.Query
    @Serializable
    data class Value(val count: Long) : StateMachine.Response

    private val lastApplied = AtomicLong()

    override val contract = Companion

    override suspend fun apply(message: Inc): Value =
        Value(lastApplied.incrementAndGet())
            .also { logger.info(entry("Applied", "client_id" to message)) }


    override suspend fun read(query: Get): Value =
        Value(lastApplied.get())

    companion object : StateMachine.Contract<Inc, Get, Value> {
        override val commandKSerializer = Inc.serializer()
        override val queryKSerializer = Get.serializer()
        override val responseKSerializer = Value.serializer()
        private val logger = LogManager.getLogger(SimpleCounter::class.java)
    }
}