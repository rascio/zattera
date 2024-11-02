package io.r.raft.test

import arrow.fx.coroutines.ResourceScope
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

suspend infix fun <E> ReceiveChannel<E>.shouldReceive(expected: E) {
    receive() shouldBe expected
}

suspend fun <E> ResourceScope.installChannel(capacity: Int = Channel.UNLIMITED) = install(
    acquire = { Channel<E>(capacity = capacity) },
    release = { c, _ -> c.close() }
)

suspend fun ResourceScope.installCoroutine(ctx: CoroutineContext = EmptyCoroutineContext) = install(
    acquire = { CoroutineScope(ctx) },
    release = { c, _ -> c.cancel() }
)
