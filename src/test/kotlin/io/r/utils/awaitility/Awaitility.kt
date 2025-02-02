package io.r.utils.awaitility

import io.kotest.common.runBlocking
import kotlinx.coroutines.yield
import org.awaitility.Awaitility
import org.awaitility.core.ConditionFactory
import org.awaitility.kotlin.untilNotNull
import kotlin.time.Duration
import kotlin.time.toJavaDuration

infix fun String.atMost(duration: Duration): ConditionFactory =
    Awaitility.await().alias(this).atMost(duration.toJavaDuration())

suspend infix fun <E> ConditionFactory.untilNotNull(block: suspend () -> E?): E & Any {
    yield()
    return this@untilNotNull.untilNotNull { runBlocking(block) }
}

suspend infix fun ConditionFactory.until(block: suspend () -> Boolean) {
    yield()
    until { runBlocking(block) }
}
