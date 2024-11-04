package io.r.utils.awaitility

import io.kotest.common.runBlocking
import org.awaitility.Awaitility
import org.awaitility.core.ConditionFactory
import org.awaitility.kotlin.untilNotNull
import kotlin.time.Duration
import kotlin.time.toJavaDuration

infix fun String.atMost(duration: Duration): ConditionFactory =
    Awaitility.await().alias(this).atMost(duration.toJavaDuration())

infix fun <E> ConditionFactory.untilNotNull(block: suspend () -> E?) =
    this@untilNotNull.untilNotNull { runBlocking(block) }

infix fun ConditionFactory.until(block: suspend () -> Boolean) =
    until { runBlocking(block) }
