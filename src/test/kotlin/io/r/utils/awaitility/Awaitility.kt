package io.r.utils.awaitility

import io.kotest.common.runBlocking
import io.r.utils.awaitility.OneOf.Companion.match
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.awaitility.Awaitility
import org.awaitility.core.ConditionFactory
import org.awaitility.kotlin.untilNotNull
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.suspendCoroutine
import kotlin.time.Duration
import kotlin.time.toJavaDuration

val String.await: ConditionFactory get() = Awaitility.await().alias(this)

infix fun ConditionFactory.timeout(duration: Duration) = timeout(duration.toJavaDuration())
infix fun <E> ConditionFactory.coUntilNotNull(block: suspend () -> E?) = untilNotNull { runBlocking(block) }



typealias SuspendCondition = suspend () -> Boolean
suspend infix fun ConditionFactory.coUntil(condition: SuspendCondition) = withContext(Dispatchers.IO) {
    suspendCoroutine { cont ->
        val result = runCatching { until { runBlocking(cont.context, condition) } }
        cont.resumeWith(result)
    }
}
suspend fun <T> ConditionFactory.coUntil(supplier: suspend () -> T, predicate: suspend (T) -> Boolean) = withContext(Dispatchers.IO) {
    suspendCoroutine { cont ->
        val res = runCatching {
            until(
                { runBlocking(cont.context, supplier) },
                { runBlocking(cont.context) { predicate(it) } }
            )
        }
        cont.resumeWith(res)
    }
}
private fun <T> runBlocking(context: CoroutineContext, block: suspend () -> T) = runBlocking {
    withContext(context) { block() }
}

interface OneOf<T> {
    val source: List<T>
    val predicate: (T) -> Boolean

    companion object {
        internal fun <T> match(list: List<T>, predicate: (T) -> Boolean) = object : OneOf<T> {
            override val source = list
            override val predicate = predicate
        }
    }
}
fun <E> oneOf(list: List<E>, predicate: (E) -> Boolean): OneOf<E> = match(list, predicate)

suspend infix fun <T> ConditionFactory.coUntil(oneOf: OneOf<T>): T = withContext(Dispatchers.IO) {
    suspendCoroutine { cont ->
        val result = runCatching {
            until(
                { oneOf.source },
                { oneOf.source.any { oneOf.predicate(it) } }
            )
        }
        result.map { oneOf.source.first { oneOf.predicate(it) } }
            .let(cont::resumeWith)
    }
}