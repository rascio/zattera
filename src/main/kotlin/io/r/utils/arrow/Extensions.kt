package io.r.utils.arrow

import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withTimeout
import java.time.Clock
import java.time.Instant
import kotlin.random.Random
import kotlin.time.Duration

interface Timeout {
    val millis: Long
    fun deadline(clock: Clock): Instant = clock.instant().plusMillis(millis)
    val deadline: Instant
        get() = deadline(Clock.systemUTC())

    fun jitter(jitter: Long) = JitteredTimedout(jitter, this)
    companion object {
        operator fun invoke(millis: Long) = FixedTimedout(millis)
        val Duration.timeout get() = FixedTimedout(this.inWholeMilliseconds)
    }
}
class FixedTimedout(override val millis: Long) : Timeout
class JitteredTimedout(val jitter: Long, val timeout: Timeout) : Timeout {
    override val millis: Long
        get() = timeout.millis + Random.nextLong(-jitter, jitter)
}

suspend inline fun <T> timeout(timeout: Timeout, crossinline block: suspend () -> T) = try {
    withTimeout(timeout.millis) { Result.success(block()) }
} catch (e: TimeoutCancellationException) {
    Result.failure(e)
}

