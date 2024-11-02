package io.r.utils.concurrency

import kotlinx.coroutines.sync.Mutex

// https://greenteapress.com/semaphores/LittleBookOfSemaphores.pdf 4.2.2
class ReadWriteLock {
    private val lightswitch = Lightswitch()
    private val write = Mutex()

    suspend inline fun <T> withReadLock(owner: Any? = null, block: () -> T): T {
        acquireRead(owner)
        return try { block() }
        finally { releaseRead(owner) }
    }
    suspend inline fun <T> withWriteLock(owner: Any? = null, block: () -> T): T {
        acquireWrite(owner)
        return try { block() }
        finally { releaseWrite(owner) }
    }

    suspend fun acquireRead(owner: Any? = null) {
        lightswitch.lock(write, owner)
    }
    suspend fun releaseRead(owner: Any? = null) {
        lightswitch.unlock(write, owner)
    }
    suspend fun acquireWrite(owner: Any? = null) {
        write.lock(owner)
    }
    fun releaseWrite(owner: Any? = null) {
        write.unlock(owner)
    }
}