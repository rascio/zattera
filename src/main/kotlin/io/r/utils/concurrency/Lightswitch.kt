package io.r.utils.concurrency

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

// https://greenteapress.com/semaphores/LittleBookOfSemaphores.pdf 4.2.2
class Lightswitch {
    private var counter = 0
    private val mutex = Mutex()

    suspend fun lock(semaphore: Mutex, owner: Any? = null) {
        mutex.withLock(owner = owner) {
            counter++
            if (counter == 1) {
                semaphore.lock()
            }
        }
    }

    suspend fun unlock(semaphore: Mutex, owner: Any? = null) {
        mutex.withLock(owner = owner) {
            counter--
            if (counter == 0) {
                semaphore.unlock()
            }
        }
    }
}