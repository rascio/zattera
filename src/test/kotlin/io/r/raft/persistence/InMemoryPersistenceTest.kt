package io.r.raft.persistence

import io.r.raft.Persistence
import io.r.raft.persistence.inmemory.InMemoryPersistence

object InMemoryPersistenceAdapter : PersistedStateTestAdapter {
    override suspend fun createPersistedState(): Persistence =
        InMemoryPersistence()
}
class InMemoryPersistenceTest : AbstractPersistedStateTest(InMemoryPersistenceAdapter)