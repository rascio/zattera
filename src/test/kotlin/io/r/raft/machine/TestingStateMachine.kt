package io.r.raft.machine

import arrow.atomic.AtomicLong
import io.r.raft.log.StateMachine
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.randomAlphabetic
import io.r.utils.logs.entry
import kotlinx.coroutines.yield
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.apache.logging.log4j.LogManager

class TestingStateMachine : StateMachine<TestCmd> {
    override val commandSerializer = TestCmd.serializer()
    var applied = 0L
    override suspend fun apply(message: StateMachine.Message<TestCmd>): ByteArray {
        logger.info(entry("apply", "command" to message.payload.value))
        yield()
        return "APPLIED_${applied++}".encodeToByteArray()
    }

    companion object {
        private val logger = LogManager.getLogger(TestingStateMachine::class.java)
    }
}

@Serializable
data class TestCmd(val value: String) : StateMachine.Command

val TEST_CMD_SEQUENCE = AtomicLong()
fun String.toTestCommand(clientId: String = randomAlphabetic(), sequence: Long = TEST_CMD_SEQUENCE.incrementAndGet()) =
    TestCmd(this)
        .let { StateMachine.Message(clientId, sequence, it) }
        .let {
            LogEntry.ClientCommand(
                Json.encodeToString(it).encodeToByteArray(),
                it.id
            )
        }
