package io.r.raft.machine

import arrow.atomic.AtomicLong
import io.r.raft.log.StateMachine
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.randomAlphabetic
import io.r.utils.logs.entry
import kotlinx.coroutines.yield
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.apache.logging.log4j.LogManager
import java.util.UUID

class TestingStateMachine : StateMachine<TestCmd, TestQuery> {

    override val contract = Companion

    var applied = 0L
    override suspend fun apply(message: TestCmd): ByteArray {
        logger.info(entry("apply", "command" to message.value))
        yield()
        return "APPLIED_${applied++}".encodeToByteArray()
    }

    companion object : StateMachine.Contract<TestCmd, TestQuery> {
        override val commandKSerializer = TestCmd.serializer()
        override val queryKSerializer = TestQuery.serializer()

        private val logger = LogManager.getLogger(TestingStateMachine::class.java)
    }
}

@Serializable
data class TestCmd(val value: String) : StateMachine.Command
@Serializable
data object TestQuery : StateMachine.Query

val TEST_CMD_SEQUENCE = AtomicLong()
fun String.toTestCommand(clientId: UUID = UUID.randomUUID(), sequence: Long = TEST_CMD_SEQUENCE.incrementAndGet()) =
    TestCmd(this)
        .let { Json.encodeToString(it) }
        .encodeToByteArray()
        .let { LogEntry.ClientCommand(it, clientId, sequence) }
