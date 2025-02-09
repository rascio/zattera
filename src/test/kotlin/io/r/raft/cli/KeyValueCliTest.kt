package io.r.raft.cli

import io.kotest.assertions.withClue
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.result.shouldBeSuccess
import io.kotest.matchers.shouldBe
import io.r.kv.StringsKeyValueStore
import kotlinx.coroutines.delay
import picocli.CommandLine
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.util.concurrent.Executors

class KeyValueCliTest : FunSpec({

    val testExecutor = Executors.newFixedThreadPool(3)

    afterTest {
        testExecutor.shutdown()
    }

    fun startCluster() = repeat(3) { node ->
        testExecutor.submit {
            CommandLine(RestRaftServer()).execute(
                "N$node",
                "--port", "${18080 + node}",
                "--peer", "N0=localhost:18080",
                "--peer", "N1=localhost:18081",
                "--peer", "N2=localhost:18082",
                "--state-machine", StringsKeyValueStore::class.qualifiedName!!
            )
        }.also { job -> afterTest { job.cancel(true) } }
    }

    fun startKVStoreShell(input: String) {
        val oldInput = System.`in`
        System.setIn(ByteArrayInputStream(input.toByteArray()))
        try {
            CommandLine(KVShellCommand()).execute(
                "--peer", "N0=localhost:18080",
                "--peer", "N1=localhost:18081",
                "--peer", "N2=localhost:18082"
            )
        } finally {
            System.setIn(oldInput)
        }
    }

    fun captureOutputStream(block: () -> Unit): Pair<ByteArray, Result<Unit>> {
        val oldOutput = System.out
        val output = ByteArrayOutputStream()
        val res = PrintStream(output).use { out ->
            println("Redirecting output")
            System.setOut(out)
            runCatching { block() }
                .also { out.flush() }
        }
        System.setOut(oldOutput)
        println("Restoring output")
        output.flush()
        return output.toByteArray() to res
    }


    context("Start a real cluster") {
        startCluster()

        delay(1000)

        test("Start a shell and interact with the cluster") {
            val input = """
                set key1 value1
                set key2 value2
                get key1
                get key2
                set key1 value3-{{key2}}
                get key1
                get key2
                delete key1
                get key 1
                exit
            """.trimIndent()
            val (output, res) = captureOutputStream {
                startKVStoreShell(input)
            }
            withClue("Output:\n${output.decodeToString()}\nEnd Output") {
                res shouldBeSuccess Unit

                val lines = output.decodeToString()
                    .lines()
                    .iterator()
                val expectedLinesIterator = listOf(
                    "Ok",
                    "Ok",
                    "value1",
                    "value2",
                    "Ok",
                    "value3-value2",
                    "value2",
                    "Ok",
                    "NotFound"
                )
                    .map { "(\\w+:\\d+|\\?{3})> $it".toRegex() }
                    .iterator()

                var expected = expectedLinesIterator.next()
                while (lines.hasNext()) {
                    val line = lines.next()
                    println("$expected.containsMatchIn($line) => ${expected.containsMatchIn(line)}")
                    when {
                        expected.containsMatchIn(line) -> {
                            if (expectedLinesIterator.hasNext()) {
                                expected = expectedLinesIterator.next()
                            } else break
                        }
                    }
                }
                val missing = expectedLinesIterator.asSequence().toList()

                withClue("Expected, but missing:\n${missing.joinToString("\n")}\n") {
                    missing.size shouldBe 0
                }
            }
        }
    }

})