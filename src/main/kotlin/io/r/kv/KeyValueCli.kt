@file:JacocoExclusionNeedsGenerated

package io.r.kv


import arrow.fx.coroutines.autoCloseable
import arrow.fx.coroutines.resourceScope
import io.r.kv.StringsKeyValueStore.KVCommand
import io.r.raft.client.RaftClusterClient
import io.r.raft.protocol.RaftRpc
import io.r.raft.protocol.toClusterNode
import io.r.raft.transport.ktor.HttpRaftCluster
import io.r.utils.JacocoExclusionNeedsGenerated
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import java.util.Scanner
import java.util.concurrent.Callable
import kotlin.system.exitProcess


fun main(args: Array<String>) {
    val exitCode = CommandLine(KeyValueCli()).execute(*args)
    exitProcess(exitCode)
}

@Command(
    name = "kv-store",
    descriptionHeading = """
        Key-Value Store CLI client.
    """,
    subcommands = [
        ShellCommand::class,
    ]
)
class KeyValueCli

@Command(
    name = "shell",
    description = ["Start a shell to interact with the key-value store"]
)
@JacocoExclusionNeedsGenerated
class ShellCommand : Callable<Int> {
    @Option(names = ["--peer"], description = ["The host to connect to"], required = true)
    lateinit var peers: List<String>

    @Option(names = ["--retry"], description = ["The number of retries"], defaultValue = "10")
    var retry: Int = 10

    @Option(names = ["--delay"], description = ["The delay between retries when no leader is available, should be in line with heartbeat"], defaultValue = "300")
    var delay: Long = 300

    @Option(names = ["--jitter"], description = ["The jitter to add to the delay, or when the server fails"], defaultValue = "50")
    var jitter: Long = 50


    override fun call(): Int {
        runBlocking {
            resourceScope {
                val cluster = autoCloseable {
                    HttpRaftCluster().apply {
                        peers.map { it.toClusterNode() }
                            .forEach { connect(it) }
                    }
                }
                val raftClient = RaftClusterClient(
                    peers = cluster,
                    contract = StringsKeyValueStore,
                    configuration = RaftClusterClient.Configuration(
                        retry = retry,
                        delay = delay..(delay + jitter),
                        jitter = 0L..jitter
                    )
                )
                val scanner = Scanner(System.`in`)
                println("Connected")
                while (true) {
                    print("${raftClient.connected().describe()}> ")
                    val result = when (val request = scanner.nextCommand()) {
                        null -> continue
                        is KVCommand -> sendCommand(raftClient, request)
                        else -> sendQuery(raftClient, request)
                    }
                    result
                        .map { Json.decodeFromString<StringsKeyValueStore.Response>(it) }
                        .map {
                            when (it) {
                                is StringsKeyValueStore.Value -> it.value
                                else -> it.toString()
                            }
                        }
                        .getOrElse { "Error: ${it.message}" }
                        .also { println(it) }
                }
            }
        }

        return 0
    }

    private suspend fun sendCommand(raftClient: RaftClusterClient<KVCommand>, cmd: KVCommand): Result<String> {
        return raftClient.request(cmd)
            .map { it.decodeToString() }
    }

    private suspend fun sendQuery(raftClient: RaftClusterClient<KVCommand>, query: StringsKeyValueStore.Request): Result<String> {
        val serializedQuery = Json.encodeToString(query).encodeToByteArray()
        return raftClient.query(serializedQuery)
            .map { it.decodeToString() }
    }

    private fun RaftRpc.ClusterNode?.describe() =
        this?.run { "$host:$port" } ?: "???"

    private fun Scanner.nextCommand(): StringsKeyValueStore.Request? {
        val line = nextLine()
        val parts = line.split(" ")
        val command = parts.firstOrNull()
        val key = parts.getOrNull(1)
        val value = parts.drop(2).joinToString(" ")
        return when (command) {
            "get" -> StringsKeyValueStore.Get(key!!)
            "set" -> StringsKeyValueStore.Set(key!!, value)
            "delete" -> StringsKeyValueStore.Delete(key!!)
            else -> {
                println("Unknown command: $command")
                null
            }
        }
    }
}
