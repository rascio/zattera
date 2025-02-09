package io.r.raft.cli


import arrow.fx.coroutines.autoCloseable
import arrow.fx.coroutines.resourceScope
import io.r.kv.StringsKeyValueStore
import io.r.kv.StringsKeyValueStore.KVCommand
import io.r.kv.StringsKeyValueStore.KVQuery
import io.r.kv.StringsKeyValueStore.KVRequest
import io.r.raft.client.RaftClusterClient
import io.r.raft.protocol.RaftRpc
import io.r.raft.protocol.toClusterNode
import io.r.raft.transport.ktor.HttpRaftCluster
import io.r.utils.requireMatch
import kotlinx.coroutines.runBlocking
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Model.CommandSpec
import picocli.CommandLine.Option
import picocli.CommandLine.Spec
import java.util.Scanner
import java.util.concurrent.Callable


@Command(
    name = "kv-shell",
    description = ["Start a shell to interact with the key-value store"]
)
class KVShellCommand : Callable<Int> {

    @Spec
    private lateinit var spec: CommandSpec

    @Option(names = ["--peer"], description = ["The host to connect to"], required = true)
    lateinit var peers: List<String>


    @Option(names = ["--retry"], description = ["The number of retries"], defaultValue = "10")
    var retry: Int = 10

    @Option(
        names = ["--delay"],
        description = ["The delay between retries when no leader is available, should be in line with heartbeat"],
        defaultValue = "300"
    )
    var delay: Long = 300

    @Option(
        names = ["--jitter"],
        description = ["The jitter to add to the delay, or when the server fails"],
        defaultValue = "50"
    )
    var jitter: Long = 50


    override fun call(): Int {
        peers.forEach { spec.requireMatch(it, PEER_PATTERN) }

        runBlocking { run() }
        return 0
    }

    private suspend fun run() {
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
                val result = when (val cmd = scanner.nextCommand()) {
                    null -> continue
                    is RaftRequest -> when (cmd.request) {
                        is KVCommand -> raftClient.request(cmd.request)
                        is KVQuery -> raftClient.query(cmd.request)
                    }
                    is ExitShell -> {
                        println("Goodbye!")
                        return@resourceScope
                    }
                }
                result
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

    private fun RaftRpc.ClusterNode?.describe() =
        this?.run { "$host:$port" } ?: "???"

    sealed interface ShellCommand
    private data object ExitShell : ShellCommand
    @JvmInline
    private value class RaftRequest(val request: KVRequest) : ShellCommand

    private fun Scanner.nextCommand(): ShellCommand? {
        val line = nextLine()
        val parts = line.split(" ")
        val command = parts.firstOrNull()
        val key = parts.getOrNull(1)
        val value = parts.drop(2).joinToString(" ")
        return when (command) {
            "get" -> RaftRequest(StringsKeyValueStore.Get(key!!))
            "set" -> RaftRequest(StringsKeyValueStore.Set(key!!, value))
            "delete" -> RaftRequest(StringsKeyValueStore.Delete(key!!))
            "exit" -> ExitShell
            else -> {
                if (command != "help") {
                    println("Unknown command: $command")
                }
                println("""
                    available commands are:
                    - get <key>
                    - set <key> <value>
                    - delete <key>
                    - help
                    - exit
                """.trimIndent())
                null
            }
        }
    }
}
