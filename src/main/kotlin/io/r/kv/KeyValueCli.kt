package io.r.kv

import arrow.fx.coroutines.autoCloseable
import arrow.fx.coroutines.resourceScope
import io.r.raft.client.RaftClusterClient
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.RaftRpc
import io.r.raft.protocol.toClusterNode
import io.r.raft.transport.ktor.HttpRaftCluster
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters
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
        GetCommand::class,
        SetCommand::class,
        DeleteCommand::class
    ]
)
class KeyValueCli

@Command(
    name = "shell",
    description = ["Start a shell to interact with the key-value store"]
)
class ShellCommand : Callable<Int> {
    @Option(names = ["--peer"], description = ["The host to connect to"], required = true)
    lateinit var peers: List<String>

    override fun call(): Int {
        runBlocking {
            resourceScope {
                val cluster = autoCloseable {
                    HttpRaftCluster().apply {
                        peers.map { it.toClusterNode() }
                            .forEach { connect(it) }
                    }
                }
                val raftClient = RaftClusterClient(cluster)
                val scanner = Scanner(System.`in`)
                println("Connected")
                while (true) {
                    print("${raftClient.connected().describe()}> ")
                    val entry = when (val request = scanner.nextCommand()) {
                        null -> continue
                        else -> request.toClientCommand()
                    }
                    raftClient.request(entry)
                        .recoverCatching { raftClient.request(entry).getOrThrow() }
                        .map { it.decodeToString() }
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

@Command(
    name = "get",
    description = ["Get a value from the key-value store"]
)
class GetCommand : RestRaftCommand() {
    @Parameters(index = "0", description = ["The key to get"])
    lateinit var key: String

    @Option(names = ["--peer"], description = ["The host to connect to"], required = true)
    override lateinit var peers: List<String>

    override fun request(): StringsKeyValueStore.Request = StringsKeyValueStore.Get(key)
}

@Command(
    name = "set",
    description = ["Set a value in the key-value store"]
)
class SetCommand : RestRaftCommand() {
    @Parameters(index = "0", description = ["The key to set"])
    lateinit var key: String

    @Parameters(index = "1", description = ["The value to set"])
    lateinit var value: String

    @Option(names = ["--peer"], description = ["The host to connect to"], required = true)
    override lateinit var peers: List<String>

    override fun request(): StringsKeyValueStore.Request = StringsKeyValueStore.Set(key, value)
}

@Command(
    name = "delete",
    description = ["Delete a value from the key-value store"]
)
class DeleteCommand : RestRaftCommand() {
    @Parameters(index = "0", description = ["The key to delete"])
    lateinit var key: String

    @Option(names = ["--peer"], description = ["The host to connect to"], required = true)
    override lateinit var peers: List<String>

    override fun request(): StringsKeyValueStore.Request = StringsKeyValueStore.Delete(key)
}


private fun StringsKeyValueStore.Request.toClientCommand(): LogEntry.ClientCommand {
    val payload = Json.encodeToString(this).encodeToByteArray()
    val entry = LogEntry.ClientCommand(payload)
    return entry
}

abstract class RestRaftCommand : Callable<String> {

    abstract var peers: List<String>

    override fun call(): String = runBlocking {
        resourceScope {
            val cluster = autoCloseable {
                HttpRaftCluster().apply {
                    peers.map { it.toClusterNode() }
                        .forEach { connect(it) }
                }
            }
            val raftClient = RaftClusterClient(cluster)
            val request = request()
            val payload = Json.encodeToString(request).encodeToByteArray()
            val entry = LogEntry.ClientCommand(payload)
            raftClient.request(entry)
                .recoverCatching { raftClient.request(entry).getOrThrow() }
                .map { it.decodeToString() }
                .map { Json.decodeFromString<StringsKeyValueStore.Response>(it) }
                .map { it.toString() }
                .getOrElse { "Error: ${it.message}" }
                .also { println(it) }
        }
    }

    abstract fun request(): StringsKeyValueStore.Request
}