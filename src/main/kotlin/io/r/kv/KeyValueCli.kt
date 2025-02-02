package io.r.kv

import arrow.fx.coroutines.resourceScope
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.RaftRpc
import io.r.raft.transport.ktor.HttpRaftService
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
    @Option(names = ["--host", "-h"], description = ["The host to connect to"], defaultValue = "localhost")
    lateinit var host: String

    @Option(names = ["--port", "-p"], description = ["The port to connect to"], required = true)
    var port: Int = -1

    override fun call(): Int {
        runBlocking {
            resourceScope {
                val client = HttpClient(CIO)
                val raftService = install(
                    acquire = { HttpRaftService(RaftRpc.ClusterNode("", host, port), client) },
                    release = { it, _ -> it.close() }
                )
                val scanner = Scanner(System.`in`)
                println("Connected to $host:$port")
                while (true) {
                    print("$host:$port> ")
                    val entry = when (val request = scanner.nextCommand()) {
                        null -> continue
                        else -> request.toClientCommand()
                    }
                    raftService.forward(entry)
                        .map { Json.decodeFromString<StringsKeyValueStore.Response>(it.decodeToString()) }
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

    private fun Scanner.nextCommand(): StringsKeyValueStore.Request? {
        val line = nextLine()
        val parts = line.split(" ")
        val command = parts.firstOrNull()
        val key = parts.getOrNull(1)
        val value = parts.drop(2).joinToString { it }
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

    @Option(names = ["--host", "-h"], description = ["The host to connect to"])
    override lateinit var host: String

    @Option(names = ["--port", "-p"], description = ["The port to connect to"], required = true)
    override var port: Int = -1

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

    @Option(names = ["--host", "-h"], description = ["The host to connect to"])
    override lateinit var host: String

    @Option(names = ["--port", "-p"], description = ["The port to connect to"], required = true)
    override var port: Int = -1

    override fun request(): StringsKeyValueStore.Request = StringsKeyValueStore.Set(key, value)
}

@Command(
    name = "delete",
    description = ["Delete a value from the key-value store"]
)
class DeleteCommand : RestRaftCommand() {
    @Parameters(index = "0", description = ["The key to delete"])
    lateinit var key: String

    @Option(names = ["--host", "-h"], description = ["The host to connect to"])
    override lateinit var host: String

    @Option(names = ["--port", "-p"], description = ["The port to connect to"], required = true)
    override var port: Int = -1

    override fun request(): StringsKeyValueStore.Request = StringsKeyValueStore.Delete(key)
}


private fun StringsKeyValueStore.Request.toClientCommand(): LogEntry.ClientCommand {
    val payload = Json.encodeToString(this).encodeToByteArray()
    val entry = LogEntry.ClientCommand(payload)
    return entry
}

abstract class RestRaftCommand : Callable<String> {

    abstract var host: String

    abstract var port: Int

    override fun call(): String = runBlocking {
        resourceScope {
            val client = HttpClient(CIO)
            val raftService = install(
                acquire = { HttpRaftService(RaftRpc.ClusterNode("", host, port), client) },
                release = { it, _ -> it.close() }
            )
            val request = request()
            val payload = Json.encodeToString(request).encodeToByteArray()
            val entry = LogEntry.ClientCommand(payload)
            raftService.forward(entry)
                .map { Json.decodeFromString<StringsKeyValueStore.Response>(it.decodeToString()) }
                .map { it.toString() }
                .getOrElse { "Error: ${it.message}" }
                .also { println(it) }
        }
    }

    abstract fun request(): StringsKeyValueStore.Request
}