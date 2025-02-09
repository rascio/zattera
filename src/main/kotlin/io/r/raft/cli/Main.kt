package io.r.raft.cli

import io.r.utils.JacocoExclusionNeedsGenerated
import picocli.CommandLine
import picocli.CommandLine.Command
import kotlin.system.exitProcess

@JacocoExclusionNeedsGenerated
fun main(args: Array<String>) {
    val exitCode = CommandLine(MainCmd())
        .execute(*args)
    exitProcess(exitCode)
}

@Command(
    name = "raft",
    description = ["Raft consensus algorithm implementation"],
    subcommands = [RestRaftServer::class, KVShellCommand::class]
)
@JacocoExclusionNeedsGenerated
class MainCmd

val PEER_PATTERN = Regex("\\w+=.*?:\\d+")