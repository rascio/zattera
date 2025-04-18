package io.r.raft.transport.ktor

import io.kotest.core.spec.style.FunSpec
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.cors.routing.CORS
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import io.r.raft.machine.RaftMachine
import io.r.raft.protocol.LogEntry
import io.r.raft.protocol.LogEntryMetadata
import io.r.raft.protocol.RaftMessage
import io.r.raft.protocol.RaftRpc

class HttpRaftServiceTest : FunSpec({


    val raftMachine = mockk<RaftMachine<*>>(relaxed = true) {
        every { id } returns "local"
    }

    val httpLocal = HttpRaftController(raftMachine, debugMessages = true)

    val server = embeddedServer(Netty, port = 0) {
        install(CORS) {
            anyHost()
        }
        routing {
            route("/raft", httpLocal.endpoints)
        }
    }
    beforeSpec { server.start() }
    afterSpec { server.stop() }

    val client = autoClose(HttpClient(CIO))

    context("HTTP implementation") {

        val port = server.resolvedConnectors().first().port
        val node = RaftRpc.ClusterNode(
            id = "remote",
            host = "localhost",
            port = port
        )
        val httpRemote = HttpRaftService(node, client)

        test("Test can send RPC") {

            val rpc = RaftRpc.RequestVote(1, "test", LogEntryMetadata.ZERO)
            val message = RaftMessage(
                from = "remote",
                to = "local",
                rpc = rpc
            )
            httpRemote.send(message)
            coVerify(exactly = 1) { raftMachine.handle(message) }
        }
        test("Test can send ConfigurationChange") {
            val entry: LogEntry.Entry = LogEntry.ConfigurationChange(
                new = listOf(
                    RaftRpc.ClusterNode(
                        id = "new_node",
                        host = "localhost",
                        port = 8888
                    )
                )
            )
            httpRemote.request(entry)
            coVerify(exactly = 1) { raftMachine.command(entry) }
        }
    }

})
