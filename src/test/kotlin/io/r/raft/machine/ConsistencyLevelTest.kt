package io.r.raft.machine

import arrow.fx.coroutines.resourceScope
import io.kotest.assertions.withClue
import io.kotest.core.NamedTag
import io.kotest.core.Tag
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.r.kv.StringsKeyValueStore
import io.r.raft.client.RaftClusterClient
import io.r.raft.transport.inmemory.installRaftClusterNetwork
import io.r.utils.loggingCtx
import io.r.utils.logs.entry
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.kotlin.logger
import kotlin.test.assertIs
import kotlin.time.Duration.Companion.seconds

object SlowTest : Tag()

// https://jepsen.io/consistency/models
class ConsistencyLevelTest : FunSpec({

    val logger = LogManager.getLogger("ConsistencyLevelTest")

    tags(SlowTest)

    context("Linearizability") {
        test("every operation appears to take place atomically, in some order, consistent with the real-time ordering of those operations").config(
            timeout = 180.seconds
        ) {
            resourceScope {
                /* C clients sending M messages each while the cluster burn down
                 *
                 * The cluster is composed of 3 nodes
                 * Clients send a "set" command to the cluster
                 * The set is mimicking a list as using a placeholder to keep the previous value of the entries
                 * example:
                 *  set A 0       -> A=0
                 *  set A {{A}},1 -> A=0,1
                 *  set A {{A}},2 -> A=0,1,2
                 *
                 * the cluster should converge to an ordered state
                 * we can then check the set values are written in order
                 */
                val C = 8
                val M = 15

                val clusterNetwork = installRaftClusterNetwork()
                val cluster = installRaftTestCluster(
                    network = clusterNetwork,
                    nodeIds = (1..3).map { "T$it" },
                    config = {
                        RaftMachine.Configuration(
                            maxLogEntriesPerAppend = 4,
                            leaderElectionTimeoutMs = 100L,
                            leaderElectionTimeoutJitterMs = 60,
                            heartbeatTimeoutMs = 30L,
                            pendingCommandsTimeout = 5000L
                        )
                    },
                    stateMachineFactory = { StringsKeyValueStore() }
                )
                val clientConfiguration = RaftClusterClient.Configuration(
                    retry = 30,
                    delay = 200..300L,
                    jitter = 100..200L
                )
                val client = RaftClusterClient(
                    peers = clusterNetwork,
                    configuration = clientConfiguration,
                    contract = StringsKeyValueStore
                )
                logger.info("----- started -----")

                coroutineScope {
                    val clientsSendingEntriesJob = launch {
                        startClientsSendingBatches(C, M) {
                            RaftClusterClient(
                                peers = clusterNetwork,
                                configuration = clientConfiguration,
                                contract = StringsKeyValueStore
                            )
                        }.joinAll()
                    }

                    startChaos(cluster, clientsSendingEntriesJob)

                    clientsSendingEntriesJob.join()
                }
                cluster.awaitLogConvergence(10.seconds)
                cluster.dumpRaftLogs(decode = true)
                ('A'..('A' + C)).forEach { key ->
                    val string = StringsKeyValueStore.Get("$key")
                        .let { client.query(it) }
                    val response = string.getOrThrow()
                    assertIs<StringsKeyValueStore.Value>(response)
                    val list = response
                        .value
                        .split(",")
                        .drop(1) // messages start with a comma

                    withClue("Key $key") {
                        list shouldBe (0 until M).map { "$it" }
                    }
                }
            }
        }

    }
})

private fun CoroutineScope.startChaos(
    cluster: RaftTestCluster<StringsKeyValueStore.KVCommand, StringsKeyValueStore.KVQuery, StringsKeyValueStore.KVResponse>,
    job: Job
) {
    launch(CoroutineName("Chaos")) {
        do {
            cluster.nodes.forEach { node ->
                if (node.isLeader()) {
                    // Disconnect the leader
                    node.disconnect()
                    // Keep the leader disconnected for a while
                    delay(node.configuration.heartbeatTimeoutMs * (1..10).random())
                    node.reconnect()
                }
            }
        } while (job.isActive)
    }
}

typealias RaftClusterClientFactory = () -> RaftClusterClient<StringsKeyValueStore.KVCommand, StringsKeyValueStore.KVQuery, StringsKeyValueStore.KVResponse>

private fun CoroutineScope.startClientsSendingBatches(
    clients: Int,
    messages: Int,
    raftClusterClientFactory: RaftClusterClientFactory
) = ('A'..('A' + clients)).map { client ->
    launch(Dispatchers.IO) {
        val raftClusterClient = raftClusterClientFactory()
        loggingCtx("client:$client") {
            repeat(messages) { n -> // message number
                loggingCtx("message-$client$n") {
                    val cmd = StringsKeyValueStore.Set("$client", "{{$client}},$n")
                    logger.info { entry("Client_Send") }
                    raftClusterClient.request(cmd)
                        .onFailure {
                            logger.error { entry("Client_Send_Failure", "result" to it.message) }
                        }
                        .onSuccess {
                            logger.info { entry("Client_Send_Success", "result" to it, "entry" to "$client$n") }
                            // Keep for debug if any weird behavior happen again
                            if (it is StringsKeyValueStore.Value) {
                                require(it.value.startsWith("key=$client")) { "expected=message-$client$n found=${it.value}" }
                            }
                        }
                        .getOrThrow()
                }
            }
        }
    }
}
