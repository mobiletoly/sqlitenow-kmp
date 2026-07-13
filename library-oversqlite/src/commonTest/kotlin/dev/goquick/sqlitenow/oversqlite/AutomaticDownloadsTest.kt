package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.http.HttpStatusCode
import io.ktor.client.HttpClient
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

internal class AutomaticDownloadsTest : CrossTargetSyncTestSupport() {
    @Test
    fun automaticDownloadConfig_validatesIntervals() {
        assertFailsWith<IllegalArgumentException> {
            OversqliteAutomaticDownloadConfig(automaticDownloadIntervalMillis = 0)
        }
        assertFailsWith<IllegalArgumentException> {
            OversqliteAutomaticDownloadConfig(bundleChangeWatchReconnectMinMillis = 0)
        }
        assertFailsWith<IllegalArgumentException> {
            OversqliteAutomaticDownloadConfig(bundleChangeWatchReconnectMaxMillis = 0)
        }
        assertFailsWith<IllegalArgumentException> {
            OversqliteAutomaticDownloadConfig(
                bundleChangeWatchReconnectMinMillis = 10,
                bundleChangeWatchReconnectMaxMillis = 9,
            )
        }

        val defaults = OversqliteAutomaticDownloadConfig()
        assertEquals(BundleChangeWatchMode.OFF, defaults.bundleChangeWatchMode)
        assertEquals(60_000, defaults.automaticDownloadIntervalMillis)
    }

    @Test
    fun automaticDownloads_pollingModePullsThroughAuthoritativePath() = runTest {
        val env = newTwoClientEnv()
        try {
            val worker = launch(Dispatchers.Default) {
                env.follower.runAutomaticDownloads(
                    OversqliteAutomaticDownloadConfig(
                        automaticDownloadIntervalMillis = 25,
                        bundleChangeWatchReconnectMinMillis = 10,
                        bundleChangeWatchReconnectMaxMillis = 20,
                    ),
                )
            }
            try {
                insertUser(env.leaderDb, "u1", "Ada")
                env.leader.pushPending().getOrThrow()

                eventually {
                    scalarLong(env.followerDb, "SELECT COUNT(*) FROM users WHERE id = 'u1'") == 1L
                }

                assertEquals("Ada", scalarText(env.followerDb, "SELECT name FROM users WHERE id = 'u1'"))
                assertTrue(env.server.pullRequestCount > 0)
                assertTrue(env.server.watchAfterBundleSeqs.isEmpty())
            } finally {
                worker.cancelAndJoin()
            }
        } finally {
            env.close()
        }
    }

    @Test
    fun automaticDownloads_pollingModePropagatesProtocolMismatchWithoutRetry() = runTest {
        val env = newTwoClientEnv()
        try {
            env.server.protocolVersion = "v1"
            val initialCapabilityAttempts = env.server.capabilitySourceIds.size
            val initialPullAttempts = env.server.pullRequestCount

            supervisorScope {
                val worker = async(Dispatchers.Default) {
                    env.follower.runAutomaticDownloads(
                        OversqliteAutomaticDownloadConfig(
                            automaticDownloadIntervalMillis = 25,
                            bundleChangeWatchReconnectMinMillis = 10,
                            bundleChangeWatchReconnectMaxMillis = 20,
                        ),
                    )
                }

                val mismatch = assertFailsWith<ProtocolVersionMismatchException> {
                    withContext(Dispatchers.Default) {
                        withTimeout(5_000) {
                            worker.await()
                        }
                    }
                }
                assertEquals("v0", mismatch.expected)
                assertEquals("v1", mismatch.actual)
            }

            assertEquals(initialCapabilityAttempts + 1, env.server.capabilitySourceIds.size)
            assertEquals(initialPullAttempts, env.server.pullRequestCount)
            assertTrue(env.server.watchAfterBundleSeqs.isEmpty())
        } finally {
            env.close()
        }
    }

    @Test
    fun automaticDownloads_pauseSuppressesBackgroundPullsOnly() = runTest {
        val env = newTwoClientEnv()
        try {
            env.follower.pauseDownloads()
            val worker = launch(Dispatchers.Default) {
                env.follower.runAutomaticDownloads(
                    OversqliteAutomaticDownloadConfig(
                        automaticDownloadIntervalMillis = 25,
                        bundleChangeWatchReconnectMinMillis = 10,
                        bundleChangeWatchReconnectMaxMillis = 20,
                    ),
                )
            }
            try {
                insertUser(env.leaderDb, "u1", "Ada")
                env.leader.pushPending().getOrThrow()
                delay(100)

                assertEquals(0L, scalarLong(env.followerDb, "SELECT COUNT(*) FROM users WHERE id = 'u1'"))
                assertEquals(0, env.server.pullRequestCount)

                env.follower.pullToStable().getOrThrow()
                assertEquals("Ada", scalarText(env.followerDb, "SELECT name FROM users WHERE id = 'u1'"))

                insertUser(env.leaderDb, "u2", "Grace")
                env.leader.pushPending().getOrThrow()
                delay(100)
                assertEquals(0L, scalarLong(env.followerDb, "SELECT COUNT(*) FROM users WHERE id = 'u2'"))

                env.follower.resumeDownloads()
                eventually {
                    scalarLong(env.followerDb, "SELECT COUNT(*) FROM users WHERE id = 'u2'") == 1L
                }
            } finally {
                worker.cancelAndJoin()
            }
        } finally {
            env.close()
        }
    }

    @Test
    fun automaticDownloads_autoWatchUsesWatchWakeupAndPullsAuthoritatively() = runTest {
        val env = newTwoClientEnv {
            bundleChangeWatchSupported = true
        }
        try {
            val worker = launch(Dispatchers.Default) {
                env.follower.runAutomaticDownloads(
                    OversqliteAutomaticDownloadConfig(
                        automaticDownloadIntervalMillis = 10_000,
                        bundleChangeWatchMode = BundleChangeWatchMode.AUTO,
                        bundleChangeWatchReconnectMinMillis = 10,
                        bundleChangeWatchReconnectMaxMillis = 20,
                    ),
                )
            }
            try {
                eventually {
                    env.server.watchAfterBundleSeqs.isNotEmpty()
                }

                insertUser(env.leaderDb, "u1", "Ada")
                env.leader.pushPending().getOrThrow()

                eventually {
                    scalarLong(env.followerDb, "SELECT COUNT(*) FROM users WHERE id = 'u1'") == 1L
                }

                assertEquals("Ada", scalarText(env.followerDb, "SELECT name FROM users WHERE id = 'u1'"))
                assertContains(env.server.watchAfterBundleSeqs, 0L)
                assertTrue(env.server.pullRequestCount > 0)
            } finally {
                worker.cancelAndJoin()
            }
        } finally {
            env.close()
        }
    }

    @Test
    fun automaticDownloads_activeWatchDoesNotRunTimerFallbackPulls() = runTest {
        val env = newTwoClientEnv {
            bundleChangeWatchSupported = true
            holdNextWatchResponseOpen()
        }
        try {
            val initialPulls = env.server.pullRequestCount
            val worker = launch(Dispatchers.Default) {
                env.follower.runAutomaticDownloads(
                    OversqliteAutomaticDownloadConfig(
                        automaticDownloadIntervalMillis = 25,
                        bundleChangeWatchMode = BundleChangeWatchMode.AUTO,
                        bundleChangeWatchReconnectMinMillis = 10,
                        bundleChangeWatchReconnectMaxMillis = 20,
                    ),
                )
            }
            try {
                eventually {
                    env.server.watchAfterBundleSeqs.size == 1
                }
                delay(150)

                assertEquals(initialPulls, env.server.pullRequestCount)
            } finally {
                worker.cancelAndJoin()
            }
        } finally {
            env.close()
        }
    }

    @Test
    fun automaticDownloads_watchSetupErrorsFallBackToPull() = runTest {
        val setupErrors = listOf(
            HttpStatusCode.BadRequest to "invalid_request",
            HttpStatusCode.Unauthorized to "authentication_failed",
            HttpStatusCode.Forbidden to "bundle_change_watch_forbidden",
            HttpStatusCode.Conflict to "scope_uninitialized",
            HttpStatusCode.ServiceUnavailable to "bundle_change_watch_disabled",
            HttpStatusCode.InternalServerError to "bundle_change_watch_failed",
        )

        for ((status, error) in setupErrors) {
            val env = newTwoClientEnv {
                bundleChangeWatchSupported = true
                enqueueWatchResponse(
                    status = status,
                    body = """{"error":"$error","message":"test setup error"}""",
                )
            }
            try {
                val rowId = "u-${status.value}"
                insertUser(env.leaderDb, rowId, "Status ${status.value}")
                env.leader.pushPending().getOrThrow()

                val worker = launch(Dispatchers.Default) {
                    env.follower.runAutomaticDownloads(autoWatchConfig())
                }
                try {
                    eventually {
                        scalarLong(env.followerDb, "SELECT COUNT(*) FROM users WHERE id = '$rowId'") == 1L
                    }

                    assertEquals(
                        "Status ${status.value}",
                        scalarText(env.followerDb, "SELECT name FROM users WHERE id = '$rowId'"),
                    )
                    assertContains(env.server.watchAfterBundleSeqs, 0L)
                    assertTrue(env.server.pullRequestCount > 0)
                } finally {
                    worker.cancelAndJoin()
                }
            } finally {
                env.close()
            }
        }
    }

    @Test
    fun automaticDownloads_watchReconnectUsesFreshCapabilitiesAndLatestDurableState() = runTest {
        val env = newTwoClientEnv {
            bundleChangeWatchSupported = true
        }
        try {
            insertUser(env.leaderDb, "u1", "Ada")
            env.leader.pushPending().getOrThrow()

            val initialCapabilityRequests = env.server.capabilitySourceIds.size
            val worker = launch(Dispatchers.Default) {
                env.follower.runAutomaticDownloads(autoWatchConfig())
            }
            try {
                eventually {
                    env.server.watchAfterBundleSeqs.contains(1L) &&
                        env.server.capabilitySourceIds.size >= initialCapabilityRequests + 2
                }

                assertEquals(0L, env.server.watchAfterBundleSeqs.first())
                assertContains(env.server.watchAfterBundleSeqs, 1L)
                assertTrue(env.server.watchSourceIds.all { it.isNotBlank() })
                assertEquals(env.server.watchSourceIds.last(), env.server.capabilitySourceIds.last())
            } finally {
                worker.cancelAndJoin()
            }
        } finally {
            env.close()
        }
    }

    @Test
    fun automaticDownloads_heartbeatsDoNotTriggerPullsByThemselves() = runTest {
        val env = newTwoClientEnv {
            bundleChangeWatchSupported = true
            enqueueWatchResponse(
                body = """
                : heartbeat

                : heartbeat

                : heartbeat

                """.trimIndent(),
            )
        }
        try {
            val worker = launch(Dispatchers.Default) {
                env.follower.runAutomaticDownloads(autoWatchConfig(reconnectMinMillis = 1_000))
            }
            try {
                eventually {
                    env.server.watchAfterBundleSeqs.size == 1 && env.server.pullRequestCount == 1
                }
                delay(100)
                assertEquals(1, env.server.watchAfterBundleSeqs.size)
                assertEquals(1, env.server.pullRequestCount)
            } finally {
                worker.cancelAndJoin()
            }
        } finally {
            env.close()
        }
    }

    @Test
    fun automaticDownloads_streamEofAndMalformedStreamReconnectWithoutStallingPulls() = runTest {
        val streamBodies = listOf(
            ": stream ended\n\n",
            """
            event: bundle
            data: {"bundle_seq":

            """.trimIndent(),
        )

        for (body in streamBodies) {
            val env = newTwoClientEnv {
                bundleChangeWatchSupported = true
                enqueueWatchResponse(body = body)
            }
            try {
                insertUser(env.leaderDb, "u1", "Ada")
                env.leader.pushPending().getOrThrow()

                val worker = launch(Dispatchers.Default) {
                    env.follower.runAutomaticDownloads(autoWatchConfig())
                }
                try {
                    eventually {
                        env.server.watchAfterBundleSeqs.size >= 2 &&
                            scalarLong(env.followerDb, "SELECT COUNT(*) FROM users WHERE id = 'u1'") == 1L
                    }

                    assertTrue(env.server.pullRequestCount > 0)
                } finally {
                    worker.cancelAndJoin()
                }
            } finally {
                env.close()
            }
        }
    }

    private suspend fun newTwoClientEnv(
        configureServer: MockSyncServer.() -> Unit = {},
    ): TwoClientEnv {
        val server = MockSyncServer().apply(configureServer)
        val leaderDb = newDb()
        val followerDb = newDb()
        val leaderHttp = server.newHttpClient()
        val followerHttp = server.newHttpClient()
        createUsersAndPostsTables(leaderDb)
        createUsersAndPostsTables(followerDb)
        val leader = newClient(leaderDb, leaderHttp)
        val follower = newClient(followerDb, followerHttp)
        leader.openAndConnect(userId = "user-1").getOrThrow()
        follower.openAndConnect(userId = "user-1").getOrThrow()
        return TwoClientEnv(
            server = server,
            leaderDb = leaderDb,
            followerDb = followerDb,
            leaderHttp = leaderHttp,
            followerHttp = followerHttp,
            leader = leader,
            follower = follower,
        )
    }

    private suspend fun eventually(
        timeoutMillis: Long = 5_000,
        condition: suspend () -> Boolean,
    ) {
        withContext(Dispatchers.Default) {
            withTimeout(timeoutMillis) {
                while (!condition()) {
                    delay(10)
                }
            }
        }
    }

    private fun autoWatchConfig(
        reconnectMinMillis: Long = 10,
    ): OversqliteAutomaticDownloadConfig =
        OversqliteAutomaticDownloadConfig(
            automaticDownloadIntervalMillis = 10_000,
            bundleChangeWatchMode = BundleChangeWatchMode.AUTO,
            bundleChangeWatchReconnectMinMillis = reconnectMinMillis,
            bundleChangeWatchReconnectMaxMillis = maxOf(reconnectMinMillis, 20),
        )

    private data class TwoClientEnv(
        val server: MockSyncServer,
        val leaderDb: SafeSQLiteConnection,
        val followerDb: SafeSQLiteConnection,
        val leaderHttp: HttpClient,
        val followerHttp: HttpClient,
        val leader: DefaultOversqliteClient,
        val follower: DefaultOversqliteClient,
    ) {
        suspend fun close() {
            server.closeHeldWatchResponses()
            leader.close()
            follower.close()
            leaderHttp.close()
            followerHttp.close()
            leaderDb.close()
            followerDb.close()
        }
    }
}
