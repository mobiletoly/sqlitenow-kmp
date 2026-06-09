package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import io.ktor.http.HttpStatusCode
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

internal class SharedWatchBehaviorFixtureTest : CrossTargetSyncTestSupport() {
    private val fixtureJson = kotlinx.serialization.json.Json { ignoreUnknownKeys = true }
    private val fixtureFile = oversqliteContractFixture("watch/behavior/automatic-downloads.json")

    @Test
    fun kmpSharedWatchBehaviorFixturesExecuteAgainstRuntime() = runTest {
        val spec = fixtureJson.decodeFromString(WatchBehaviorSpec.serializer(), fixtureFile.readText())
        assertEquals(1, spec.formatVersion)
        for (case in spec.cases) {
            runCase(case)
        }
    }

    private suspend fun runCase(case: WatchBehaviorCase) {
        val env = newTwoClientEnv {
            bundleChangeWatchSupported = case.serverScript.bundleChangeWatchSupported ?: true
            configureWatchScript(case.serverScript)
        }
        try {
            for (bundle in case.serverScript.startBundles) {
                env.server.addBundleForWatchFixture(bundle)
            }
            coroutineScope {
                val worker = launch(Dispatchers.Default) {
                    env.follower.runAutomaticDownloads(autoWatchConfig(case))
                }
                try {
                    deliverWakeBundlesAfterWatchStart(case, env)
                    try {
                        awaitExpectedState(case, env)
                    } catch (failure: Throwable) {
                        error("${case.name}: timed out waiting for expected state; ${failure.message.orEmpty()}")
                    }
                    case.expectedState.observeForMillis?.let { delay(it) }
                    if (case.expectedState.stopActiveWatchBeforeAssert) {
                        withContext(Dispatchers.Default) {
                            withTimeout(1_000) {
                                worker.cancelAndJoin()
                            }
                        }
                    }
                    assertExpectedState(case, env)
                } finally {
                    if (worker.isActive) {
                        worker.cancelAndJoin()
                    }
                }
            }
        } finally {
            env.close()
        }
    }

    private fun MockSyncServer.configureWatchScript(script: WatchServerScript) {
        for (bundle in script.remoteBundles) {
            addBundleForWatchFixture(bundle)
        }
        when (script.kind) {
            "hold_watch_open" -> holdNextWatchResponseOpen()
            "unsupported_capability" -> Unit
            "non_ok_watch_response" -> {
                val response = script.response ?: error("missing non-OK response")
                enqueueWatchResponse(
                    status = HttpStatusCode.fromValue(response.status),
                    body = response.body.toString(),
                )
            }
            "watch_lines" -> enqueueWatchResponse(body = script.lines.joinToString(separator = "\n"))
            else -> error("unknown watch script kind ${script.kind}")
        }
    }

    private suspend fun deliverWakeBundlesAfterWatchStart(case: WatchBehaviorCase, env: TwoClientEnv) {
        if (case.serverScript.wakeBundles.isEmpty()) {
            return
        }
        eventually {
            env.server.watchAfterBundleSeqs.isNotEmpty()
        }
        for (bundle in case.serverScript.wakeBundles) {
            env.server.addBundleAndNotifyForWatchFixture(bundle)
        }
    }

    private fun MockSyncServer.addBundleForWatchFixture(bundle: RemoteBundleScript) {
        val rows = bundle.rows.mapIndexed { index, row ->
            BundleRow(
                schema = row.schema,
                table = row.table,
                key = row.key.mapValues { it.value.contentOrNull.orEmpty() },
                op = row.op,
                rowVersion = row.rowVersion ?: (index + 1).toLong(),
                payload = row.payload,
            )
        }
        addRemoteBundleForTest(rows)
    }

    private suspend fun MockSyncServer.addBundleAndNotifyForWatchFixture(bundle: RemoteBundleScript) {
        val rows = bundle.rows.mapIndexed { index, row ->
            BundleRow(
                schema = row.schema,
                table = row.table,
                key = row.key.mapValues { it.value.contentOrNull.orEmpty() },
                op = row.op,
                rowVersion = row.rowVersion ?: (index + 1).toLong(),
                payload = row.payload,
            )
        }
        addRemoteBundleAndNotifyForTest(rows)
    }

    private suspend fun awaitExpectedState(case: WatchBehaviorCase, env: TwoClientEnv) {
        eventually {
            case.expectedState.users.all { user ->
                scalarLong(env.followerDb, "SELECT COUNT(*) FROM users WHERE id = '${user.id}' AND name = '${user.name}'") == 1L
            } &&
                case.expectedState.watchAttemptsAtLeast?.let { env.server.watchAfterBundleSeqs.size >= it } != false &&
                case.expectedState.watchAttemptsExactly?.let { env.server.watchAfterBundleSeqs.size == it } != false &&
                case.expectedState.watchAfterBundleSeqsContains.all { env.server.watchAfterBundleSeqs.contains(it) } &&
                case.expectedState.fallbackPullsAtLeast?.let { env.server.pullRequestCount >= it } != false &&
                case.expectedState.fallbackPullsExactly?.let { env.server.pullRequestCount == it } != false
        }
    }

    private fun assertExpectedState(case: WatchBehaviorCase, env: TwoClientEnv) {
        case.expectedState.watchCloseCount?.let {
            assertEquals(it, env.server.watchCloseCount, "${case.name}: watch close count")
        }
        case.expectedState.fallbackPullsAtLeast?.let {
            assertTrue(env.server.pullRequestCount >= it, "${case.name}: fallback pulls")
        }
        case.expectedState.fallbackPullsExactly?.let {
            assertEquals(it, env.server.pullRequestCount, "${case.name}: fallback pulls")
        }
        case.expectedState.watchAttemptsAtLeast?.let {
            assertTrue(env.server.watchAfterBundleSeqs.size >= it, "${case.name}: watch attempts")
        }
        case.expectedState.watchAttemptsExactly?.let {
            assertEquals(it, env.server.watchAfterBundleSeqs.size, "${case.name}: watch attempts")
        }
        for (afterBundleSeq in case.expectedState.watchAfterBundleSeqsContains) {
            assertTrue(
                env.server.watchAfterBundleSeqs.contains(afterBundleSeq),
                "${case.name}: watch after_bundle_seq contains $afterBundleSeq",
            )
        }
    }

    private suspend fun newTwoClientEnv(
        configureServer: MockSyncServer.() -> Unit,
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
        return TwoClientEnv(server, leaderDb, followerDb, leaderHttp, followerHttp, leader, follower)
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

    private fun autoWatchConfig(case: WatchBehaviorCase): OversqliteAutomaticDownloadConfig {
        val reconnect = if (case.expectedState.watchAttemptsExactly != null) 1_000L else 10L
        return OversqliteAutomaticDownloadConfig(
            automaticDownloadIntervalMillis = 10_000,
            bundleChangeWatchMode = BundleChangeWatchMode.AUTO,
            bundleChangeWatchReconnectMinMillis = reconnect,
            bundleChangeWatchReconnectMaxMillis = reconnect,
        )
    }

    @Serializable
    private data class WatchBehaviorSpec(
        val formatVersion: Int,
        val cases: List<WatchBehaviorCase>,
    )

    @Serializable
    private data class WatchBehaviorCase(
        val name: String,
        val description: String,
        val serverScript: WatchServerScript,
        val expectedState: ExpectedWatchState,
    )

    @Serializable
    private data class WatchServerScript(
        val kind: String,
        val bundleChangeWatchSupported: Boolean? = null,
        val response: FixtureHttpResponse? = null,
        val lines: List<String> = emptyList(),
        val remoteBundles: List<RemoteBundleScript> = emptyList(),
        val startBundles: List<RemoteBundleScript> = emptyList(),
        val wakeBundles: List<RemoteBundleScript> = emptyList(),
    )

    @Serializable
    private data class FixtureHttpResponse(
        val status: Int,
        val body: JsonObject,
    )

    @Serializable
    private data class RemoteBundleScript(
        val rows: List<RemoteRowScript>,
    )

    @Serializable
    private data class RemoteRowScript(
        val schema: String,
        val table: String,
        val key: Map<String, JsonPrimitive>,
        val op: String,
        val rowVersion: Long? = null,
        val payload: JsonObject? = null,
    )

    @Serializable
    private data class ExpectedWatchState(
        val watchCloseCount: Int? = null,
        val fallbackPullsAtLeast: Int? = null,
        val fallbackPullsExactly: Int? = null,
        val watchAttemptsAtLeast: Int? = null,
        val watchAttemptsExactly: Int? = null,
        val watchAfterBundleSeqsContains: List<Long> = emptyList(),
        val observeForMillis: Long? = null,
        val stopActiveWatchBeforeAssert: Boolean = false,
        val users: List<ExpectedUser> = emptyList(),
    )

    @Serializable
    private data class ExpectedUser(
        val id: String,
        val name: String,
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
