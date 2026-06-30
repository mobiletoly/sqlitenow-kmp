package dev.goquick.sqlitenow.oversqlite

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.request.get
import io.ktor.client.request.header
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

internal class RealServerBundleChangeWatchTest : RealServerSupport() {
    @Test
    fun watchEnabledCapabilityProbe_reportsBundleChangeWatch() = runTest {
        val config = requireRealServerConfig() ?: return@runTest

        val db = newDb()
        var http: HttpClient? = null
        try {
            createBusinessSubsetTables(db)
            val sourceId = bootstrapManagedSourceId(db, config.baseUrl)
            val token = issueDummySigninToken(config.baseUrl, randomRealServerId("watch-cap-user"), sourceId)
            http = newRealServerHttpClient(config.baseUrl, token)

            val capabilities = http.get("/sync/capabilities") {
                header("Oversync-Source-ID", sourceId)
            }.body<CapabilitiesResponse>()

            assertTrue(
                capabilities.bundleChangeWatchSupported,
                "realserver at ${config.baseUrl} must advertise features.bundle_change_watch=true",
            )
        } finally {
            http?.close()
            db.close()
        }
    }

    @Test
    fun watchTriggeredTwoClientConvergence_usesSseWakeup() = runTest {
        val config = requireRealServerConfig() ?: return@runTest
        resetRealServerState(config.baseUrl)

        val userId = randomRealServerId("watch-user")
        val writerDb = newDb()
        val observerDb = newDb()
        val watchRequestSeen = CompletableDeferred<Unit>()
        var writerHttp: HttpClient? = null
        var observerHttp: HttpClient? = null
        try {
            createBusinessSubsetTables(writerDb)
            createBusinessSubsetTables(observerDb)
            val writerSourceId = bootstrapManagedSourceId(writerDb, config.baseUrl)
            val observerSourceId = bootstrapManagedSourceId(observerDb, config.baseUrl)
            val writerToken = issueDummySigninToken(config.baseUrl, userId, writerSourceId)
            val observerToken = issueDummySigninToken(config.baseUrl, userId, observerSourceId)
            writerHttp = newRealServerHttpClient(config.baseUrl, writerToken)
            observerHttp = newRealServerHttpClient(
                baseUrl = config.baseUrl,
                token = observerToken,
                beforeRequest = { path ->
                    if (path == "/sync/watch" && !watchRequestSeen.isCompleted) {
                        watchRequestSeen.complete(Unit)
                    }
                },
            )
            val writer = newRealServerClient(writerDb, writerHttp)
            val observer = newRealServerClient(observerDb, observerHttp)

            writer.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.STARTED_EMPTY,
                actual = writer.attach(userId).getOrThrow(),
                expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
            )
            observer.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                actual = observer.attach(userId).getOrThrow(),
            )

            val worker = launch(Dispatchers.Default) {
                observer.runAutomaticDownloads(
                    OversqliteAutomaticDownloadConfig(
                        automaticDownloadIntervalMillis = 60_000,
                        bundleChangeWatchMode = BundleChangeWatchMode.AUTO,
                        bundleChangeWatchReconnectMinMillis = 25,
                        bundleChangeWatchReconnectMaxMillis = 50,
                    ),
                )
            }
            try {
                withRealTimeout(5_000) { watchRequestSeen.await() }

                val rowUserId = randomRealServerUuid()
                val rowPostId = randomRealServerUuid()
                insertBusinessUserAndPost(writerDb, rowUserId, rowPostId, "watch")
                writer.pushPending().getOrThrow()

                eventually {
                    scalarLong(observerDb, "SELECT COUNT(*) FROM users WHERE id = '$rowUserId'") == 1L
                }

                assertEquals("User watch", scalarText(observerDb, "SELECT name FROM users WHERE id = '$rowUserId'"))
                assertEquals("Title watch", scalarText(observerDb, "SELECT title FROM posts WHERE id = '$rowPostId'"))
                assertFalse(observer.syncStatus().getOrThrow().pending.hasPendingSyncData)
            } finally {
                worker.cancelAndJoin()
            }
        } finally {
            writerHttp?.close()
            observerHttp?.close()
            writerDb.close()
            observerDb.close()
        }
    }

    @Test
    fun watchCancellation_stopsIdleWorkerPromptly() = runTest {
        val config = requireRealServerConfig() ?: return@runTest
        resetRealServerState(config.baseUrl)

        val userId = randomRealServerId("watch-cancel-user")
        val db = newDb()
        val watchRequestSeen = CompletableDeferred<Unit>()
        var http: HttpClient? = null
        try {
            createBusinessSubsetTables(db)
            val sourceId = bootstrapManagedSourceId(db, config.baseUrl)
            val token = issueDummySigninToken(config.baseUrl, userId, sourceId)
            http = newRealServerHttpClient(
                baseUrl = config.baseUrl,
                token = token,
                beforeRequest = { path ->
                    if (path == "/sync/watch" && !watchRequestSeen.isCompleted) {
                        watchRequestSeen.complete(Unit)
                    }
                },
            )
            val client = newRealServerClient(db, http)
            client.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.STARTED_EMPTY,
                actual = client.attach(userId).getOrThrow(),
                expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
            )

            val worker = launch(Dispatchers.Default) {
                client.runAutomaticDownloads(
                    OversqliteAutomaticDownloadConfig(
                        automaticDownloadIntervalMillis = 60_000,
                        bundleChangeWatchMode = BundleChangeWatchMode.AUTO,
                        bundleChangeWatchReconnectMinMillis = 25,
                        bundleChangeWatchReconnectMaxMillis = 50,
                    ),
                )
            }
            withRealTimeout(5_000) { watchRequestSeen.await() }

            withRealTimeout(1_000) {
                worker.cancelAndJoin()
            }
        } finally {
            http?.close()
            db.close()
        }
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

    private suspend fun withRealTimeout(
        timeoutMillis: Long,
        block: suspend () -> Unit,
    ) {
        withContext(Dispatchers.Default) {
            withTimeout(timeoutMillis) {
                block()
            }
        }
    }
}
