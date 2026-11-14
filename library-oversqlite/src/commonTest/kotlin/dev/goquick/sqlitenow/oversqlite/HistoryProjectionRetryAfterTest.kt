package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.MockRequestHandleScope
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.HttpRequestData
import io.ktor.client.request.HttpResponseData
import io.ktor.client.request.request
import io.ktor.client.request.setBody
import io.ktor.client.request.url
import io.ktor.client.statement.bodyAsBytes
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.content.OutgoingContent
import io.ktor.http.content.TextContent
import io.ktor.http.headersOf
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
internal class HistoryProjectionRetryAfterTest : CrossTargetSyncTestSupport() {
    // p0001 history-projection retry timing, amended 2026-11-14: a refused push, push-outcome or pull
    // request is re-sent within its elapsed-time window (30 s, Retry-After or 1 s fallback, positive
    // jitter); the refusals do not consume the generic attempts.
    @Test fun honorsHeaderAndKeepsRequestIdentity() = fixture(2, "1") { operations, api, times ->
        val response = operations.withTransientRetry("pull") { api.sendPullRequest(0, 1, 0, "source-a", 4096) }
        assertEquals(0L, response.stableBundleSeq)
        assertEquals(3, times.size)
        assertTrue(times.zipWithNext().all { (before, after) -> after - before in 1000L..2000L })
    }

    @Test fun capacityRefusalsDoNotConsumeGenericAttempts() = fixture(4, "1", maxAttempts = 1) { operations, api, times ->
        val response = operations.withTransientRetry("pull") { api.sendPullRequest(0, 1, 0, "source-a", 4096) }
        assertEquals(0L, response.stableBundleSeq)
        assertEquals(5, times.size)
    }

    @Test fun persistentCapacityExhaustsTheElapsedWindow() = fixture(Int.MAX_VALUE, "1", window = HistoryCapacityRetryPolicy(jitterRatio = 0.0)) { operations, api, times ->
        val error = assertFailsWith<TransientRetryExhaustedException> {
            operations.withTransientRetry("pull") { api.sendPullRequest(0, 1, 0, "source-a", 4096) }
        }
        // Dispatches at 0..30 s: a 31st wait would end past the 30-second window.
        assertEquals(31, times.size)
        assertTrue(times.zipWithNext().all { (before, after) -> after - before == 1000L })
        assertTrue(!error.message.orEmpty().contains("private"))
    }

    @Test fun retryAfterBeyondTheWindowExhaustsAtOnce() = fixture(Int.MAX_VALUE, "31") { operations, api, times ->
        assertFailsWith<TransientRetryExhaustedException> {
            operations.withTransientRetry("pull") { api.sendPullRequest(0, 1, 0, "source-a", 4096) }
        }
        assertEquals(1, times.size)
    }

    @Test fun disabledWindowKeepsTheGenericAttemptLimit() = fixture(Int.MAX_VALUE, "1", window = HistoryCapacityRetryPolicy(enabled = false)) { operations, api, times ->
        assertFailsWith<TransientRetryExhaustedException> {
            operations.withTransientRetry("pull") { api.sendPullRequest(0, 1, 0, "source-a", 4096) }
        }
        assertEquals(3, times.size)
    }

    @Test fun callerDeadlineInterruptsWaitBeforeAnotherDispatch() = fixture(Int.MAX_VALUE, "1") { operations, api, times ->
        assertFailsWith<TimeoutCancellationException> {
            withTimeout(500) { operations.withTransientRetry("pull") { api.sendPullRequest(0, 1, 0, "source-a", 4096) } }
        }
        assertEquals(1, times.size)
    }

    @Test fun invalidHeaderUsesTheWindowFallbackDelay() = fixture(1, "9223372036854775807", window = HistoryCapacityRetryPolicy(jitterRatio = 0.0)) { operations, api, times ->
        operations.withTransientRetry("pull") { api.sendPullRequest(0, 1, 0, "source-a", 4096) }
        assertEquals(listOf(0L, 1000L), times.map { it - times.first() })
    }

    @Test fun terminalResponseIsNotRetriedEvenWithHeader() = fixture(Int.MAX_VALUE, "1", terminal = true) { operations, api, times ->
        val error = assertFailsWith<OversqliteRemoteContractException> {
            operations.withTransientRetry("pull") { api.sendPullRequest(0, 1, 0, "source-a", 4096) }
        }
        assertEquals("committed_bundle_unrepresentable", error.code)
        assertEquals(1, times.size)
    }

    // A fresh push refused for capacity is re-sent as the same frozen request inside the window and
    // commits in the same call.
    @Test fun refusedPushIsResentUnchangedWithinTheWindow() = pushFixture(refusals = 2, retryAfter = "1") { db, client, server, pushBodies ->
        db.execSQL("INSERT INTO users(id,name) VALUES('item','sent')")
        assertEquals(PushOutcome.COMMITTED, client.pushPending().getOrThrow().outcome)
        assertEquals(3, pushBodies.size)
        assertEquals(1, pushBodies.distinct().size)
        assertEquals(0, server.resolveRequests.size)
        assertEquals(0L, scalarLong(db, pendingIntent))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_upload_journal"))
        assertTrue(server.remoteRowsForBaselineTest().keys.any { it.startsWith("users/") && it.contains("item") })
    }

    // A push refused until its window is exhausted fails with the generic retryable error; the local
    // row and its intent survive and a later push commits them.
    @Test fun exhaustedPushWindowKeepsLocalIntent() = pushFixture(refusals = 1, retryAfter = "31") { db, client, server, pushBodies ->
        db.execSQL("INSERT INTO users(id,name) VALUES('item','kept')")
        val failure = client.pushPending().exceptionOrNull()
        assertIs<TransientRetryExhaustedException>(failure)
        assertTrue(!failure.message.orEmpty().contains("private"))
        assertEquals(1, pushBodies.size)
        // The intent is either still dirty or frozen in the outbox for the same upload.
        assertEquals(1L, scalarLong(db, pendingIntent))
        assertEquals("kept", scalarText(db, "SELECT name FROM users WHERE id='item'"))
        // The refused upload was possibly dispatched, so the next push may first settle it as not
        // committed; the intent then stays pending for the following push.
        var pushes = 0
        while (scalarLong(db, pendingIntent) > 0L && pushes < 3) { client.pushPending().getOrThrow(); pushes++ }
        assertEquals(0L, scalarLong(db, pendingIntent))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_upload_journal"))
        assertTrue(server.remoteRowsForBaselineTest().keys.any { it.startsWith("users/") && it.contains("item") })
    }

    private fun pushFixture(
        refusals: Int,
        retryAfter: String,
        assertions: suspend (SafeSQLiteConnection, DefaultOversqliteClient, MockSyncServer, List<String>) -> Unit,
    ) = runOwnedWebDatabaseTest {
        val db = newDb()
        val server = MockSyncServer()
        val inner = server.newHttpClient()
        val pushBodies = mutableListOf<String>()
        val http = HttpClient(MockEngine { request ->
            if (request.method == HttpMethod.Post && request.url.encodedPath == "/sync/push") {
                pushBodies += request.bodyText()
                if (pushBodies.size <= refusals) {
                    return@MockEngine respond("""{"error":"history_projection_capacity_exceeded","message":"private","retryable":true,"remediation":"retry"}""",
                        HttpStatusCode.ServiceUnavailable, headersOf(HttpHeaders.ContentType to listOf("application/json"), HttpHeaders.RetryAfter to listOf(retryAfter)))
                }
            }
            forward(inner, request)
        }) {
            install(ContentNegotiation) { json(Json) }
            defaultRequest { url("https://sync.test") }
        }
        val client = newClient(db, http)
        try {
            createUsersAndPostsTables(db)
            client.openAndConnect("user-1").getOrThrow()
            assertions(db, client, server, pushBodies)
        } finally {
            client.closeFromCoordinatedOwner(); http.close(); inner.close(); db.close()
        }
    }

    private val pendingIntent = "SELECT (SELECT count(*) FROM _sync_dirty_rows) + (SELECT count(*) FROM _sync_outbox_rows)"

    private suspend fun MockRequestHandleScope.forward(inner: HttpClient, request: HttpRequestData): HttpResponseData {
        val skipped = setOf(HttpHeaders.ContentEncoding, HttpHeaders.ContentLength, HttpHeaders.ContentType).map { it.lowercase() }
        val response = inner.request {
            method = request.method
            url(request.url)
            request.headers.forEach { name, values -> if (name.lowercase() !in skipped) headers.appendAll(name, values) }
            if (request.body !is OutgoingContent.NoContent) {
                setBody(TextContent(request.bodyText(), request.body.contentType ?: ContentType.Application.Json))
            }
        }
        return respond(response.bodyAsBytes(), response.status, response.headers)
    }

    private fun fixture(
        capacityResponses: Int,
        retryAfter: String,
        terminal: Boolean = false,
        maxAttempts: Int = 3,
        window: HistoryCapacityRetryPolicy = HistoryCapacityRetryPolicy(),
        assertions: suspend TestScope.(OversqliteClientOperationController, OversqliteRemoteApi, List<Long>) -> Unit,
    ) = runOwnedWebDatabaseTest {
        val times = mutableListOf<Long>()
        var identity: String? = null
        val http = HttpClient(MockEngine) {
          engine {
            dispatcher = StandardTestDispatcher(testScheduler)
            addHandler { request ->
            val current = request.url.toString() + request.headers["Oversync-Source-ID"] + request.headers["Oversync-Schema-Generation-Fingerprint"]
            identity?.let { assertEquals(it, current) } ?: run { identity = current }
            times += testScheduler.currentTime
            if (times.size <= capacityResponses) {
                val body = if (terminal) """{"error":"committed_bundle_unrepresentable","message":"private"}"""
                else """{"error":"history_projection_capacity_exceeded","message":"private","retryable":true,"remediation":"retry"}"""
                respond(body, if (terminal) HttpStatusCode.InternalServerError else HttpStatusCode.ServiceUnavailable,
                    headersOf(HttpHeaders.RetryAfter, retryAfter))
            } else respond("""{"stable_bundle_seq":0,"has_more":false,"bundles":[]}""", HttpStatusCode.OK)
            }
          }
          defaultRequest { url("https://sync.test/") }
        }
        val db = newDb()
        val resources = OversqliteClientResourceOwner(db, http)
        val operations = OversqliteClientOperationController(
            OversqliteConfig(1, listOf(SyncTable("users", "id")), "fixture", "main",
                transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = maxAttempts, initialBackoffMillis = 1, maxBackoffMillis = 10, jitterRatio = 0.0)),
            resources, SnapshotDiagnosticsState(),
        ) {}
        val api = OversqliteRemoteApi(resources.http, Json { ignoreUnknownKeys = true }, generation = { testGeneration() }, log = {},
            historyCapacityRetryPolicy = window)
        try { assertions(operations, api, times) }
        finally { resources.closeFromCoordinatedOwner(); http.close(); db.close() }
    }
}
