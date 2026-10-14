package dev.goquick.sqlitenow.oversqlite

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.defaultRequest
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
internal class HistoryProjectionRetryAfterTest : CrossTargetSyncTestSupport() {
    @Test fun honorsHeaderWithoutChangingAttemptCountOrIdentity() = fixture(2, "1") { operations, api, times ->
        val response = operations.withTransientRetry("pull") { api.sendPullRequest(0, 1, 0, "source-a", 4096) }
        assertEquals(0L, response.stableBundleSeq)
        assertEquals(3, times.size)
        assertTrue(times.zipWithNext().all { (before, after) -> after - before >= 1000L })
    }

    @Test fun persistentCapacityStillExhaustsThreeAttempts() = fixture(Int.MAX_VALUE, "1") { operations, api, times ->
        val error = assertFailsWith<TransientRetryExhaustedException> {
            operations.withTransientRetry("pull") { api.sendPullRequest(0, 1, 0, "source-a", 4096) }
        }
        assertEquals(3, times.size)
        assertTrue(!error.message.orEmpty().contains("private"))
    }

    @Test fun callerDeadlineInterruptsWaitBeforeAnotherDispatch() = fixture(Int.MAX_VALUE, "1") { operations, api, times ->
        assertFailsWith<TimeoutCancellationException> {
            withTimeout(500) { operations.withTransientRetry("pull") { api.sendPullRequest(0, 1, 0, "source-a", 4096) } }
        }
        assertEquals(1, times.size)
    }

    @Test fun invalidHeaderRetainsConfiguredBackoff() = fixture(1, "9223372036854775807") { operations, api, times ->
        operations.withTransientRetry("pull") { api.sendPullRequest(0, 1, 0, "source-a", 4096) }
        assertEquals(listOf(0L, 1L), times.map { it - times.first() })
    }

    @Test fun terminalResponseIsNotRetriedEvenWithHeader() = fixture(Int.MAX_VALUE, "1", terminal = true) { operations, api, times ->
        val error = assertFailsWith<OversqliteRemoteContractException> {
            operations.withTransientRetry("pull") { api.sendPullRequest(0, 1, 0, "source-a", 4096) }
        }
        assertEquals("committed_bundle_unrepresentable", error.code)
        assertEquals(1, times.size)
    }

    private fun fixture(
        capacityResponses: Int,
        retryAfter: String,
        terminal: Boolean = false,
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
                transientRetryPolicy = OversqliteTransientRetryPolicy(initialBackoffMillis = 1, maxBackoffMillis = 10, jitterRatio = 0.0)),
            resources, SnapshotDiagnosticsState(),
        ) {}
        val api = OversqliteRemoteApi(resources.http, Json { ignoreUnknownKeys = true }, generation = { testGeneration() }, log = {})
        try { assertions(operations, api, times) }
        finally { resources.closeFromCoordinatedOwner(); http.close(); db.close() }
    }
}
