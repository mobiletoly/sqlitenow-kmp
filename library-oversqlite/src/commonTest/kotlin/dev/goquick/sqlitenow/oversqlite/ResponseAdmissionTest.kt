package dev.goquick.sqlitenow.oversqlite

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.defaultRequest
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlin.test.*

internal class ResponseAdmissionTest : SurvivorTestSupport() {
    @Test fun localFailuresSurviveEveryResponsePhase() = runTest {
        val unit = "a".repeat(64)
        val gzip = "1f8b0800000000000003abae050043bfa6a302000000".chunked(2).map { it.toInt(16).toByte() }.toByteArray()
        val encodings = if (oversqliteFetchManagesResponseCompression) listOf(false) else listOf(false, true)
        for (compressed in encodings) for (phase in 1..if (compressed) 4 else 3) {
            for (failure in listOf(
                SyncWorkspaceLimitException(unit, "entries", 7, 6, 2, true),
                SyncWorkspaceLimitException(unit, "bytes", 700, 600, 200, true),
                SyncWorkspaceLimitException(unit, "atomic_apply_work", 15, 0, 16, false),
                SyncWorkLimitException(unit, 32, 32),
                CancellationException("local cancellation"),
            )) {
                var calls = 0
                val http = HttpClient(MockEngine {
                    respond(if (compressed) gzip else "{}".encodeToByteArray(), HttpStatusCode.OK,
                        if (compressed) headersOf(HttpHeaders.ContentEncoding, "gzip") else headersOf())
                }) { defaultRequest { url("https://sync.test/") } }
                try {
                    val api = OversqliteRemoteApi(http, Json, generation = { testGeneration() }, log = {})
                    val actual = assertFails {
                        api.push("source-a", 1, "a".repeat(64), null,
                            listOf(PushRequestRow("main", "users", mapOf("id" to "u1"), "INSERT", 0,
                                Json.parseToJsonElement("""{"id":"u1","name":"value"}"""))),
                            4096, 4096, onBufferGrowth = { _, _ -> if (++calls == phase) throw failure })
                    }
                    if (failure is CancellationException) {
                        assertEquals(failure.message, assertIs<CancellationException>(actual).message)
                    } else assertSame(failure, actual, "gzip=$compressed phase=$phase calls=$calls")
                    assertEquals(phase, calls)
                } finally { http.close() }
            }
        }
    }

    @Test fun typedWorkFailureReceivesFinalQueueProgress() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, _, seed ->
            db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
            server.beforePushCommit = { throw CancellationException("freeze original") }
            assertFailsWith<CancellationException> { seed.pushPending().getOrThrow() }
            seed.closeFromCoordinatedOwner()
            val before = baselineQualificationVector(db, listOf("nodes"))
            val http = HttpClient(MockEngine { respond("{}", HttpStatusCode.OK) }) {
                defaultRequest { url("https://sync.test/") }
            }
            val resources = OversqliteClientResourceOwner(db, http)
            try {
                val operations = OversqliteClientOperationController(
                    OversqliteConfig(generationId = 1, databaseId = "fixture", serviceId = "main", syncTables = nodeTables),
                    resources, SnapshotDiagnosticsState(), {},
                )
                val failure = SyncWorkLimitException("b".repeat(64), 32, 32)
                val api = OversqliteRemoteApi(resources.http, Json, generation = { testGeneration() }, log = {})
                val result = operations.runExclusiveOperation {
                    api.push("source-a", 1, "a".repeat(64), null,
                        listOf(PushRequestRow("main", "users", mapOf("id" to "u1"), "INSERT", 0,
                            Json.parseToJsonElement("""{"id":"u1","name":"value"}"""))),
                        4096, 4096, onBufferGrowth = { _, _ -> throw failure })
                }
                assertSame(failure, result.exceptionOrNull())
                assertEquals(SyncProgress(0, 1, 0), failure.progress)
                assertEquals("b".repeat(64), failure.unit)
                assertEquals(32L, failure.used)
                assertEquals(32L, failure.limit)
                assertEquals(before, baselineQualificationVector(db, listOf("nodes")))
            } finally { resources.closeFromCoordinatedOwner(); http.close() }
        }
    }

    @Test fun publicRefusalCarriesFinalProgressAndReplaysOriginal() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, http, seed ->
            db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
            seed.closeFromCoordinatedOwner()
            val limited = newClient(db, http, syncTables = nodeTables, workspaceBytes = 1L shl 20)
            try {
                limited.openAndConnect("user-1").getOrThrow()
                server.normalizePushEffects = { effects() }
                val error = assertIs<SyncWorkspaceLimitException>(limited.pushPending().exceptionOrNull())
                assertEquals("bytes", error.dimension)
                assertEquals(1L shl 20, error.limit)
                assertTrue(isCanonicalSha256(error.unit))
                assertTrue(error.used > 0 && error.reserved > 0)
                assertEquals(scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"), error.progress.dirtyRows)
                assertEquals(1L, error.progress.outboundRows)
                assertEquals(0L, error.progress.committedBundles)
                assertEquals(2, server.pushRequestCount, "seed plus actual committed response refusal")
                assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            } finally { limited.closeFromCoordinatedOwner() }
            val resumed = newClient(db, http, syncTables = nodeTables)
            try {
                resumed.openAndConnect("user-1").getOrThrow()
                resumed.pushPending().getOrThrow()
                assertEquals(server.pushBodies[1], server.pushBodies[2])
                requireSurvivors(db)
                assertEquals(3L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
                assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            } finally { resumed.closeFromCoordinatedOwner() }
        }
    }
}
