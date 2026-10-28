package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.common.SqliteNowLogger
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.*
import io.ktor.http.HttpStatusCode
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.*
import kotlin.test.*

internal class SyncDiagnosticsCoverageTest : CrossTargetSyncTestSupport() {
    private class Capture : SqliteNowLogger {
        val lines = mutableListOf<String>()
        private fun write(message: () -> String) { message().let { if (it.startsWith("SQLITENOW_DIAG ")) lines += it } }
        override fun e(throwable: Throwable?, message: () -> String) = write(message)
        override fun w(throwable: Throwable?, message: () -> String) = write(message)
        override fun i(throwable: Throwable?, message: () -> String) = write(message)
        override fun d(throwable: Throwable?, message: () -> String) = write(message)
        fun records() = lines.map { Json.parseToJsonElement(it.substringAfter("SQLITENOW_DIAG ")).jsonObject }
    }

    @Test fun restoreConflictRetryRecoveryAndDrainPreserveRequestsSqlAndRows() = runOwnedWebDatabaseTest {
        data class ResultVector(val rows: String, val requests: List<String>, val statements: Int)
        suspend fun scenario(enabled: Boolean): Pair<ResultVector, List<JsonObject>> {
            val previous = sqliteNowLogger
            val capture = Capture(); sqliteNowLogger = capture
            val diagnostics = SqliteNowDiagnostics(enabled)
            val server = MockSyncServer()
            val leaderDb = newDb(); createUsersAndPostsTables(leaderDb)
            val leaderHttp = server.newHttpClient(); val leader = newClient(leaderDb, leaderHttp)
            var statements = 0
            val db = BundledSqliteConnectionProvider.openConnection(":memory:", false,
                SqliteConnectionConfig(diagnostics = diagnostics, sqlTrace = SqlTracePolicy({ statements++ }, maxEvents = 100_000, durationMillis = 120_000)))
            createUsersAndPostsTables(db)
            val http = server.newHttpClient()
            val config = OversqliteConfig(1, listOf(SyncTable("users", "id"), SyncTable("posts", "id")), "fixture", "main",
                diagnostics = diagnostics, maxSyncWorkUnitsPerInvocation = 256,
                transientRetryPolicy = OversqliteTransientRetryPolicy(initialBackoffMillis = 1, maxBackoffMillis = 1))
            val client = DefaultOversqliteClient.forOpening(db, config, http, DeferConflictResolver, Any())
            try {
                leader.openAndConnect("private-account").getOrThrow()
                insertUser(leaderDb,"private-row-one","private-server-name")
                insertUser(leaderDb,"private-row-two","second")
                insertPost(leaderDb,"private-post","private-row-one","private-post-title")
                leader.pushPending().getOrThrow()
                server.requestTrace.clear()
                client.openAndConnect("private-account").getOrThrow()
                assertEquals(2L, scalarLong(db,"SELECT count(*) FROM users"))
                assertEquals(1L, scalarLong(db,"SELECT count(*) FROM posts"))
                updateUserName(leaderDb,"private-row-one","private-new-server-name")
                leader.pushPending().getOrThrow()
                updateUserName(db,"private-row-one","private-local-name")
                assertIs<ConflictNeedsResolutionException>(client.pushPending().exceptionOrNull())
                val conflict = checkNotNull(client.pendingConflict().getOrThrow())
                client.resolveConflict(conflict, MergeResult.AcceptServer).getOrThrow()
                assertEquals("private-new-server-name", scalarText(db,"SELECT name FROM users WHERE id='private-row-one'"))
                server.remainingTransientPullFailures = 1
                client.pullToStable().getOrThrow()
                client.recoveryStatus().getOrThrow()
                client.setRecoveryMode(RecoveryMode.AUTOMATIC_CLEAN).getOrThrow()
                var commits = 0
                client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(beforeCommit = {
                    if (++commits == 2) throw CancellationException("private cancellation")
                }))
                assertFailsWith<CancellationException> { client.rebuild().getOrThrow() }
                client.setSnapshotApplyFaultInjectorForTest(null)
                val sessions = server.snapshotSessionCreateRequestCount
                assertEquals(3L, scalarLong(db,"SELECT count(*) FROM _sync_snapshot_stage"))
                client.rebuild().getOrThrow()
                assertEquals(sessions, server.snapshotSessionCreateRequestCount, "resume reuses the retained stage")
                client.syncThenDetach().getOrThrow()
                assertEquals(0L, scalarLong(db,"SELECT count(*) FROM pragma_foreign_key_check"))
                val rows = scalarText(db,"SELECT group_concat(id||':'||name,'|') FROM (SELECT id,name FROM users ORDER BY id)")
                val vector = ResultVector(rows, server.requestTrace.toList(), statements)
                val records = capture.records()
                if (enabled) capture.lines.forEach(::println) // actual SDK stream for the offline analyzer regression
                else assertTrue(records.isEmpty())
                return vector to records
            } finally {
                client.closeFromCoordinatedOwner(); leader.closeFromCoordinatedOwner()
                http.close(); leaderHttp.close(); db.close(); leaderDb.close(); sqliteNowLogger = previous
            }
        }
        val (disabled, noRecords) = withContext(Dispatchers.Default) { scenario(false) }
        val (enabled, records) = withContext(Dispatchers.Default) { scenario(true) }
        assertEquals(disabled, enabled, "diagnostics must not add SQL/transport work or change rows")
        assertTrue(noRecords.isEmpty())
        val names = records.mapNotNull { it["name"]?.jsonPrimitive?.content }.toSet()
        for (name in listOf("sync.attach", "sync.pushPending", "sync.pullToStable", "sync.resolveConflict", "sync.recoveryStatus", "sync.syncThenDetach",
                            "http.response_body", "snapshot.preservation", "snapshot.unique_state_installation", "snapshot.final_scope_check", "snapshot.trigger_definitions", "schema.catalog_enumeration",
                            "snapshot.final.restoration", "snapshot.final.live_verification",
                            "snapshot.final.authoritative_verification", "snapshot.final.scope_verification", "snapshot.final.restoration_pass"))
            assertTrue(name in names, "missing responsibility: $name")
        assertTrue(records.any { it["name"]?.jsonPrimitive?.content == "sync.retry_decision" && it["outcome"]?.jsonPrimitive?.content == "retryable" })
        assertTrue(records.any { it["name"]?.jsonPrimitive?.content == "snapshot.committed" && it["count_basis"]?.jsonPrimitive?.content == "confirmed_committed" })
        val text = records.toString()
        for (privateValue in listOf("private-account", "private-row-one", "private-server-name", "private-post-title", "private-local-name")) assertFalse(privateValue in text)
        val steps = records.filter { it["kind"]?.jsonPrimitive?.content == "sql_metric" && it["name"]?.jsonPrimitive?.content == "step" }
        assertTrue(steps.sumOf { it["counts"]!!.jsonObject["calls"]!!.jsonPrimitive.long } > 0)
    }

    @Test fun finalValidationChildrenCloseOnSuccessFailureAndCancellation() = runTest {
        val previous = sqliteNowLogger
        val capture = Capture()
        sqliteNowLogger = capture
        val names = listOf("snapshot.final.restoration", "snapshot.final.live_verification",
            "snapshot.final.authoritative_verification", "snapshot.final.scope_verification")
        try {
            for (outcome in listOf("success", "failure", "cancelled")) for (name in names) {
                try {
                    SqliteNowDiagnostics(true).operation("fixture") {
                        syncDiagnosticWork("final_validation") {
                            syncDiagnosticWork(name) {
                                when (outcome) {
                                    "failure" -> error("fixture failure")
                                    "cancelled" -> throw CancellationException("fixture cancellation")
                                }
                            }
                        }
                    }
                } catch (_: CancellationException) { check(outcome == "cancelled") }
                catch (_: IllegalStateException) { check(outcome == "failure") }
            }
            val records = capture.records()
            val starts = records.filter { it["kind"]?.jsonPrimitive?.content == "span_start" }
            val ends = records.filter { it["kind"]?.jsonPrimitive?.content == "span_end" }
            assertEquals(starts.size, ends.size)
            for (start in starts.filter { it["name"]?.jsonPrimitive?.content in names }) {
                val parent = starts.single { it["capture"] == start["capture"] && it["span"] == start["parent"] }
                assertEquals("final_validation", parent["name"]?.jsonPrimitive?.content)
                val end = ends.single { it["capture"] == start["capture"] && it["span"] == start["span"] }
                assertTrue(end["outcome"]?.jsonPrimitive?.content in setOf("success", "failure", "cancelled"))
            }
        } finally { sqliteNowLogger = previous }
    }

    @Test fun watchRequestsTakeFreshPolicySnapshotsAndPreserveCancellation() = runOwnedWebDatabaseTest {
        val old = sqliteNowLogger; val capture = Capture(); sqliteNowLogger = capture
        val diagnostics = SqliteNowDiagnostics()
        val server = MockSyncServer().also { it.bundleChangeWatchSupported = true }
        val http = server.newHttpClient()
        val api = OversqliteRemoteApi(http = http, diagnostics = diagnostics, json = Json,
            generation = { testGeneration(testRegisteredTableSpecs("users", "posts")) }, log = {})
        try {
            suspend fun watch() {
                server.enqueueWatchResponse(HttpStatusCode.OK, "")
                api.watchBundleChanges("00000000-0000-4000-8000-000000000099",0) { error("no data expected") }
            }
            watch(); assertTrue(capture.lines.isEmpty())
            diagnostics.setEnabled(true); watch()
            assertTrue(capture.records().any { it["name"]?.jsonPrimitive?.content == "sync.watch_request" && it["kind"]?.jsonPrimitive?.content == "span_end" })
            diagnostics.setEnabled(false); val before = capture.lines.size; watch(); assertEquals(before,capture.lines.size)
            assertEquals(3,server.watchAfterBundleSeqs.size)
            diagnostics.setEnabled(true)
            server.enqueueWatchResponse(HttpStatusCode.OK, "event: bundle\ndata: {\"bundle_seq\":1}\n\n")
            assertFailsWith<CancellationException> {
                api.watchBundleChanges("00000000-0000-4000-8000-000000000099",0) { throw CancellationException("private callback") }
            }
            assertTrue(capture.records().any { it["name"]?.jsonPrimitive?.content == "sync.watch_request" && it["outcome"]?.jsonPrimitive?.content == "cancelled" })
        } finally { http.close(); sqliteNowLogger = old }
    }
}
