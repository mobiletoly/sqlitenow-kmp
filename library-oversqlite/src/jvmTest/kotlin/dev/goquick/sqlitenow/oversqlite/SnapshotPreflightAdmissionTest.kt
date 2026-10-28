package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.common.SqliteNowLogger
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.SqliteNowDiagnostics
import kotlinx.serialization.json.*
import kotlin.test.*

internal class PipelineDiagnosticCapture : SqliteNowLogger {
    private val events = java.util.Collections.synchronizedList(mutableListOf<JsonObject>())
    private fun record(message: () -> String) {
        val text = message()
        if (text.startsWith("SQLITENOW_DIAG ")) events += Json.parseToJsonElement(text.substringAfter("SQLITENOW_DIAG ")).jsonObject
    }
    override fun e(throwable: Throwable?, message: () -> String) = record(message)
    override fun w(throwable: Throwable?, message: () -> String) = record(message)
    override fun i(throwable: Throwable?, message: () -> String) = record(message)
    override fun d(throwable: Throwable?, message: () -> String) = record(message)
    fun clear() = events.clear()
    fun records(): List<JsonObject> = synchronized(events) { events.toList() }
}

internal class SnapshotPreflightAdmissionTest : SurvivorTestSupport() {
    @Test fun refusedAdmissionPrecedesBothExpensivePassesAndResumeRunsEachOnce() = runOwnedWebDatabaseTest {
        for ((target, rows) in listOf(1L to 4, 8L to 4, 64L to 16, 1_000_000L to 1024)) {
            withSurvivorFixture { server, db, http, seed ->
                seed.closeFromCoordinatedOwner()
                val capture = PipelineDiagnosticCapture(); val previous = sqliteNowLogger; sqliteNowLogger = capture
                val config = OversqliteConfig(1, nodeTables, "fixture", "main", maxSyncWorkUnitsPerInvocation = target, diagnostics = SqliteNowDiagnostics(true))
                val client = DefaultOversqliteClient.forOpening(db, config, http, ServerWinsResolver, Any())
                try {
                    client.openAndConnect("user-1").getOrThrow()
                    if (rows > 4) server.addRemoteBundleForTest((0 until rows - 4).map { index -> BundleRow("main", "nodes", mapOf("id" to "R$index"), "INSERT", 2,
                        buildJsonObject { put("id", "R$index"); put("name", "payload-$index"); put("parent_id", JsonNull) }) })
                    capture.clear()
                    val sessions = server.snapshotSessionCreateRequestCount
                    client.rebuild().getOrThrow()
                    val events = capture.records()
                    val admission = events.filter { it["kind"]?.jsonPrimitive?.content == "span_end" && it["name"]?.jsonPrimitive?.content == "snapshot.preflight_admission" }
                    assertTrue(admission.isNotEmpty())
                    assertTrue(admission.any { it["outcome"]?.jsonPrimitive?.content == "work_yield" }, "fixture must exercise a refused second reservation")
                    for (refusal in admission.filter { it["outcome"]?.jsonPrimitive?.content == "work_yield" }) {
                        val preparation = refusal["parent"]
                        assertTrue(events.none { it["parent"] == preparation && it["kind"]?.jsonPrimitive?.content == "span_start" &&
                            it["name"]?.jsonPrimitive?.content in setOf("snapshot.capture_preflight", "snapshot.dependency_validation") }, "refused admission must not enter either scan")
                    }
                    for (phase in listOf("snapshot.capture_preflight", "snapshot.dependency_validation"))
                        assertEquals(1, events.count { it["kind"]?.jsonPrimitive?.content == "span_end" && it["name"]?.jsonPrimitive?.content == phase && it["outcome"]?.jsonPrimitive?.content == "success" })
                    assertEquals(sessions + 1, server.snapshotSessionCreateRequestCount)
                    assertEquals(rows.toLong(), scalarLong(db, "SELECT count(*) FROM nodes"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_snapshot_stage"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                    assertTrue(client.workSliceDiagnostics().totalWork > 0)
                } finally { client.closeFromCoordinatedOwner(); sqliteNowLogger = previous }
            }
        }
    }
}
