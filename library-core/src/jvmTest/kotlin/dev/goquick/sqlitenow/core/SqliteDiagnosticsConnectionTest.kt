package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.launch
import kotlinx.serialization.json.*
import kotlin.test.*

@OptIn(SqliteNowInternalApi::class)
class SqliteDiagnosticsConnectionTest {
    @Test fun cachedStatementUsesEachExecutionPolicyAndCountsPhysicalCalls() = runBlocking {
        val events = mutableListOf<JsonObject>()
        val d = recordingDiagnostics(false)
        d.recording.sink = { events += Json.parseToJsonElement(it.substringAfter("SQLITENOW_DIAG ")).jsonObject; true }
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", false, SqliteConnectionConfig(diagnostics = d))
        try {
            val statement = db.prepare("SELECT 'private value'") // prepared while disabled
            db.withExclusiveAccess { assertTrue(statement.step()); statement.reset() }
            assertTrue(events.isEmpty())
            d.setEnabled(true)
            d.operation("cached_execution") {
                db.withExclusiveAccess {
                    repeat(3) { assertTrue(statement.step()); assertFalse(statement.step()); statement.reset(); statement.clearBindings() }
                }
            }
            val physical = events.filter { it["kind"]?.jsonPrimitive?.content == "sql_metric" }
            assertEquals(0, physical.count { it["name"]?.jsonPrimitive?.content == "prepare" })
            assertEquals(6, physical.single { it["name"]?.jsonPrimitive?.content == "step" }["counts"]!!.jsonObject["calls"]!!.jsonPrimitive.int)
            assertEquals(3, physical.single { it["name"]?.jsonPrimitive?.content == "step" }["counts"]!!.jsonObject["returned_rows"]!!.jsonPrimitive.int)
            d.setEnabled(false)
            val before = events.size
            db.withExclusiveAccess { assertTrue(statement.step()); statement.reset(); statement.close() }
            assertEquals(before, events.size)
            assertFalse("private value" in events.toString())
        } finally { db.close() }
    }

    @Test fun formattingIsSharedByPrepareCacheAndRepeatedExecutionAcrossCaptures() = runBlocking {
        val d = recordingDiagnostics(false)
        var formats = 0
        d.recording.formatter = { formats++; sqlStatementShape(it, 4_096, 512) }
        val events = mutableListOf<JsonObject>()
        d.recording.sink = { events += Json.parseToJsonElement(it.substringAfter("SQLITENOW_DIAG ")).jsonObject; true }
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", false, SqliteConnectionConfig(diagnostics = d))
        try {
            val st = db.prepare("SELECT 'private value'")
            assertEquals(0, formats)
            d.setEnabled(true)
            repeat(2) { capture ->
                d.operation("execution") {
                    db.withExclusiveAccess {
                        repeat(50) {
                            db.recordDiagnosticCacheAccess("unused raw input", st, true)
                            assertTrue(st.step()); assertFalse(st.step()); st.reset(); st.clearBindings()
                        }
                    }
                }
                assertEquals(1, formats)
                if (capture == 0) {
                    d.setEnabled(false)
                    db.withExclusiveAccess { assertTrue(st.step()); st.reset() }
                    assertEquals(1, formats)
                    d.setEnabled(true)
                }
            }
            val roots = events.filter { it["kind"]?.jsonPrimitive?.content == "span_start" && it["name"]?.jsonPrimitive?.content == "execution" }
            assertEquals(2, roots.size)
            for (root in roots) {
                val steps = events.single { it["capture"] == root["capture"] && it["span"] == root["span"] && it["kind"]?.jsonPrimitive?.content == "sql_metric" && it["name"]?.jsonPrimitive?.content == "step" }
                assertEquals(100, steps["counts"]!!.jsonObject["calls"]!!.jsonPrimitive.int)
            }
            d.operation("prepared_enabled") {
                db.withExclusiveAccess {
                    val enabled = db.prepare("SELECT 2")
                    repeat(20) { assertTrue(enabled.step()); enabled.reset(); enabled.clearBindings() }
                    enabled.close()
                }
            }
            assertEquals(2, formats)
            d.setEnabled(false); st.close()
        } finally { db.close() }
    }

    @Test fun concurrentFamilySlotsKeepLeafAndRootCountersExact() = runBlocking {
        val events = java.util.concurrent.ConcurrentLinkedQueue<JsonObject>()
        val d = recordingDiagnostics()
        d.recording.sink = { events += Json.parseToJsonElement(it.substringAfter("SQLITENOW_DIAG ")).jsonObject; true }
        val root = d.beginOperation("root")!!
        kotlinx.coroutines.coroutineScope {
            repeat(4) {
                launch(kotlinx.coroutines.Dispatchers.Default) {
                    val child = root.child("worker")!!
                    val descriptor = DiagnosticSqlDescriptor("SELECT ?")
                    repeat(500) { child.recordSql(descriptor, "step", 2, returnedRow = true) }
                    child.finish()
                }
            }
        }
        root.finish()
        val counts = events.single { it["kind"]?.jsonPrimitive?.content == "sql_metric" && it["span"]?.jsonPrimitive?.long == root.id && it["name"]?.jsonPrimitive?.content == "step" }["counts"]!!.jsonObject
        assertEquals(2000, counts["calls"]!!.jsonPrimitive.int)
        assertEquals(4000, counts["total_ns"]!!.jsonPrimitive.int)
        assertEquals(2000, counts["returned_rows"]!!.jsonPrimitive.int)
        assertEquals(1, events.count { it["kind"]?.jsonPrimitive?.content == "sql_family" })
    }

    @Test fun cacheCheckoutLabelsIncludeResetAndClearAndExpireWithTheirCapture() = runBlocking {
        val events = mutableListOf<JsonObject>()
        val d = recordingDiagnostics(false)
        var formats = 0
        d.recording.formatter = { formats++; "SELECT ?" }
        d.recording.sink = { events += Json.parseToJsonElement(it.substringAfter("SQLITENOW_DIAG ")).jsonObject; true }
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", false, SqliteConnectionConfig(diagnostics = d))
        try {
            val st = db.prepare("SELECT 1")
            d.setEnabled(true)
            d.operation("labelled") {
                db.withExclusiveAccess {
                    val span = currentDiagnosticSpan()
                    withSqliteNowDiagnosticContext(span, "fixed.label") {
                        db.recordDiagnosticCacheAccess("unused", st, true)
                        assertTrue(st.step()); st.reset(); st.clearBindings()
                    }
                }
            }
            assertEquals(0, formats, "semantic label bypasses formatting for every action")
            val sqlFamilies = events.filter { it["kind"]?.jsonPrimitive?.content == "sql_metric" }.map { it["family"] }.toSet()
            val names = events.filter { it["kind"]?.jsonPrimitive?.content == "sql_family" && it["family"] in sqlFamilies }.map { it["name"]!!.jsonPrimitive.content }
            assertEquals(listOf("fixed.label"), names)
            d.operation("unlabelled") { db.withExclusiveAccess { assertTrue(st.step()); st.reset(); st.clearBindings() } }
            assertEquals(1, formats)
            d.setEnabled(false); st.close()
        } finally { db.close() }
    }

    private suspend fun currentDiagnosticSpan() = checkNotNull(SqliteNowDiagnosticContext.current(kotlin.coroutines.coroutineContext)?.span)

    @Test fun disabledConnectionNeverCallsDiagnosticProducersAndRollbackIsUnchanged() = runBlocking {
        val d = recordingDiagnostics(false)
        var calls = 0
        d.recording.clockFactory = { calls++; error("disabled clock") }
        d.recording.sink = { calls++; error("disabled emission") }
        d.recording.formatter = { calls++; error("disabled shape") }
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", false, SqliteConnectionConfig(diagnostics = d))
        try {
            db.execSQL("CREATE TABLE item(id INTEGER)")
            assertFailsWith<IllegalStateException> {
                db.transaction { db.execSQL("INSERT INTO item VALUES(1)"); error("rollback") }
            }
            db.prepare("SELECT count(*) FROM item").use { assertTrue(it.step()); assertEquals(0, it.getLong(0)) }
        } finally { db.close() }
        assertEquals(0, calls)
    }

    @Test fun enablingInsideAnOffConnectionOwnerWaitsForTheNextOperation() = runBlocking {
        val d = recordingDiagnostics(false)
        var clocks = 0; var records = 0
        d.recording.clockFactory = { clocks++; { 0L } }
        d.recording.sink = { records++; true }
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", false, SqliteConnectionConfig(diagnostics = d))
        try {
            db.withExclusiveAccess {
                d.setEnabled(true)
                d.operation("nested_stays_off") { db.execSQL("CREATE TABLE item(id INTEGER)") }
            }
            assertEquals(0, clocks); assertEquals(0, records)
            db.execSQL("INSERT INTO item VALUES(1)")
            assertTrue(clocks > 0); assertTrue(records > 0)
        } finally { db.close() }
    }

    @Test fun enabledFailureDoesNotChangeDatabaseAndCommitIsConfirmed() = runBlocking {
        for (throwing in listOf(false, true)) {
            val events = mutableListOf<JsonObject>()
            val d = recordingDiagnostics(true)
            d.recording.sink = { if (throwing) error("sink"); events += Json.parseToJsonElement(it.substringAfter("SQLITENOW_DIAG ")).jsonObject; true }
            val db = BundledSqliteConnectionProvider.openConnection(":memory:", false, SqliteConnectionConfig(diagnostics = d))
            try {
                db.execSQL("CREATE TABLE item(id INTEGER)")
                db.transaction { db.execSQL("INSERT INTO item VALUES(1)") }
                db.prepare("SELECT count(*) FROM item").use { assertTrue(it.step()); assertEquals(1, it.getLong(0)) }
                if (!throwing) assertTrue(events.any { it["name"]?.jsonPrimitive?.content == "db.transaction_committed" && it["outcome"]?.jsonPrimitive?.content == "confirmed" })
            } finally { db.close() }
        }
    }
}
