package dev.goquick.sqlitenow.core

import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.*
import kotlin.test.*
import kotlin.coroutines.CoroutineContext

class SqliteNowDiagnosticsTest {
    private fun recorder(enabled: Boolean = true): Pair<SqliteNowDiagnostics, MutableList<JsonObject>> {
        val events = mutableListOf<JsonObject>()
        val diagnostics = recordingDiagnostics(enabled)
        var now = 0L
        diagnostics.recording.clockFactory = { { ++now } }
        diagnostics.recording.sink = { line -> events += Json.parseToJsonElement(line.substringAfter("SQLITENOW_DIAG ")).jsonObject; true }
        return diagnostics to events
    }

    @Test fun disabledDoesNoDiagnosticWorkAndKeepsItsSnapshot() = runTest {
        val d = recordingDiagnostics(false)
        var calls = 0
        d.recording.clockFactory = { calls++; error("disabled clock") }
        d.recording.sink = { calls++; error("disabled sink") }
        d.recording.formatter = { calls++; error("disabled formatter") }
        d.operation("off") {
            d.setEnabled(true)
            sqliteNowDiagnosticPhase("still_off", { error("disabled details") }) { assertEquals(42, 42) }
        }
        d.setEnabled(false)
        assertNull(d.beginOperation("off"))
        assertEquals(0, calls)
    }

    @Test fun togglesAffectNextOperationAndChildrenShareCapture() = runTest {
        val (d, events) = recorder()
        d.operation("first") {
            d.setEnabled(false)
            sqliteNowDiagnosticPhase("child") { }
        }
        d.operation("off") { }
        d.setEnabled(true)
        d.operation("next") { }
        assertEquals(listOf("first", "child", "next"), events.filter { it["kind"]?.jsonPrimitive?.content == "span_start" }.map { it["name"]!!.jsonPrimitive.content })
        assertEquals(2, events.map { it["capture"] }.distinct().size)
        assertEquals(1L, events[1]["parent"]!!.jsonPrimitive.long)
    }

    @Test fun countersRedactValuesAndKeepActualExecutionsSeparateFromPrepares() {
        val (d, events) = recorder()
        val root = d.beginOperation("sql")!!
        val sql = "SELECT secret_column FROM private_table WHERE secret_column='secret-value' AND key=12345"
        root.sql(sql, "prepare", 10)
        repeat(4) { root.sql(sql, "step", (it + 1).toLong(), returnedRow = it < 3) }
        root.finish()
        val text = events.joinToString()
        for (secret in listOf("secret_column", "private_table", "secret-value", "12345")) assertFalse(secret in text)
        val steps = events.single { it["kind"]?.jsonPrimitive?.content == "sql_metric" && it["name"]?.jsonPrimitive?.content == "step" }["counts"]!!.jsonObject
        assertEquals(4, steps["calls"]!!.jsonPrimitive.int)
        assertEquals(3, steps["returned_rows"]!!.jsonPrimitive.int)
        assertEquals(10, steps["total_ns"]!!.jsonPrimitive.int)
        assertEquals(1, steps["min_ns"]!!.jsonPrimitive.int)
        assertEquals(4, steps["max_ns"]!!.jsonPrimitive.int)
    }

    @Test fun descriptorReusesClassificationButLabelsAndFamiliesRemainCaptureLocal() {
        val events = mutableListOf<JsonObject>()
        val d = recordingDiagnostics(maxSqlFamilies = 1)
        var formats = 0
        d.recording.formatter = { formats++; "SELECT ?" }
        d.recording.sink = { events += Json.parseToJsonElement(it.substringAfter("SQLITENOW_DIAG ")).jsonObject; true }
        val descriptor = DiagnosticSqlDescriptor("SELECT secret")
        repeat(2) {
            val root = d.beginOperation("root")!!
            val child = root.child("child")!!
            repeat(20) { child.recordSql(descriptor, "step", 3, returnedRow = true) }
            repeat(5) { child.recordSql(descriptor, "reset", 2, familyLabel = "fixed.semantic") }
            child.finish(); root.finish()
            assertNull(child.binding.span); assertNull(root.binding.span)
        }
        assertEquals(1, formats)
        val ends = events.filter { it["kind"]?.jsonPrimitive?.content == "span_end" && it["name"]?.jsonPrimitive?.content == "root" }
        assertEquals(2, ends.size)
        assertTrue(ends.all { it["family_overflow_calls"]!!.jsonPrimitive.int == 5 })
        assertEquals(4, events.count { it["kind"]?.jsonPrimitive?.content == "sql_metric" && it["family"]?.jsonPrimitive?.content == "overflow" })
        assertFalse("secret" in events.toString())
    }

    @Test fun boundedFamiliesAndSpansKeepTotalsAndDeclareOverflow() {
        val events = mutableListOf<JsonObject>()
        val d = recordingDiagnostics(true, maxSqlFamilies = 1, maxActiveSpans = 2)
        d.recording.sink = { events += Json.parseToJsonElement(it.substringAfter("SQLITENOW_DIAG ")).jsonObject; true }
        val root = d.beginOperation("root")!!
        val child = root.child("child")!!
        assertNull(child.child("overflow"))
        root.sql("", "prepare", 1, familyLabel = "first")
        repeat(5) { root.sql("", "step", 1, familyLabel = "second") }
        child.finish(); root.finish()
        val end = events.last()
        assertEquals(1, end["lost_spans"]!!.jsonPrimitive.int)
        assertEquals(5, end["family_overflow_calls"]!!.jsonPrimitive.int)
        assertEquals(5, events.single { it["family"]?.jsonPrimitive?.content == "overflow" }["counts"]!!.jsonObject["calls"]!!.jsonPrimitive.int)
    }

    @Test fun sinkAndFormatterFailuresPreserveResultsAndExceptions() = runTest {
        val (d, _) = recorder()
        var calls = 0
        d.recording.sink = { calls++; error("sink") }
        assertEquals(42, d.operation("sink") { 42 })
        assertEquals(1, calls)
        val failure = IllegalStateException("private data")
        val observed = assertFailsWith<IllegalStateException> { d.operation("failed") { throw failure } }
        assertTrue(observed === failure || observed.cause === failure, "coroutine stack recovery preserves the original cause")
        val (other, events) = recorder()
        other.recording.formatter = { error("formatter") }
        other.operation("format") { currentCoroutineContext()[SqliteNowDiagnosticContext]!!.span!!.sql("SQL", "step", 1) }
        assertTrue(events.none { "private data" in it.toString() || "formatter" in it.toString() })
    }

    @Test fun cancellationAndConcurrentChildrenHaveTerminalParents() = runTest {
        val (d, events) = recorder()
        d.operation("parallel") {
            coroutineScope {
                val barrier = CompletableDeferred<Unit>()
                val a = launch { sqliteNowDiagnosticPhase("a") { barrier.await() } }
                val b = launch { sqliteNowDiagnosticPhase("b") { barrier.complete(Unit) } }
                joinAll(a,b)
            }
            assertFailsWith<CancellationException> { sqliteNowDiagnosticPhase("cancelled") { throw CancellationException("private") } }
        }
        val starts = events.filter { it["kind"]?.jsonPrimitive?.content == "span_start" }
        val ends = events.filter { it["kind"]?.jsonPrimitive?.content == "span_end" }
        assertEquals(starts.map { it["span"] }.toSet(), ends.map { it["span"] }.toSet())
        assertEquals("cancelled", ends.single { it["name"]?.jsonPrimitive?.content == "cancelled" }["outcome"]!!.jsonPrimitive.content)
    }

    @Test fun diagnosticMetadataPreservesTheCallersJob() = runTest {
        val (d, _) = recorder()
        val caller = currentCoroutineContext()[Job]
        d.operation("root") {
            assertSame(caller, currentCoroutineContext()[Job])
            sqliteNowDiagnosticPhase("child") { assertSame(caller, currentCoroutineContext()[Job]) }
        }
        d.setEnabled(false)
        d.operation("off") { assertSame(caller, currentCoroutineContext()[Job]) }
    }

    @Test fun completedOffSnapshotDoesNotDisableLaterOperationsInRetainedContexts() = runTest {
        val (d, events) = recorder(false)
        var retained: CoroutineContext? = null
        d.operation("off") { retained = currentCoroutineContext() }
        d.setEnabled(true)
        withContext(checkNotNull(retained)) { d.operation("next") { } }
        assertEquals(listOf("next", "next"), events.map { it["name"]!!.jsonPrimitive.content })
    }

    @Test fun expiredChildContextFallsBackToItsLiveParent() = runTest {
        val (d, events) = recorder()
        var retained: CoroutineContext? = null
        d.operation("outer") {
            sqliteNowDiagnosticPhase("inner") { retained = currentCoroutineContext() }
            withContext(checkNotNull(retained)) { sqliteNowDiagnosticPhase("after") { } }
        }
        val after = events.single { it["kind"]?.jsonPrimitive?.content == "span_start" && it["name"]?.jsonPrimitive?.content == "after" }
        assertEquals(1L, after["parent"]!!.jsonPrimitive.long)
    }

    @Test fun failedCaptureCannotResumeWhenTheControllerIsReenabled() = runTest {
        val (d, events) = recorder()
        d.operation("old") {
            d.recording.sink = { error("sink") }
            val span = currentCoroutineContext()[SqliteNowDiagnosticContext]!!.span!!
            span.event("break_sink")
            d.setEnabled(false)
            d.recording.sink = { line -> events += Json.parseToJsonElement(line.substringAfter("SQLITENOW_DIAG ")).jsonObject; true }
            d.setEnabled(true)
            span.event("must_not_resume")
        }
        d.operation("new") { }
        assertFalse(events.any { it["name"]?.jsonPrimitive?.content == "must_not_resume" })
        assertEquals(1, events.count { it["name"]?.jsonPrimitive?.content == "old" })
        assertEquals(2, events.count { it["name"]?.jsonPrimitive?.content == "new" })
    }

    @Test fun largeLogicalEventUsesBoundedUnicodeSafeFragments() {
        val lines = mutableListOf<String>()
        val d = recordingDiagnostics(true)
        d.recording.sink = { lines += it; true }
        val root = d.beginOperation("fragments")!!
        root.event("large", counts = { (0..300).associate { "counter_$it" to it.toLong() } })
        root.finish()
        assertTrue(lines.all { it.encodeToByteArray().size <= 3000 })
        assertTrue(lines.any { "\"encoding\":\"base64\"" in it })
    }
}
