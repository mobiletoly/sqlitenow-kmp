package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class SyncPhaseDiagnosticsTest {
    @Test fun startTerminalAndInclusiveNestingAreBounded() = runBlocking {
        val events = mutableListOf<String>(); var now = 0L
        val d = SyncPhaseDiagnostics(events::add, { now })
        d.start(); d.start()
        d.phase("download_staging", 10, 200) { d.phase("verification_last_chunk", 10, 200) { now = 50 } }
        d.finish("success", 10, 200)
        assertEquals(6, events.size)
        assertTrue(events[2].contains("depth=1 inclusive=true"))
        assertTrue(events.last().contains("elapsed_ms=50 outcome=success"))
        assertEquals(1, events.map { it.substringAfter("operation=").substringBefore(' ') }.distinct().size)
    }
    @Test fun failurePreservesOriginalException() = runBlocking {
        val events = mutableListOf<String>(); val d = SyncPhaseDiagnostics(events::add, { 0 }); val failure = IllegalStateException("private")
        d.start()
        assertSame(failure, assertFailsWith<IllegalStateException> { d.phase("row_installation", 1, 2) { throw failure } })
        d.finish("failure", 1, 2)
        assertEquals(2, events.count { "outcome=failure" in it }); assertTrue(events.none { "private" in it })
    }
    @Test fun cancellationHasTerminalOutcome() = runBlocking {
        val events = mutableListOf<String>(); val d = SyncPhaseDiagnostics(events::add, { 0 }); val failure = CancellationException("caller")
        d.start()
        assertSame(failure, assertFailsWith<CancellationException> { d.phase("verification_seal", 0, 0) { throw failure } })
        d.finish("cancelled", 0, 0)
        assertEquals(2, events.count { "outcome=cancelled" in it })
    }
    @Test fun progressIsThrottledAcrossPhases() {
        val events = mutableListOf<String>(); var now = 0L; val d = SyncPhaseDiagnostics(events::add, { now })
        d.start(); repeat(1000) { d.progress("download_staging", it.toLong(), 0, "validated") }
        now = 999; d.progress("row_installation", 1000, 0, "installed_uncommitted")
        now = 1000; d.progress("row_installation", 1001, 0, "installed_uncommitted")
        assertEquals(2, events.count { "outcome=progress" in it })
    }
    @Test fun throwingSinkCannotChangeExecution() = runBlocking {
        var calls = 0; val d = SyncPhaseDiagnostics({ calls++; error("sink") }, { 0 })
        d.start(); assertEquals(42, d.phase("final_validation", 1, 2) { 42 }); d.finish("success", 1, 2)
        assertEquals(1, calls)
    }
    @Test fun recorderAccumulatesValidatedAndInstalledWorkWithoutClaimingCommitOnFailure() {
        for (committed in listOf(false, true)) {
            val events = mutableListOf<String>(); var now = 0L
            val recorder = SnapshotDiagnosticsRecorder(SyncPhaseDiagnostics(events::add, { now }))
            recorder.markRestoreAttempt()
            recorder.recordValidatedChunk(1000, 100)
            now = 1000; recorder.recordValidatedChunk(1000, 200)
            now = 2000; recorder.recordValidatedChunk(19, 30)
            recorder.recordStagedRows(2019)
            val validated = events.filter { "counts=validated" in it }
            assertEquals(listOf(1000L, 2000L, 2019L), validated.map { it.substringAfter("rows=").substringBefore(' ').toLong() })
            assertEquals(listOf(100L, 300L, 330L), validated.map { it.substringAfter("bytes=").substringBefore(' ').toLong() })
            for ((index, rows) in listOf(256, 256, 256, 256, 256, 256, 256, 227).withIndex()) {
                val before = events.size
                recorder.recordApplyPage(rows, rows * 10L)
                assertEquals(before, events.size, "loading a page has not installed any of its rows")
                if (index == 7 && !committed) break // failure/cancellation before the consumer succeeds
                now += 1000; recorder.recordInstalledPage(rows, rows * 10L)
            }
            val installed = events.filter { "counts=installed_uncommitted" in it }
            assertEquals(if (committed) listOf(256L, 512L, 768L, 1024L, 1280L, 1536L, 1792L, 2019L) else listOf(256L, 512L, 768L, 1024L, 1280L, 1536L, 1792L),
                installed.map { it.substringAfter("rows=").substringBefore(' ').toLong() })
            assertEquals(if (committed) listOf(2560L, 5120L, 7680L, 10240L, 12800L, 15360L, 17920L, 20190L) else listOf(2560L, 5120L, 7680L, 10240L, 12800L, 15360L, 17920L),
                installed.map { it.substringAfter("bytes=").substringBefore(' ').toLong() })
            if (committed) recorder.recordAppliedRows(2019)
            recorder.finish(if (committed) "success" else "cancelled")
            assertTrue(events.last().contains("rows=${if (committed) 2019 else 0} bytes=330 counts=confirmed_committed_rows_validated_bytes"))
            assertEquals(if (committed) 2019L else 0L, recorder.snapshot().finalAppliedRows)
        }
    }

}
