package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.common.sqliteNowLogger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.random.Random
import kotlin.time.TimeSource

/** Only counters and live nesting are retained; no per-row events or SQL are collected. */
internal class SyncPhaseDiagnostics(
    private val sink: (String) -> Unit = { sqliteNowLogger.i { it } },
    private val clock: () -> Long = monotonicMillis(),
) {
    private val operation = Random.nextLong().toULong().toString(16)
    private var active = false
    private var failed = false
    private var started = 0L
    private var depth = 0
    private var lastProgress = Long.MIN_VALUE

    fun start() {
        if (active) return
        active = true
        started = clock()
        emit("restore", "start", 0, 0, 0, "none")
    }

    fun finish(outcome: String, rows: Long, bytes: Long) {
        if (!active) return
        emit("restore", outcome, rows, bytes, clock() - started, "confirmed_committed_rows_validated_bytes")
        active = false
    }

    suspend fun <T> phase(name: String, rows: Long, bytes: Long, block: suspend () -> T): T {
        if (!active || failed) return block()
        val began = clock()
        emit(name, "start", rows, bytes, 0, "expected")
        depth++
        var outcome = "success"
        try { return block() }
        catch (error: Throwable) {
            outcome = if (error is CancellationException) "cancelled" else "failure"
            throw error
        } finally {
            depth--
            emit(name, outcome, rows, bytes, clock() - began, "expected")
        }
    }

    fun progress(name: String, rows: Long, bytes: Long, countBasis: String) {
        if (!active || failed) return
        val now = clock()
        if (lastProgress != Long.MIN_VALUE && now - lastProgress < 1000) return
        lastProgress = now
        emit(name, "progress", rows, bytes, now - started, countBasis)
    }

    private fun emit(phase: String, outcome: String, rows: Long, bytes: Long, elapsed: Long, countBasis: String) {
        if (failed) return
        try {
            sink("oversqlite restore operation=$operation phase=$phase depth=$depth inclusive=true " +
                "rows=$rows bytes=$bytes counts=$countBasis elapsed_ms=$elapsed outcome=$outcome")
        } catch (_: Throwable) { failed = true }
    }
}

private fun monotonicMillis(): () -> Long {
    val started = TimeSource.Monotonic.markNow()
    return { started.elapsedNow().inWholeMilliseconds }
}

internal class SyncDiagnosticsContext(val diagnostics: SyncPhaseDiagnostics) :
    AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<SyncDiagnosticsContext>
}

internal suspend fun <T> syncPhase(name: String, rows: Long = 0, bytes: Long = 0, block: suspend () -> T): T {
    val diagnostics = currentCoroutineContext()[SyncDiagnosticsContext]?.diagnostics ?: return block()
    return diagnostics.phase(name, rows, bytes, block)
}
