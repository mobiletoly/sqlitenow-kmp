/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.oversqlite

import kotlin.time.TimeSource

/** Deterministic high-water facts for snapshot acceptance tests. */
internal data class SnapshotRestoreDiagnostics(
    val sessionCount: Long = 0,
    val fetchedChunks: Long = 0,
    val maxValidatedChunkRows: Int = 0,
    val maxDeclaredChunkBytes: Long = 0,
    val maxCompletelyDecodedChunkBodyBytes: Long = 0,
    val maxLiveApplyPageRows: Int = 0,
    val maxLiveApplyPageStagedTextBytes: Long = 0,
    val applyPages: Long = 0,
    val finalStagedRows: Long = 0,
    val finalAppliedRows: Long = 0,
    val restoreDurationMillis: Long = 0,
    val capacityResponses: Long = 0,
    val capacityRetries: Long = 0,
    val capacityWaitMillis: Long = 0,
)

internal class SnapshotDiagnosticsRecorder(
    val phases: SyncPhaseDiagnostics = SyncPhaseDiagnostics(),
    private val span: dev.goquick.sqlitenow.core.SqliteNowDiagnosticSpan? = null,
) {
    private val started = if (span != null) TimeSource.Monotonic.markNow() else null
    private var lastProgress = Long.MIN_VALUE

    private fun progress(name: String, rows: Long, bytes: Long, basis: String) {
        val active = span ?: return
        val now = active.nowNanos()
        if (lastProgress != Long.MIN_VALUE && now - lastProgress < 1_000_000_000L) return
        lastProgress = now
        active.event(name, "progress", { mapOf("rows" to rows, "bytes" to bytes) }, basis)
    }
    private var sessionCount = 0L
    private var fetchedChunks = 0L
    private var maxValidatedChunkRows = 0
    private var maxDeclaredChunkBytes = 0L
    private var maxCompletelyDecodedChunkBodyBytes = 0L
    private var maxLiveApplyPageRows = 0
    private var maxLiveApplyPageStagedTextBytes = 0L
    private var applyPages = 0L
    private var stagedBytes = 0L
    private var validatedRows = 0L
    private var installedRows = 0L
    private var installedBytes = 0L
    private var finalStagedRows = 0L
    private var finalAppliedRows = 0L
    private var restoreAttempted = false
    private var capacityResponses = 0L
    private var capacityRetries = 0L
    private var capacityWaitMillis = 0L

    fun markRestoreAttempt() {
        restoreAttempted = true
        phases.start()
        span?.event("snapshot.restore", "started")
    }

    fun hasRestoreAttempt(): Boolean = restoreAttempted

    fun recordSession() {
        sessionCount++
    }

    fun recordCompletelyDecodedChunkBody(decodedBytes: Long) {
        require(decodedBytes >= 0L) { "decoded chunk body bytes must be non-negative" }
        maxCompletelyDecodedChunkBodyBytes = maxOf(maxCompletelyDecodedChunkBodyBytes, decodedBytes)
    }

    fun recordValidatedChunk(rows: Int, declaredBytes: Long) {
        fetchedChunks++
        stagedBytes += declaredBytes
        validatedRows += rows
        phases.progress("download_staging", validatedRows, stagedBytes, "validated")
        progress("download_staging", validatedRows, stagedBytes, "validated")
        maxValidatedChunkRows = maxOf(maxValidatedChunkRows, rows)
        maxDeclaredChunkBytes = maxOf(maxDeclaredChunkBytes, declaredBytes)
    }

    fun recordApplyPage(rows: Int, stagedTextBytes: Long) {
        require(rows > 0) { "observed apply page must be non-empty" }
        require(stagedTextBytes >= 0L) { "observed apply page bytes must be non-negative" }
        applyPages++
        maxLiveApplyPageRows = maxOf(maxLiveApplyPageRows, rows)
        maxLiveApplyPageStagedTextBytes = maxOf(maxLiveApplyPageStagedTextBytes, stagedTextBytes)
    }

    fun recordInstalledPage(rows: Int, stagedTextBytes: Long) {
        require(rows > 0 && stagedTextBytes >= 0L)
        installedRows += rows
        installedBytes += stagedTextBytes
        phases.progress("row_installation", installedRows, installedBytes, "installed_uncommitted")
        progress("row_installation", installedRows, installedBytes, "installed_uncommitted")
    }

    fun recordStagedRows(rows: Long) {
        finalStagedRows = rows
    }

    fun recordAppliedRows(rows: Long) {
        finalAppliedRows = rows
        span?.event("snapshot.committed", "confirmed", { mapOf("rows" to rows) }, "confirmed_committed")
    }

    fun recordCapacityResponse() {
        capacityResponses++
    }

    fun recordCapacityRetry() {
        capacityRetries++
    }

    fun recordCapacityWait(waitedMillis: Long) {
        capacityWaitMillis += waitedMillis.coerceAtLeast(0L)
    }

    fun finish(outcome: String) {
        phases.finish(outcome, finalAppliedRows, stagedBytes)
        span?.event("snapshot.resources", outcome, {
            mapOf("sessions" to sessionCount, "fetched_chunks" to fetchedChunks,
                "max_validated_chunk_rows" to maxValidatedChunkRows.toLong(),
                "max_declared_chunk_bytes" to maxDeclaredChunkBytes,
                "max_decoded_chunk_body_bytes" to maxCompletelyDecodedChunkBodyBytes,
                "max_live_apply_page_rows" to maxLiveApplyPageRows.toLong(),
                "max_live_apply_page_bytes" to maxLiveApplyPageStagedTextBytes,
                "capacity_responses" to capacityResponses, "capacity_retries" to capacityRetries,
                "capacity_wait_ms" to capacityWaitMillis)
        }, "observed_high_water_and_totals")
    }

    fun snapshot(): SnapshotRestoreDiagnostics = SnapshotRestoreDiagnostics(
        sessionCount = sessionCount,
        fetchedChunks = fetchedChunks,
        maxValidatedChunkRows = maxValidatedChunkRows,
        maxDeclaredChunkBytes = maxDeclaredChunkBytes,
        maxCompletelyDecodedChunkBodyBytes = maxCompletelyDecodedChunkBodyBytes,
        maxLiveApplyPageRows = maxLiveApplyPageRows,
        maxLiveApplyPageStagedTextBytes = maxLiveApplyPageStagedTextBytes,
        applyPages = applyPages,
        finalStagedRows = finalStagedRows,
        finalAppliedRows = finalAppliedRows,
        restoreDurationMillis = started?.elapsedNow()?.inWholeMilliseconds ?: 0,
        capacityResponses = capacityResponses,
        capacityRetries = capacityRetries,
        capacityWaitMillis = capacityWaitMillis,
    )
}

internal data class SnapshotDiagnosticsScope(
    val recorder: SnapshotDiagnosticsRecorder,
    val owner: Boolean,
)

internal class SnapshotDiagnosticsState {
    @kotlin.concurrent.Volatile
    private var published = SnapshotRestoreDiagnostics()
    private var active: SnapshotDiagnosticsRecorder? = null

    fun begin(span: dev.goquick.sqlitenow.core.SqliteNowDiagnosticSpan? = null): SnapshotDiagnosticsScope {
        active?.let { return SnapshotDiagnosticsScope(it, owner = false) }
        val recorder = SnapshotDiagnosticsRecorder(span = span)
        active = recorder
        return SnapshotDiagnosticsScope(recorder, owner = true)
    }

    fun current(): SnapshotDiagnosticsRecorder? = active

    fun finish(scope: SnapshotDiagnosticsScope, outcome: String) {
        if (!scope.owner) return
        check(active === scope.recorder) { "snapshot diagnostics scope ownership changed" }
        if (scope.recorder.hasRestoreAttempt()) {
            published = scope.recorder.snapshot()
            scope.recorder.finish(outcome)
        }
        active = null
    }

    fun published(): SnapshotRestoreDiagnostics = published

    fun reset() {
        check(active == null) { "cannot reset snapshot diagnostics during a restore" }
        published = SnapshotRestoreDiagnostics()
    }
}
