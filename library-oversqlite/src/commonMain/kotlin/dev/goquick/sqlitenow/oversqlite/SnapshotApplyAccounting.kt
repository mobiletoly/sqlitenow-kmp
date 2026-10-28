package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SqliteNowDiagnosticSpan
import androidx.sqlite.async.step
import kotlinx.coroutines.currentCoroutineContext

internal val snapshotApplyAccountingCharge = WorkspaceCharge(8, 1024)

/** Explicit external-base capability for owned apply-planning DML, never business DML. */
internal class SnapshotApplyAccounting private constructor(
    private val workspace: OversqliteWorkspace,
    private val cache: StatementCache,
    private val owner: Any,
    private val slice: OversqliteWorkSlice,
    private val currentApply: () -> WorkspaceCharge,
    private var external: WorkspaceCharge?,
    private val counters: Counters?,
) {
    val connectionOwner get() = workspace.connectionOwner
    val retainedCharge get() = if (external == null) WorkspaceCharge() else snapshotApplyAccountingCharge
    private var closed = false

    suspend fun usage(requester: OversqliteWorkspace, writer: StatementCache?, plan: Any = owner): WorkspaceCharge {
        check(!closed && requester.connectionOwner === connectionOwner && requester.configurationOwner === workspace.configurationOwner &&
            requester.permitsReadBase && writer === cache && plan === owner && currentCoroutineContext()[OversqliteWorkSlice] === slice) {
            "apply accounting belongs to another writer or owner"
        }
        val base = external?.also { counters?.let { it.reuses++ } }
            ?: requester.readExternalApplyBase(ReplayFixedUnits.apply, cache).also { counters?.let { it.baseReads++ } }
        return requester.withCurrentTransientCharges(ReplayFixedUnits.apply,
            requester.add(ReplayFixedUnits.apply, base, currentApply()))
    }

    fun retire(): Boolean {
        if (external == null) return false
        external = null
        counters?.let { it.fallbacks++ }
        return true
    }

    fun close() {
        if (closed) return
        external = null; closed = true
        if (slice.snapshotApplyAccounting === this) slice.snapshotApplyAccounting = null
    }

    /** One bounded report per writer invocation, including all callback-delimited root segments. */
    internal class Counters(private var span: SqliteNowDiagnosticSpan?) {
        var baseReads = 0L
        var reuses = 0L
        var fallbacks = 0L
        var segments = 0L
        fun emit() {
            span?.event("snapshot.apply_workspace.summary", "observed", {
                mapOf("external_base_reads" to baseReads, "reuse_checks" to reuses, "fallbacks" to fallbacks,
                    "admitted_segments" to segments, "retained_peak_entries" to if (segments > 0) 8L else 0L,
                    "retained_peak_bytes" to if (segments > 0) 1024L else 0L)
            }, "actual_external_reads_and_segment_checks")
            span = null
        }
    }

    companion object {
        suspend fun eligible(workspace: OversqliteWorkspace, cache: StatementCache?): Boolean {
            if (cache == null || !workspace.permitsReadBase || currentCoroutineContext()[OversqliteWorkSlice] == null || !workspace.connectionOwner.inTransaction()) return false
            cache.requireOpenForReservationBinding()
            return workspace.connectionOwner.withPreparedStatement("SELECT 1 FROM sqlite_schema WHERE type='trigger' AND lower(tbl_name) IN ('_sync_apply_rows','_sync_apply_edges','_sync_apply_plan') UNION ALL SELECT 1 FROM sqlite_temp_schema WHERE type='trigger' AND lower(tbl_name) IN ('_sync_apply_rows','_sync_apply_edges','_sync_apply_plan') LIMIT 1", cache) { !it.step() }
        }

        suspend fun create(workspace: OversqliteWorkspace, unit: String, cache: StatementCache, owner: Any,
            counters: Counters? = null, currentApply: () -> WorkspaceCharge): SnapshotApplyAccounting? {
            val slice = checkNotNull(currentCoroutineContext()[OversqliteWorkSlice])
            check(slice.snapshotApplyAccounting == null && workspace.connectionOwner.inTransaction())
            cache.requireOpenForReservationBinding()
            val external = workspace.readExternalApplyBase(unit, cache)
            counters?.let { it.baseReads++ }
            val base = workspace.add(unit, external, currentApply())
            val admitted = try { workspace.checkCapturedReadBase(unit, base, snapshotApplyAccountingCharge); true }
                catch (capacity: SyncWorkspaceLimitException) { if (capacity.suppressedExceptions.isNotEmpty()) throw capacity; false }
            if (!admitted) { counters?.let { it.fallbacks++ }; return null }
            counters?.let { it.segments++ }
            return SnapshotApplyAccounting(workspace, cache, owner, slice, currentApply, external, counters).also { slice.snapshotApplyAccounting = it }
        }
    }
}
