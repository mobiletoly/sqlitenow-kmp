package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowDiagnosticSpan

internal val snapshotReadWorkspaceCharge = WorkspaceCharge(8,1024)

/** Explicit capability for a lexical, mutation-free region inside one transaction/cache owner. */
internal class SnapshotReadWorkspace private constructor(
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
    private val cache: StatementCache,
    private var base: WorkspaceCharge?,
    private var span: SqliteNowDiagnosticSpan?,
) {
    private var ended=false
    private var reuses=0L
    private var fallback=0L
    fun base(workspace: OversqliteWorkspace, owner: StatementCache?): WorkspaceCharge? {
        check(!ended && db === workspace.connectionOwner && config === workspace.configurationOwner && workspace.permitsReadBase && cache === owner) {
            "read accounting belongs to a different region or writer"
        }
        return base?.also { if (span != null) { reuses++; span?.aggregateWork("snapshot.workspace.base_reuse",0) } }
    }
    fun retire(): Boolean {
        if(base==null)return false
        base=null
        if (span != null) { fallback++; span?.aggregateWork("snapshot.workspace.fallback",0) }
        return true
    }
    private fun close() {
        base=null;ended=true
        span?.event("snapshot.workspace.summary","observed",{
            mapOf("view_base_reads" to 1L,"reuse_checks" to reuses,"fallbacks" to fallback,
                "retained_peak_entries" to snapshotReadWorkspaceCharge.entries,"retained_peak_bytes" to snapshotReadWorkspaceCharge.bytes)
        },"read_view_initial_load_and_checks")
        span=null
    }
    companion object {
        suspend fun <T> use(workspace: OversqliteWorkspace, unit: String, cache: StatementCache?, block: suspend (SnapshotReadWorkspace?) -> T): T {
            val db=workspace.connectionOwner
            if(cache==null || !workspace.permitsReadBase || !db.inTransaction())return block(null)
            cache.requireOpenForReservationBinding()
            val base=workspace.readDurableBase(unit,cache)
            val admitted=try { workspace.checkCapturedReadBase(unit,base,snapshotReadWorkspaceCharge);true }
                catch(capacity:SyncWorkspaceLimitException) { if(capacity.suppressedExceptions.isNotEmpty())throw capacity;false }
            if(!admitted) { snapshotRowWork("snapshot.workspace.fallback");return block(null) }
            val span=dev.goquick.sqlitenow.core.SqliteNowDiagnosticContext.current(kotlinx.coroutines.currentCoroutineContext())?.span
            val view=SnapshotReadWorkspace(db,workspace.configurationOwner,cache,workspace.add(unit,base,snapshotReadWorkspaceCharge),span)
            span?.aggregateWork("snapshot.workspace.base_read",0)
            try { return block(view) } finally { view.close() }
        }
    }
}
