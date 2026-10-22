package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteStatement
import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.add

class SyncWorkLimitException(val unit: String, val limit: Long, val used: Long) :
    RuntimeException("sync_work_limit: unit=$unit limit=$limit used=$used"), OversqliteCategorizedException {
    var progress = SyncProgress(); internal set
    override val category = OversqliteErrorCategory.STATE
}

class SyncWorkspaceLimitException(
    val unit: String, val dimension: String, val limit: Long, val used: Long,
    val reserved: Long, val resumable: Boolean,
) : RuntimeException("sync_workspace_limit: unit=$unit dimension=$dimension limit=$limit used=$used reserved=$reserved"),
    OversqliteCategorizedException {
    var progress = SyncProgress(); internal set
    override val category = OversqliteErrorCategory.STATE
}

// Only preparation can emit this signal. The existing executor commits the
// staging transaction, then delivers the public incomplete result to the caller.
internal class ApplyProgressPending(val result: SyncWorkLimitException) : RuntimeException()

internal const val applyPlanDdl = """CREATE TABLE IF NOT EXISTS _sync_apply_plan (
    singleton_key INTEGER PRIMARY KEY CHECK(singleton_key=1), unit TEXT NOT NULL, guard TEXT NOT NULL,
    phase TEXT NOT NULL, root_cursor INTEGER NOT NULL, visit_cursor INTEGER NOT NULL,
    table_cursor INTEGER NOT NULL, fk_cursor INTEGER NOT NULL, row_cursor TEXT,
    entries INTEGER NOT NULL, bytes INTEGER NOT NULL,
    edge_entries INTEGER NOT NULL DEFAULT 0, edge_bytes INTEGER NOT NULL DEFAULT 0,
    operation_kind TEXT NOT NULL DEFAULT 'pull' CHECK(operation_kind IN ('pull','settlement','snapshot','conflict')),
    source_snapshot_id TEXT NOT NULL DEFAULT '',
    preserve_schema TEXT NOT NULL DEFAULT '', preserve_table TEXT NOT NULL DEFAULT '', preserve_key TEXT NOT NULL DEFAULT '',
    atomic_work INTEGER NOT NULL DEFAULT 0 CHECK(typeof(atomic_work)='integer' AND atomic_work>=0),
    preservation_complete INTEGER NOT NULL DEFAULT 0 CHECK(preservation_complete IN (0,1)))"""
internal const val applyRowsDdl = """CREATE TABLE IF NOT EXISTS _sync_apply_rows (
    ordinal INTEGER PRIMARY KEY, table_name TEXT NOT NULL, local_pk TEXT NOT NULL,
    payload TEXT, wire INTEGER NOT NULL, pending INTEGER NOT NULL, bytes INTEGER NOT NULL,
    role TEXT NOT NULL DEFAULT 'survivor' CHECK(role IN ('intended','survivor','exception','snapshot_ref')),
    owner_revision INTEGER NOT NULL DEFAULT 0 CHECK(typeof(owner_revision)='integer' AND owner_revision>=0),
    saved_rowid INTEGER,
    source_ordinal INTEGER CHECK((role='snapshot_ref')=(source_ordinal IS NOT NULL)
        AND (role<>'snapshot_ref' OR (source_ordinal>0 AND payload IS NULL AND wire=1))),
    UNIQUE(table_name,local_pk))"""
internal const val applyEdgesDdl = """CREATE TABLE IF NOT EXISTS _sync_apply_edges (
    ordinal INTEGER PRIMARY KEY, parent_table TEXT NOT NULL, parent_pk TEXT NOT NULL,
    child_table TEXT NOT NULL, child_pk TEXT NOT NULL, bytes INTEGER NOT NULL,
    edge_kind TEXT NOT NULL DEFAULT 'f' CHECK(edge_kind IN ('f','u')),
    UNIQUE(parent_table,parent_pk,child_table,child_pk,edge_kind))"""
internal const val applyRoleIndexDdl = "CREATE INDEX IF NOT EXISTS _sync_apply_rows_role_ordinal_idx ON _sync_apply_rows(role,ordinal)"

internal const val applyPendingIndexDdl =
    "CREATE INDEX IF NOT EXISTS _sync_apply_rows_pending_idx ON _sync_apply_rows(pending,ordinal)"

internal data class ApplySurvivor(
    val table: String, val pk: String, val payload: String?, val wire: Boolean = false,
    val role: String = if (wire) "intended" else "survivor",
    val stageOrdinal: Long? = null,
    val rowId: Long? = null,
)

private data class ApplyPlan(
    var unit: String, var guard: String, var phase: String = "roots",
    var root: Long = 0, var visit: Long = 0, var table: Long = 0, var fk: Long = 0,
    var cursor: String? = null, var entries: Long = 0, var bytes: Long = 0,
    var edgeEntries: Long = 0, var edgeBytes: Long = 0,
    var kind: String = "pull", var preserveSchema: String = "", var preserveTable: String = "", var preserveKey: String = "",
    var atomicWork: Long = 0, var preservationComplete: Boolean = false, var sourceSnapshotId: String = "",
)

/** Includes the optional two-entry/256-byte holder, never the changing apply owner. */
private class IndexAccountingSnapshot(val chargedExternal: Pair<Long, Long>)

internal fun applyUnit(kind: String, sequence: Long, hash: String): String =
    sha256Hex("$kind:$sequence:$hash".encodeToByteArray())

internal data class DecisionReservationCharge(val held: WorkspaceCharge, val binding: WorkspaceReservationBinding?)

internal class OversqliteApplySurvivors(
    private val db: SafeSQLiteConnection,
    private val local: OversqliteLocalStore,
    private val config: OversqliteConfig,
    private val operations: OversqliteClientOperationController,
    private val json: Json,
) {
    private var uniqueState: RuntimeState? = null
    init { local.uniqueReplay = { table,pk,cache -> reuseInstalledUniqueRow(table,pk,cache) } }
    private val snapshotLifecycle by lazy { OversqliteApplyLifecycle(db, config, local, this, json) }
    suspend fun reservationCharge(token: String): WorkspaceCharge = OversqliteWorkspace(db,config).reserved(token)

    fun recordDecisionContinuationBuild() { operations.workSliceDiagnostics.replayDecisionBuilds++ }
    fun recordDecisionContinuationHit() { operations.workSliceDiagnostics.replayDecisionReuses++ }
    fun recordDecisionContinuationInvalidation() { operations.workSliceDiagnostics.replayDecisionInvalidations++ }

    suspend fun reserveAcknowledgementCheck() {
        operations.reserveSyncWork(ReplayFixedUnits.settlementDecisions, 16)
    }

    suspend fun currentDecisionBinding(state: RuntimeState, token: String, base: WorkspaceCharge, held: WorkspaceCharge, writer: StatementCache): WorkspaceReservationBinding {
        operations.reserveSyncWork(ReplayFixedUnits.settlementDecisions, 16)
        val workspace = OversqliteWorkspace(db, config)
        val requested = workspace.add(ReplayFixedUnits.settlementDecisions, workspace.add(ReplayFixedUnits.settlementDecisions, base, held), decisionReservationBindingCharge)
        return checkNotNull(workspace.resizeInWriter(state, token, requested, writer = writer))
    }

    suspend fun finishDecisionBinding(token: String, base: WorkspaceCharge, held: WorkspaceCharge, writer: StatementCache) {
        val workspace = OversqliteWorkspace(db, config)
        workspace.finishDecisionBindingInWriter(token, workspace.add(ReplayFixedUnits.settlementDecisions, base, held), writer)
    }

    suspend fun reserveDecisionCopies(state: RuntimeState, token: String?, base: WorkspaceCharge, held: WorkspaceCharge,
        schema: String, table: String, key: String, pk: String,
        binding: WorkspaceReservationBinding? = null, writer: StatementCache? = null, memoizeBinding: Boolean = false,
    ): DecisionReservationCharge {
        if(token==null) return DecisionReservationCharge(held,null) // Unconverted conflict work belongs to A3-03.
        val unit=ReplayFixedUnits.settlementDecisions
        operations.reserveSyncWork(unit,16)
        val sql=state.validated.replaySql.table(table).decisionCopySize
        val bytes=checkNotNull(one(sql,physicalKey(state,table,pk),schema,table,key) { it.getLong(0) })
        val workspace=OversqliteWorkspace(db,config)
        val next=workspace.add(unit,held,WorkspaceCharge(32,workspace.scale(unit,bytes,64)))
        val requested=workspace.add(unit,base,next)
        if (!memoizeBinding) {
            workspace.resizeInWriter(state,token,requested)
            return DecisionReservationCharge(next,null)
        }
        try {
            val charged=workspace.add(unit,requested,decisionReservationBindingCharge)
            val captured=workspace.resizeInWriter(state,token,charged,binding,checkNotNull(writer))
            if (binding == null) operations.workSliceDiagnostics.reservationBindingCacheBuilds++
            else operations.workSliceDiagnostics.reservationBindingCacheHits++
            return DecisionReservationCharge(next,captured)
        } catch (capacity: SyncWorkspaceLimitException) {
            if (capacity.suppressedExceptions.isNotEmpty()) throw capacity
            // A later row can need the optional holder's capacity. Drop it before
            // the original admission; the monotonic previous prefix is smaller
            // than the next complete record charge.
            operations.workSliceDiagnostics.reservationBindingCacheFallbacks++
            workspace.resizeInWriter(state,token,requested)
            return DecisionReservationCharge(next,null)
        }
    }

    suspend fun prepare(state: RuntimeState, unit: String, roots: Int, operationKind: String = "pull", inputSequence: Long? = null, workspaceStatementCache: StatementCache? = null, replayStatementCache: StatementCache? = null, sourceSnapshotId: String = "", discoverDependents: Boolean = true, seed: suspend (Int) -> ApplySurvivor) {
        try {
            require(discoverDependents || operationKind == "settlement") { "only unchanged push acknowledgements may omit dependency discovery" }
            preparePlan(state, unit, roots, operationKind, inputSequence, workspaceStatementCache, replayStatementCache, sourceSnapshotId, discoverDependents, seed)
        } catch (limit: SyncWorkLimitException) {
            throw ApplyProgressPending(limit)
        }
    }

    suspend fun hasMatchingSnapshotPlan(state: RuntimeState, unit: String, snapshotId: String): Boolean {
        val plan = load() ?: return false
        return plan.kind == "snapshot" && plan.unit == unit && plan.sourceSnapshotId == snapshotId &&
            plan.guard == finalStateGuard(state, unit, true, null)
    }


    private suspend fun hasUniqueIndexes(state: RuntimeState, cache: StatementCache?): Boolean =
        one("SELECT EXISTS(SELECT 1 FROM sqlite_schema s JOIN pragma_index_list(s.name) i WHERE s.type='table' AND s.name IN (" +
            state.validated.tables.joinToString(",") { "?" } + ") AND i.\"unique\"=1 AND i.origin<>'pk')",
            *state.validated.tables.map { it.tableName }.toTypedArray(), replayStatementCache = cache) { it.getLong(0) != 0L } == true

    private suspend fun finalStateGuard(state: RuntimeState, unit: String, hasUnique: Boolean, cache: StatementCache?): String {
        val guard = OversqliteWorkspace(db, config).contextGuard(state, unit) + ":" + config.serviceId
        if (!hasUnique) return guard
        val main = checkNotNull(one("PRAGMA main.schema_version", replayStatementCache = cache) { it.getLong(0) })
        val temporary = checkNotNull(one("PRAGMA temp.schema_version", replayStatementCache = cache) { it.getLong(0) })
        return "$guard:$main:$temporary"
    }

    private suspend fun preparePlan(state: RuntimeState, unit: String, roots: Int, operationKind: String, inputSequence: Long?, workspaceStatementCache: StatementCache?, replayStatementCache: StatementCache?, sourceSnapshotId: String, discoverDependents: Boolean, seed: suspend (Int) -> ApplySurvivor) {
        val hasUnique = operationKind == "snapshot" || hasUniqueIndexes(state, replayStatementCache)
        replayStatementCache?.uniqueRequired=hasUnique
        val guard = finalStateGuard(state, unit, hasUnique, replayStatementCache)
        val plan = load(replayStatementCache = replayStatementCache) ?: ApplyPlan(unit, guard, kind = operationKind, sourceSnapshotId = sourceSnapshotId)
        checkSpace(plan, workspaceStatementCache = workspaceStatementCache)
        if (plan.unit != unit || plan.guard != guard || plan.kind != operationKind || plan.sourceSnapshotId != sourceSnapshotId) {
            plan.unit = unit; plan.guard = guard; plan.phase = "clear"
            plan.kind = operationKind; plan.sourceSnapshotId = sourceSnapshotId
        }
        save(plan, replayStatementCache = replayStatementCache)
        var indexSnapshot: IndexAccountingSnapshot? = null
        var indexSnapshotAttempted = false
        suspend fun checkIndexSpace(entries: Long, bytes: Long) {
            val held = indexSnapshot
            if (held != null) {
                try {
                    checkSpaceWithPush(plan, held.chargedExternal, entries, bytes)
                    return
                } catch (_: SyncWorkspaceLimitException) {
                    // Only the pure admission above can take this optional fallback.
                    // Release the scalar holder before refreshing or reporting refusal.
                    indexSnapshot = null
                }
            }
            val external = pushWorkspace(workspaceStatementCache)
            checkSpaceWithPush(plan, external, entries, bytes)
            if (workspaceStatementCache == null || indexSnapshotAttempted) return
            indexSnapshotAttempted = true
            val trigger = one("SELECT 1 FROM sqlite_schema WHERE type='trigger' AND lower(tbl_name) IN ('_sync_apply_edges','_sync_apply_plan') " +
                "UNION ALL SELECT 1 FROM sqlite_temp_schema WHERE type='trigger' AND lower(tbl_name) IN ('_sync_apply_edges','_sync_apply_plan') LIMIT 1",
                replayStatementCache = workspaceStatementCache) { true }
            if (trigger == true) return
            try {
                // The original admission succeeded, so adding these finite temporary
                // charges cannot overflow before the existing checked admission.
                checkSpaceWithPush(plan, external, entries + 2, bytes + 256)
            } catch (_: SyncWorkspaceLimitException) {
                return
            }
            indexSnapshot = IndexAccountingSnapshot(external.first + 2 to external.second + 256)
        }
        try {
            while (plan.phase != "ready" && plan.phase != "installed") {
                operations.reserveSyncWork(unit, 16)
                val phaseBefore = plan.phase
                when (plan.phase) {
                    "clear" -> clearStep(plan, replayStatementCache = replayStatementCache)
                    "roots" -> if (plan.root == roots.toLong()) {
                        plan.phase = if (!discoverDependents) (if(hasUnique) "unique" else "ready") else "index"; plan.fk = -1; if(plan.phase=="unique"){plan.root=0;plan.fk=0}
                    } else {
                        put(plan, seed(plan.root.toInt()), workspaceStatementCache, replayStatementCache = replayStatementCache); plan.root++
                    }
                    "index" -> indexStep(state, plan, ::checkIndexSpace, replayStatementCache = replayStatementCache)
                    "walk" -> walk(state, plan, workspaceStatementCache, replayStatementCache = replayStatementCache)
                    "unique" -> uniqueStep(state,plan,workspaceStatementCache,replayStatementCache)
                    "unique_schedule" -> uniqueScheduleStep(plan,replayStatementCache)
                    else -> error("invalid survivor plan phase")
                }
                if (phaseBefore == "index" && plan.phase == "index") checkIndexSpace(0, 0)
                else {
                    indexSnapshot = null
                    checkSpace(plan, workspaceStatementCache = workspaceStatementCache)
                }
                // Root callbacks reload the retained plan. Publish ready state and the
                // last completed private step before the next work reservation refuses.
                if (phaseBefore == "roots" || plan.phase == "roots" || plan.phase == "ready" ||
                    16 > config.maxSyncWorkUnitsPerInvocation - operations.usedSyncWork()) {
                    save(plan, replayStatementCache = replayStatementCache)
                }
                checkpointWorkStep()
            }
        } finally {
            indexSnapshot = null
        }
        if (operationKind == "settlement" || operationKind == "conflict") prepareSettlementIntent(state, plan, workspaceStatementCache, replayStatementCache = replayStatementCache)
        val lifecycle = OversqliteApplyLifecycle(db, config, local, this, json,
            if (operationKind == "snapshot") replayStatementCache else null)
        operations.reserveSyncWork(unit, lifecycle.atomicWork(state, plan.kind, unit, plan.entries, atomicApplyWork(plan, roots.toLong())), readyAtomic = true)
        // Preflight simultaneous intended/live copies before business mutation.
        for (ordinal in 1..plan.entries) {
            val (row, bytes) = loadRow(state, plan, ordinal, workspaceStatementCache, replayStatementCache = replayStatementCache)
            capture(state, plan, row.table, row.pk, heldEntries = 6, heldBytes = 6 * bytes, workspaceStatementCache = workspaceStatementCache, replayStatementCache = replayStatementCache)
        }
        lifecycle.validateDependencies(state, plan.kind, plan.guard, plan.entries, inputSequence)
        if(hasUnique)preflightUnique(state,plan,workspaceStatementCache,replayStatementCache)
    }

    internal suspend fun reserveSnapshotWork(unit: String, work: Long) = operations.reserveSyncWork(unit, work, readyAtomic = true)

    // The completed page transaction has released its handles. This is a safe
    // computation-slice boundary; transport/recovery invocation limits persist.
    internal suspend fun beginSnapshotVerification() = operations.nextWorkSliceIfNeeded()

    internal suspend fun inspectSnapshotCapabilities(state: RuntimeState) {
        val p = ApplyPlan("snapshot-inspection", "", kind = "snapshot")
        for (table in state.validated.tables) {
            p.cursor = null
            while (true) {
                val index = nextUniqueIndex(state, p, ApplySurvivor(table.tableName, "", null), null, null) ?: break
                p.cursor = index.name
            }
        }
    }

    internal suspend fun validateSnapshotPreservation(state: RuntimeState, cache: StatementCache) {
        val p = checkNotNull(load(cache))
        OversqliteSnapshotPreservation(db, config, local, this, json).validate(state, p.unit, p.entries, p.visit, p.preservationComplete, cache)
        p.preservationComplete = true
        save(p, cache)
        cache.snapshotPreserved = true
    }

    suspend fun capture(state: RuntimeState, table: String, pk: String, workspaceStatementCache: StatementCache? = null, replayStatementCache: StatementCache? = null): ApplySurvivor =
        capture(state, checkNotNull(load(replayStatementCache = replayStatementCache)), table, pk, workspaceStatementCache = workspaceStatementCache, replayStatementCache = replayStatementCache)

    // Count the received tree without creating its encoded payload string.
    // Admit both copies and recursion frames before serialization can allocate.
    suspend fun wire(table: String, pk: String, payload: JsonElement?): ApplySurvivor {
        val plan = checkNotNull(load())
        val context = currentCoroutineContext()
        val metadata = 6 * utf8Size(table) + 6 * utf8Size(pk) + 64
        val retainedPush = pushWorkspace()
        val size = payload?.let { value -> encodedJsonSize(value) { depth ->
            context.ensureActive()
            checkSpaceWithPush(plan, retainedPush, 7 + depth.toLong(), 6 * metadata + 64L * depth)
        } } ?: 0
        checkSpace(plan, 7, 6 * (size + metadata))
        return ApplySurvivor(table, pk, payload?.toString(), wire = true)
    }

    suspend fun contains(table: String, pk: String, replayStatementCache: StatementCache? = null): Boolean =
        one("SELECT 1 FROM _sync_apply_rows WHERE table_name=? AND local_pk=?", table, pk, replayStatementCache = replayStatementCache) { true } == true

    // The caller retains this reservation while capturing/comparing the live
    // row. Neither simultaneous encoded copy receives sharing credit.
    suspend fun loadPlannedRow(state: RuntimeState, ordinal: Long, workspaceStatementCache: StatementCache? = null,
        replayStatementCache: StatementCache? = null): Pair<ApplySurvivor, Long> =
        loadRow(state, checkNotNull(load(replayStatementCache)), ordinal, workspaceStatementCache, replayStatementCache)

    private suspend fun loadRow(state: RuntimeState, plan: ApplyPlan, ordinal: Long, workspaceStatementCache: StatementCache?, replayStatementCache: StatementCache?): Pair<ApplySurvivor, Long> {
        currentCoroutineContext().ensureActive()
        val bytes = checkNotNull(one("SELECT bytes FROM _sync_apply_rows WHERE ordinal=?", ordinal, replayStatementCache = replayStatementCache) { it.getLong(0) })
        checkSpace(plan, 6, 6 * bytes, workspaceStatementCache)
        val row = checkNotNull(one("SELECT table_name,local_pk,payload,wire,role,source_ordinal,saved_rowid FROM _sync_apply_rows WHERE ordinal=?", ordinal, replayStatementCache = replayStatementCache) {
            ApplySurvivor(it.getText(0), it.getText(1), it.nullText(2), it.getLong(3) != 0L,
                it.getText(4), if (it.isNull(5)) null else it.getLong(5), if (it.isNull(6)) null else it.getLong(6))
        })
        if (row.role == "snapshot_ref") {
            check(plan.sourceSnapshotId.isNotEmpty()) { "snapshot source is not bound to the plan" }
            val (_, staged, charge) = snapshotLifecycle.readSnapshot(state, plan.sourceSnapshotId, checkNotNull(row.stageOrdinal), plan.unit, replayStatementCache)
            check(staged.table == row.table && staged.pk == row.pk) { "snapshot reference identity differs from stage" }
            checkSpace(plan, 6, 6 * charge, workspaceStatementCache)
            return staged.copy(rowId = row.rowId) to charge
        }
        return row to bytes
    }

    suspend fun restore(state: RuntimeState, cache: StatementCache, verifyOnly: Boolean = false, workspaceStatementCache: StatementCache? = null, replayStatementCache: StatementCache? = null): Set<String> {
        val plan = checkNotNull(load(replayStatementCache = replayStatementCache))
        val changed = linkedSetOf<String>()
        for (ordinal in 1..plan.entries) {
            currentCoroutineContext().ensureActive()
            val (row, bytes) = loadRow(state, plan, ordinal, workspaceStatementCache, replayStatementCache = replayStatementCache)
            val live = capture(state, plan, row.table, row.pk, heldEntries = 6, heldBytes = 6 * bytes, workspaceStatementCache = workspaceStatementCache, replayStatementCache = replayStatementCache)
            val matches = when {
                row.payload == null -> live.payload == null
                live.payload == null -> false
                else -> OversqliteValueCodec.equivalentPayloadTexts(
                    state.validated.tableInfoByName.getValue(row.table), row.payload, live.payload,
                    if (row.wire) PayloadSource.AUTHORITATIVE_WIRE else PayloadSource.LOCAL_STATE,
                    PayloadSource.LOCAL_STATE, json,
                )
            }
            val identityMatches = row.payload == null || row.rowId == null || local.captureRowId(row.table, row.pk, replayStatementCache) == row.rowId
            if (matches && identityMatches) continue
            check(!verifyOnly) { "survivor final state mismatch in ${row.table}" }
            if (row.payload == null) local.deleteLocalRow(state, row.table, row.pk, cache)
            else local.upsertRow(row.table, json.parseToJsonElement(row.payload) as JsonObject,
                if (row.wire) PayloadSource.AUTHORITATIVE_WIRE else PayloadSource.LOCAL_STATE, cache, preservedRowId = row.rowId)
            changed += row.table
        }
        return changed
    }

    suspend fun finish(replayStatementCache: StatementCache? = null) {
        for (table in listOf("_sync_apply_edges", "_sync_apply_rows")) {
            var changes: Long
            do {
                currentCoroutineContext().ensureActive()
                val sql = "DELETE FROM $table WHERE ordinal IN (SELECT ordinal FROM $table ORDER BY ordinal LIMIT 128)"
                if (replayStatementCache == null) db.execSQL(sql)
                else exec(sql, replayStatementCache = replayStatementCache)
                changes = if (replayStatementCache == null) db.scalarLong("SELECT changes()")
                else checkNotNull(one("SELECT changes()", replayStatementCache = replayStatementCache) { it.getLong(0) })
            } while (changes != 0L)
        }
        if (replayStatementCache == null) db.execSQL("DELETE FROM _sync_apply_plan")
        else exec("DELETE FROM _sync_apply_plan", replayStatementCache = replayStatementCache)
    }

    private suspend fun indexStep(state: RuntimeState, p: ApplyPlan, admit: suspend (Long, Long) -> Unit, replayStatementCache: StatementCache?) {
        if (p.table >= state.validated.tables.size) {
            p.phase = "walk"; p.table = 0; p.fk = 0; p.cursor = null
            return
        }
        val table = state.validated.tables[p.table.toInt()].tableName
        val info = state.validated.tableInfoByName.getValue(table)
        if (info.foreignKeys.isEmpty()) { p.table++; p.fk = -1; p.cursor = null; return }
        if (p.fk < 0) { nextIndexKey(state, p, table, info, admit, replayStatementCache = replayStatementCache); return }
        if (p.fk >= info.foreignKeys.size) { p.fk = -1; return }
        indexForeignKey(state, p, table, info, info.foreignKeys[p.fk.toInt()], admit, replayStatementCache = replayStatementCache)
        p.fk++
    }

    private suspend fun nextIndexKey(state: RuntimeState, p: ApplyPlan, table: String, info: TableInfo, admit: suspend (Long, Long) -> Unit, replayStatementCache: StatementCache? = null) {
        val description = state.validated.replaySql.table(table)
        val sql = if (p.cursor == null) description.initialKey else description.afterCursorKey
        val args = p.cursor?.let { arrayOf(physicalKey(state, table, it)) } ?: emptyArray()
        val size = one(sql.size, *args, replayStatementCache = replayStatementCache) { it.getLong(0) }
        if (size == null) { p.table++; p.fk = -1; p.cursor = null; return }
        admit(3, 18 * size + 128)
        p.cursor = checkNotNull(one(sql.value, *args, replayStatementCache = replayStatementCache) { it.getText(0) })
        p.fk = 0
    }

    private suspend fun indexForeignKey(state: RuntimeState, p: ApplyPlan, table: String, info: TableInfo, fk: ForeignKeyInfo, admit: suspend (Long, Long) -> Unit, replayStatementCache: StatementCache?) {
        val parentTable = fk.refTable.lowercase()
        val sql = state.validated.replaySql.table(table).foreignKey(p.fk.toInt())
        val value = physicalKey(state, table, checkNotNull(p.cursor))
        val parentBytes = one(sql.size, value, replayStatementCache = replayStatementCache) { it.getLong(0) } ?: return
        val size = 6 * (parentBytes + utf8Size(parentTable) + utf8Size(table) + utf8Size(checkNotNull(p.cursor))) + 64
        admit(7, 6 * size)
        val parent = checkNotNull(one(sql.value, value, replayStatementCache = replayStatementCache) { it.getText(0) })
        exec("INSERT OR IGNORE INTO _sync_apply_edges(ordinal,parent_table,parent_pk,child_table,child_pk,bytes) VALUES(?,?,?,?,?,?)", p.edgeEntries + 1, parentTable, parent, table, p.cursor, size, replayStatementCache = replayStatementCache)
        // Read immediately after the insert, reusing the transaction-owned handle.
        val inserted = checkNotNull(one("SELECT changes()", replayStatementCache = replayStatementCache) { it.getLong(0) })
        if (inserted != 0L) { p.edgeEntries++; p.edgeBytes += size }
    }

    private suspend fun walk(state: RuntimeState, p: ApplyPlan, workspaceStatementCache: StatementCache?, replayStatementCache: StatementCache?) {
        if (p.visit == 0L) {
            p.visit = one("SELECT ordinal FROM _sync_apply_rows WHERE pending=1 ORDER BY ordinal LIMIT 1", replayStatementCache = replayStatementCache) { it.getLong(0) }
                ?: run {
                    p.phase = "ready"
                    if (replayStatementCache?.uniqueRequired == true) {
                        p.phase = "unique"
                        p.root = 0
                        p.fk = 0
                        p.cursor = null
                    }
                    return
                }
        }
        if (p.edgeEntries == 0L) {
            require(one("SELECT 1 FROM _sync_apply_rows WHERE ordinal=? AND pending=1", p.visit, replayStatementCache = replayStatementCache) { true } == true) {
                "pending dependency root is missing"
            }
            // The completed index has no child relationships. Confirm the
            // retained counter before skipping every table's empty lookup.
            require(p.edgeBytes == 0L && one("SELECT 1 FROM _sync_apply_edges LIMIT 1", replayStatementCache = replayStatementCache) { true } != true) {
                "empty dependency index contradicts retained edges"
            }
            exec("UPDATE _sync_apply_rows SET pending=2 WHERE ordinal=?", p.visit, replayStatementCache = replayStatementCache)
            p.visit = 0; p.table = 0; p.fk = 0; p.cursor = null
            return
        }
        if (p.table >= state.validated.tables.size) {
            exec("UPDATE _sync_apply_rows SET pending=2 WHERE ordinal=?", p.visit, replayStatementCache = replayStatementCache)
            p.visit = 0; p.table = 0; p.fk = 0; p.cursor = null
            return
        }
        val parent = checkNotNull(one("SELECT table_name,local_pk FROM _sync_apply_rows WHERE ordinal=?", p.visit, replayStatementCache = replayStatementCache) {
            it.getText(0) to it.getText(1)
        })
        val table = state.validated.tables[p.table.toInt()].tableName
        var tail = " FROM _sync_apply_edges WHERE edge_kind='f' AND parent_table=? AND parent_pk=? AND child_table=?"
        val args = mutableListOf<Any?>(parent.first, parent.second, table)
        p.cursor?.let { tail += " AND child_pk>?"; args += it }
        tail += " ORDER BY child_pk LIMIT 1"
        val size = one("SELECT length(CAST(child_pk AS BLOB))$tail", *args.toTypedArray(), replayStatementCache = replayStatementCache) { it.getLong(0) }
        if (size == null) { p.table++; p.cursor = null; return }
        checkSpace(p, 3, 18 * size + 128, workspaceStatementCache)
        val key = checkNotNull(one("SELECT child_pk$tail", *args.toTypedArray(), replayStatementCache = replayStatementCache) { it.getText(0) })
        if (!contains(table, key, replayStatementCache = replayStatementCache)) put(p, capture(state, p, table, key, workspaceStatementCache = workspaceStatementCache, replayStatementCache = replayStatementCache), workspaceStatementCache, replayStatementCache = replayStatementCache)
        p.cursor = key
    }

    private suspend fun capture(
        state: RuntimeState, p: ApplyPlan, table: String, pk: String,
        heldEntries: Long = 0, heldBytes: Long = 0, workspaceStatementCache: StatementCache? = null, replayStatementCache: StatementCache? = null): ApplySurvivor {
        val sql = state.validated.replaySql.table(table).captureSize
        val size = one(sql,
            physicalKey(state, table, pk), replayStatementCache = replayStatementCache) { it.getLong(0) } ?: return ApplySurvivor(table, pk, null)
        checkSpace(p, heldEntries + 6, heldBytes + 6 * (size + 6 * utf8Size(table) + 6 * utf8Size(pk) + 64), workspaceStatementCache)
        val payload=local.serializeExistingRow(table, pk, replayStatementCache)
        operations.a3Stats.serializedBytes+=utf8Size(payload.orEmpty())
        operations.a3Stats.peakPrimaryCopies=maxOf(operations.a3Stats.peakPrimaryCopies,heldEntries+6)
        operations.a3Stats.peakPrimaryBytes=maxOf(operations.a3Stats.peakPrimaryBytes,heldBytes+6*size)
        return ApplySurvivor(table, pk, payload)
    }

    private suspend fun put(p: ApplyPlan, row: ApplySurvivor, workspaceStatementCache: StatementCache?, replayStatementCache: StatementCache?) {
        val pending = one("SELECT pending FROM _sync_apply_rows WHERE table_name=? AND local_pk=?", row.table, row.pk, replayStatementCache = replayStatementCache) { it.getLong(0) }
        if (pending != null) return
        val retainedPayload = if (row.stageOrdinal == null) row.payload else null
        val size = utf8Size(retainedPayload.orEmpty()) + 6 * utf8Size(row.table) + 6 * utf8Size(row.pk) + 96
        checkSpace(p, 7, 6 * size, workspaceStatementCache)
        // Snapshot deltas preserve physical identity; ordinary settlement keeps
        // its established whole-row restoration semantics.
        val rowId = if (p.kind == "snapshot") local.captureRowId(row.table, row.pk, replayStatementCache) else null
        exec("INSERT INTO _sync_apply_rows(ordinal,table_name,local_pk,payload,wire,pending,bytes,role,owner_revision,source_ordinal,saved_rowid) VALUES(?,?,?,?,?,?,?,?,(SELECT dirty_revision FROM _sync_conflict_state WHERE singleton_key=1),?,?)", p.entries + 1, row.table, row.pk, retainedPayload, row.wire, if(row.role=="exception") 0L else 1L, size, row.role, row.stageOrdinal, rowId, replayStatementCache = replayStatementCache)
        p.entries++; p.bytes += size
        operations.a3Stats.savedCopies=maxOf(operations.a3Stats.savedCopies,p.entries)
        operations.a3Stats.savedBytes=maxOf(operations.a3Stats.savedBytes,p.bytes)
    }

    private suspend fun clearStep(p: ApplyPlan, replayStatementCache: StatementCache?) {
        val table = if (p.edgeEntries > 0) "_sync_apply_edges" else "_sync_apply_rows"
        val row = one("SELECT ordinal,bytes FROM $table ORDER BY ordinal LIMIT 1", replayStatementCache = replayStatementCache) { it.getLong(0) to it.getLong(1) }
        if (row == null) {
            p.phase = "roots"; p.root = 0; p.visit = 0; p.table = 0; p.fk = 0; p.cursor = null
            p.entries = 0; p.bytes = 0; p.edgeEntries = 0; p.edgeBytes = 0
            p.preserveSchema = ""; p.preserveTable = ""; p.preserveKey = ""; p.atomicWork = 0; p.preservationComplete = false
        } else {
            exec("DELETE FROM $table WHERE ordinal=?", row.first, replayStatementCache = replayStatementCache)
            if (table == "_sync_apply_edges") { p.edgeEntries--; p.edgeBytes -= row.second }
            else { p.entries--; p.bytes -= row.second }
        }
    }

    private suspend fun checkSpace(p: ApplyPlan, entries: Long = 0, bytes: Long = 0, workspaceStatementCache: StatementCache? = null) {
        checkSpaceWithPush(p, pushWorkspace(workspaceStatementCache), entries, bytes)
    }

    private suspend fun pushWorkspace(workspaceStatementCache: StatementCache? = null): Pair<Long, Long> =
        OversqliteWorkspace(db, config).usage(ReplayFixedUnits.apply, apply = WorkspaceCharge(), statementCache = workspaceStatementCache).let { it.entries to it.bytes }

    private fun checkSpaceWithPush(p: ApplyPlan, push: Pair<Long, Long>, entries: Long, bytes: Long) {
        fun check(dimension: String, limit: Long, used: Long, extra: Long) {
            if (used < 0 || extra < 0 || used > limit || extra > limit - used) throw SyncWorkspaceLimitException(p.unit, dimension, limit, used, extra, false)
        }
        fun add(a: Long,b: Long,dimension: String): Long {
            val limit=if(dimension=="entries") config.maxSyncWorkspaceEntries else config.maxSyncWorkspaceBytes
            if(a<0 || b<0 || b>Long.MAX_VALUE-a) throw SyncWorkspaceLimitException(p.unit,dimension,limit,a,b,false)
            return a+b
        }
        check("entries", config.maxSyncWorkspaceEntries, add(add(add(p.entries,p.edgeEntries,"entries"),1,"entries"),push.first,"entries"), entries)
        check("bytes", config.maxSyncWorkspaceBytes, add(add(add(p.bytes,p.edgeBytes,"bytes"),512+6*(utf8Size(p.unit)+utf8Size(p.guard)+utf8Size(p.sourceSnapshotId)+utf8Size(p.cursor.orEmpty())+utf8Size(p.preserveSchema)+utf8Size(p.preserveTable)+utf8Size(p.preserveKey)),"bytes"),push.second,"bytes"), bytes)
    }

    private suspend fun load(replayStatementCache: StatementCache? = null): ApplyPlan? = one("SELECT unit,guard,phase,root_cursor,visit_cursor,table_cursor,fk_cursor,row_cursor,entries,bytes,edge_entries,edge_bytes,operation_kind,preserve_schema,preserve_table,preserve_key,atomic_work,preservation_complete,source_snapshot_id FROM _sync_apply_plan WHERE singleton_key=1", replayStatementCache = replayStatementCache) {
        ApplyPlan(it.getText(0),it.getText(1),it.getText(2),it.getLong(3),it.getLong(4),it.getLong(5),it.getLong(6),it.nullText(7),it.getLong(8),it.getLong(9),it.getLong(10),it.getLong(11),it.getText(12),it.getText(13),it.getText(14),it.getText(15),it.getLong(16),it.getLong(17)!=0L,it.getText(18))
    }

    private suspend fun save(p: ApplyPlan, replayStatementCache: StatementCache?) {
        currentCoroutineContext().ensureActive()
        exec("""INSERT INTO _sync_apply_plan(singleton_key,unit,guard,phase,root_cursor,visit_cursor,table_cursor,fk_cursor,row_cursor,entries,bytes,edge_entries,edge_bytes,operation_kind,preserve_schema,preserve_table,preserve_key,atomic_work,preservation_complete,source_snapshot_id)
        VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(singleton_key) DO UPDATE SET unit=excluded.unit,guard=excluded.guard,
        phase=excluded.phase,root_cursor=excluded.root_cursor,visit_cursor=excluded.visit_cursor,
        table_cursor=excluded.table_cursor,fk_cursor=excluded.fk_cursor,row_cursor=excluded.row_cursor,
        entries=excluded.entries,bytes=excluded.bytes,edge_entries=excluded.edge_entries,edge_bytes=excluded.edge_bytes,
        operation_kind=excluded.operation_kind,source_snapshot_id=excluded.source_snapshot_id,preserve_schema=excluded.preserve_schema,
        preserve_table=excluded.preserve_table,preserve_key=excluded.preserve_key,
        atomic_work=excluded.atomic_work,preservation_complete=excluded.preservation_complete""",
            p.unit,p.guard,p.phase,p.root,p.visit,p.table,p.fk,p.cursor,p.entries,p.bytes,p.edgeEntries,p.edgeBytes,
            p.kind,p.preserveSchema,p.preserveTable,p.preserveKey,p.atomicWork,p.preservationComplete,p.sourceSnapshotId, replayStatementCache = replayStatementCache)
        currentCoroutineContext().ensureActive()
    }

    private data class PrimaryIntent(val schema: String, val table: String, val key: String, val row: ApplySurvivor, val bytes: Long)

    // Single-reference keyset pages: measure before reading TEXT or decoding the
    // key. No whole backlog list or mirror; historical B stays with its owner.
    private suspend fun nextIntent(state: RuntimeState, p: ApplyPlan, schema: String, table: String, key: String, workspaceStatementCache: StatementCache?, replayStatementCache: StatementCache?): PrimaryIntent? {
        val tail = if(p.kind=="conflict") """ FROM (
 SELECT r.schema_name,r.table_name,r.key_json,CASE WHEN d.key_json IS NOT NULL THEN d.payload ELSE o.local_payload END AS payload
 FROM (SELECT * FROM (
 SELECT schema_name,table_name,key_json FROM _sync_dirty_rows WHERE (schema_name,table_name,key_json)>(?,?,?) ORDER BY schema_name,table_name,key_json LIMIT 1)
 UNION ALL SELECT * FROM (
 SELECT schema_name,table_name,key_json FROM _sync_outbox_rows WHERE source_bundle_id=(SELECT source_bundle_id FROM _sync_outbox_bundle WHERE singleton_key=1)
 AND (schema_name,table_name,key_json)>(?,?,?) ORDER BY schema_name,table_name,key_json LIMIT 1)
 ORDER BY schema_name,table_name,key_json LIMIT 1) r
 LEFT JOIN _sync_dirty_rows d ON (d.schema_name,d.table_name,d.key_json)=(r.schema_name,r.table_name,r.key_json)
 LEFT JOIN _sync_outbox_rows o ON o.source_bundle_id=(SELECT source_bundle_id FROM _sync_outbox_bundle WHERE singleton_key=1)
 AND (o.schema_name,o.table_name,o.key_json)=(r.schema_name,r.table_name,r.key_json))""" else
            " FROM _sync_dirty_rows WHERE (schema_name,table_name,key_json)>(?,?,?) ORDER BY schema_name,table_name,key_json LIMIT 1"
        val args = if(p.kind=="conflict") arrayOf(schema,table,key,schema,table,key) else arrayOf(schema,table,key)
        val size = one("SELECT 64+6*(length(CAST(schema_name AS BLOB))+length(CAST(table_name AS BLOB))+length(CAST(key_json AS BLOB)))+length(CAST(COALESCE(payload,'') AS BLOB))$tail",*args, replayStatementCache = replayStatementCache) { it.getLong(0) } ?: return null
        checkSpace(p, 8, OversqliteWorkspace(db,config).scale(p.unit,size,8), workspaceStatementCache)
        return one("SELECT schema_name,table_name,key_json,payload$tail",*args, replayStatementCache = replayStatementCache) {
            Triple(Triple(it.getText(0),it.getText(1),it.getText(2)),it.nullText(3),size)
        }?.let { value ->
            operations.a3Stats.primaryReads++; operations.a3Stats.primaryBytes+=size; operations.a3Stats.indexedVisits+=2
            val identity=value.first
            val (pk,_) = local.decodeDirtyKeyForPush(state,identity.second,identity.third)
            PrimaryIntent(identity.first,identity.second,identity.third,ApplySurvivor(identity.second,pk,value.second),size)
        }
    }

    private suspend fun prepareSettlementIntent(state: RuntimeState, p: ApplyPlan, workspaceStatementCache: StatementCache?, replayStatementCache: StatementCache?) {
        while (!p.preservationComplete) {
            operations.reserveSyncWork(p.unit,32)
            val ref = nextIntent(state,p,p.preserveSchema,p.preserveTable,p.preserveKey,workspaceStatementCache, replayStatementCache = replayStatementCache)
            if (ref==null) { p.preservationComplete=true; save(p, replayStatementCache = replayStatementCache); checkpointWorkStep(); return }
            operations.a3Stats.duplicateChecks++
            if (!contains(ref.table,ref.row.pk, replayStatementCache = replayStatementCache)) {
                val live=capture(state,p,ref.table,ref.row.pk,8,OversqliteWorkspace(db,config).scale(p.unit,ref.bytes,8),workspaceStatementCache, replayStatementCache = replayStatementCache)
                if (!sameLocalIntent(state,ref.row,live)) put(p,live.copy(role="exception"),workspaceStatementCache, replayStatementCache = replayStatementCache)
            }
            p.atomicWork=addAtomicWork(p,p.atomicWork,1,1024)
            p.preserveSchema=ref.schema; p.preserveTable=ref.table; p.preserveKey=ref.key
            // Preserve the original save-before-refusal failure order. The final
            // null-reference branch still publishes preservation completion.
            if (32 > config.maxSyncWorkUnitsPerInvocation - operations.usedSyncWork()) {
                save(p, replayStatementCache = replayStatementCache)
            }
            checkpointWorkStep()
        }
    }

    private fun addAtomicWork(p: ApplyPlan, used: Long, count: Long, weight: Long): Long {
        require(used >= 0 && count >= 0 && weight > 0)
        return if (count > (Long.MAX_VALUE - used) / weight) Long.MAX_VALUE else used + count * weight
    }

    private fun atomicApplyWork(p: ApplyPlan, roots: Long): Long {
        val settlement=p.kind in setOf("settlement", "conflict", "snapshot")
        var work=addAtomicWork(p,p.atomicWork,roots,if(settlement) 32 else 16)
        work=addAtomicWork(p,work,p.entries,if(settlement) 1024 else 528)
        work=addAtomicWork(p,work,p.edgeEntries,if(settlement) 4 else 2)
        return addAtomicWork(p,work,1,if(settlement) 64 else 8)
    }

    private fun sameLocalIntent(state: RuntimeState, desired: ApplySurvivor, live: ApplySurvivor): Boolean =
        OversqliteValueCodec.equivalentPayloadTexts(state.validated.tableInfoByName.getValue(desired.table),
            desired.payload,live.payload,PayloadSource.LOCAL_STATE,PayloadSource.LOCAL_STATE,json)

    // The complete maximum 64-pass work bound was reserved before any apply.
    // Check cancellation per reference; changed primary payloads are never B.
    suspend fun restorePrimaryIntent(state: RuntimeState, cache: StatementCache, verifyOnly: Boolean = false, workspaceStatementCache: StatementCache? = null, replayStatementCache: StatementCache? = null): Set<String> {
        operations.a3Stats.restorationPasses++
        val p=checkNotNull(load(replayStatementCache = replayStatementCache))
        var schema=""; var table=""; var key=""
        val changed=linkedSetOf<String>()
        while (true) {
            currentCoroutineContext().ensureActive()
            val ref=nextIntent(state,p,schema,table,key,workspaceStatementCache, replayStatementCache = replayStatementCache) ?: return changed
            schema=ref.schema; table=ref.table; key=ref.key
            operations.a3Stats.duplicateChecks++
            if (contains(ref.table,ref.row.pk, replayStatementCache = replayStatementCache)) continue
            val live=capture(state,p,ref.table,ref.row.pk,8,OversqliteWorkspace(db,config).scale(p.unit,ref.bytes,8),workspaceStatementCache, replayStatementCache = replayStatementCache)
            if (sameLocalIntent(state,ref.row,live)) continue
            check(!verifyOnly) { "primary settlement intent final state mismatch" }
            if (ref.row.payload==null) local.deleteLocalRow(state,ref.table,ref.row.pk,cache)
            else local.upsertRow(ref.table,json.parseToJsonElement(ref.row.payload) as JsonObject,PayloadSource.LOCAL_STATE,cache)
            changed+=ref.table
        }
    }

    private fun uniqueBlocked(
        p: ApplyPlan,
        row: ApplySurvivor,
        reason: String,
        index: String = "",
        trigger: String = "",
        other: String? = null,
    ) =
        FinalStateApplyBlockedException(
            reason,
            p.kind,
            row.table,
            index,
            trigger,
            listOfNotNull(row.pk, other),
        )

    private suspend fun uniqueStep(
        state: RuntimeState,
        p: ApplyPlan,
        workspace: StatementCache?,
        cache: StatementCache?,
    ) {
        if ((p.kind == "settlement" || p.kind == "conflict") && !p.preservationComplete)
            prepareSettlementIntent(state, p, workspace, cache)
        if (p.root >= p.entries) {
            p.phase = "unique_schedule"
            p.visit = 0
            return
        }
        val (row, _) = loadRow(state, p, p.root + 1, workspace, cache)
        if (row.payload == null) {
            p.root++
            p.fk = 0
            p.cursor = null
            return
        }
        val index = nextUniqueIndex(state, p, row, workspace, cache)
        if (index == null) {
            p.root++
            p.fk = 0
            p.cursor = null
            return
        }
        val values = uniqueValues(state, row, index)
        if (values != null && p.fk == 0L) {
            val holder = uniqueHolder(state, row, index, values, cache)
            if (holder != null) {
                val ordinal =
                    one(
                        "SELECT ordinal FROM _sync_apply_rows WHERE table_name=? AND local_pk=?",
                        row.table,
                        holder,
                        replayStatementCache = cache,
                    ) {
                        it.getLong(0)
                    }
                        ?: throw uniqueBlocked(
                            p,
                            row,
                            "conflicting_desired_state",
                            index.name,
                            other = holder,
                        )
                val (other, _) = loadRow(state, p, ordinal, workspace, cache)
                if (uniqueEqual(index, values, uniqueValues(state, other, index), cache))
                    throw uniqueBlocked(
                        p,
                        row,
                        "conflicting_desired_state",
                        index.name,
                        other = holder,
                    )
                val size = 64 + 12 * utf8Size(row.table) + 6 * (utf8Size(row.pk) + utf8Size(holder))
                checkSpace(p, 1, size, workspace)
                val exists =
                    one(
                        "SELECT 1 FROM _sync_apply_edges WHERE edge_kind='u' AND parent_table=? AND parent_pk=? AND child_table=? AND child_pk=?",
                        row.table,
                        row.pk,
                        row.table,
                        holder,
                        replayStatementCache = cache,
                    ) {
                        true
                    } == true
                if (!exists) {
                    exec(
                        "INSERT INTO _sync_apply_edges(ordinal,parent_table,parent_pk,child_table,child_pk,bytes,edge_kind) VALUES(?,?,?,?,?,?,'u')",
                        p.edgeEntries + 1,
                        row.table,
                        row.pk,
                        row.table,
                        holder,
                        size,
                        replayStatementCache = cache,
                    )
                    p.edgeEntries++
                    p.edgeBytes += size
                }
            }
        }
        if (
            values != null &&
                !checkPriorUniqueDesired(state, p, row, index, values, workspace, cache)
        )
            return
        p.cursor = index.name
        p.fk = 0
    }

    // Retire the acyclic prefix in durable staging before admitting SCC work.
    private suspend fun uniqueScheduleStep(p: ApplyPlan, cache: StatementCache?) {
        var sql = "SELECT r.ordinal FROM _sync_apply_rows r WHERE pending<>5"
        sql +=
                " AND NOT EXISTS(SELECT 1 FROM _sync_apply_edges e JOIN _sync_apply_rows h ON h.table_name=e.child_table AND h.local_pk=e.child_pk WHERE e.edge_kind='u' AND e.parent_table=r.table_name AND e.parent_pk=r.local_pk AND h.pending<>5)"
        val ordinal =
            one(sql + " ORDER BY r.ordinal LIMIT 1", replayStatementCache = cache) { it.getLong(0) }
        if (ordinal == null) {
            p.visit =
                checkNotNull(
                    one(
                        "SELECT count(*) FROM _sync_apply_rows WHERE pending<>5",
                        replayStatementCache = cache,
                    ) {
                        it.getLong(0)
                    }
                )
            exec(
                "UPDATE _sync_apply_rows SET pending=6 WHERE pending=5",
                replayStatementCache = cache,
            )
            p.phase = "ready"
        } else
            exec(
                "UPDATE _sync_apply_rows SET pending=5 WHERE ordinal=?",
                ordinal,
                replayStatementCache = cache,
            )
    }

    private suspend fun checkPriorUniqueDesired(
        state: RuntimeState,
        p: ApplyPlan,
        row: ApplySurvivor,
        index: ApplyUniqueIndex,
        values: List<Any?>,
        workspace: StatementCache?,
        cache: StatementCache?,
    ): Boolean {
        val ordinal =
            one(
                "SELECT ordinal FROM _sync_apply_rows WHERE table_name=? AND ordinal>? AND ordinal<? AND (payload IS NOT NULL OR role='snapshot_ref') ORDER BY ordinal LIMIT 1",
                row.table,
                p.fk,
                p.root + 1,
                replayStatementCache = cache,
            ) {
                it.getLong(0)
            } ?: return true
        val size =
            checkNotNull(
                one(
                    "SELECT bytes FROM _sync_apply_rows WHERE ordinal=?",
                    ordinal,
                    replayStatementCache = cache,
                ) {
                    it.getLong(0)
                }
            )
        checkSpace(p, 16, 12 * (size + utf8Size(row.payload.orEmpty()) + 4096), workspace)
        val (other, _) = loadRow(state, p, ordinal, workspace, cache)
        if (uniqueEqual(index, values, uniqueValues(state, other, index), cache))
            throw uniqueBlocked(p, row, "conflicting_desired_state", index.name, other = other.pk)
        p.fk = ordinal
        return false
    }

    private suspend fun nextUniqueIndex(
        state: RuntimeState,
        p: ApplyPlan,
        row: ApplySurvivor,
        workspace: StatementCache?,
        cache: StatementCache?,
    ): ApplyUniqueIndex? {
        val entry =
            one(
                "SELECT name,partial FROM pragma_index_list(?) WHERE \"unique\"=1 AND origin<>'pk' AND name>? ORDER BY name LIMIT 1",
                row.table,
                p.cursor.orEmpty(),
                replayStatementCache = cache,
            ) {
                it.getText(0) to it.getLong(1)
            } ?: return null
        checkSpace(p, 16, 4096 + 12 * utf8Size(entry.first), workspace)
        if (entry.second != 0L) throw uniqueBlocked(p, row, "unsupported_uniqueness", entry.first)
        val info = state.validated.tableInfoByName.getValue(row.table)
        checkSpace(p, 8L * info.columns.size, 1024L * info.columns.size, workspace)
        val columns = mutableListOf<Pair<String, String>>()
        db.withPreparedStatement(
            "SELECT name,coll,cid FROM pragma_index_xinfo(?) WHERE \"key\"=1 ORDER BY seqno",
            statementCache = cache,
        ) { st ->
            st.bindText(1, entry.first)
            while (st.step()) {
                if (st.getLong(2) < 0 || st.isNull(0))
                    throw uniqueBlocked(p, row, "unsupported_uniqueness", entry.first)
                columns += st.getText(0) to st.getText(1)
            }
        }
        return ApplyUniqueIndex(entry.first, columns)
    }

    private fun uniqueValues(
        state: RuntimeState,
        row: ApplySurvivor,
        index: ApplyUniqueIndex,
    ): List<Any?>? {
        val text = row.payload ?: return null
        val payload = json.parseToJsonElement(text) as JsonObject
        val info = state.validated.tableInfoByName.getValue(row.table)
        val result = mutableListOf<Any?>()
        for ((name, _) in index.columns) {
            val column = info.columns.first { it.name.equals(name, true) }
            val value =
                OversqliteValueCodec.decodePayloadValue(
                    column,
                    payload.getValue(name.lowercase()),
                    if (row.wire) PayloadSource.AUTHORITATIVE_WIRE else PayloadSource.LOCAL_STATE,
                )
            result +=
                when (value) {
                    TypedValue.Null -> return null
                    is TypedValue.Text -> value.value
                    is TypedValue.Integer -> value.canonicalText.toLong()
                    is TypedValue.Real -> value.canonicalText.toDouble()
                    is TypedValue.Blob -> value.bytes
                }
        }
        return result
    }

    private suspend fun uniqueEqual(
        index: ApplyUniqueIndex,
        left: List<Any?>,
        right: List<Any?>?,
        cache: StatementCache?,
    ): Boolean {
        if (right == null) return false
        val args = mutableListOf<Any?>()
        val terms =
            index.columns.mapIndexed { i, (_, coll) ->
                args += left[i]
                args += right[i]
                "? = ? COLLATE ${quoteIdent(coll)}"
            }
        return one(
            "SELECT " + terms.joinToString(" AND "),
            *args.toTypedArray(),
            replayStatementCache = cache,
        ) {
            it.getLong(0) != 0L
        } == true
    }

    private suspend fun uniqueHolder(
        state: RuntimeState,
        row: ApplySurvivor,
        index: ApplyUniqueIndex,
        values: List<Any?>,
        cache: StatementCache?,
    ): String? {
        val info = state.validated.tableInfoByName.getValue(row.table)
        val pk = quoteIdent(checkNotNull(info.primaryKey).name)
        val key = if (info.primaryKeyIsBlob) "lower(hex($pk))" else "CAST($pk AS TEXT)"
        val terms =
            index.columns.joinToString(" AND ") { (col, coll) ->
                "${quoteIdent(col)} COLLATE ${quoteIdent(coll)}=?"
            }
        return one(
            "SELECT $key FROM ${quoteIdent(row.table)} WHERE $terms AND $pk<>? LIMIT 1",
            *(values + physicalKey(state, row.table, row.pk)).toTypedArray(),
            replayStatementCache = cache,
        ) {
            it.getText(0)
        }
    }

    private suspend fun preflightUnique(
        state: RuntimeState,
        p: ApplyPlan,
        workspace: StatementCache?,
        cache: StatementCache?,
    ) {
        val edges =
            one(
                "SELECT count(*) FROM _sync_apply_edges WHERE edge_kind='u'",
                replayStatementCache = cache,
            ) {
                it.getLong(0)
            } ?: 0
        if (edges == 0L && p.kind != "snapshot") return
        val pairs = p.entries + OversqliteWorkspace(db, config).scale(p.unit, p.visit, p.visit)
        checkSpace(
            p,
            OversqliteWorkspace(db, config).scale(p.unit, pairs, 4),
            4096 + OversqliteWorkspace(db, config).scale(p.unit, pairs, 128),
            workspace,
        )
        operations.reserveSyncWork(p.unit, 8 * pairs + 256 * p.entries, readyAtomic = true)
    }

    suspend fun installUniqueFinalState(state: RuntimeState, cache: StatementCache) {
        if (!cache.uniqueRequired) return
        uniqueState = state
        val p = checkNotNull(load(cache))
        if (p.phase == "installed") {
            cache.uniqueInstalled = true
            return
        }
        val edges =
            one(
                "SELECT count(*) FROM _sync_apply_edges WHERE edge_kind='u'",
                replayStatementCache = cache,
            ) {
                it.getLong(0)
            } ?: 0
        if (edges == 0L && p.kind != "snapshot") return
        check(p.phase == "ready") { "unique application requires ready state" }
        var steps = 0L
        while (
            one(
                "SELECT EXISTS(SELECT 1 FROM _sync_apply_rows WHERE pending<>3)",
                replayStatementCache = cache,
            ) {
                it.getLong(0) != 0L
            } == true
        ) {
            check(steps++ <= p.entries * p.entries + p.entries) {
                "unique apply planner exceeded its admitted progress bound"
            }
            var ordinal = nextUniqueReady(false, cache)
            if (ordinal == null) {
                vacateUniqueComponent(state, p, cache)
                ordinal =
                    checkNotNull(nextUniqueReady(false, cache)) {
                        "unique apply planner made no progress"
                    }
            }
            val (row, bytes) = loadRow(state, p, ordinal, cache, cache)
            val live = capture(state, p, row.table, row.pk, 6, 6 * bytes, cache, cache)
            val same =
                OversqliteValueCodec.equivalentPayloadTexts(
                    state.validated.tableInfoByName.getValue(row.table),
                    row.payload,
                    live.payload,
                    if (row.wire) PayloadSource.AUTHORITATIVE_WIRE else PayloadSource.LOCAL_STATE,
                    PayloadSource.LOCAL_STATE,
                    json,
                )
            if (!same) restoreUniqueRow(state, row, cache)
            exec(
                "UPDATE _sync_apply_rows SET pending=3 WHERE ordinal=?",
                ordinal,
                replayStatementCache = cache,
            )
        }
        restore(
            state,
            cache,
            verifyOnly = true,
            workspaceStatementCache = cache,
            replayStatementCache = cache,
        )
        exec(
            "UPDATE _sync_apply_plan SET phase='installed' WHERE singleton_key=1",
            replayStatementCache = cache,
        )
        cache.uniqueInstalled = true
    }

    private suspend fun nextUniqueReady(snapshot: Boolean, cache: StatementCache): Long? {
        var sql = "SELECT r.ordinal FROM _sync_apply_rows r WHERE pending<>3"
        if (!snapshot)
            sql +=
                " AND NOT EXISTS(SELECT 1 FROM _sync_apply_edges e JOIN _sync_apply_rows h ON h.table_name=e.child_table AND h.local_pk=e.child_pk WHERE e.edge_kind='u' AND e.parent_table=r.table_name AND e.parent_pk=r.local_pk AND h.pending<>3)"
        return one(sql + " ORDER BY r.ordinal LIMIT 1", replayStatementCache = cache) {
            it.getLong(0)
        }
    }

    private suspend fun vacateUniqueComponent(
        state: RuntimeState,
        p: ApplyPlan,
        cache: StatementCache,
    ) {
        exec(
            uniqueCycleSql +
                "UPDATE _sync_apply_rows SET pending=4 WHERE ordinal IN(SELECT id FROM closure)",
            replayStatementCache = cache,
        )
        check(
            (one(
                "SELECT count(*) FROM _sync_apply_rows WHERE pending=4",
                replayStatementCache = cache,
            ) {
                it.getLong(0)
            } ?: 0) > 0
        )
        for (ordinal in 1..p.entries) {
            if (
                one(
                    "SELECT pending=4 FROM _sync_apply_rows WHERE ordinal=?",
                    ordinal,
                    replayStatementCache = cache,
                ) {
                    it.getLong(0) != 0L
                } != true
            )
                continue
            val (row, _) = loadRow(state, p, ordinal, cache, cache)
            validateUniqueVacate(state, p, row, cache)
        }
        for (ordinal in 1..p.entries) {
            if (
                one(
                    "SELECT pending=4 FROM _sync_apply_rows WHERE ordinal=?",
                    ordinal,
                    replayStatementCache = cache,
                ) {
                    it.getLong(0) != 0L
                } != true
            )
                continue
            val (row, _) = loadRow(state, p, ordinal, cache, cache)
            local.deleteLocalRow(state, row.table, row.pk, cache)
        }
        exec(
            "DELETE FROM _sync_apply_edges WHERE edge_kind='u' AND EXISTS(SELECT 1 FROM _sync_apply_rows r WHERE r.pending=4 AND r.table_name=child_table AND r.local_pk=child_pk)",
            replayStatementCache = cache,
        )
        exec("UPDATE _sync_apply_rows SET pending=2 WHERE pending=4", replayStatementCache = cache)
        val counts =
            checkNotNull(
                one(
                    "SELECT count(*),COALESCE(sum(bytes),0) FROM _sync_apply_edges",
                    replayStatementCache = cache,
                ) {
                    it.getLong(0) to it.getLong(1)
                }
            )
        p.edgeEntries = counts.first
        p.edgeBytes = counts.second
        save(p, cache)
    }

    private suspend fun validateUniqueVacate(
        state: RuntimeState,
        p: ApplyPlan,
        row: ApplySurvivor,
        cache: StatementCache,
    ) {
        if (p.kind == "snapshot") {
            check(cache.snapshotPreserved) { "snapshot vacate requires final-writer preservation proof" }
            return
        }
        val knownTables = state.validated.tableInfoByName.keys
        val unknown =
            one(
                "SELECT s.name FROM sqlite_schema s JOIN pragma_foreign_key_list(s.name) f WHERE s.type='table' AND lower(f.\"table\")=? AND s.name NOT IN (" +
                    knownTables.joinToString(",") { "?" } +
                    ") LIMIT 1",
                *(listOf(row.table) + knownTables).toTypedArray(),
                replayStatementCache = cache,
            ) {
                it.getText(0)
            }
        if (unknown != null) throw uniqueBlocked(p, row, "unsafe_trigger_or_dependency")
        for ((table, info) in state.validated.tableInfoByName) {
            if (
                info.foreignKeys.any { it.refTable.equals(row.table, true) } &&
                    one(
                        "SELECT 1 FROM _sync_row_state WHERE relationships_known=0 AND table_name=? LIMIT 1",
                        table,
                        replayStatementCache = cache,
                    ) {
                        true
                    } == true
            )
                throw uniqueBlocked(p, row, "unsafe_trigger_or_dependency")
        }
        val allowed =
            listOf("bi_guard", "bu_guard", "bd_guard", "ai", "au", "ad").map {
                "trg_${row.table}_$it"
            }
        val marks = allowed.joinToString(",") { "?" }
        val trigger =
            one(
                "SELECT name FROM (SELECT name,tbl_name FROM sqlite_schema WHERE type='trigger' UNION ALL SELECT name,tbl_name FROM sqlite_temp_schema WHERE type='trigger') WHERE tbl_name=? AND name NOT IN ($marks) LIMIT 1",
                *(listOf(row.table) + allowed).toTypedArray(),
                replayStatementCache = cache,
            ) {
                it.getText(0)
            }
        if (trigger != null)
            throw uniqueBlocked(p, row, "unsafe_trigger_or_dependency", trigger = trigger)
    }

    private suspend fun restoreUniqueRow(
        state: RuntimeState,
        row: ApplySurvivor,
        cache: StatementCache?,
    ) {
        if (row.payload == null)
            local.deleteLocalRow(state, row.table, row.pk, cache, skipUniqueReplay = true)
        else
            local.upsertRow(
                row.table,
                json.parseToJsonElement(row.payload) as JsonObject,
                if (row.wire) PayloadSource.AUTHORITATIVE_WIRE else PayloadSource.LOCAL_STATE,
                cache,
                skipUniqueReplay = true,
                preservedRowId = row.rowId,
            )
    }

    private suspend fun reuseInstalledUniqueRow(
        table: String,
        pk: String,
        cache: StatementCache?,
    ): Boolean {
        if (cache?.uniqueInstalled != true) return false
        val ordinal =
            one(
                "SELECT r.ordinal FROM _sync_apply_rows r JOIN _sync_apply_plan p ON p.singleton_key=1 AND p.phase='installed' WHERE r.table_name=? AND r.local_pk=?",
                table,
                pk,
                replayStatementCache = cache,
            ) {
                it.getLong(0)
            } ?: return false
        val state = checkNotNull(uniqueState)
        val p = checkNotNull(load(cache))
        val (row, bytes) = loadRow(state, p, ordinal, cache, cache)
        val live = capture(state, p, table, pk, 6, 6 * bytes, cache, cache)
        if (
            !OversqliteValueCodec.equivalentPayloadTexts(
                state.validated.tableInfoByName.getValue(table),
                row.payload,
                live.payload,
                if (row.wire) PayloadSource.AUTHORITATIVE_WIRE else PayloadSource.LOCAL_STATE,
                PayloadSource.LOCAL_STATE,
                json,
            )
        )
            restoreUniqueRow(state, row, cache)
        return true
    }

    private fun physicalKey(state: RuntimeState, table: String, pk: String): Any =
        if (state.validated.tableInfoByName.getValue(table).primaryKeyIsBlob) decodeLocalUuidBytes(pk) else pk

    private suspend fun exec(sql: String, vararg args: Any?, replayStatementCache: StatementCache? = null) = db.withPreparedStatement(sql, statementCache = replayStatementCache) {
        bind(it, args); it.step(); Unit
    }

    private suspend fun <T> one(sql: String, vararg args: Any?, replayStatementCache: StatementCache? = null, read: (SQLiteStatement) -> T): T? =
        db.withPreparedStatement(sql, statementCache = replayStatementCache) { bind(it, args); if (it.step()) read(it) else null }

    private fun bind(st: SQLiteStatement, args: Array<out Any?>) {
        args.forEachIndexed { index, value -> when (value) {
            null -> st.bindNull(index + 1)
            is String -> st.bindText(index + 1, value)
            is Long -> st.bindLong(index + 1, value)
            is Boolean -> st.bindLong(index + 1, if (value) 1 else 0)
            is ByteArray -> st.bindBlob(index + 1, value)
            is Double -> st.bindDouble(index+1,value)
            else -> error("unsupported apply binding")
        } }
    }

    private fun SQLiteStatement.nullText(index: Int): String? = if (isNull(index)) null else getText(index)
}

// Count without allocating another encoded copy before reserving its bytes.
internal fun utf8Size(text: String): Long {
    var bytes = 0L
    var i = 0
    while (i < text.length) {
        val ch = text[i++]
        bytes += when {
            ch.code < 0x80 -> 1
            ch.code < 0x800 -> 2
            ch.isHighSurrogate() && i < text.length && text[i].isLowSurrogate() -> { i++; 4 }
            else -> 3
        }
    }
    return bytes
}

// Measure the compact kotlinx JSON representation without constructing it. The
// caller bounds/cancels recursion before each frame and checks long strings too.
internal fun encodedJsonSize(value: JsonElement, visit: (Int) -> Unit = {}): Long {
    fun quoted(text: String, depth: Int): Long {
        var size = 2L
        var i = 0
        while (i < text.length) {
            if (i % 256 == 0) visit(depth)
            val ch = text[i++]
            size += when {
                ch == '"' || ch == '\\' || ch == '\b' || ch == '\u000C' || ch == '\n' || ch == '\r' || ch == '\t' -> 2
                ch.code < 32 -> 6
                ch.code < 0x80 -> 1
                ch.code < 0x800 -> 2
                ch.isHighSurrogate() && i < text.length && text[i].isLowSurrogate() -> { i++; 4 }
                else -> 3
            }
        }
        return size
    }
    fun measure(element: JsonElement, depth: Int): Long {
        visit(depth)
        return when (element) {
            is JsonObject -> {
                var size = 2L + (element.size - 1).coerceAtLeast(0)
                for ((key, child) in element) size += quoted(key, depth) + 1 + measure(child, depth + 1)
                size
            }
            is JsonArray -> {
                var size = 2L + (element.size - 1).coerceAtLeast(0)
                for (child in element) size += measure(child, depth + 1)
                size
            }
            is JsonPrimitive -> if (element.isString) quoted(element.content, depth) else utf8Size(element.content)
        }
    }
    return measure(value, 0)
}

internal fun livePayloadByteBound(info: TableInfo, alias: String): String =
    (listOf("2")+info.columns.map { column ->
        val value="${quoteIdent(alias)}.${quoteIdent(column.name)}"
        "${80+6*utf8Size(column.name)}+CASE typeof($value) WHEN 'blob' THEN 2*length($value)+2 WHEN 'text' THEN 6*length(CAST($value AS BLOB))+2 ELSE 32 END"
    }).joinToString("+")

internal data class A3AccountingStats(
    var primaryReads: Long=0,var primaryBytes: Long=0,var indexedVisits: Long=0,var duplicateChecks: Long=0,
    var restorationPasses: Long=0,var savedCopies: Long=0,var savedBytes: Long=0,var serializedBytes: Long=0,
    var peakPrimaryCopies: Long=0,var peakPrimaryBytes: Long=0,var work: Long=0,
)

private data class ApplyUniqueIndex(val name: String,val columns: List<Pair<String,String>>)
private const val uniqueCycleSql = """WITH RECURSIVE arcs(a,b) AS (
 SELECT a.ordinal,b.ordinal FROM _sync_apply_edges e
 JOIN _sync_apply_rows a ON a.table_name=e.parent_table AND a.local_pk=e.parent_pk
 JOIN _sync_apply_rows b ON b.table_name=e.child_table AND b.local_pk=e.child_pk
 WHERE e.edge_kind='u' AND a.pending<>3 AND b.pending<>3
), reach(a,b) AS (SELECT a,b FROM arcs UNION SELECT r.a,e.b FROM reach r JOIN arcs e ON e.a=r.b),
 anchor(id) AS (SELECT min(a) FROM reach WHERE a=b),
 component(id) AS (SELECT a FROM reach,anchor WHERE b=anchor.id AND a IN(SELECT b FROM reach WHERE a=anchor.id)),
 closure(id) AS (SELECT id FROM component UNION SELECT child.ordinal FROM closure c
 JOIN _sync_apply_rows parent ON parent.ordinal=c.id
 JOIN _sync_apply_edges e ON e.edge_kind='f' AND e.parent_table=parent.table_name AND e.parent_pk=parent.local_pk
 JOIN _sync_apply_rows child ON child.table_name=e.child_table AND child.local_pk=e.child_pk) """
