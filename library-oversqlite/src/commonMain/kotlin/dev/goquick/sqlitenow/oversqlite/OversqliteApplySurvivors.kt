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
    preserve_schema TEXT NOT NULL DEFAULT '', preserve_table TEXT NOT NULL DEFAULT '', preserve_key TEXT NOT NULL DEFAULT '',
    atomic_work INTEGER NOT NULL DEFAULT 0 CHECK(typeof(atomic_work)='integer' AND atomic_work>=0),
    preservation_complete INTEGER NOT NULL DEFAULT 0 CHECK(preservation_complete IN (0,1)))"""
internal const val applyRowsDdl = """CREATE TABLE IF NOT EXISTS _sync_apply_rows (
    ordinal INTEGER PRIMARY KEY, table_name TEXT NOT NULL, local_pk TEXT NOT NULL,
    payload TEXT, wire INTEGER NOT NULL, pending INTEGER NOT NULL, bytes INTEGER NOT NULL,
    role TEXT NOT NULL DEFAULT 'survivor' CHECK(role IN ('intended','survivor','exception')),
    owner_revision INTEGER NOT NULL DEFAULT 0 CHECK(typeof(owner_revision)='integer' AND owner_revision>=0),
    UNIQUE(table_name,local_pk))"""
internal const val applyEdgesDdl = """CREATE TABLE IF NOT EXISTS _sync_apply_edges (
    ordinal INTEGER PRIMARY KEY, parent_table TEXT NOT NULL, parent_pk TEXT NOT NULL,
    child_table TEXT NOT NULL, child_pk TEXT NOT NULL, bytes INTEGER NOT NULL,
    UNIQUE(parent_table,parent_pk,child_table,child_pk))"""
internal const val applyRoleIndexDdl = "CREATE INDEX IF NOT EXISTS _sync_apply_rows_role_ordinal_idx ON _sync_apply_rows(role,ordinal)"

internal const val applyPendingIndexDdl =
    "CREATE INDEX IF NOT EXISTS _sync_apply_rows_pending_idx ON _sync_apply_rows(pending,ordinal)"

internal data class ApplySurvivor(
    val table: String, val pk: String, val payload: String?, val wire: Boolean = false,
    val role: String = if (wire) "intended" else "survivor",
)

private data class ApplyPlan(
    var unit: String, var guard: String, var phase: String = "roots",
    var root: Long = 0, var visit: Long = 0, var table: Long = 0, var fk: Long = 0,
    var cursor: String? = null, var entries: Long = 0, var bytes: Long = 0,
    var edgeEntries: Long = 0, var edgeBytes: Long = 0,
    var kind: String = "pull", var preserveSchema: String = "", var preserveTable: String = "", var preserveKey: String = "",
    var atomicWork: Long = 0, var preservationComplete: Boolean = false,
)

internal fun applyUnit(kind: String, sequence: Long, hash: String): String =
    sha256Hex("$kind:$sequence:$hash".encodeToByteArray())

internal class OversqliteApplySurvivors(
    private val db: SafeSQLiteConnection,
    private val local: OversqliteLocalStore,
    private val config: OversqliteConfig,
    private val operations: OversqliteClientOperationController,
    private val json: Json,
) {
    suspend fun reservationCharge(token: String): WorkspaceCharge = OversqliteWorkspace(db,config).reserved(token)

    suspend fun reserveDecisionCopies(state: RuntimeState, token: String?, base: WorkspaceCharge, held: WorkspaceCharge,
        schema: String, table: String, key: String, pk: String): WorkspaceCharge {
        if(token==null) return held // Unconverted conflict work belongs to A3-03.
        val unit=ReplayFixedUnits.settlementDecisions
        operations.reserveSyncWork(unit,16)
        val sql=state.validated.replaySql.table(table).decisionCopySize
        val bytes=checkNotNull(one(sql,physicalKey(state,table,pk),schema,table,key) { it.getLong(0) })
        val workspace=OversqliteWorkspace(db,config)
        val next=workspace.add(unit,held,WorkspaceCharge(32,workspace.scale(unit,bytes,64)))
        workspace.resizeInWriter(state,token,workspace.add(unit,base,next))
        return next
    }

    suspend fun prepare(state: RuntimeState, unit: String, roots: Int, operationKind: String = "pull", inputSequence: Long? = null, workspaceStatementCache: StatementCache? = null, replayStatementCache: StatementCache? = null, seed: suspend (Int) -> ApplySurvivor) {
        try {
            preparePlan(state, unit, roots, operationKind, inputSequence, workspaceStatementCache, replayStatementCache, seed)
        } catch (limit: SyncWorkLimitException) {
            throw ApplyProgressPending(limit)
        }
    }

    private suspend fun preparePlan(state: RuntimeState, unit: String, roots: Int, operationKind: String, inputSequence: Long?, workspaceStatementCache: StatementCache?, replayStatementCache: StatementCache?, seed: suspend (Int) -> ApplySurvivor) {
        val guard = OversqliteWorkspace(db, config).contextGuard(state, unit)
        val plan = load(replayStatementCache = replayStatementCache) ?: ApplyPlan(unit, guard, kind = operationKind)
        checkSpace(plan, workspaceStatementCache = workspaceStatementCache)
        if (plan.unit != unit || plan.guard != guard || plan.kind != operationKind) {
            plan.unit = unit; plan.guard = guard; plan.phase = "clear"
            plan.kind = operationKind
        }
        // Older snapshot plans skipped this traversal; completed counts retain
        // the exhausted table cursor under the existing guarded plan owner.
        if (operationKind == "snapshot" && plan.phase == "ready" && plan.table < state.validated.tables.size) {
            plan.phase = "index"; plan.table = 0; plan.fk = -1; plan.cursor = null; plan.atomicWork = 0
        }
        save(plan, replayStatementCache = replayStatementCache)
        while (plan.phase != "ready") {
            operations.reserveSyncWork(unit, 16)
            when (plan.phase) {
                "clear" -> clearStep(plan, replayStatementCache = replayStatementCache)
                "roots" -> if (plan.root == roots.toLong()) {
                    plan.phase = "index"; plan.fk = -1
                } else {
                    put(plan, seed(plan.root.toInt()), workspaceStatementCache, replayStatementCache = replayStatementCache); plan.root++
                }
                "index" -> if (operationKind == "snapshot") countSnapshotBusinessStep(state, plan) else indexStep(state, plan, workspaceStatementCache, replayStatementCache = replayStatementCache)
                "walk" -> if (operationKind == "snapshot") countSnapshotBaselineStep(state, plan) else walk(state, plan, workspaceStatementCache, replayStatementCache = replayStatementCache)
                else -> error("invalid survivor plan phase")
            }
            checkSpace(plan, workspaceStatementCache = workspaceStatementCache)
            save(plan, replayStatementCache = replayStatementCache)
        }
        if (operationKind == "settlement" || operationKind == "conflict") prepareSettlementIntent(state, plan, workspaceStatementCache, replayStatementCache = replayStatementCache)
        val lifecycle = OversqliteApplyLifecycle(db, config, local, this, json)
        operations.reserveSyncWork(unit, lifecycle.atomicWork(state, plan.kind, unit, plan.entries, atomicApplyWork(plan, roots.toLong())))
        // Preflight simultaneous intended/live copies before business mutation.
        for (ordinal in 1..plan.entries) {
            val (row, bytes) = loadRow(plan, ordinal, workspaceStatementCache, replayStatementCache = replayStatementCache)
            capture(state, plan, row.table, row.pk, heldEntries = 6, heldBytes = 6 * bytes, workspaceStatementCache = workspaceStatementCache, replayStatementCache = replayStatementCache)
        }
        lifecycle.validateDependencies(state, plan.kind, plan.guard, plan.entries, inputSequence)
    }

    // Each step's 16 work units are admitted by preparePlan before either
    // indexed lookup. The saved allowance counts old scope exactly once.
    private suspend fun countSnapshotBusinessStep(state: RuntimeState, p: ApplyPlan) {
        if (p.table >= state.validated.tables.size) {
            p.phase = "walk"; p.table = 0; p.fk = 0; p.cursor = null
            return
        }
        val before = p.table
        val table = state.validated.tables[p.table.toInt()].tableName
        nextIndexKey(state, p, table, state.validated.tableInfoByName.getValue(table))
        if (p.table == before) p.atomicWork = addAtomicWork(p, p.atomicWork, 1, 32)
    }

    private suspend fun countSnapshotBaselineStep(state: RuntimeState, p: ApplyPlan) {
        if (p.table >= state.validated.tables.size) { p.phase = "ready"; return }
        val table = state.validated.tables[p.table.toInt()].tableName
        var tail = " FROM _sync_row_state WHERE schema_name=? AND table_name=?"
        val args = mutableListOf<Any?>(state.validated.schemaForTable(table), table)
        p.cursor?.let { tail += " AND key_json>?"; args += it }
        tail += " ORDER BY key_json LIMIT 1"
        val size = one("SELECT length(CAST(key_json AS BLOB))$tail", *args.toTypedArray()) { it.getLong(0) }
        if (size == null) { p.table++; p.cursor = null; return }
        val workspace = OversqliteWorkspace(db, config)
        val charge = workspace.add(p.unit, WorkspaceCharge(3, workspace.scale(p.unit, size, 18)), WorkspaceCharge(bytes = 128))
        checkSpace(p, charge.entries, charge.bytes)
        p.cursor = checkNotNull(one("SELECT key_json$tail", *args.toTypedArray()) { it.getText(0) })
        p.atomicWork = addAtomicWork(p, p.atomicWork, 1, 32)
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
    private suspend fun loadRow(plan: ApplyPlan, ordinal: Long, workspaceStatementCache: StatementCache?, replayStatementCache: StatementCache?): Pair<ApplySurvivor, Long> {
        currentCoroutineContext().ensureActive()
        val bytes = checkNotNull(one("SELECT bytes FROM _sync_apply_rows WHERE ordinal=?", ordinal, replayStatementCache = replayStatementCache) { it.getLong(0) })
        checkSpace(plan, 6, 6 * bytes, workspaceStatementCache)
        val row = checkNotNull(one("SELECT table_name,local_pk,payload,wire FROM _sync_apply_rows WHERE ordinal=?", ordinal, replayStatementCache = replayStatementCache) {
            ApplySurvivor(it.getText(0), it.getText(1), it.nullText(2), it.getLong(3) != 0L)
        })
        return row to bytes
    }

    suspend fun restore(state: RuntimeState, cache: StatementCache, verifyOnly: Boolean = false, workspaceStatementCache: StatementCache? = null, replayStatementCache: StatementCache? = null): Set<String> {
        val plan = checkNotNull(load(replayStatementCache = replayStatementCache))
        val changed = linkedSetOf<String>()
        for (ordinal in 1..plan.entries) {
            currentCoroutineContext().ensureActive()
            val (row, bytes) = loadRow(plan, ordinal, workspaceStatementCache, replayStatementCache = replayStatementCache)
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
            if (matches) continue
            check(!verifyOnly) { "survivor final state mismatch in ${row.table}" }
            if (row.payload == null) local.deleteLocalRow(state, row.table, row.pk, cache)
            else local.upsertRow(row.table, json.parseToJsonElement(row.payload) as JsonObject,
                if (row.wire) PayloadSource.AUTHORITATIVE_WIRE else PayloadSource.LOCAL_STATE, cache)
            changed += row.table
        }
        return changed
    }

    suspend fun finish() {
        for (table in listOf("_sync_apply_edges", "_sync_apply_rows")) {
            do {
                currentCoroutineContext().ensureActive()
                db.execSQL("DELETE FROM $table WHERE ordinal IN (SELECT ordinal FROM $table ORDER BY ordinal LIMIT 128)")
            } while (db.scalarLong("SELECT changes()") != 0L)
        }
        db.execSQL("DELETE FROM _sync_apply_plan")
    }

    private suspend fun indexStep(state: RuntimeState, p: ApplyPlan, workspaceStatementCache: StatementCache?, replayStatementCache: StatementCache?) {
        if (p.table >= state.validated.tables.size) {
            p.phase = "walk"; p.table = 0; p.fk = 0; p.cursor = null
            return
        }
        val table = state.validated.tables[p.table.toInt()].tableName
        val info = state.validated.tableInfoByName.getValue(table)
        if (info.foreignKeys.isEmpty()) { p.table++; p.fk = -1; p.cursor = null; return }
        if (p.fk < 0) { nextIndexKey(state, p, table, info, workspaceStatementCache, replayStatementCache = replayStatementCache); return }
        if (p.fk >= info.foreignKeys.size) { p.fk = -1; return }
        indexForeignKey(state, p, table, info, info.foreignKeys[p.fk.toInt()], workspaceStatementCache, replayStatementCache = replayStatementCache)
        p.fk++
    }

    private suspend fun nextIndexKey(state: RuntimeState, p: ApplyPlan, table: String, info: TableInfo, workspaceStatementCache: StatementCache? = null, replayStatementCache: StatementCache? = null) {
        val description = state.validated.replaySql.table(table)
        val sql = if (p.cursor == null) description.initialKey else description.afterCursorKey
        val args = p.cursor?.let { arrayOf(physicalKey(state, table, it)) } ?: emptyArray()
        val size = one(sql.size, *args, replayStatementCache = replayStatementCache) { it.getLong(0) }
        if (size == null) { p.table++; p.fk = -1; p.cursor = null; return }
        checkSpace(p, 3, 18 * size + 128, workspaceStatementCache)
        p.cursor = checkNotNull(one(sql.value, *args, replayStatementCache = replayStatementCache) { it.getText(0) })
        p.fk = 0
    }

    private suspend fun indexForeignKey(state: RuntimeState, p: ApplyPlan, table: String, info: TableInfo, fk: ForeignKeyInfo, workspaceStatementCache: StatementCache?, replayStatementCache: StatementCache?) {
        val parentTable = fk.refTable.lowercase()
        val sql = state.validated.replaySql.table(table).foreignKey(p.fk.toInt())
        val value = physicalKey(state, table, checkNotNull(p.cursor))
        val parentBytes = one(sql.size, value, replayStatementCache = replayStatementCache) { it.getLong(0) } ?: return
        val size = 6 * (parentBytes + utf8Size(parentTable) + utf8Size(table) + utf8Size(checkNotNull(p.cursor))) + 64
        checkSpace(p, 7, 6 * size, workspaceStatementCache)
        val parent = checkNotNull(one(sql.value, value, replayStatementCache = replayStatementCache) { it.getText(0) })
        exec("INSERT OR IGNORE INTO _sync_apply_edges VALUES(?,?,?,?,?,?)", p.edgeEntries + 1, parentTable, parent, table, p.cursor, size, replayStatementCache = replayStatementCache)
        if (db.scalarLong("SELECT changes()") != 0L) { p.edgeEntries++; p.edgeBytes += size }
    }

    private suspend fun walk(state: RuntimeState, p: ApplyPlan, workspaceStatementCache: StatementCache?, replayStatementCache: StatementCache?) {
        if (p.visit == 0L) {
            p.visit = one("SELECT ordinal FROM _sync_apply_rows WHERE pending=1 ORDER BY ordinal LIMIT 1", replayStatementCache = replayStatementCache) { it.getLong(0) }
                ?: run { p.phase = "ready"; return }
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
        var tail = " FROM _sync_apply_edges WHERE parent_table=? AND parent_pk=? AND child_table=?"
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
        val size = utf8Size(row.payload.orEmpty()) + 6 * utf8Size(row.table) + 6 * utf8Size(row.pk) + 64
        checkSpace(p, 7, 6 * size, workspaceStatementCache)
        exec("INSERT INTO _sync_apply_rows(ordinal,table_name,local_pk,payload,wire,pending,bytes,role,owner_revision) VALUES(?,?,?,?,?,?,?,?,(SELECT dirty_revision FROM _sync_conflict_state WHERE singleton_key=1))", p.entries + 1, row.table, row.pk, row.payload, row.wire, if(row.role=="exception") 0L else 1L, size, row.role, replayStatementCache = replayStatementCache)
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
        check("bytes", config.maxSyncWorkspaceBytes, add(add(add(p.bytes,p.edgeBytes,"bytes"),512+6*(utf8Size(p.unit)+utf8Size(p.guard)+utf8Size(p.cursor.orEmpty())+utf8Size(p.preserveSchema)+utf8Size(p.preserveTable)+utf8Size(p.preserveKey)),"bytes"),push.second,"bytes"), bytes)
    }

    private suspend fun load(replayStatementCache: StatementCache? = null): ApplyPlan? = one("SELECT unit,guard,phase,root_cursor,visit_cursor,table_cursor,fk_cursor,row_cursor,entries,bytes,edge_entries,edge_bytes,operation_kind,preserve_schema,preserve_table,preserve_key,atomic_work,preservation_complete FROM _sync_apply_plan WHERE singleton_key=1", replayStatementCache = replayStatementCache) {
        ApplyPlan(it.getText(0),it.getText(1),it.getText(2),it.getLong(3),it.getLong(4),it.getLong(5),it.getLong(6),it.nullText(7),it.getLong(8),it.getLong(9),it.getLong(10),it.getLong(11),it.getText(12),it.getText(13),it.getText(14),it.getText(15),it.getLong(16),it.getLong(17)!=0L)
    }

    private suspend fun save(p: ApplyPlan, replayStatementCache: StatementCache?) {
        currentCoroutineContext().ensureActive()
        exec("""INSERT INTO _sync_apply_plan(singleton_key,unit,guard,phase,root_cursor,visit_cursor,table_cursor,fk_cursor,row_cursor,entries,bytes,edge_entries,edge_bytes,operation_kind,preserve_schema,preserve_table,preserve_key,atomic_work,preservation_complete)
        VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(singleton_key) DO UPDATE SET unit=excluded.unit,guard=excluded.guard,
        phase=excluded.phase,root_cursor=excluded.root_cursor,visit_cursor=excluded.visit_cursor,
        table_cursor=excluded.table_cursor,fk_cursor=excluded.fk_cursor,row_cursor=excluded.row_cursor,
        entries=excluded.entries,bytes=excluded.bytes,edge_entries=excluded.edge_entries,edge_bytes=excluded.edge_bytes,
        operation_kind=excluded.operation_kind,preserve_schema=excluded.preserve_schema,
        preserve_table=excluded.preserve_table,preserve_key=excluded.preserve_key,
        atomic_work=excluded.atomic_work,preservation_complete=excluded.preservation_complete""",
            p.unit,p.guard,p.phase,p.root,p.visit,p.table,p.fk,p.cursor,p.entries,p.bytes,p.edgeEntries,p.edgeBytes,
            p.kind,p.preserveSchema,p.preserveTable,p.preserveKey,p.atomicWork,p.preservationComplete, replayStatementCache = replayStatementCache)
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
            if (ref==null) { p.preservationComplete=true; save(p, replayStatementCache = replayStatementCache); return }
            operations.a3Stats.duplicateChecks++
            if (!contains(ref.table,ref.row.pk, replayStatementCache = replayStatementCache)) {
                val live=capture(state,p,ref.table,ref.row.pk,8,OversqliteWorkspace(db,config).scale(p.unit,ref.bytes,8),workspaceStatementCache, replayStatementCache = replayStatementCache)
                if (!sameLocalIntent(state,ref.row,live)) put(p,live.copy(role="exception"),workspaceStatementCache, replayStatementCache = replayStatementCache)
            }
            p.atomicWork=addAtomicWork(p,p.atomicWork,1,1024)
            p.preserveSchema=ref.schema; p.preserveTable=ref.table; p.preserveKey=ref.key
            save(p, replayStatementCache = replayStatementCache)
        }
    }

    private fun addAtomicWork(p: ApplyPlan, used: Long, count: Long, weight: Long): Long {
        if (used<0 || count<0 || count>(Long.MAX_VALUE-used)/weight) throw SyncWorkspaceLimitException(
            p.unit,"atomic_apply_work",config.maxSyncWorkUnitsPerInvocation,operations.usedSyncWork(),Long.MAX_VALUE,false)
        return used+count*weight
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
