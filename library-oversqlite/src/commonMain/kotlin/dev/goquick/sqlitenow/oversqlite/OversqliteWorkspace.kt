package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteStatement
import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.add

/** Independently retained representation, aggregated in the existing operation owner. */
internal data class WorkspaceCharge(val entries: Long = 0, val bytes: Long = 0)

/** Retained only by read-only replay decision construction in one live writer. */
internal data class WorkspaceReservationBinding(
    val db: SafeSQLiteConnection,
    val state: RuntimeState,
    val writer: StatementCache,
    val token: String,
    val unit: String,
    val context: String,
)

internal val decisionReservationBindingCharge = WorkspaceCharge(8, 1024)

internal class OversqliteWorkspace(
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
    private val abandonedReservations: Boolean = false,
) {
    private data class Reservation(val token: String, val unit: String, val context: String, val entries: Long, val bytes: Long)

    @OptIn(kotlin.uuid.ExperimentalUuidApi::class)
    suspend fun reserve(state: RuntimeState, unit: String, requested: WorkspaceCharge): String = db.transaction(TransactionMode.IMMEDIATE) {
        currentCoroutineContext().ensureActive()
        require(isCanonicalSha256(unit)) { "invalid workspace unit" }
        val binding=contextGuard(state,unit)
        val charge=add(unit,requested,WorkspaceCharge(1,4096))
        check(unit,charge)
        val records=loadReservations().toMutableList()
        val token=sha256Hex((kotlin.uuid.Uuid.random().toString()+kotlin.uuid.Uuid.random().toString()).encodeToByteArray())
        records+=Reservation(token,unit,binding,charge.entries,charge.bytes)
        storeReservations(records)
        check(unit)
        token
    }

    // Called only after all structured request/consumer jobs and their buffer
    // scopes finish. Cancellation cannot skip the once-only writer release.
    suspend fun release(token: String) = withContext(NonCancellable) {
        db.transaction(TransactionMode.IMMEDIATE) {
            val records=loadReservations()
            storeReservations(records.filter { it.token!=token })
        }
    }

    suspend fun resize(state: RuntimeState, token: String, requested: WorkspaceCharge) = db.transaction(TransactionMode.IMMEDIATE) {
        resizeInWriter(state,token,requested)
        Unit
    }

    suspend fun resizeInWriter(
        state: RuntimeState, token: String, requested: WorkspaceCharge,
        binding: WorkspaceReservationBinding? = null, writer: StatementCache? = null,
    ): WorkspaceReservationBinding? {
        currentCoroutineContext().ensureActive()
        check(ReplayFixedUnits.reservation)
        val records=loadReservations().toMutableList()
        val index=records.indexOfFirst {it.token==token}
        check(index>=0) { "live reservation is missing" }
        val old=records[index]
        writer?.requireOpenForReservationBinding()
        if (binding != null) {
            require(binding.db === db && binding.state === state && binding.writer === writer &&
                binding.token == token && binding.unit == old.unit && isCanonicalSha256(binding.context)) {
                "reservation binding belongs to a different writer or owner"
            }
        }
        val proposed=add(old.unit,requested,WorkspaceCharge(1,4096))
        val next=WorkspaceCharge(maxOf(old.entries,proposed.entries),maxOf(old.bytes,proposed.bytes))
        check(old.unit,WorkspaceCharge(maxOf(0,next.entries-old.entries),maxOf(0,next.bytes-old.bytes)))
        val context = binding?.context ?: contextGuard(state,old.unit)
        records[index]=old.copy(context=context,entries=next.entries,bytes=next.bytes)
        storeReservations(records)
        check(old.unit)
        return binding ?: writer?.let { WorkspaceReservationBinding(db,state,it,token,old.unit,context) }
    }

    suspend fun reserved(token: String): WorkspaceCharge = checkNotNull(loadReservations().find {it.token==token}).let {
        WorkspaceCharge(it.entries-1,it.bytes-4096)
    }

    // The decision map remains live; only its separate binding holder has ended.
    suspend fun finishDecisionBindingInWriter(token: String, retained: WorkspaceCharge, writer: StatementCache) {
        writer.requireOpenForReservationBinding()
        val records = loadReservations().toMutableList()
        val index = records.indexOfFirst { it.token == token }
        check(index >= 0) { "live decision reservation is missing" }
        val old = records[index]
        val charge = add(old.unit, retained, WorkspaceCharge(1, 4096))
        require(charge.entries <= old.entries && charge.bytes <= old.bytes) { "binding release cannot grow a reservation" }
        records[index] = old.copy(entries = charge.entries, bytes = charge.bytes)
        storeReservations(records)
        check(old.unit)
    }

    private suspend fun loadReservations(): List<Reservation> {
        validateReservationMetadata()
        val snapshot=checkNotNull(one("SELECT reserved_entries,reserved_bytes,workspace_context FROM _sync_operation_state WHERE singleton_key=1") {
            Triple(it.getLong(0),it.getLong(1),it.getText(2))
        })
        if (snapshot.third=="") { require(snapshot.first==0L && snapshot.second==0L); return emptyList() }
        val array=Json.parseToJsonElement(snapshot.third) as? JsonArray ?: error("reservation ledger must be array")
        require(array.isNotEmpty()) { "empty ledger must use empty TEXT" }
        var total=WorkspaceCharge()
        val seen=mutableSetOf<String>()
        val records=array.map { element ->
            val obj=element as? JsonObject ?: error("invalid reservation record")
            require(obj.keys==setOf("token","unit","context","entries","bytes")) { "invalid reservation fields" }
            fun identity(name: String): String = checkNotNull(obj[name]).jsonPrimitive.let {
                require(it.isString && isCanonicalSha256(it.content)) { "invalid reservation identity" }; it.content
            }
            fun count(name: String): Long = checkNotNull(obj[name]).jsonPrimitive.let {
                require(!it.isString && it.content.matches(Regex("0|[1-9][0-9]*"))) { "invalid reservation count" }
                checkNotNull(it.longOrNull)
            }
            val record=Reservation(identity("token"),identity("unit"),identity("context"),count("entries"),count("bytes"))
            require(seen.add(record.token)) { "duplicate reservation token" }
            total=add(record.unit,total,WorkspaceCharge(record.entries,record.bytes))
            record
        }
        require(total.entries==snapshot.first && total.bytes==snapshot.second) { "reservation totals differ" }
        return records
    }

    private suspend fun storeReservations(records: List<Reservation>) {
        var total=WorkspaceCharge()
        for (record in records) total=add(record.unit,total,WorkspaceCharge(record.entries,record.bytes))
        val text=if(records.isEmpty()) "" else buildJsonArray {
            for (record in records) add(buildJsonObject {
                put("token",record.token); put("unit",record.unit); put("context",record.context)
                put("entries",record.entries); put("bytes",record.bytes)
            })
        }.toString()
        db.withPreparedStatement("UPDATE _sync_operation_state SET workspace_context=?,reserved_entries=?,reserved_bytes=? WHERE singleton_key=1") {
            it.bindText(1,text); it.bindLong(2,total.entries); it.bindLong(3,total.bytes); it.step()
        }
    }

    // The coordinated opening calls this once after acquiring its existing
    // lifetime lease and validating the retained layout. Client.open and an
    // operation gate are never abandonment proof.
    suspend fun reclaimAfterLifetimeAcquisition() {
        validateReservationMetadata()
        db.execSQL("UPDATE _sync_operation_state SET workspace_context='',reserved_entries=0,reserved_bytes=0 WHERE singleton_key=1")
    }

    private suspend fun validateReservationMetadata() {
        val snapshot=checkNotNull(one("""SELECT reserved_entries,reserved_bytes,workspace_context='',
            CASE WHEN workspace_context='' THEN 1 ELSE json_valid(workspace_context) END FROM _sync_operation_state WHERE singleton_key=1""") {
            listOf(it.getLong(0),it.getLong(1),it.getLong(2),it.getLong(3))
        })
        if(snapshot[2]==1L) { require(snapshot[0]==0L && snapshot[1]==0L); return }
        require(snapshot[3]==1L && one("SELECT json_type(workspace_context) FROM _sync_operation_state WHERE singleton_key=1") {it.getText(0)}=="array")
        var total=WorkspaceCharge(); var count=0L
        db.withPreparedStatement("""SELECT json_type(j.value),(SELECT count(*) FROM json_each(j.value)),
            length(json_extract(j.value,'$.token')),length(json_extract(j.value,'$.unit')),length(json_extract(j.value,'$.context')),
            json_type(j.value,'$.entries'),json_type(j.value,'$.bytes'),json_extract(j.value,'$.entries'),json_extract(j.value,'$.bytes'),
            json_extract(j.value,'$.token') NOT GLOB '*[^0-9a-f]*',json_extract(j.value,'$.unit') NOT GLOB '*[^0-9a-f]*',
            json_extract(j.value,'$.context') NOT GLOB '*[^0-9a-f]*'
            FROM _sync_operation_state o,json_each(o.workspace_context) j WHERE singleton_key=1""") { st ->
            while(st.step()) {
                currentCoroutineContext().ensureActive()
                require(st.getText(0)=="object" && st.getLong(1)==5L && (2..4).all {st.getLong(it)==64L} &&
                    st.getText(5)=="integer" && st.getText(6)=="integer" && (9..11).all {st.getLong(it)==1L}) {"invalid reservation record"}
                total=add(ReplayFixedUnits.reservationReconciliation,total,WorkspaceCharge(st.getLong(7),st.getLong(8))); count++
            }
        }
        val identities=checkNotNull(one("""SELECT count(*),count(DISTINCT json_extract(j.value,'$.token'))
            FROM _sync_operation_state o,json_each(o.workspace_context) j WHERE singleton_key=1""") { it.getLong(0) to it.getLong(1) })
        require(identities.first==identities.second) { "duplicate reservation token" }
        require(count>0 && total.entries==snapshot[0] && total.bytes==snapshot[1]) { "invalid reservation scalar totals" }
    }
    // Only scalar lengths are loaded, one indexed row per cancellable step.
    // Opening must complete retained validation even above the invocation target.
    suspend fun reconcileRetainedOwners() {
        validateReservationMetadata()
        val unit=ReplayFixedUnits.retainedReconciliation
        val owners=listOf(
            listOf("_sync_snapshot_stage","charge","64+6*(length(CAST(snapshot_id AS BLOB))+length(CAST(schema_name AS BLOB))+length(CAST(table_name AS BLOB))+length(CAST(key_json AS BLOB)))+length(CAST(COALESCE(payload,'') AS BLOB))","SELECT snapshot_stage_entries,snapshot_stage_bytes FROM _sync_operation_state WHERE singleton_key=1"),
            listOf("_sync_apply_rows","bytes","64+6*(length(CAST(table_name AS BLOB))+length(CAST(local_pk AS BLOB)))+length(CAST(COALESCE(payload,'') AS BLOB))","SELECT entries,bytes FROM _sync_apply_plan WHERE singleton_key=1"),
            listOf("_sync_apply_edges","bytes","64+6*(length(CAST(parent_table AS BLOB))+length(CAST(parent_pk AS BLOB))+length(CAST(child_table AS BLOB))+length(CAST(child_pk AS BLOB)))","SELECT edge_entries,edge_bytes FROM _sync_apply_plan WHERE singleton_key=1"),
        )
        for(owner in owners) {
            var cursor=Long.MIN_VALUE; var total=WorkspaceCharge()
            while(true) {
                currentCoroutineContext().ensureActive()
                val row=db.withPreparedStatement("SELECT rowid,${owner[1]},${owner[2]},typeof(${owner[1]}) FROM ${owner[0]} WHERE rowid>? ORDER BY rowid LIMIT 1") { st ->
                    st.bindLong(1,cursor)
                    if(st.step()) { require(st.getText(3)=="integer" && st.getLong(1)==st.getLong(2)) { "invalid retained charge in ${owner[0]}" }; st.getLong(0) to st.getLong(1) } else null
                } ?: break
                require(row.first>0) { "invalid retained ordinal" }
                total=add(unit,total,WorkspaceCharge(1,row.second)); cursor=row.first
            }
            val recorded=one(owner[3]) { WorkspaceCharge(it.getLong(0),it.getLong(1)) } ?: WorkspaceCharge()
            require(total==recorded) { "retained totals differ in ${owner[0]}" }
        }
    }

    fun add(unit: String, total: WorkspaceCharge, extra: WorkspaceCharge): WorkspaceCharge {
        val entries = checkedAdd(unit, "entries", total.entries, extra.entries)
        val bytes = checkedAdd(unit, "bytes", total.bytes, extra.bytes)
        return WorkspaceCharge(entries, bytes)
    }

    private fun checkedAdd(unit: String, dimension: String, used: Long, requested: Long): Long {
        require(used >= 0 && requested >= 0) { "invalid negative workspace $dimension metadata" }
        val limit = if (dimension == "entries") config.maxSyncWorkspaceEntries else config.maxSyncWorkspaceBytes
        if (requested > Long.MAX_VALUE - used) throw SyncWorkspaceLimitException(unit, dimension, limit, used, requested, false)
        return used + requested
    }

    fun scale(unit: String, bytes: Long, copies: Long): Long {
        require(bytes >= 0 && copies >= 0) { "invalid negative workspace byte measurement" }
        if (copies != 0L && bytes > Long.MAX_VALUE / copies) throw SyncWorkspaceLimitException(
            unit, "bytes", config.maxSyncWorkspaceBytes, bytes, Long.MAX_VALUE, false)
        return bytes * copies
    }

    private suspend fun <T> one(sql: String, read: (SQLiteStatement) -> T): T? =
        db.withPreparedStatement(sql) { if (it.step()) read(it) else null }

    // One current scalar snapshot, at most one row per owner; never scan payload tables.
    suspend fun usage(unit: String, apply: WorkspaceCharge? = null, push: WorkspaceCharge? = null, statementCache: StatementCache? = null): WorkspaceCharge {
        val snapshot = currentCoroutineContext()[PushPlanningWorkspaceSnapshot]?.takeIf { it.db === db }
        if (snapshot == null) return usageUncached(unit, apply, push, statementCache)
        val cacheCharge = if (snapshot.guard == null) pushPlanningWorkspaceCharge
            else add(unit, pushPlanningWorkspaceCharge, pushPlanningGuardCharge)
        if (apply == null && !abandonedReservations) {
            val base = snapshot.base ?: usageUncached(unit, push = WorkspaceCharge(), statementCache = statementCache).also { snapshot.base = it }
            val currentPush = push ?: one("SELECT entries,bytes,length(CAST(unit AS BLOB))+length(CAST(guard AS BLOB))+length(CAST(state AS BLOB)) FROM _sync_push_plan WHERE singleton_key=1") {
                add(unit, WorkspaceCharge(it.getLong(0), it.getLong(1)), WorkspaceCharge(1, checkedAdd(unit, "bytes", 2048, scale(unit, it.getLong(2), 6))))
            } ?: WorkspaceCharge()
            return add(unit, add(unit, base, currentPush), cacheCharge)
        }
        return add(unit, usageUncached(unit, apply, push, statementCache), cacheCharge)
    }

    private suspend fun usageUncached(unit: String, apply: WorkspaceCharge? = null, push: WorkspaceCharge? = null, statementCache: StatementCache? = null): WorkspaceCharge {
        val applyColumns = if (apply != null) "NULL,0,0,0,0,0" else """a.singleton_key,a.entries,a.bytes,a.edge_entries,a.edge_bytes,
            length(CAST(a.unit AS BLOB))+length(CAST(a.guard AS BLOB))+length(CAST(a.source_snapshot_id AS BLOB))+
            length(CAST(COALESCE(a.row_cursor,'') AS BLOB))+
            length(CAST(a.preserve_schema AS BLOB))+length(CAST(a.preserve_table AS BLOB))+length(CAST(a.preserve_key AS BLOB))"""
        val pushColumns = if (push != null) "NULL,0,0,0" else """p.singleton_key,p.entries,p.bytes,
            length(CAST(p.unit AS BLOB))+length(CAST(p.guard AS BLOB))+length(CAST(p.state AS BLOB))"""
        return db.withPreparedStatement("""SELECT ${if (abandonedReservations) "0,0" else "o.reserved_entries,o.reserved_bytes"},
            o.snapshot_stage_entries,o.snapshot_stage_bytes,
            ${if (abandonedReservations) "0" else "length(CAST(o.workspace_context AS BLOB))"}+length(CAST(o.kind AS BLOB))+
            length(CAST(o.target_user_id AS BLOB))+length(CAST(o.reason AS BLOB))+
            length(CAST(o.replacement_source_id AS BLOB))+length(CAST(o.staged_snapshot_id AS BLOB))+
            length(CAST(o.snapshot_stage_guard AS BLOB)),
            $applyColumns,$pushColumns
            FROM _sync_operation_state o
            ${if (apply == null) "LEFT JOIN _sync_apply_plan a ON a.singleton_key=1" else ""}
            ${if (push == null) "LEFT JOIN _sync_push_plan p ON p.singleton_key=1" else ""}
            WHERE o.singleton_key=1""", statementCache = statementCache) { statement ->
            check(statement.step()) { "missing workspace operation owner" }
            // Check the scalar snapshot while its statement is owned: an accounting
            // failure stays primary if finalization also fails.
            var total = add(unit, WorkspaceCharge(1, 512), WorkspaceCharge(bytes = scale(unit, statement.getLong(4), 6)))
            total = add(unit, total, WorkspaceCharge(statement.getLong(0), statement.getLong(1)))
            total = add(unit, total, WorkspaceCharge(statement.getLong(2), statement.getLong(3)))
            val applyCharge = apply ?: run {
                currentCoroutineContext().ensureActive()
                if (statement.isNull(5)) WorkspaceCharge() else
                    add(unit, add(unit, WorkspaceCharge(statement.getLong(6), statement.getLong(7)),
                        WorkspaceCharge(statement.getLong(8), statement.getLong(9))),
                        WorkspaceCharge(1, checkedAdd(unit, "bytes", 512, scale(unit, statement.getLong(10), 6))))
            }
            total = add(unit, total, applyCharge)
            val pushCharge = push ?: run {
                currentCoroutineContext().ensureActive()
                if (statement.isNull(11)) WorkspaceCharge() else
                    add(unit, WorkspaceCharge(statement.getLong(12), statement.getLong(13)),
                        WorkspaceCharge(1, checkedAdd(unit, "bytes", 2048, scale(unit, statement.getLong(14), 6))))
            }
            add(unit, total, pushCharge)
        }
    }

    suspend fun check(unit: String, extra: WorkspaceCharge = WorkspaceCharge(), apply: WorkspaceCharge? = null,
        push: WorkspaceCharge? = null, statementCache: StatementCache? = null) {
        try {
            val used = usage(unit, apply, push, statementCache)
            checkDimension(unit, "entries", config.maxSyncWorkspaceEntries, used.entries, extra.entries)
            checkDimension(unit, "bytes", config.maxSyncWorkspaceBytes, used.bytes, extra.bytes)
        } catch (capacity: SyncWorkspaceLimitException) {
            val snapshot = currentCoroutineContext()[PushPlanningWorkspaceSnapshot]?.takeIf { it.db === db }
            if (snapshot?.guard == null || capacity.suppressedExceptions.isNotEmpty()) throw capacity
            snapshot.guard = null
            check(unit, extra, apply, push, statementCache)
        }
    }

    private fun checkDimension(unit: String, dimension: String, limit: Long, used: Long, requested: Long) {
        require(used >= 0 && requested >= 0) { "invalid negative workspace $dimension metadata" }
        if (used > limit || requested > limit - used) throw SyncWorkspaceLimitException(unit, dimension, limit, used, requested, false)
    }

    // One row: 64 fixed bytes, worst-case escaped identities, exact payload UTF8.
    fun stageCharge(unit: String, payloadBytes: Long, vararg identities: String): WorkspaceCharge {
        var charge = WorkspaceCharge(1, 64)
        for (identity in identities) charge = add(unit, charge, WorkspaceCharge(bytes = scale(unit, utf8Size(identity), 6)))
        return add(unit, charge, WorkspaceCharge(bytes = payloadBytes))
    }

    suspend fun contextGuard(state: RuntimeState, unit: String): String {
        var charge=WorkspaceCharge(32,checkNotNull(one("""SELECT 8192+6*(length(CAST(i.database_id AS BLOB))+
            length(CAST(i.service_id AS BLOB))+length(CAST(i.physical_shape AS BLOB))+length(CAST(i.fingerprint AS BLOB))+
            length(CAST(a.current_source_id AS BLOB))+length(CAST(a.attached_user_id AS BLOB))+
            length(CAST(a.pending_initialization_id AS BLOB))+length(CAST(o.source_id AS BLOB))+
            length(CAST(o.canonical_request_hash AS BLOB))) FROM _sync_contract_identity i
            CROSS JOIN _sync_attachment_state a CROSS JOIN _sync_outbox_bundle o""") { it.getLong(0) }))
        for (table in state.validated.tables) {
            charge=add(unit,charge,WorkspaceCharge(4,512+12*(utf8Size(table.tableName)+utf8Size(table.syncKeyColumnName))))
            for (fk in state.validated.tableInfoByName.getValue(table.tableName).foreignKeys) charge=add(unit,charge,
                WorkspaceCharge(8,512+12*(utf8Size(fk.refTable)+utf8Size(fk.fromCol)+utf8Size(fk.toCol)+utf8Size(fk.onUpdate)+utf8Size(fk.onDelete))))
        }
        check(unit,charge)
        return checkNotNull(one("""SELECT i.database_id,i.service_id,i.generation_id,i.local_version,i.physical_shape,
            i.fingerprint,a.attachment_epoch,a.current_source_id,a.binding_state,a.attached_user_id,
            a.pending_initialization_id,a.last_bundle_seq_seen,f.dirty_revision,f.baseline_revision,
            s.next_source_bundle_id,o.state,o.source_id,o.source_bundle_id,o.canonical_request_hash
            FROM _sync_contract_identity i CROSS JOIN _sync_attachment_state a CROSS JOIN _sync_conflict_state f
            JOIN _sync_source_state s ON s.source_id=a.current_source_id CROSS JOIN _sync_outbox_bundle o""") { st ->
            val numeric=setOf(2,3,6,11,12,13,14,17)
            val context=buildJsonArray {
                add("a3-context"); add(unit)
                for (i in 0..18) add(if (i in numeric) JsonPrimitive(st.getLong(i)) else JsonPrimitive(st.getText(i)))
                add(state.admittedContract.limits.toString())
                add(config.maxSyncWorkspaceEntries); add(config.maxSyncWorkspaceBytes); add(config.maxSyncWorkUnitsPerInvocation)
                for (table in state.validated.tables) add(buildJsonArray {
                    add(table.tableName); add(table.syncKeyColumnName)
                    for (key in state.validated.keyByTable.getValue(table.tableName)) add(key)
                    for (fk in state.validated.tableInfoByName.getValue(table.tableName).foreignKeys) add(buildJsonArray {
                        add(fk.seq); add(fk.refTable); add(fk.fromCol); add(fk.toCol); add(fk.onUpdate); add(fk.onDelete); add(fk.match)
                    })
                })
            }
            sha256Hex(context.toString().encodeToByteArray())
        })
    }
}

/** Derived snapshot-stage totals stay writer-atomic with all stage mutations. */
internal val snapshotStageAccountingDdl: Map<String, String> = run {
    val charge = "64+6*(length(CAST(NEW.snapshot_id AS BLOB))+length(CAST(NEW.schema_name AS BLOB))+" +
        "length(CAST(NEW.table_name AS BLOB))+length(CAST(NEW.key_json AS BLOB)))+length(CAST(COALESCE(NEW.payload,'') AS BLOB))"
    val setCharge = "UPDATE _sync_snapshot_stage SET charge=$charge WHERE snapshot_id=NEW.snapshot_id AND row_ordinal=NEW.row_ordinal;"
    mapOf(
        "_sync_snapshot_stage_charge_insert" to """CREATE TRIGGER IF NOT EXISTS _sync_snapshot_stage_charge_insert
            AFTER INSERT ON _sync_snapshot_stage BEGIN $setCharge
            UPDATE _sync_operation_state SET snapshot_stage_entries=snapshot_stage_entries+1,
            snapshot_stage_bytes=snapshot_stage_bytes+($charge) WHERE singleton_key=1; END""",
        "_sync_snapshot_stage_charge_update" to """CREATE TRIGGER IF NOT EXISTS _sync_snapshot_stage_charge_update
            AFTER UPDATE OF snapshot_id,row_ordinal,schema_name,table_name,key_json,row_version,payload ON _sync_snapshot_stage
            BEGIN $setCharge UPDATE _sync_operation_state SET snapshot_stage_bytes=snapshot_stage_bytes-OLD.charge+($charge)
            WHERE singleton_key=1; END""",
        "_sync_snapshot_stage_charge_delete" to """CREATE TRIGGER IF NOT EXISTS _sync_snapshot_stage_charge_delete
            AFTER DELETE ON _sync_snapshot_stage BEGIN UPDATE _sync_operation_state
            SET snapshot_stage_entries=snapshot_stage_entries-1,snapshot_stage_bytes=snapshot_stage_bytes-OLD.charge
            WHERE singleton_key=1; END""",
    )
}
