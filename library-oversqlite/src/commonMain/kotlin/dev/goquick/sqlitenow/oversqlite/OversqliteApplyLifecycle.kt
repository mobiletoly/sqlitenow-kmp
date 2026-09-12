package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteStatement
import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.serialization.json.*

// Snapshot and conflict application share the existing durable final-state owner.
// All SQL references stay immutable under its guarded writer; no new journal.
internal class OversqliteApplyLifecycle(
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
    private val local: OversqliteLocalStore,
    private val survivors: OversqliteApplySurvivors,
    private val json: Json,
) {
    private val workspace = OversqliteWorkspace(db, config)

    suspend fun prepareSnapshot(state: RuntimeState, session: SnapshotSession, preserve: Boolean) {
        val outbox = if (preserve) db.scalarLong("SELECT count(*) FROM _sync_outbox_rows") else 0
        val unit = applyUnit("snapshot", session.snapshotBundleSeq,
            "${session.snapshotId}/${session.rowCount}/${session.logicalByteCount}/$preserve")
        val roots = workspace.add(unit, WorkspaceCharge(session.rowCount, 0), WorkspaceCharge(outbox, 0)).entries
        if (roots > Int.MAX_VALUE) throw SyncWorkspaceLimitException(unit, "atomic_apply_work",
            config.maxSyncWorkUnitsPerInvocation, 0, Long.MAX_VALUE, false)
        check(one("SELECT count(*) FROM _sync_snapshot_stage WHERE snapshot_id=?", session.snapshotId) { it.getLong(0) } == session.rowCount) {
            "staged snapshot row count mismatch"
        }
        survivors.prepare(state, unit, roots.toInt(), "snapshot", session.snapshotBundleSeq) { index ->
            // Frozen L overrides only the business view, never snapshot A.
            if (index.toLong() < outbox) readOutbox(state, index.toLong(), unit)
            else readSnapshot(state, session.snapshotId, index.toLong() - outbox + 1, unit).second
        }
    }

    private suspend fun readOutbox(state: RuntimeState, ordinal: Long, unit: String): ApplySurvivor {
        val size = checkNotNull(one("SELECT 64+6*(length(CAST(table_name AS BLOB))+length(CAST(key_json AS BLOB)))+length(CAST(COALESCE(local_payload,'') AS BLOB)) FROM _sync_outbox_rows WHERE row_ordinal=?", ordinal) { it.getLong(0) })
        workspace.check(unit, WorkspaceCharge(8, workspace.scale(unit, size, 8)))
        return checkNotNull(one("SELECT table_name,key_json,local_payload FROM _sync_outbox_rows WHERE row_ordinal=?", ordinal) {
            Triple(it.getText(0), it.getText(1), if (it.isNull(2)) null else it.getText(2))
        }).let { (table, key, payload) ->
            val (pk, _) = local.decodeDirtyKeyForPush(state, table, key)
            ApplySurvivor(table, pk, payload, role = "intended")
        }
    }

    private suspend fun readSnapshot(state: RuntimeState, id: String, ordinal: Long, unit: String): Pair<Long, ApplySurvivor> {
        currentCoroutineContext().ensureActive()
        val size = checkNotNull(one("SELECT charge FROM _sync_snapshot_stage WHERE snapshot_id=? AND row_ordinal=?", id, ordinal) { it.getLong(0) })
        workspace.check(unit, WorkspaceCharge(8, workspace.scale(unit, size, 8)))
        val row = checkNotNull(one("SELECT table_name,key_json,payload,row_version FROM _sync_snapshot_stage WHERE snapshot_id=? AND row_ordinal=?", id, ordinal) {
            Triple(it.getText(0), it.getText(1), it.getText(2)) to it.getLong(3)
        })
        val (table, key, payload) = row.first
        require(row.second > 0) { "invalid snapshot version" }
        val (pk, wireKey) = local.decodeDirtyKeyForPush(state, table, key)
        val normalized = local.validateAndNormalizeSnapshotPayload(state, table, wireKey,
            json.parseToJsonElement(payload) as JsonObject)
        return row.second to ApplySurvivor(table, pk, normalized.toString(), wire = true, role = "intended")
    }

    suspend fun atomicWork(state: RuntimeState, kind: String, unit: String, entries: Long, base: Long): Long {
        var work = base
        fun add(count: Long, weight: Long) {
            if (count < 0 || count > (Long.MAX_VALUE - work) / weight)
                throw SyncWorkspaceLimitException(unit, "atomic_apply_work", config.maxSyncWorkUnitsPerInvocation, 0, Long.MAX_VALUE, false)
            work += count * weight
        }
        var maxFKs = 0L
        for (table in state.validated.tables) {
            maxFKs = maxOf(maxFKs, state.validated.tableInfoByName.getValue(table.tableName).foreignKeys.size.toLong())
        }
        add(entries, 16 * (maxFKs + state.validated.tables.size))
        return work
    }

    suspend fun validateDependencies(state: RuntimeState, kind: String, guard: String, entries: Long, inputSequence: Long?) {
        var blocker: FKReconciliationRequiredException? = null
        for (ordinal in 1..entries) {
            currentCoroutineContext().ensureActive()
            val size = checkNotNull(one("SELECT bytes FROM _sync_apply_rows WHERE ordinal=?", ordinal) { it.getLong(0) })
            workspace.check(guard, workspace.add(guard, WorkspaceCharge(if(blocker==null) 8 else 16, if(blocker==null) 0 else 16*4096), WorkspaceCharge(bytes=workspace.scale(guard,size,8))))
            val row = checkNotNull(one("SELECT table_name,local_pk,payload,wire FROM _sync_apply_rows WHERE ordinal=?", ordinal) {
                ApplySurvivor(it.getText(0), it.getText(1), if(it.isNull(2)) null else it.getText(2), it.getLong(3)!=0L)
            })
            try { validateRowDependencies(state, kind, guard, row, inputSequence) }
            catch (pending: FKReconciliationRequiredException) {
                val first = blocker
                if(first==null) workspace.check(guard, WorkspaceCharge(8,16*4096))
                blocker = if(first == null) pending else FKReconciliationRequiredException(first.detail, first.unit, first.sample, true)
            }
        }
        currentCoroutineContext().ensureActive()
        blocker?.let { throw it }
    }

    private suspend fun validateRowDependencies(state: RuntimeState, kind: String, guard: String, row: ApplySurvivor, inputSequence: Long?) {
            if (row.payload == null) {
                if (kind != "snapshot") for ((table, info) in state.validated.tableInfoByName) {
                    if (info.foreignKeys.none { it.refTable.equals(row.table, ignoreCase = true) }) continue
                    if (one("SELECT 1 FROM _sync_row_state WHERE relationships_known=0 AND table_name=? LIMIT 1", table) { true } == true)
                        pending(state, guard, row, "relationship_unknown")
                }
                return
            }
            val payload = json.parseToJsonElement(row.payload) as JsonObject
            val info = state.validated.tableInfoByName.getValue(row.table)
            val source = if (row.wire) PayloadSource.AUTHORITATIVE_WIRE else PayloadSource.LOCAL_STATE
            for (fk in info.foreignKeys) {
                val column = info.column(fk.fromCol)
                val value = OversqliteValueCodec.decodePayloadValue(column, payload.getValue(fk.fromCol.lowercase()), source)
                val pk = when (value) {
                    TypedValue.Null -> continue
                    is TypedValue.Text -> value.value
                    is TypedValue.Blob -> bytesToHexLower(value.bytes)
                    else -> error("unsupported admitted FK identity")
                }
                validateParent(state, kind, guard, row, fk.refTable.lowercase(), pk, inputSequence)
            }
    }

    private suspend fun validateParent(state: RuntimeState, kind: String, guard: String, child: ApplySurvivor, table: String, pk: String, inputSequence: Long?) {
        val selected = one("SELECT payload IS NOT NULL FROM _sync_apply_rows WHERE table_name=? AND local_pk=?", table, pk) { it.getLong(0) != 0L }
        if (selected == true) return
        if (selected == false || kind == "snapshot") pending(state, guard, child, "dependency_unavailable")
        val key = keyJson(state, table, pk)
        val observed = one("SELECT server_kind,relationships_known,row_version FROM _sync_row_state WHERE schema_name=? AND table_name=? AND key_json=?", state.validated.schemaForTable(table), table, key) {
            Triple(it.getText(0), it.getLong(1) != 0L, it.getLong(2))
        }
        if (observed == null) {
            val info = state.validated.tableInfoByName.getValue(table)
            val physical = if(info.primaryKeyIsBlob) decodeLocalUuidBytes(pk) else pk
            check(one("SELECT 1 FROM ${quoteIdent(table)} WHERE ${quoteIdent(checkNotNull(info.primaryKey).name)}=?", physical) { true } != true) {
                "mandatory parent authoritative metadata is missing"
            }
            pending(state, guard, child, "dependency_unavailable")
        }
        if (observed.first == "unknown" && !observed.second) pending(state, guard, child, "relationship_unknown")
        if (observed.first != "live") pending(state, guard, child,
            if (inputSequence != null && observed.third > inputSequence) "historical_repair_required" else "dependency_unavailable")
        val info = state.validated.tableInfoByName.getValue(table)
        val physical = if (info.primaryKeyIsBlob) decodeLocalUuidBytes(pk) else pk
        if (one("SELECT 1 FROM ${quoteIdent(table)} WHERE ${quoteIdent(checkNotNull(info.primaryKey).name)}=?", physical) { true } != true)
            pending(state, guard, child, "historical_repair_required")
    }

    private suspend fun pending(state: RuntimeState, guard: String, row: ApplySurvivor, detail: String): Nothing {
        currentCoroutineContext().ensureActive()
        val (_, key) = local.decodeDirtyKeyForPush(state, row.table, keyJson(state, row.table, row.pk))
        val ref = PushDependencyRecordRef(state.validated.schemaForTable(row.table), row.table, key)
        val encoded = buildJsonObject {
            put("schema", ref.schema); put("table", ref.table)
            put("key", JsonObject(key.mapValues { JsonPrimitive(it.value) }))
        }
        val size = encodedJsonSize(encoded)
        workspace.check(guard, WorkspaceCharge(4, workspace.scale(guard, size, 4)))
        throw FKReconciliationRequiredException(detail, guard, if (size + 2 <= 4096) listOf(ref) else emptyList(), size + 2 > 4096)
    }

    suspend fun restoreAndVerify(state: RuntimeState, cache: StatementCache) : Set<String> {
        val updated = linkedSetOf<String>()
        for (pass in 0 until 64) {
            val changed = survivors.restore(state, cache) + survivors.restorePrimaryIntent(state, cache)
            updated += changed
            if (changed.isEmpty()) break
        }
        survivors.restore(state, cache, verifyOnly = true)
        survivors.restorePrimaryIntent(state, cache, verifyOnly = true)
        return updated
    }

    suspend fun verifySnapshot(state: RuntimeState, session: SnapshotSession, preserve: Boolean, cache: StatementCache) {
        val unit = applyUnit("snapshot-final", session.snapshotBundleSeq, session.snapshotId)
        for (ordinal in 1..session.rowCount) {
            val (version, row) = readSnapshot(state, session.snapshotId, ordinal, unit)
            val expected = local.captureAuthoritativePayload(row.table, json.parseToJsonElement(checkNotNull(row.payload)) as JsonObject, cache)
            val observed = checkNotNull(one("SELECT row_version,server_kind,server_payload FROM _sync_row_state WHERE schema_name=? AND table_name=? AND key_json=?", state.validated.schemaForTable(row.table), row.table, keyJson(state, row.table, row.pk)) {
                Triple(it.getLong(0), it.getText(1), if (it.isNull(2)) null else it.getText(2))
            })
            local.validateStoredServerProjection(state.validated.schemaForTable(row.table),row.table,keyJson(state,row.table,row.pk))
            check(one("SELECT json_extract(server_provenance,'$.origin')='snapshot' AND json_extract(server_provenance,'$.authoritative_version')=? AND json_extract(server_provenance,'$.bundle_sequence')=? FROM _sync_row_state WHERE schema_name=? AND table_name=? AND key_json=?", version,session.snapshotBundleSeq,state.validated.schemaForTable(row.table),row.table,keyJson(state,row.table,row.pk)) { it.getLong(0)!=0L } == true) { "snapshot A provenance differs from S" }
            check(observed.first == version && observed.second == "live" && observed.third != null &&
                OversqliteValueCodec.equivalentPayloadTexts(state.validated.tableInfoByName.getValue(row.table), expected, observed.third,
                    PayloadSource.LOCAL_STATE, PayloadSource.LOCAL_STATE, json)) { "snapshot A differs from staged S" }
        }
        if(preserve) check(one("SELECT count(*) FROM _sync_outbox_rows o JOIN _sync_row_state a ON (a.schema_name,a.table_name,a.key_json)=(o.schema_name,o.table_name,o.key_json) WHERE NOT EXISTS(SELECT 1 FROM _sync_snapshot_stage s WHERE s.snapshot_id=? AND (s.schema_name,s.table_name,s.key_json)=(o.schema_name,o.table_name,o.key_json)) AND (a.server_kind<>'absent' OR a.row_version<>0 OR a.server_payload IS NOT NULL OR a.relationships_known<>1 OR json_extract(a.server_provenance,'$.origin') IS NOT 'snapshot' OR json_extract(a.server_provenance,'$.authoritative_version') IS NOT 0 OR json_extract(a.server_provenance,'$.bundle_sequence') IS NOT ?)",session.snapshotId,session.snapshotBundleSeq) { it.getLong(0) } == 0L) { "excluded prepared intent has fabricated snapshot A" }
        for (table in state.validated.tables) {
            val desired = checkNotNull(one("SELECT count(*) FROM _sync_apply_rows WHERE table_name=? AND payload IS NOT NULL", table.tableName) { it.getLong(0) })
            val live = db.scalarLong("SELECT count(*) FROM ${quoteIdent(table.tableName)}")
            val a = checkNotNull(one("SELECT count(*) FROM _sync_row_state WHERE table_name=?", table.tableName) { it.getLong(0) })
            val staged = checkNotNull(one("SELECT count(*) FROM _sync_snapshot_stage WHERE snapshot_id=? AND table_name=?", session.snapshotId, table.tableName) { it.getLong(0) })
            val excluded = if (!preserve) 0 else checkNotNull(one("SELECT count(*) FROM _sync_outbox_rows o WHERE table_name=? AND NOT EXISTS(SELECT 1 FROM _sync_snapshot_stage s WHERE s.snapshot_id=? AND s.schema_name=o.schema_name AND s.table_name=o.table_name AND s.key_json=o.key_json)", table.tableName, session.snapshotId) { it.getLong(0) })
            check(live == desired && a == staged + excluded) { "snapshot final scope mismatch" }
        }
    }

    private fun keyJson(state: RuntimeState, table: String, pk: String): String =
        buildJsonObject { put(state.validated.keyByTable.getValue(table).single().lowercase(), pk) }.toString()

    private suspend fun <T> one(sql: String, vararg args: Any?, read: (SQLiteStatement) -> T): T? = db.withPreparedStatement(sql) { st ->
        args.forEachIndexed { i, value -> when (value) {
            is String -> st.bindText(i + 1, value)
            is Long -> st.bindLong(i + 1, value)
            is ByteArray -> st.bindBlob(i + 1, value)
            else -> error("invalid lifecycle bind")
        } }
        if (st.step()) read(st) else null
    }
}
