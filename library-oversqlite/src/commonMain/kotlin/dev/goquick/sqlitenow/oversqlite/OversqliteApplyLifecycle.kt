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
    private val statementCache: StatementCache? = null,
) {
    private val workspace = OversqliteWorkspace(db, config)

    suspend fun prepareSnapshot(state: RuntimeState, session: SnapshotSession) {
        requireCleanSnapshotWork(db)
        val installation = OversqliteSnapshotInstallation(db, config, local, survivors, json)
        val absent = installation.absentQuery(state, session.snapshotId)
        val count = db.scalarLong("SELECT count(*) FROM ($absent)")
        val unit = snapshotUnit(session)
        val roots = workspace.add(unit, WorkspaceCharge(session.rowCount, 0), WorkspaceCharge(count, 0)).entries
        if (roots > Int.MAX_VALUE) throw SyncWorkspaceLimitException(unit, "entries", config.maxSyncWorkspaceEntries, 0, roots, false)
        check(one("SELECT count(*) FROM _sync_snapshot_stage WHERE snapshot_id=?", session.snapshotId) { it.getLong(0) } == session.rowCount)
        survivors.prepare(state, unit, roots.toInt(), "snapshot", session.snapshotBundleSeq,
            sourceSnapshotId = session.snapshotId, readOnlySnapshotSeed = true,
            workspaceStatementCache = statementCache, replayStatementCache = statementCache) { index ->
            if (index.toLong() < session.rowCount) withSnapshotRow(state, session.snapshotId, index.toLong() + 1, unit, statementCache, null) { it.referenceRow() }
            else {
                val previous = if (index.toLong() == session.rowCount) "" to "" else checkNotNull(one(
                    "SELECT table_name,local_pk FROM _sync_apply_rows ORDER BY ordinal DESC LIMIT 1") { it.getText(0) to it.getText(1) })
                checkNotNull(one("SELECT table_name,local_pk FROM ($absent) WHERE table_name>? OR (table_name=? AND local_pk>?) ORDER BY table_name,local_pk LIMIT 1",
                    previous.first, previous.first, previous.second) { ApplySurvivor(it.getText(0), it.getText(1), null, role = "intended") })
            }
        }
    }

    suspend fun canResumeSnapshot(state: RuntimeState, session: SnapshotSession): Boolean =
        survivors.hasMatchingSnapshotPlan(state, snapshotUnit(session), session.snapshotId)

    suspend fun snapshotStageGuard(state: RuntimeState, session: SnapshotSession): String =
        OversqliteSnapshotInstallation(db, config, local, survivors, json).context(state, snapshotUnit(session))

    private suspend fun snapshotUnit(session: SnapshotSession): String {
        val operation = OversqliteOperationStateStore(db).loadState()
        val identity = buildJsonArray {
            add(session.snapshotId); add(session.rowCount); add(session.logicalByteCount); add(session.snapshotHash)
            add(operation.kind); add(operation.targetUserId); add(operation.reason); add(operation.replacementSourceId)
        }
        return applyUnit("snapshot", session.snapshotBundleSeq, identity.toString())
    }

    internal suspend fun readSnapshot(state: RuntimeState, id: String, ordinal: Long, unit: String, cache: StatementCache? = statementCache): Triple<Long, ApplySurvivor, Long> =
        withSnapshotRow(state, id, ordinal, unit, cache, null) { Triple(it.version, it.legacyRow(), it.charge) }

    internal suspend fun <T> withSnapshotRow(state: RuntimeState, id: String, ordinal: Long, unit: String,
        cache: StatementCache?, rowId: Long?, read: SnapshotReadWorkspace? = null, accounting: SnapshotApplyAccounting? = null, block: suspend (SnapshotRowLease) -> T): T {
        currentCoroutineContext().ensureActive()
        val size = checkNotNull(syncDiagnosticSql("snapshot.stage_charge") { one("SELECT charge FROM _sync_snapshot_stage WHERE snapshot_id=? AND row_ordinal=?", id, ordinal, cache = cache) { it.getLong(0) } })
        val required = WorkspaceCharge(8, workspace.scale(unit, size, 8))
        val combineAdmission = cache != null && db.inTransaction()
        val admitted = workspace.readAndCheck(unit, required, statementCache = cache, read = read, accounting = accounting)
        val row = checkNotNull(syncDiagnosticSql("snapshot.stage_payload") { one("SELECT table_name,key_json,payload,row_version FROM _sync_snapshot_stage WHERE snapshot_id=? AND row_ordinal=?", id, ordinal, cache = cache) {
            Triple(it.getText(0), it.getText(1), it.getText(2)) to it.getLong(3)
        } })
        val (table, key, payload) = row.first
        require(row.second > 0) { "invalid snapshot version" }
        val (pk, wireKey) = local.decodeDirtyKeyForPush(state, table, key)
        snapshotRowWork("row_processing.wire_decode")
        val parsed = json.parseToJsonElement(payload) as JsonObject
        snapshotRowWork("row_processing.wire_normalize")
        val info = state.validated.tableInfoByName.getValue(table)
        val extra = SnapshotTypedValues.charge(workspace, unit, info, size)
        // Only immutable payload/key reads occurred since the fresh admission, in this same writer.
        val retain = currentCoroutineContext()[OversqliteWorkSlice] != null &&
            if (combineAdmission) workspace.admitOptionalFromFreshRow(unit, admitted, required, extra)
            else try { workspace.check(unit, extra, statementCache = cache, read = read); true }
            catch (capacity: SyncWorkspaceLimitException) { if (capacity.suppressedExceptions.isNotEmpty()) throw capacity; false }
        val values = if (retain) mutableListOf<TypedValue>() else null
        val normalized = local.validateAndNormalizeSnapshotPayload(state, table, wireKey, parsed, values)
        val span = dev.goquick.sqlitenow.core.SqliteNowDiagnosticContext.current(currentCoroutineContext())?.span
        val typed = values?.let { SnapshotTypedValues(db, info, PayloadSource.AUTHORITATIVE_WIRE, it, normalized, span) }
        return SnapshotRowLease.use(db, ApplySurvivor(table, pk, null, wire = true, role = "snapshot_ref", stageOrdinal = ordinal, rowId = rowId),
            row.second, size, unit, cache, workspace, json, normalized, typed, if(retain) extra else WorkspaceCharge(), read, block)

    }

    suspend fun atomicWork(state: RuntimeState, kind: String, unit: String, entries: Long, base: Long): Long {
        var work = base
        fun add(count: Long, weight: Long) {
            require(count >= 0 && weight > 0)
            work = if (count > (Long.MAX_VALUE - work) / weight) Long.MAX_VALUE else work + count * weight
        }
        var maxFKs = 0L
        for (table in state.validated.tables) {
            maxFKs = maxOf(maxFKs, state.validated.tableInfoByName.getValue(table.tableName).foreignKeys.size.toLong())
        }
        add(entries, 16 * (maxFKs + state.validated.tables.size))
        return work
    }

    suspend fun validateDependencies(state: RuntimeState, kind: String, guard: String, entries: Long, inputSequence: Long?) {
        if (kind != "snapshot") return validateDependenciesRead(state,kind,guard,entries,inputSequence,null)
        SnapshotReadWorkspace.use(workspace,guard,statementCache) { read -> validateDependenciesRead(state,kind,guard,entries,inputSequence,read) }
    }

    private suspend fun validateDependenciesRead(state: RuntimeState, kind: String, guard: String, entries: Long, inputSequence: Long?, read: SnapshotReadWorkspace?) {
        var blocker: FKReconciliationRequiredException? = null
        survivors.withPlannedRows(state, statementCache, read) { reader ->
        for (ordinal in 1..entries) {
            currentCoroutineContext().ensureActive()
            reader.withRow(ordinal) { lease ->
                val size = lease.charge
                workspace.check(guard, workspace.add(guard, WorkspaceCharge(if(blocker==null) 8 else 16, if(blocker==null) 0 else 16*4096), WorkspaceCharge(bytes=workspace.scale(guard,size,8))), statementCache = statementCache, read = read)
                try { validateRowDependencies(state, kind, guard, lease, inputSequence) }
                catch (pending: FKReconciliationRequiredException) {
                    val first = blocker
                    if(first==null) workspace.check(guard, WorkspaceCharge(8,16*4096), statementCache = statementCache, read = read)
                    blocker = if(first == null) pending else FKReconciliationRequiredException(first.detail, first.unit, first.sample, true)
                }
            }
        }
        }
        currentCoroutineContext().ensureActive()
        blocker?.let { throw it }
    }

    private suspend fun validateRowDependencies(state: RuntimeState, kind: String, guard: String, lease: SnapshotRowLease, inputSequence: Long?) {
            val row = lease.row
            if (!lease.hasPayload) {
                if (kind != "snapshot") for ((table, info) in state.validated.tableInfoByName) {
                    if (info.foreignKeys.none { it.refTable.equals(row.table, ignoreCase = true) }) continue
                    if (one("SELECT 1 FROM _sync_row_state WHERE relationships_known=0 AND table_name=? LIMIT 1", table) { true } == true)
                        pending(state, guard, row, "relationship_unknown")
                }
                return
            }
            val typed = lease.takeTypedValues()
            val payload = checkNotNull(lease.takePayloadObject())
            val info = state.validated.tableInfoByName.getValue(row.table)
            val source = if (row.wire) PayloadSource.AUTHORITATIVE_WIRE else PayloadSource.LOCAL_STATE
            for (fk in info.foreignKeys) {
                val column = info.column(fk.fromCol)
                val value = typed?.value(fk.fromCol) ?: OversqliteValueCodec.decodePayloadValue(column, payload.getValue(fk.fromCol.lowercase()), source)
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
        val selected = one("SELECT role='snapshot_ref' OR payload IS NOT NULL FROM _sync_apply_rows WHERE table_name=? AND local_pk=?", table, pk) { it.getLong(0) != 0L }
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
        workspace.check(guard, WorkspaceCharge(4, workspace.scale(guard, size, 4)), statementCache = statementCache)
        throw FKReconciliationRequiredException(detail, guard, if (size + 2 <= 4096) listOf(ref) else emptyList(), size + 2 > 4096)
    }

    suspend fun restoreAndVerify(state: RuntimeState, cache: StatementCache) : Set<String> {
        val updated = linkedSetOf<String>()
        syncDiagnosticWork("snapshot.final.restoration") {
            for (pass in 0 until 64) {
                snapshotRowWork("snapshot.final.restoration_pass")
                val changed = survivors.restore(state, cache, workspaceStatementCache = cache, replayStatementCache = cache) +
                    survivors.restorePrimaryIntent(state, cache, workspaceStatementCache = cache, replayStatementCache = cache)
                updated += changed
                if (changed.isEmpty()) break
            }
        }
        syncDiagnosticWork("snapshot.final.live_verification") {
            survivors.restore(state, cache, verifyOnly = true, workspaceStatementCache = cache, replayStatementCache = cache)
            survivors.restorePrimaryIntent(state, cache, verifyOnly = true, workspaceStatementCache = cache, replayStatementCache = cache)
        }
        return updated
    }

    suspend fun verifySnapshot(state: RuntimeState, session: SnapshotSession, cache: StatementCache) {
        val unit = applyUnit("snapshot-final", session.snapshotBundleSeq, session.snapshotId)
        SnapshotReadWorkspace.use(workspace,unit,cache) { read -> verifySnapshotRead(state,session,cache,unit,read) }
    }

    private suspend fun verifySnapshotRead(state: RuntimeState, session: SnapshotSession, cache: StatementCache, unit: String, read: SnapshotReadWorkspace?) {
        syncDiagnosticWork("snapshot.final.authoritative_verification") {
            for (ordinal in 1..session.rowCount) {
                withSnapshotRow(state, session.snapshotId, ordinal, unit, cache, null, read) { lease ->
                    val row = lease.row
                    val key = keyJson(state, row.table, row.pk)
                    val schema = state.validated.schemaForTable(row.table)
                    val typed = lease.takeTypedValues()
                    val payload = checkNotNull(lease.takePayloadObject())
                    val info = state.validated.tableInfoByName.getValue(row.table)
                    snapshotRowWork("row_processing.authoritative_read")
                    val observed = checkNotNull(syncDiagnosticSql("snapshot.authoritative_projection") { one("SELECT r.row_version,r.server_kind,r.server_payload,r.relationships_known,r.server_revision,c.context_id,json_extract(r.server_provenance,'$.origin')='snapshot' AND r.row_version=? AND json_extract(r.server_provenance,'$.bundle_sequence')=? FROM _sync_row_state r LEFT JOIN _sync_provenance_contexts c ON c.context_id=r.server_context_id WHERE r.schema_name=? AND r.table_name=? AND r.key_json=?",
                        lease.version, session.snapshotBundleSeq, schema, row.table, key) {
                        AuthoritativeRowProjection(it.getLong(0), it.getText(1), if(it.isNull(2)) null else it.getText(2),
                            it.getLong(3)==1L, it.getLong(4), !it.isNull(5), !it.isNull(6) && it.getLong(6)!=0L)
                    } })
                    // Charge both optional holders before constructing either one. The expected
                    // holder is bounded by the freshly validated wire row, not by emitted JSON.
                    val reuse = observed.payload != null && lease.tryRetain(workspace.add(unit,
                        SnapshotTypedValues.charge(workspace, unit, info, lease.charge),
                        SnapshotTypedValues.charge(workspace, unit, info, utf8Size(observed.payload))))
                    var decoded: SnapshotTypedValues? = null
                    var expectedTyped: SnapshotTypedValues? = null
                    try {
                        decoded = local.validateStoredServerProjection(schema, row.table, key, observed, reuse, cache)
                        check(observed.provenanceMatches) { "snapshot A provenance differs from S" }
                        check(observed.version == lease.version && observed.kind == "live" && observed.payload != null) { "snapshot A differs from staged S" }
                        val same = if (reuse) {
                            snapshotRowWork("snapshot.final.typed_projection")
                            expectedTyped = local.captureAuthoritativeTypedProjection(row.table, payload, cache, typed)
                            checkNotNull(decoded).matches(checkNotNull(expectedTyped))
                        } else {
                            snapshotRowWork("snapshot.final.typed_projection_fallback")
                            val expected = local.captureAuthoritativePayload(row.table, payload, cache, typed)
                            OversqliteValueCodec.equivalentPayloadTexts(info, expected, observed.payload,
                                PayloadSource.LOCAL_STATE, PayloadSource.LOCAL_STATE, json)
                        }
                        check(same) { "snapshot A differs from staged S" }
                    } finally { decoded?.close(); expectedTyped?.close() }

                }
            }
        }
        syncDiagnosticWork("snapshot.final.scope_verification") {
            for (table in state.validated.tables) {
                val desired = checkNotNull(one("SELECT count(*) FROM _sync_apply_rows WHERE table_name=? AND (role='snapshot_ref' OR payload IS NOT NULL)", table.tableName) { it.getLong(0) })
                val live = db.scalarLong("SELECT count(*) FROM ${quoteIdent(table.tableName)}")
                val a = checkNotNull(one("SELECT count(*) FROM _sync_row_state WHERE table_name=? AND server_kind='live'", table.tableName) { it.getLong(0) })
                val staged = checkNotNull(one("SELECT count(*) FROM _sync_snapshot_stage WHERE snapshot_id=? AND table_name=?", session.snapshotId, table.tableName) { it.getLong(0) })
                syncDiagnosticEvent("snapshot.final_scope_check", if (live == desired && a == staged) "passed" else "failed", {
                    mapOf("expected_live_rows" to desired, "observed_live_rows" to live,
                        "expected_baseline_rows" to staged, "observed_baseline_rows" to a)
                }, "checked_uncommitted", state.validated.schemaForTable(table.tableName), table.tableName)
                check(live == desired && a == staged) { "snapshot final scope mismatch" }
            }
        }
    }

    private fun keyJson(state: RuntimeState, table: String, pk: String): String =
        buildJsonObject { put(state.validated.keyByTable.getValue(table).single().lowercase(), pk) }.toString()

    private suspend fun <T> one(sql: String, vararg args: Any?, cache: StatementCache? = statementCache, read: (SQLiteStatement) -> T): T? = db.withPreparedStatement(sql, cache) { st ->
        args.forEachIndexed { i, value -> when (value) {
            is String -> st.bindText(i + 1, value)
            is Long -> st.bindLong(i + 1, value)
            is ByteArray -> st.bindBlob(i + 1, value)
            else -> error("invalid lifecycle bind")
        } }
        if (st.step()) read(st) else null
    }
}
