package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*

/** The existing operation owns the seal; staged pages remain its immutable input. */
@Serializable
internal data class RetainedSnapshotSeal(val session: SnapshotSession, val source: String, val authority: String)

internal class OversqliteSnapshotInstallation(
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
    private val local: OversqliteLocalStore,
    private val survivors: OversqliteApplySurvivors,
    private val json: Json,
) {
    private val workspace = OversqliteWorkspace(db, config)

    suspend fun floor(): Long {
        val count=db.scalarLong("SELECT count(*) FROM _sync_row_state")
        survivors.reserveSnapshotWork("snapshot-observation-floor",snapshotWorkAdd(32,snapshotWorkScale(count,3)))
        return snapshotObservationFloor(db)
    }

    suspend fun context(state: RuntimeState, unit: String, automatic: Boolean = true): String {
        val main = db.scalarLong("PRAGMA main.schema_version")
        val temporary = temporarySchemaIdentity(unit)
        workspace.check(unit, WorkspaceCharge(4, 512 + 12 * utf8Size(config.serviceId)))
        return sha256Hex(buildJsonArray {
            add(config.serviceId); add(workspace.contextGuard(state, unit)); add(main); add(temporary)
            if (automatic) currentCoroutineContext()[AutomaticRecoveryContext]?.pin?.let { add(it.id);add(it.modeRevision) }
        }.toString().encodeToByteArray())
    }

    // TEMP counters reset with the connection. Bind definitions, which give an
    // empty TEMP schema the same identity in every reopened lifetime.
    private suspend fun temporarySchemaIdentity(unit: String): String {
        val (entries, bytes) = db.withPreparedStatement("""SELECT count(*),COALESCE(sum(
            length(CAST(name AS BLOB))+length(CAST(type AS BLOB))+length(CAST(tbl_name AS BLOB))+
            length(CAST(COALESCE(sql,'') AS BLOB))),0) FROM sqlite_temp_schema""") { st ->
            check(st.step()); st.getLong(0) to st.getLong(1)
        }
        survivors.reserveSnapshotWork(unit, snapshotWorkAdd(snapshotWorkScale(entries,128),snapshotWorkScale(bytes,2)))
        workspace.check(unit,WorkspaceCharge(entries+4,snapshotWorkAdd(4096,snapshotWorkScale(bytes,16))))
        val hash = Sha256Accumulator()
        db.withPreparedStatement("SELECT name,type,tbl_name,COALESCE(sql,'') FROM sqlite_temp_schema ORDER BY name,type") { st ->
            while (st.step()) {
                val frame = buildJsonArray { for (index in 0..3) add(st.getText(index)) }
                hash.append(frame.toString().encodeToByteArray()); hash.appendAscii('\n')
            }
        }
        return hash.finishHex()
    }

    suspend fun encodeSeal(session: SnapshotSession, source: String): String {
        val unit = applyUnit("snapshot-seal", session.snapshotBundleSeq, session.snapshotId)
        val strings = utf8Size(session.snapshotId) + utf8Size(session.expiresAt) + utf8Size(source) + utf8Size(config.serviceId)
        workspace.check(unit, WorkspaceCharge(8, 4096 + workspace.scale(unit, strings, 16)))
        return json.encodeToString(RetainedSnapshotSeal.serializer(), RetainedSnapshotSeal(session, source, config.serviceId))
    }

    fun retained(operation: OversqliteOperationState, source: String): SnapshotSession? {
        require(operation.snapshotEngine == generalSnapshotEngine) { "typed snapshot requires its pinned bootstrap lifecycle" }
        if (operation.stagedSnapshotId.isEmpty()) return null
        val seal = try {json.decodeFromString(RetainedSnapshotSeal.serializer(), operation.snapshotTransfer)} catch(failure: IllegalArgumentException){throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_SESSION)}
        validateSnapshotSession(seal.session)
        val s = seal.session
        check(s.snapshotId == operation.stagedSnapshotId && s.snapshotBundleSeq == operation.snapshotBundleSeq &&
            s.rowCount == operation.snapshotRowCount && s.logicalByteCount == operation.snapshotByteCount) {
            "retained snapshot seal differs from operation progress"
        }
        return s.takeIf { seal.source == source && seal.authority == config.serviceId }
    }

    suspend fun verify(state: RuntimeState, session: SnapshotSession, source: String, cache: StatementCache) {
        validateSnapshotSession(session)
        val unit = applyUnit("snapshot-hash", session.snapshotBundleSeq, session.snapshotId)
        val count = db.withPreparedStatement("SELECT count(*),COALESCE(sum(charge),0) FROM _sync_snapshot_stage WHERE snapshot_id=?", cache) { st ->
            st.bindText(1, session.snapshotId); check(st.step()); st.getLong(0) to st.getLong(1)
        }
        if(count.first!=session.rowCount)throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK)
        survivors.reserveSnapshotWork(unit, snapshotWorkAdd(snapshotWorkScale(count.first, 256), snapshotWorkScale(count.second, 2)))
        SnapshotReadWorkspace.use(workspace,unit,cache) { read -> verifyRows(state,session,source,cache,unit,read) }
    }

    private suspend fun verifyRows(state: RuntimeState, session: SnapshotSession, source: String, cache: StatementCache, unit: String, read: SnapshotReadWorkspace?) {
        val generation = state.admittedContract.generation
        val hash = SnapshotHashWriter(source, generation, session.snapshotId, session.snapshotBundleSeq, session.recovery,
            dev.goquick.sqlitenow.core.SqliteNowDiagnosticContext.current(currentCoroutineContext())?.span)
        for (ordinal in 1..session.rowCount) {
            currentCoroutineContext().ensureActive()
            val size = syncDiagnosticSql("snapshot.hash_stage_charge") { db.withPreparedStatement("SELECT charge,length(CAST(schema_name AS BLOB))+length(CAST(table_name AS BLOB))+length(CAST(key_json AS BLOB))+length(CAST(payload AS BLOB)) FROM _sync_snapshot_stage WHERE snapshot_id=? AND row_ordinal=?", cache) { st ->
                st.bindText(1, session.snapshotId); st.bindLong(2, ordinal); if(!st.step())throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK);if(st.getLong(1)>st.getLong(0))throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK); st.getLong(0)
            } }
            workspace.check(unit, WorkspaceCharge(16, 4096 + workspace.scale(unit, size, 16)), statementCache = cache, read = read)
            syncDiagnosticSql("snapshot.hash_stage_payload") { db.withPreparedStatement("SELECT schema_name,table_name,key_json,row_version,payload FROM _sync_snapshot_stage WHERE snapshot_id=? AND row_ordinal=?", cache) { st ->
                st.bindText(1, session.snapshotId); st.bindLong(2, ordinal); check(st.step())
                try {
                    val schema = st.getText(0); val table = st.getText(1); val key = st.getText(2)
                val version = st.getLong(3); val payload = json.parseToJsonElement(st.getText(4)).jsonObject
                val index = generation.tables.indexOfFirst { it.schema == schema && it.table == table }
                check(index >= 0 && version <= session.snapshotBundleSeq) { "snapshot row outside sealed context" }
                val wireKey = local.decodeDirtyKeyForPush(state, table, key).second
                hash.add(index, JsonArray(generation.tables[index].syncKeyColumns.map { JsonPrimitive(wireKey.getValue(it)) }), version, payload)
                } catch(error:CancellationException){throw error}
                  catch(error:IllegalArgumentException){throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_ROW)}
                  catch(error:IllegalStateException){throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_ROW)}
            } }
        }
        val actual=try{hash.finish(session.rowCount,session.logicalByteCount)}catch(error:IllegalArgumentException){throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK)}catch(error:IllegalStateException){throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK)}
        if(actual!=session.snapshotHash)throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK)
    }

    /** Indexed candidate plus exact decoded identity, never JSON substring matching. */
    fun absentQuery(state: RuntimeState, id: String): String = state.validated.tables.joinToString(" UNION ALL ") { table ->
        val name = table.tableName
        val info = state.validated.tableInfoByName.getValue(name)
        val pk = checkNotNull(info.primaryKey).name
        val key = if (info.primaryKeyIsBlob) "lower(hex(b.${quoteIdent(pk)}))" else "b.${quoteIdent(pk)}"
        "SELECT ${snapshotSqlText(name)} AS table_name,$key AS local_pk FROM ${quoteIdent(name)} b WHERE NOT EXISTS(" +
            "SELECT 1 FROM _sync_snapshot_stage s WHERE s.snapshot_id=${snapshotSqlText(id)} AND s.table_name=${snapshotSqlText(name)} " +
            "AND s.schema_name=${snapshotSqlText(state.validated.schemaForTable(name))} AND s.key_json=json_object(${snapshotSqlText(pk.lowercase())},$key) " +
            "AND json_extract(s.key_json,${snapshotSqlText("$.\"${pk.lowercase()}\"")}) COLLATE BINARY=$key COLLATE BINARY)"
    }.ifEmpty { "SELECT '' AS table_name,'' AS local_pk WHERE 0" }

    suspend fun installAbsence(state: RuntimeState, session: SnapshotSession, cache: StatementCache, targetContext: Long) {
        db.withPreparedStatement("""UPDATE _sync_row_state AS a SET row_version=0,server_kind='absent',server_payload=NULL,
            relationships_known=1,server_provenance=${baselineProvenanceSql("'snapshot'", "?", targetContext.toString())}
            WHERE NOT EXISTS(SELECT 1 FROM _sync_snapshot_stage s WHERE s.snapshot_id=? AND
              (s.schema_name,s.table_name,s.key_json)=(a.schema_name,a.table_name,a.key_json))""", cache) { st ->
            st.bindLong(1, session.snapshotBundleSeq); st.bindText(2, session.snapshotId); st.step()
        }
        val baselines = OversqliteSyncStateStore(db) { local }
        var after = 0L
        while (true) {
            val row = db.withPreparedStatement("""SELECT ordinal,table_name,local_pk FROM _sync_apply_rows
                WHERE ordinal>? AND payload IS NULL AND role<>'snapshot_ref' ORDER BY ordinal LIMIT 1""",cache) { st ->
                st.bindLong(1,after)
                if (st.step()) Triple(st.getLong(0),st.getText(1),st.getText(2)) else null
            } ?: break
            val column = state.validated.keyByTable.getValue(row.second).single()
            val key = db.withPreparedStatement("SELECT json_object(?,?)",cache) { st ->
                st.bindText(1,column.lowercase()); st.bindText(2,row.third); check(st.step()); st.getText(0)
            }
            baselines.installAuthoritativeBaseline(state.validated.schemaForTable(row.second),row.second,key,
                "absent",0,null,"snapshot",session.snapshotBundleSeq,cache,cache,targetContext)
            after = row.first
        }
    }
}

internal suspend fun snapshotObservationFloor(db: SafeSQLiteConnection): Long = db.scalarLong("""SELECT max(v) FROM (
    SELECT last_bundle_seq_seen v FROM _sync_attachment_state
    UNION ALL SELECT row_version FROM _sync_row_state
    UNION ALL SELECT COALESCE(json_extract(server_provenance,'$.bundle_sequence'),0) FROM _sync_row_state
    UNION ALL SELECT COALESCE(json_extract(NULLIF(o.recovery_state,''),'$.status.observedBoundary'),0)
    FROM _sync_operation_state o CROSS JOIN _sync_attachment_state a CROSS JOIN _sync_contract_identity i
    WHERE json_extract(NULLIF(o.recovery_state,''),'$.sourceId')=a.current_source_id
      AND json_extract(NULLIF(o.recovery_state,''),'$.generationFingerprint')=i.fingerprint
      AND json_extract(NULLIF(o.recovery_state,''),'$.serviceId')=i.service_id)""")

internal fun snapshotSqlText(value: String): String = "'" + value.replace("'", "''") + "'"
internal fun snapshotWorkAdd(a: Long, b: Long): Long = if (b > Long.MAX_VALUE - a) Long.MAX_VALUE else a + b
internal fun snapshotWorkScale(n: Long, weight: Long): Long = if (n > Long.MAX_VALUE / weight) Long.MAX_VALUE else n * weight
