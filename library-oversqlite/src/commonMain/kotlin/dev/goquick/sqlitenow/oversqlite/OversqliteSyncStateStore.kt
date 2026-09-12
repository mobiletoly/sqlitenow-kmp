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

import androidx.sqlite.SQLiteStatement
import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.serialization.json.Json

internal class OversqliteSyncStateStore(
    internal val db: SafeSQLiteConnection,
    private val localStore: () -> OversqliteLocalStore,
) {
    private val json = Json
    suspend fun forEachDirtyIntent(
        statementCache: StatementCache,
        visit: suspend (schemaName: String, tableName: String, keyJson: String, payload: String?) -> Unit,
    ) {
        db.withPreparedStatement(
            sql = """
                SELECT schema_name, table_name, key_json, op, payload
                FROM _sync_dirty_rows
                ORDER BY dirty_ordinal, schema_name, table_name, key_json
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            while (st.step()) {
                visit(
                    st.getText(0), st.getText(1), st.getText(2),
                    if (st.getText(3) == "DELETE" || st.isNull(4)) null
                    else st.getText(4),
                )
            }
        }
    }

    suspend fun loadStructuredRowState(
        schemaName: String,
        tableName: String,
        keyJson: String,
        statementCache: StatementCache? = null,
    ): StructuredRowState {
        return loadBySchemaTableKey(
            sql = """
                SELECT row_version, server_kind='deleted'
                FROM _sync_row_state
                WHERE schema_name = ? AND table_name = ? AND key_json = ?
            """.trimIndent(),
            schemaName = schemaName,
            tableName = tableName,
            keyJson = keyJson,
            statementCache = statementCache,
            missing = ::StructuredRowState,
        ) { st ->
            StructuredRowState(
                exists = true,
                rowVersion = st.getLong(0),
                deleted = st.getLong(1) == 1L,
            )
        }
    }

    suspend fun deleteDirtyRow(
        schemaName: String,
        tableName: String,
        keyJson: String,
        statementCache: StatementCache? = null,
    ) {
        deleteBySchemaTableKey(
            sql = "DELETE FROM _sync_dirty_rows WHERE schema_name = ? AND table_name = ? AND key_json = ?",
            schemaName = schemaName,
            tableName = tableName,
            keyJson = keyJson,
            statementCache = statementCache,
        )
    }

    suspend fun deleteStructuredRowState(
        schemaName: String,
        tableName: String,
        keyJson: String,
        statementCache: StatementCache? = null,
    ) {
        installAuthoritativeBaseline(schemaName,tableName,keyJson,"absent",0,null,"explicit_conflict",null,statementCache)
    }

    private suspend fun deleteBySchemaTableKey(
        sql: String,
        schemaName: String,
        tableName: String,
        keyJson: String,
        statementCache: StatementCache?,
    ) {
        db.withPreparedStatement(
            sql = sql,
            statementCache = statementCache,
        ) { st ->
            st.bindText(1, schemaName)
            st.bindText(2, tableName)
            st.bindText(3, keyJson)
            st.step()
        }
    }

    private suspend fun <T> loadBySchemaTableKey(
        sql: String,
        schemaName: String,
        tableName: String,
        keyJson: String,
        statementCache: StatementCache?,
        missing: () -> T,
        found: (SQLiteStatement) -> T,
    ): T {
        return db.withPreparedStatement(
            sql = sql,
            statementCache = statementCache,
        ) { st ->
            st.bindText(1, schemaName)
            st.bindText(2, tableName)
            st.bindText(3, keyJson)
            if (!st.step()) missing() else found(st)
        }
    }

    suspend fun clearStructuredRowState(
        schemaName: String,
        tableName: String,
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = "DELETE FROM _sync_row_state WHERE schema_name = ? AND table_name = ?",
            statementCache = statementCache,
        ) { st ->
            st.bindText(1, schemaName)
            st.bindText(2, tableName)
            st.step()
        }
    }

    suspend fun countLiveStructuredRows(): Long {
        return db.withExclusiveAccess {
            db.prepare(
                """
                SELECT COUNT(*)
                FROM _sync_row_state
                WHERE server_kind='live'
                """.trimIndent(),
            ).use { st ->
                check(st.step())
                st.getLong(0)
            }
        }
    }

    suspend fun countDirtyRows(): Long {
        return db.scalarLong("SELECT COUNT(*) FROM _sync_dirty_rows")
    }

    suspend fun loadDirtyUploadState(
        schemaName: String,
        tableName: String,
        keyJson: String,
        statementCache: StatementCache? = null,
    ): DirtyUploadState {
        val current = loadBySchemaTableKey(
            sql = """
                SELECT op, payload, base_row_version, dirty_ordinal
                FROM _sync_dirty_rows
                WHERE schema_name = ? AND table_name = ? AND key_json = ?
            """.trimIndent(),
            schemaName = schemaName,
            tableName = tableName,
            keyJson = keyJson,
            statementCache = statementCache,
            missing = ::DirtyUploadState,
        ) { st ->
            DirtyUploadState(
                exists = true,
                op = st.getText(0),
                payload = if (st.isNull(1)) null else st.getText(1),
                baseRowVersion = st.getLong(2),
                currentOrdinal = st.getLong(3),
            )
        }
        if (current.exists) validateStoredDesiredProjection(db, schemaName, tableName, keyJson)
        return current
    }

    suspend fun requeueDirtyIntent(
        schemaName: String,
        tableName: String,
        keyJson: String,
        op: String,
        baseRowVersion: Long,
        payload: String?,
        base: HistoricalBase,
        statementCache: StatementCache? = null,
    ) {
        localStore().validateHistoricalBase(tableName,baseRowVersion,base)
        db.withPreparedStatement(
            sql = """
                INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, base_kind, base_payload, base_provenance, dirty_ordinal, updated_at)
                VALUES (
                  ?, ?, ?, ?, ?, ?, ?, ?, ?,
                  COALESCE(
                    (SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name = ? AND table_name = ? AND key_json = ?),
                    (SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
                  ),
                  strftime('%Y-%m-%dT%H:%M:%fZ','now')
                )
                ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
                  op = excluded.op,
                  base_row_version = excluded.base_row_version,
                  payload = excluded.payload,
                  base_kind=excluded.base_kind,base_payload=excluded.base_payload,base_provenance=excluded.base_provenance,
                  updated_at = excluded.updated_at
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            st.bindText(1, schemaName)
            st.bindText(2, tableName)
            st.bindText(3, keyJson)
            st.bindText(4, op)
            st.bindLong(5, baseRowVersion)
            if (payload == null) st.bindNull(6) else st.bindText(6, payload)
            st.bindText(7,base.kind)
            if(base.payload==null) st.bindNull(8) else st.bindText(8,base.payload)
            st.bindText(9,base.provenance)
            st.bindText(10, schemaName)
            st.bindText(11, tableName)
            st.bindText(12, keyJson)
            st.step()
        }
    }

    suspend fun requeueSnapshotRows(
        snapshot: PushOutboundSnapshot,
        skipRow: DirtyRowCapture? = null,
        statementCache: StatementCache? = null,
    ) {
        for (row in snapshot.rows) {
            if (row == skipRow) continue
            // Rejected snapshots precede any dirty row captured during their upload.
            val current = loadDirtyUploadState(row.schemaName, row.tableName, row.keyJson, statementCache)
            requeueDirtyIntent(
                schemaName = row.schemaName,
                tableName = row.tableName,
                keyJson = row.keyJson,
                op = if(current.exists) current.op else row.op,
                baseRowVersion = row.baseRowVersion,
                payload = if(current.exists) current.payload else row.localPayload,
                base = row.base,
                statementCache = statementCache,
            )
        }
    }

    suspend fun rebaseDirtyIntent(
        schemaName: String, tableName: String, keyJson: String, op: String,
        baseRowVersion: Long, payload: String?, statementCache: StatementCache? = null,
    ) {
        val base = loadBySchemaTableKey(
            "SELECT row_version, CASE server_kind WHEN 'live' THEN 'present' WHEN 'deleted' THEN 'deleted' WHEN 'unknown' THEN 'unavailable' ELSE 'absent' END,server_payload,server_provenance FROM _sync_row_state WHERE schema_name=? AND table_name=? AND key_json=?",
            schemaName,tableName,keyJson,statementCache, { error("required authoritative baseline is missing") },
        ) { st ->
            check(st.getLong(0)==baseRowVersion) { "historical base version mismatch" }
            HistoricalBase(st.getText(1),if(st.isNull(2)) null else st.getText(2),st.getText(3))
        }
        requeueDirtyIntent(schemaName,tableName,keyJson,op,baseRowVersion,payload,base,statementCache)
    }

    suspend fun installAuthoritativeBaseline(
        schemaName: String, tableName: String, keyJson: String, kind: String,
        rowVersion: Long, payload: String?, origin: String, bundleSeq: Long?,
        statementCache: StatementCache? = null,
    ) {
        localStore().validateStoredServerProjection(schemaName, tableName, keyJson)
        if(rowVersion>0) {
            val old = loadBySchemaTableKey("SELECT row_version,server_kind,server_payload FROM _sync_row_state WHERE schema_name=? AND table_name=? AND key_json=?",schemaName,tableName,keyJson,statementCache,{null}) { st ->
                Triple(st.getLong(0),st.getText(1),if(st.isNull(2)) null else st.getText(2))
            }
            if(old!=null && old.first>=rowVersion) {
                check(old.first==rowVersion) { "authoritative observation predates retained baseline" }
                check(old.second==kind && old.third?.let(json::parseToJsonElement)==payload?.let(json::parseToJsonElement)) { "inconsistent authoritative payload at equal version" }
                return
            }
        }
        db.withPreparedStatement(authoritativeBaselineUpsertSql,statementCache) { st ->
            st.bindText(1,schemaName); st.bindText(2,tableName); st.bindText(3,keyJson)
            st.bindLong(4,rowVersion); st.bindText(5,kind)
            if(payload==null) st.bindNull(6) else st.bindText(6,payload)
            st.bindText(7, origin)
            if (bundleSeq == null) st.bindNull(8) else st.bindLong(8, bundleSeq)
            st.bindLong(9, rowVersion)
            st.step()
        }
    }

    suspend fun clearDirtyRows() {
        db.execSQL("DELETE FROM _sync_dirty_rows")
    }
}

internal val authoritativeBaselineUpsertSql = """
 INSERT INTO _sync_row_state(schema_name,table_name,key_json,row_version,server_kind,server_payload,server_provenance,relationships_known)
 VALUES(?,?,?,?,?,?,${baselineProvenanceSql("?","?","?")},1)
 ON CONFLICT(schema_name,table_name,key_json) DO UPDATE SET row_version=excluded.row_version,
 server_kind=excluded.server_kind,server_payload=excluded.server_payload,server_provenance=excluded.server_provenance,
 relationships_known=excluded.relationships_known,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
""".trimIndent()
