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

import dev.goquick.sqlitenow.core.SafeSQLiteConnection

internal class OversqliteSyncStateStore(
    private val db: SafeSQLiteConnection,
) {
    suspend fun loadStructuredRowState(
        schemaName: String,
        tableName: String,
        keyJson: String,
        statementCache: StatementCache? = null,
    ): StructuredRowState {
        return db.withPreparedStatement(
            sql = """
                SELECT row_version, deleted
                FROM _sync_row_state
                WHERE schema_name = ? AND table_name = ? AND key_json = ?
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            st.bindText(1, schemaName)
            st.bindText(2, tableName)
            st.bindText(3, keyJson)
            if (!st.step()) {
                StructuredRowState()
            } else {
                StructuredRowState(
                    exists = true,
                    rowVersion = st.getLong(0),
                    deleted = st.getLong(1) == 1L,
                )
            }
        }
    }

    suspend fun deleteDirtyRow(
        schemaName: String,
        tableName: String,
        keyJson: String,
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = "DELETE FROM _sync_dirty_rows WHERE schema_name = ? AND table_name = ? AND key_json = ?",
            statementCache = statementCache,
        ) { st ->
            st.bindText(1, schemaName)
            st.bindText(2, tableName)
            st.bindText(3, keyJson)
            st.step()
        }
    }

    suspend fun deleteStructuredRowState(
        schemaName: String,
        tableName: String,
        keyJson: String,
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = "DELETE FROM _sync_row_state WHERE schema_name = ? AND table_name = ? AND key_json = ?",
            statementCache = statementCache,
        ) { st ->
            st.bindText(1, schemaName)
            st.bindText(2, tableName)
            st.bindText(3, keyJson)
            st.step()
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

    suspend fun loadDirtyUploadState(
        schemaName: String,
        tableName: String,
        keyJson: String,
        statementCache: StatementCache? = null,
    ): DirtyUploadState {
        return db.withPreparedStatement(
            sql = """
                SELECT op, payload, base_row_version, dirty_ordinal
                FROM _sync_dirty_rows
                WHERE schema_name = ? AND table_name = ? AND key_json = ?
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            st.bindText(1, schemaName)
            st.bindText(2, tableName)
            st.bindText(3, keyJson)
            if (!st.step()) {
                DirtyUploadState()
            } else {
                DirtyUploadState(
                    exists = true,
                    op = st.getText(0),
                    payload = if (st.isNull(1)) null else st.getText(1),
                    baseRowVersion = st.getLong(2),
                    currentOrdinal = st.getLong(3),
                )
            }
        }
    }

    suspend fun requeueDirtyIntent(
        schemaName: String,
        tableName: String,
        keyJson: String,
        op: String,
        baseRowVersion: Long,
        payload: String?,
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = """
                INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
                VALUES (
                  ?, ?, ?, ?, ?, ?,
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
            st.bindText(7, schemaName)
            st.bindText(8, tableName)
            st.bindText(9, keyJson)
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
            requeueDirtyIntent(
                schemaName = row.schemaName,
                tableName = row.tableName,
                keyJson = row.keyJson,
                op = row.op,
                baseRowVersion = row.baseRowVersion,
                payload = row.localPayload,
                statementCache = statementCache,
            )
        }
    }

    suspend fun updateStructuredRowState(
        schemaName: String,
        tableName: String,
        keyJson: String,
        rowVersion: Long,
        deleted: Boolean,
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = """
                INSERT INTO _sync_row_state(schema_name, table_name, key_json, row_version, deleted, updated_at)
                VALUES(?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
                ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
                  row_version = excluded.row_version,
                  deleted = excluded.deleted,
                  updated_at = excluded.updated_at
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            st.bindText(1, schemaName)
            st.bindText(2, tableName)
            st.bindText(3, keyJson)
            st.bindLong(4, rowVersion)
            st.bindLong(5, if (deleted) 1 else 0)
            st.step()
        }
    }

    suspend fun clearDirtyRows() {
        db.execSQL("DELETE FROM _sync_dirty_rows")
    }
}
