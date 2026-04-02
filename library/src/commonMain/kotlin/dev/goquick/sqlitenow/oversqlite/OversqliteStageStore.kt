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
import dev.goquick.sqlitenow.core.TransactionMode
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject

internal class OversqliteStageStore(
    internal val db: SafeSQLiteConnection,
    private val localStore: OversqliteLocalStore,
    private val syncStateStore: OversqliteSyncStateStore,
    private val json: Json,
) {
    suspend fun preparePush(
        state: RuntimeState,
        sourceBundleId: Long,
    ): PreparedPush {
        val dirtyRows = loadDirtySnapshotRows()
        val preparedRows = mutableListOf<DirtyRowCapture>()
        val discardedRows = mutableListOf<DirtyRowRef>()
        val tableOrder = state.validated.tableOrder

        for (snapshot in dirtyRows) {
            val (localPk, wireKey) = localStore.decodeDirtyKeyForPush(state, snapshot.tableName, snapshot.keyJson)
            if (snapshot.payload == null && (!snapshot.stateExists || snapshot.stateDeleted)) {
                discardedRows += DirtyRowRef(snapshot.schemaName, snapshot.tableName, snapshot.keyJson)
                continue
            }

            val op = when {
                snapshot.payload == null -> "DELETE"
                snapshot.stateExists && !snapshot.stateDeleted -> "UPDATE"
                else -> "INSERT"
            }
            preparedRows += DirtyRowCapture(
                schemaName = snapshot.schemaName,
                tableName = snapshot.tableName,
                keyJson = snapshot.keyJson,
                localPk = localPk,
                wireKey = wireKey,
                op = op,
                baseRowVersion = snapshot.baseRowVersion,
                localPayload = snapshot.payload,
                dirtyOrdinal = snapshot.dirtyOrdinal,
            )
        }

        preparedRows.sortWith(
            compareBy<DirtyRowCapture> { if (it.op == "DELETE") 1 else 0 }
                .thenComparator { left, right ->
                    val leftOrder = tableOrder[left.tableName] ?: Int.MAX_VALUE
                    val rightOrder = tableOrder[right.tableName] ?: Int.MAX_VALUE
                    when {
                        left.op == "DELETE" && leftOrder != rightOrder -> rightOrder - leftOrder
                        left.op != "DELETE" && leftOrder != rightOrder -> leftOrder - rightOrder
                        else -> left.dirtyOrdinal.compareTo(right.dirtyOrdinal)
                    }
                }
        )

        return PreparedPush(
            sourceBundleId = sourceBundleId,
            rows = preparedRows,
            discardedRows = discardedRows,
        )
    }

    suspend fun stageSnapshotChunk(
        state: RuntimeState,
        session: SnapshotSession,
        chunk: SnapshotChunkResponse,
        afterRowOrdinal: Long,
    ) {
        db.transaction(TransactionMode.IMMEDIATE) {
            val statementCache = StatementCache(db)
            try {
                var rowOrdinal = afterRowOrdinal
                for (row in chunk.rows) {
                    require(row.schema == state.validated.schema) {
                        "snapshot row schema ${row.schema} does not match client schema ${state.validated.schema}"
                    }
                    validateSnapshotRow(row)
                    val (keyJson, _) = localStore.bundleRowKeyToLocalKey(state, row.table, row.key)
                    rowOrdinal++
                    db.withPreparedStatement(
                        sql = """
                            INSERT INTO _sync_snapshot_stage (
                              snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """.trimIndent(),
                        statementCache = statementCache,
                    ) { st ->
                        st.bindText(1, session.snapshotId)
                        st.bindLong(2, rowOrdinal)
                        st.bindText(3, row.schema)
                        st.bindText(4, row.table)
                        st.bindText(5, keyJson)
                        st.bindLong(6, row.rowVersion)
                        st.bindText(7, row.payload.toString())
                        st.step()
                    }
                }
            } finally {
                statementCache.close()
            }
        }
    }

    suspend fun loadStagedSnapshotRows(
        state: RuntimeState,
        snapshotId: String,
    ): List<StagedSnapshotRow> {
        return db.withExclusiveAccess {
            val rows = mutableListOf<StagedSnapshotRow>()
            db.prepare(
                """
                SELECT schema_name, table_name, key_json, row_version, payload
                FROM _sync_snapshot_stage
                WHERE snapshot_id = ?
                ORDER BY row_ordinal
                """.trimIndent()
            ).use { st ->
                st.bindText(1, snapshotId)
                while (st.step()) {
                    val schemaName = st.getText(0)
                    val tableName = st.getText(1)
                    val keyJson = st.getText(2)
                    val (localPk, wireKey) = localStore.decodeDirtyKeyForPush(state, tableName, keyJson)
                    rows += StagedSnapshotRow(
                        schemaName = schemaName,
                        tableName = tableName,
                        keyJson = keyJson,
                        localPk = localPk,
                        wireKey = wireKey,
                        rowVersion = st.getLong(3),
                        payload = st.getText(4),
                    )
                }
            }
            rows
        }
    }

    suspend fun deleteSnapshotStage(
        snapshotId: String,
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = "DELETE FROM _sync_snapshot_stage WHERE snapshot_id = ?",
            statementCache = statementCache,
        ) { st ->
            st.bindText(1, snapshotId)
            st.step()
        }
    }

    suspend fun clearAllSnapshotStages(
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = "DELETE FROM _sync_snapshot_stage",
            statementCache = statementCache,
        ) { st ->
            st.step()
        }
    }

    private suspend fun loadDirtySnapshotRows(): List<DirtySnapshotRow> {
        return db.withExclusiveAccess {
            val rows = mutableListOf<DirtySnapshotRow>()
            db.prepare(
                """
                SELECT
                  d.schema_name,
                  d.table_name,
                  d.key_json,
                  d.base_row_version,
                  d.payload,
                  d.dirty_ordinal,
                  CASE WHEN rs.key_json IS NULL THEN 0 ELSE 1 END AS state_exists,
                  COALESCE(rs.deleted, 0) AS state_deleted
                FROM _sync_dirty_rows AS d
                LEFT JOIN _sync_row_state AS rs
                  ON rs.schema_name = d.schema_name
                 AND rs.table_name = d.table_name
                 AND rs.key_json = d.key_json
                ORDER BY d.dirty_ordinal, d.table_name, d.key_json
                """.trimIndent()
            ).use { st ->
                while (st.step()) {
                    rows += DirtySnapshotRow(
                        schemaName = st.getText(0),
                        tableName = st.getText(1),
                        keyJson = st.getText(2),
                        baseRowVersion = st.getLong(3),
                        payload = if (st.isNull(4)) null else st.getText(4),
                        dirtyOrdinal = st.getLong(5),
                        stateExists = st.getLong(6) == 1L,
                        stateDeleted = st.getLong(7) == 1L,
                    )
                }
            }
            rows
        }
    }
}
