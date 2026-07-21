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
import dev.goquick.sqlitenow.core.sqlite.SqliteStatement
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject

internal class OversqliteStageStore(
    internal val db: SafeSQLiteConnection,
    private val localStore: OversqliteLocalStore,
    private val syncStateStore: OversqliteSyncStateStore,
    private val json: Json,
) {
    internal var snapshotStatementCloseForTest: ((SqliteStatement) -> Unit)? = null
    internal var reusableStatementCleanupForTest: ReusableStatementCleanup? = null

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
            db.withPreparedStatement(
                sql = """
                    INSERT INTO _sync_snapshot_stage (
                      snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """.trimIndent(),
                closeStatement = { statement ->
                    snapshotStatementCloseForTest?.invoke(statement) ?: statement.close()
                },
            ) { st ->
                var rowOrdinal = afterRowOrdinal
                var statementUsed = false
                for (row in chunk.rows) {
                    require(row.schema == state.validated.schema) {
                        "snapshot row ${safeSyncTableDiagnosticIdentifier(row.schema, row.table)} " +
                            "does not match configured schema " +
                            safeSnapshotDiagnosticIdentifier(state.validated.schema)
                    }
                    validateSnapshotRow(row)
                    require(row.table in state.validated.tableInfoByName) {
                        "snapshot row ${safeSyncTableDiagnosticIdentifier(row.schema, row.table)} " +
                            "is not configured for sync"
                    }
                    val payload = row.payload as? JsonObject
                        ?: error(
                            "snapshot row payload must be a JSON object for " +
                                safeSyncTableDiagnosticIdentifier(row.schema, row.table),
                        )
                    val normalizedPayload = localStore.validateAndNormalizeSnapshotPayload(
                        state = state,
                        tableName = row.table,
                        key = row.key,
                        payload = payload,
                    )
                    val (keyJson, _) = localStore.bundleRowKeyToLocalKey(state, row.table, row.key)
                    rowOrdinal++
                    if (statementUsed) {
                        (reusableStatementCleanupForTest ?: DefaultReusableStatementCleanup)(st)
                    } else {
                        statementUsed = true
                    }

                    st.bindText(1, session.snapshotId)
                    st.bindLong(2, rowOrdinal)
                    st.bindText(3, row.schema)
                    st.bindText(4, row.table)
                    st.bindText(5, keyJson)
                    st.bindLong(6, row.rowVersion)
                    st.bindText(7, normalizedPayload.toString())
                    st.step()
                }
            }
        }
    }

    suspend fun loadStagedSnapshotPage(
        state: RuntimeState,
        snapshotId: String,
        afterRowOrdinal: Long,
        maxRows: Int,
        maxBytes: Long,
    ): StagedSnapshotPage {
        require(maxRows > 0) { "snapshot apply page row limit must be positive" }
        require(maxBytes > 0L) { "snapshot apply page byte limit must be positive" }

        var selectedBytes = 0L
        var selectedCount = 0
        var lastOrdinal: Long? = null
        db.withExclusiveAccess {
            db.prepare(
                """
                SELECT row_ordinal,
                       length(CAST(schema_name AS BLOB)) +
                       length(CAST(table_name AS BLOB)) +
                       length(CAST(key_json AS BLOB)) +
                       length(CAST(payload AS BLOB)) AS retained_text_bytes
                FROM _sync_snapshot_stage
                WHERE snapshot_id = ? AND row_ordinal > ?
                ORDER BY row_ordinal
                LIMIT ?
                """.trimIndent(),
            ).use { st ->
                st.bindText(1, snapshotId)
                st.bindLong(2, afterRowOrdinal)
                st.bindLong(3, maxRows.toLong())
                while (st.step()) {
                    val rowOrdinal = st.getLong(0)
                    val retainedTextBytes = st.getLong(1)
                    require(retainedTextBytes >= 0L) {
                        "snapshot staged row metadata byte count must be non-negative"
                    }
                    val nextBytes = checkedAddSnapshotLong(selectedBytes, retainedTextBytes) {
                        "snapshot apply page byte total overflow"
                    }
                    if (nextBytes > maxBytes) {
                        if (selectedCount == 0) {
                            throw SnapshotApplyRowTooLargeException(
                                rowOrdinal = rowOrdinal,
                                retainedTextBytes = retainedTextBytes,
                                limit = maxBytes,
                            )
                        }
                        break
                    }
                    selectedBytes = nextBytes
                    selectedCount++
                    lastOrdinal = rowOrdinal
                }
            }
        }
        if (selectedCount == 0) {
            return StagedSnapshotPage(emptyList(), 0L)
        }
        val selectedLastOrdinal = checkNotNull(lastOrdinal) {
            "snapshot apply page selection is missing its last row ordinal"
        }

        val rows = db.withExclusiveAccess {
            db.prepare(
                """
                SELECT row_ordinal, schema_name, table_name, key_json, row_version, payload
                FROM _sync_snapshot_stage
                WHERE snapshot_id = ? AND row_ordinal > ? AND row_ordinal <= ?
                ORDER BY row_ordinal
                """.trimIndent(),
            ).use { st ->
                st.bindText(1, snapshotId)
                st.bindLong(2, afterRowOrdinal)
                st.bindLong(3, selectedLastOrdinal)
                buildList {
                    while (st.step()) {
                        add(
                            StagedSnapshotRow(
                                rowOrdinal = st.getLong(0),
                                schemaName = st.getText(1),
                                tableName = st.getText(2),
                                keyJson = st.getText(3),
                                rowVersion = st.getLong(4),
                                payload = st.getText(5),
                            ),
                        )
                    }
                }
            }
        }
        require(rows.size == selectedCount) {
            "snapshot staged page row count changed between metadata and row load"
        }
        require(rows.last().rowOrdinal == selectedLastOrdinal) {
            "snapshot staged page last ordinal changed between metadata and row load"
        }
        rows.forEach { row ->
            require(row.schemaName == state.validated.schema) {
                "staged snapshot row " +
                    safeSyncTableDiagnosticIdentifier(row.schemaName, row.tableName) +
                    " does not match configured schema " +
                    safeSnapshotDiagnosticIdentifier(state.validated.schema)
            }
            require(row.tableName in state.validated.tableInfoByName) {
                "staged snapshot row " +
                    safeSyncTableDiagnosticIdentifier(row.schemaName, row.tableName) +
                    " is not configured for sync"
            }
        }
        return StagedSnapshotPage(rows = rows, retainedTextBytes = selectedBytes)
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
        return db.queryList(
            sql = """
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
            """.trimIndent(),
        ) { st ->
            DirtySnapshotRow(
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
}
