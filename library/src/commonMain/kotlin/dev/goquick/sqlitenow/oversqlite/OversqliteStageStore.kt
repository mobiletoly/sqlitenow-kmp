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
    private val db: SafeSQLiteConnection,
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

    suspend fun freezePushOutboundSnapshot(prepared: PreparedPush) {
        if (prepared.rows.isEmpty() && prepared.discardedRows.isEmpty()) return

        db.transaction(TransactionMode.IMMEDIATE) {
            val statementCache = StatementCache(db)
            try {
                prepared.rows.forEachIndexed { index, row ->
                    db.withPreparedStatement(
                        sql = """
                            INSERT INTO _sync_push_outbound (
                              source_bundle_id, row_ordinal, schema_name, table_name, key_json, op, base_row_version, payload
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """.trimIndent(),
                        statementCache = statementCache,
                    ) { st ->
                        st.bindLong(1, prepared.sourceBundleId)
                        st.bindLong(2, index.toLong())
                        st.bindText(3, row.schemaName)
                        st.bindText(4, row.tableName)
                        st.bindText(5, row.keyJson)
                        st.bindText(6, row.op)
                        st.bindLong(7, row.baseRowVersion)
                        if (row.localPayload == null) st.bindNull(8) else st.bindText(8, row.localPayload)
                        st.step()
                    }
                }

                for (discarded in prepared.discardedRows) {
                    syncStateStore.deleteDirtyRow(discarded.schemaName, discarded.tableName, discarded.keyJson, statementCache)
                }
                for (row in prepared.rows) {
                    syncStateStore.deleteDirtyRow(row.schemaName, row.tableName, row.keyJson, statementCache)
                }
            } finally {
                statementCache.close()
            }
        }
    }

    suspend fun loadPushOutboundSnapshot(state: RuntimeState): PushOutboundSnapshot? {
        var sourceBundleId: Long? = null
        val rows = mutableListOf<DirtyRowCapture>()
        db.prepare(
            """
            SELECT source_bundle_id, schema_name, table_name, key_json, op, base_row_version, payload, row_ordinal
            FROM _sync_push_outbound
            ORDER BY source_bundle_id, row_ordinal
            """.trimIndent()
        ).use { st ->
            while (st.step()) {
                val rowSourceBundleId = st.getLong(0)
                if (sourceBundleId == null) {
                    sourceBundleId = rowSourceBundleId
                } else {
                    require(sourceBundleId == rowSourceBundleId) {
                        "outbound push snapshot contains multiple source_bundle_id values ($sourceBundleId and $rowSourceBundleId)"
                    }
                }
                val tableName = st.getText(2)
                val keyJson = st.getText(3)
                val (localPk, wireKey) = localStore.decodeDirtyKeyForPush(state, tableName, keyJson)
                rows += DirtyRowCapture(
                    schemaName = st.getText(1),
                    tableName = tableName,
                    keyJson = keyJson,
                    localPk = localPk,
                    wireKey = wireKey,
                    op = st.getText(4),
                    baseRowVersion = st.getLong(5),
                    localPayload = if (st.isNull(6)) null else st.getText(6),
                    dirtyOrdinal = st.getLong(7),
                )
            }
        }

        return sourceBundleId?.let {
            PushOutboundSnapshot(
                sourceBundleId = it,
                rows = rows,
            )
        }
    }

    suspend fun stageCommittedBundleChunk(
        state: RuntimeState,
        chunk: CommittedBundleRowsResponse,
        afterRowOrdinal: Long?,
    ) {
        var rowOrdinal = afterRowOrdinal ?: -1L
        db.transaction(TransactionMode.IMMEDIATE) {
            val statementCache = StatementCache(db)
            try {
                for (row in chunk.rows) {
                    require(row.schema == state.validated.schema) {
                        "committed bundle row schema ${row.schema} does not match client schema ${state.validated.schema}"
                    }
                    validateBundleRow(row)
                    val (keyJson, _) = localStore.bundleRowKeyToLocalKey(state, row.table, row.key)
                    rowOrdinal++
                    db.withPreparedStatement(
                        sql = """
                            INSERT INTO _sync_push_stage (
                              bundle_seq, row_ordinal, schema_name, table_name, key_json, op, row_version, payload
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """.trimIndent(),
                        statementCache = statementCache,
                    ) { st ->
                        st.bindLong(1, chunk.bundleSeq)
                        st.bindLong(2, rowOrdinal)
                        st.bindText(3, row.schema)
                        st.bindText(4, row.table)
                        st.bindText(5, keyJson)
                        st.bindText(6, row.op)
                        st.bindLong(7, row.rowVersion)
                        if (row.op == "DELETE" || row.payload == null) st.bindNull(8) else st.bindText(8, row.payload.toString())
                        st.step()
                    }
                }
            } finally {
                statementCache.close()
            }
        }
    }

    suspend fun computeStagedPushBundleHash(
        state: RuntimeState,
        bundleSeq: Long,
    ): String {
        val rows = mutableListOf<JsonElement>()
        db.prepare(
            """
            SELECT row_ordinal, schema_name, table_name, key_json, op, row_version, payload
            FROM _sync_push_stage
            WHERE bundle_seq = ?
            ORDER BY row_ordinal
            """.trimIndent()
        ).use { st ->
            st.bindLong(1, bundleSeq)
            while (st.step()) {
                val rowOrdinal = st.getLong(0)
                val schemaName = st.getText(1)
                val tableName = st.getText(2)
                val keyJson = st.getText(3)
                val op = st.getText(4)
                val rowVersion = st.getLong(5)
                val payloadText = if (st.isNull(6)) null else st.getText(6)
                val (_, wireKey) = localStore.decodeDirtyKeyForPush(state, tableName, keyJson)
                rows += buildJsonObject {
                    put("row_ordinal", JsonPrimitive(rowOrdinal))
                    put("schema", JsonPrimitive(schemaName))
                    put("table", JsonPrimitive(tableName))
                    put("key", buildJsonObject {
                        for ((key, value) in wireKey.entries.sortedBy { it.key }) {
                            put(key, JsonPrimitive(value))
                        }
                    })
                    put("op", JsonPrimitive(op))
                    put("row_version", JsonPrimitive(rowVersion))
                    put("payload", payloadText?.let { json.parseToJsonElement(it) } ?: JsonNull)
                }
            }
        }
        return sha256Hex(canonicalizeJsonElement(JsonArray(rows)).encodeToByteArray())
    }

    suspend fun loadStagedPushBundleRows(
        state: RuntimeState,
        bundleSeq: Long,
    ): List<StagedPushBundleRow> {
        val rows = mutableListOf<StagedPushBundleRow>()
        db.prepare(
            """
            SELECT schema_name, table_name, key_json, op, row_version, payload
            FROM _sync_push_stage
            WHERE bundle_seq = ?
            ORDER BY row_ordinal
            """.trimIndent()
        ).use { st ->
            st.bindLong(1, bundleSeq)
            while (st.step()) {
                val schemaName = st.getText(0)
                val tableName = st.getText(1)
                val keyJson = st.getText(2)
                val (localPk, wireKey) = localStore.decodeDirtyKeyForPush(state, tableName, keyJson)
                rows += StagedPushBundleRow(
                    schemaName = schemaName,
                    tableName = tableName,
                    keyJson = keyJson,
                    localPk = localPk,
                    wireKey = wireKey,
                    op = st.getText(3),
                    rowVersion = st.getLong(4),
                    payload = if (st.isNull(5)) null else st.getText(5),
                )
            }
        }
        return rows
    }

    suspend fun deletePushStage(
        bundleSeq: Long,
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = "DELETE FROM _sync_push_stage WHERE bundle_seq = ?",
            statementCache = statementCache,
        ) { st ->
            st.bindLong(1, bundleSeq)
            st.step()
        }
    }

    suspend fun deletePushOutboundSnapshot(
        sourceBundleId: Long,
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = "DELETE FROM _sync_push_outbound WHERE source_bundle_id = ?",
            statementCache = statementCache,
        ) { st ->
            st.bindLong(1, sourceBundleId)
            st.step()
        }
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
        return rows
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

    private suspend fun loadDirtySnapshotRows(): List<DirtySnapshotRow> {
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
        return rows
    }
}
