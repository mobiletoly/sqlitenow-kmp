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

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive

internal const val outboxStateNone = "none"
internal const val outboxStatePrepared = "prepared"
internal const val outboxStateRejected = "rejected"

internal data class OversqliteOutboxBundleState(
    val state: String = outboxStateNone,
    val sourceId: String = "",
    val sourceBundleId: Long = 0,
    val initializationId: String = "",
    val canonicalRequestHash: String = "",
    val rowCount: Long = 0,
)

internal data class OversqliteOutboxRow(
    val sourceBundleId: Long,
    val rowOrdinal: Long,
    val schemaName: String,
    val tableName: String,
    val keyJson: String,
    val wireKeyJson: String,
    val op: String,
    val baseRowVersion: Long,
    val localPayload: String?,
    val wirePayload: String?,
    val base: HistoricalBase,
)

internal data class PreparedOutboxReplayRow(
    val rowOrdinal: Long,
    val tableName: String,
    val keyJson: String,
    val op: String,
    val localPayload: String?,
)

internal class OversqliteOutboxStateStore(
    internal val db: SafeSQLiteConnection,
) {
    suspend fun loadBundleState(): OversqliteOutboxBundleState {
        return db.queryRequiredSingle(
            sql = """
                SELECT state,
                       source_id,
                       source_bundle_id,
                       initialization_id,
                       canonical_request_hash,
                       row_count
                FROM _sync_outbox_bundle
                WHERE singleton_key = 1
            """.trimIndent(),
            missingMessage = "_sync_outbox_bundle singleton row is missing",
        ) { st ->
            OversqliteOutboxBundleState(
                state = st.getText(0),
                sourceId = st.getText(1).let { sourceId ->
                    if (st.getText(0) == outboxStateNone) {
                        requireValidOptionalOversqliteSourceId(sourceId)
                    } else {
                        requireValidOversqliteSourceId(sourceId)
                    }
                },
                sourceBundleId = st.getLong(2),
                initializationId = requireValidOptionalOversqliteSessionToken(st.getText(3)),
                canonicalRequestHash = st.getText(4),
                rowCount = st.getLong(5),
            )
        }
    }

    suspend fun persistBundleState(
        state: OversqliteOutboxBundleState,
        statementCache: StatementCache? = null,
    ) {
        if (state.state == outboxStateNone) {
            requireValidOptionalOversqliteSourceId(state.sourceId)
        } else {
            requireValidOversqliteSourceId(state.sourceId)
        }
        requireValidOptionalOversqliteSessionToken(state.initializationId)
        db.withPreparedStatement(
            sql = """
                UPDATE _sync_outbox_bundle
                SET state = ?,
                    source_id = ?,
                    source_bundle_id = ?,
                    initialization_id = ?,
                    canonical_request_hash = ?,
                    row_count = ?
                WHERE singleton_key = 1
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            st.bindText(1, state.state)
            st.bindText(2, state.sourceId)
            st.bindLong(3, state.sourceBundleId)
            st.bindText(4, state.initializationId)
            st.bindText(5, state.canonicalRequestHash)
            st.bindLong(6, state.rowCount)
            st.step()
        }
    }

    suspend fun loadRows(): List<OversqliteOutboxRow> {
        return db.withExclusiveAccess {
            db.prepare(
                """
                SELECT source_bundle_id,
                       row_ordinal,
                       schema_name,
                       table_name,
                       key_json,
                       wire_key_json,
                       op,
                       base_row_version,
                       local_payload,
                       wire_payload,base_kind,base_payload,base_provenance
                FROM _sync_outbox_rows
                ORDER BY source_bundle_id, row_ordinal
                """.trimIndent(),
            ).use { st ->
                buildList {
                    while (st.step()) {
                        add(
                            OversqliteOutboxRow(
                                sourceBundleId = st.getLong(0),
                                rowOrdinal = st.getLong(1),
                                schemaName = st.getText(2),
                                tableName = st.getText(3),
                                keyJson = st.getText(4),
                                wireKeyJson = st.getText(5),
                                op = st.getText(6),
                                baseRowVersion = st.getLong(7),
                                localPayload = if (st.isNull(8)) null else st.getText(8),
                                wirePayload = if (st.isNull(9)) null else st.getText(9),
                                base=HistoricalBase(st.getText(10),if(st.isNull(11)) null else st.getText(11),st.getText(12)),
                            ),
                        )
                    }
                }
            }
        }
    }

    suspend fun loadReplayRowAfter(
        sourceBundleId: Long,
        afterRowOrdinal: Long,
        statementCache: StatementCache,
    ): PreparedOutboxReplayRow? {
        return db.withPreparedStatement(
            sql = """
                SELECT row_ordinal,
                       table_name,
                       key_json,
                       op,
                       local_payload
                FROM _sync_outbox_rows
                WHERE source_bundle_id = ? AND row_ordinal > ?
                ORDER BY row_ordinal
                LIMIT 1
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            st.bindLong(1, sourceBundleId)
            st.bindLong(2, afterRowOrdinal)
            if (!st.step()) return@withPreparedStatement null
            PreparedOutboxReplayRow(
                rowOrdinal = st.getLong(0),
                tableName = st.getText(1),
                keyJson = st.getText(2),
                op = st.getText(3),
                localPayload = if (st.isNull(4)) null else st.getText(4),
            )
        }
    }

    suspend fun countRows(): Long {
        return db.scalarLong("SELECT COUNT(*) FROM _sync_outbox_rows")
    }

    suspend fun countRowsForSourceBundle(sourceBundleId: Long): Long {
        return db.withExclusiveAccess {
            db.prepare(
                "SELECT COUNT(*) FROM _sync_outbox_rows WHERE source_bundle_id = ?",
            ).use { st ->
                st.bindLong(1, sourceBundleId)
                check(st.step())
                st.getLong(0)
            }
        }
    }

    suspend fun clearRows(statementCache: StatementCache? = null) {
        db.withPreparedStatement(
            sql = "DELETE FROM _sync_outbox_rows",
            statementCache = statementCache,
        ) { st ->
            st.step()
        }
    }

    suspend fun clearBundleAndRows(statementCache: StatementCache? = null, pagedSettlement: Boolean = false, terminalDisposition: Boolean = false) {
        val current=loadBundleState()
        if(terminalDisposition && current.state==outboxStatePrepared) markUploadDisposition(db,statementCache)
        else requireUploadRelease(db)
        if (pagedSettlement) {
            // Settlement prepaid this cleanup in its atomic work bound. Pages
            // keep cancellation observable; its enclosing writer owns rollback.
            while (true) {
                currentCoroutineContext().ensureActive()
                db.withPreparedStatement("DELETE FROM _sync_outbox_rows WHERE rowid IN (SELECT rowid FROM _sync_outbox_rows ORDER BY rowid LIMIT 128)", statementCache = statementCache) { it.step() }
                val remaining = db.withPreparedStatement("SELECT EXISTS(SELECT 1 FROM _sync_outbox_rows LIMIT 1)", statementCache = statementCache) { check(it.step()); it.getLong(0) != 0L }
                if (!remaining) break
            }
        } else clearRows(statementCache)
        persistBundleState(
            state = OversqliteOutboxBundleState(),
            statementCache = statementCache,
        )
    }

    // Called inside the existing policy writer; admit all capture copies before
    // loading them, then reuse the same original-base/current-intent requeue.
    suspend fun requeueRejectedIntent(config: OversqliteConfig,generationId: Long=config.generationId) {
        check(loadBundleState().state==outboxStateRejected)
        val generation=OversqliteContractStore(db).readGeneration(generationId)
        val oldConfig=config.copy(generationId=generationId,syncTables=generation.tables.map {SyncTable(it.table,syncKeyColumns=it.syncKeyColumns)})
        val validated=SyncRuntimeInitializer(oldConfig,TableInfoCache()).validateConfig(db).withGeneration(generation)
        val local=OversqliteLocalStore(db,kotlinx.serialization.json.Json) {validated}
        val sync=OversqliteSyncStateStore(db) {local}
        val workspace=OversqliteWorkspace(db,config)
        val bytes=db.scalarLong("""SELECT COALESCE(sum(2048+length(CAST(COALESCE(local_payload,'') AS BLOB))+length(CAST(COALESCE(wire_payload,'') AS BLOB))+length(CAST(COALESCE(base_payload,'') AS BLOB))+length(CAST(base_provenance AS BLOB))+length(CAST(key_json AS BLOB))+length(CAST(wire_key_json AS BLOB))),0) FROM _sync_outbox_rows""")
        workspace.check("rejected intent",WorkspaceCharge(32*countRows(),workspace.scale("rejected intent",bytes,64)+65536))
        for(row in loadRows()) sync.requeueOriginalIntent(row.schemaName,row.tableName,row.keyJson,row.op,row.baseRowVersion,row.localPayload,row.base)
    }

    suspend fun retainRejected() {
        check(loadBundleState().state==outboxStatePrepared) { "rejection requires frozen ownership" }
        markUploadDisposition(db)
        db.execSQL("UPDATE _sync_outbox_bundle SET state='rejected' WHERE singleton_key=1")
    }

    /**
     * Releases the frozen request, its journal and any acknowledgment obligation bound to a source the
     * server has replaced. Replacement is admitted only while that source holds no unacknowledged
     * outcome, so none of them can be needed again. The caller has already requeued or dropped the
     * intent these rows carried.
     */
    suspend fun releaseRetiredSourceUpload(statementCache: StatementCache? = null) {
        db.withPreparedStatement("DELETE FROM _sync_upload_journal WHERE singleton_key=1", statementCache) { it.step() }
        clearRows(statementCache)
        persistBundleState(OversqliteOutboxBundleState(), statementCache)
    }

    suspend fun rebindBundleSource(
        sourceId: String,
        sourceBundleId: Long,
        statementCache: StatementCache? = null,
    ) {
        requireValidOversqliteSourceId(sourceId)
        db.withPreparedStatement(
            sql = """
                UPDATE _sync_outbox_bundle
                SET source_id = ?,
                    source_bundle_id = ?
                WHERE singleton_key = 1
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            st.bindText(1, sourceId)
            st.bindLong(2, sourceBundleId)
            st.step()
        }
        db.withPreparedStatement(
            sql = """
                UPDATE _sync_outbox_rows
                SET source_bundle_id = ?
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            st.bindLong(1, sourceBundleId)
            st.step()
        }
    }

    suspend fun appendRows(
        rows: List<OversqliteOutboxRow>,
        statementCache: StatementCache? = null,
    ) {
        for (row in rows) {
            db.withPreparedStatement(
                sql = """
                    INSERT INTO _sync_outbox_rows(
                      source_bundle_id, row_ordinal, schema_name, table_name, key_json,
                      wire_key_json, op, base_row_version, local_payload, wire_payload,base_kind,base_payload,base_provenance
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """.trimIndent(),
                statementCache = statementCache,
            ) { st ->
                st.bindLong(1, row.sourceBundleId)
                st.bindLong(2, row.rowOrdinal)
                st.bindText(3, row.schemaName)
                st.bindText(4, row.tableName)
                st.bindText(5, row.keyJson)
                st.bindText(6, row.wireKeyJson)
                st.bindText(7, row.op)
                st.bindLong(8, row.baseRowVersion)
                if (row.localPayload == null) st.bindNull(9) else st.bindText(9, row.localPayload)
                if (row.wirePayload == null) st.bindNull(10) else st.bindText(10, row.wirePayload)
                st.bindText(11,row.base.kind)
                if(row.base.payload==null) st.bindNull(12) else st.bindText(12,row.base.payload)
                st.bindText(13,row.base.provenance)
                st.step()
            }
        }
    }
}
