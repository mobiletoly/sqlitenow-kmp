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
import dev.goquick.sqlitenow.core.sqlite.use

internal const val outboxStateNone = "none"
internal const val outboxStatePrepared = "prepared"
internal const val outboxStateCommittedRemote = "committed_remote"

internal data class OversqliteOutboxBundleState(
    val state: String = outboxStateNone,
    val sourceId: String = "",
    val sourceBundleId: Long = 0,
    val initializationId: String = "",
    val canonicalRequestHash: String = "",
    val rowCount: Long = 0,
    val remoteBundleHash: String = "",
    val remoteBundleSeq: Long = 0,
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
)

internal class OversqliteOutboxStateStore(
    internal val db: SafeSQLiteConnection,
) {
    suspend fun loadBundleState(): OversqliteOutboxBundleState {
        return db.prepare(
            """
            SELECT state,
                   source_id,
                   source_bundle_id,
                   initialization_id,
                   canonical_request_hash,
                   row_count,
                   remote_bundle_hash,
                   remote_bundle_seq
            FROM _sync_outbox_bundle
            WHERE singleton_key = 1
            """.trimIndent(),
        ).use { st ->
            check(st.step()) { "_sync_outbox_bundle singleton row is missing" }
            OversqliteOutboxBundleState(
                state = st.getText(0),
                sourceId = st.getText(1),
                sourceBundleId = st.getLong(2),
                initializationId = st.getText(3),
                canonicalRequestHash = st.getText(4),
                rowCount = st.getLong(5),
                remoteBundleHash = st.getText(6),
                remoteBundleSeq = st.getLong(7),
            )
        }
    }

    suspend fun persistBundleState(
        state: OversqliteOutboxBundleState,
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = """
                UPDATE _sync_outbox_bundle
                SET state = ?,
                    source_id = ?,
                    source_bundle_id = ?,
                    initialization_id = ?,
                    canonical_request_hash = ?,
                    row_count = ?,
                    remote_bundle_hash = ?,
                    remote_bundle_seq = ?
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
            st.bindText(7, state.remoteBundleHash)
            st.bindLong(8, state.remoteBundleSeq)
            st.step()
        }
    }

    suspend fun loadRows(): List<OversqliteOutboxRow> {
        return db.prepare(
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
                   wire_payload
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
                        ),
                    )
                }
            }
        }
    }

    suspend fun countRows(): Int {
        return db.prepare("SELECT COUNT(*) FROM _sync_outbox_rows").use { st ->
            check(st.step())
            st.getLong(0).toInt()
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

    suspend fun clearBundleAndRows(statementCache: StatementCache? = null) {
        clearRows(statementCache)
        persistBundleState(
            state = OversqliteOutboxBundleState(),
            statementCache = statementCache,
        )
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
                      wire_key_json, op, base_row_version, local_payload, wire_payload
                    )
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                st.step()
            }
        }
    }
}
