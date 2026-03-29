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

internal const val operationKindNone = "none"
internal const val operationKindRemoteReplace = "remote_replace"

internal data class OversqliteOperationState(
    val kind: String = operationKindNone,
    val targetUserId: String = "",
    val stagedSnapshotId: String = "",
    val snapshotBundleSeq: Long = 0,
    val snapshotRowCount: Long = 0,
)

internal class OversqliteOperationStateStore(
    private val db: SafeSQLiteConnection,
) {
    suspend fun loadState(): OversqliteOperationState {
        return db.prepare(
            """
            SELECT kind, target_user_id, staged_snapshot_id, snapshot_bundle_seq, snapshot_row_count
            FROM _sync_operation_state
            WHERE singleton_key = 1
            """.trimIndent(),
        ).use { st ->
            check(st.step()) { "_sync_operation_state singleton row is missing" }
            OversqliteOperationState(
                kind = st.getText(0),
                targetUserId = st.getText(1),
                stagedSnapshotId = st.getText(2),
                snapshotBundleSeq = st.getLong(3),
                snapshotRowCount = st.getLong(4),
            )
        }
    }

    suspend fun persistState(
        state: OversqliteOperationState,
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = """
                UPDATE _sync_operation_state
                SET kind = ?,
                    target_user_id = ?,
                    staged_snapshot_id = ?,
                    snapshot_bundle_seq = ?,
                    snapshot_row_count = ?
                WHERE singleton_key = 1
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            st.bindText(1, state.kind)
            st.bindText(2, state.targetUserId)
            st.bindText(3, state.stagedSnapshotId)
            st.bindLong(4, state.snapshotBundleSeq)
            st.bindLong(5, state.snapshotRowCount)
            st.step()
        }
    }
}
