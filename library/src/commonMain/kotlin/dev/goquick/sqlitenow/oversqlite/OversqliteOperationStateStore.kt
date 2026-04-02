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
internal const val operationKindSourceRecovery = "source_recovery"
internal const val sourceRecoveryIntentStateNone = ""
internal const val sourceRecoveryIntentStateOutbox = "outbox"

internal data class OversqliteOperationState(
    val kind: String = operationKindNone,
    val targetUserId: String = "",
    val stagedSnapshotId: String = "",
    val snapshotBundleSeq: Long = 0,
    val snapshotRowCount: Long = 0,
    val sourceRecoveryReason: String = "",
    val sourceRecoverySourceId: String = "",
    val sourceRecoverySourceBundleId: Long = 0,
    val sourceRecoveryIntentState: String = sourceRecoveryIntentStateNone,
)

internal fun OversqliteOperationState.isSourceRecoveryRequired(): Boolean {
    return kind == operationKindSourceRecovery
}

internal fun OversqliteOperationState.sourceRecoveryReasonOrNull(): SourceRecoveryReason? {
    return when (sourceRecoveryReason) {
        "history_pruned" -> SourceRecoveryReason.HISTORY_PRUNED
        "source_sequence_out_of_order" -> SourceRecoveryReason.SOURCE_SEQUENCE_OUT_OF_ORDER
        "source_sequence_changed" -> SourceRecoveryReason.SOURCE_SEQUENCE_CHANGED
        else -> null
    }
}

internal fun OversqliteOperationState.requireSourceRecoveryReason(): SourceRecoveryReason {
    return sourceRecoveryReasonOrNull()
        ?: error("source recovery operation state is missing a valid recovery reason")
}

internal fun SourceRecoveryReason.toPersistedOperationReason(): String {
    return when (this) {
        SourceRecoveryReason.HISTORY_PRUNED -> "history_pruned"
        SourceRecoveryReason.SOURCE_SEQUENCE_OUT_OF_ORDER -> "source_sequence_out_of_order"
        SourceRecoveryReason.SOURCE_SEQUENCE_CHANGED -> "source_sequence_changed"
    }
}

internal class OversqliteOperationStateStore(
    private val db: SafeSQLiteConnection,
) {
    suspend fun loadState(): OversqliteOperationState {
        return db.withExclusiveAccess {
            db.prepare(
                """
                SELECT kind, target_user_id, staged_snapshot_id, snapshot_bundle_seq, snapshot_row_count,
                       source_recovery_reason, source_recovery_source_id, source_recovery_source_bundle_id,
                       source_recovery_intent_state
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
                    sourceRecoveryReason = st.getText(5),
                    sourceRecoverySourceId = st.getText(6),
                    sourceRecoverySourceBundleId = st.getLong(7),
                    sourceRecoveryIntentState = st.getText(8),
                )
            }
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
                    snapshot_row_count = ?,
                    source_recovery_reason = ?,
                    source_recovery_source_id = ?,
                    source_recovery_source_bundle_id = ?,
                    source_recovery_intent_state = ?
                WHERE singleton_key = 1
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            st.bindText(1, state.kind)
            st.bindText(2, state.targetUserId)
            st.bindText(3, state.stagedSnapshotId)
            st.bindLong(4, state.snapshotBundleSeq)
            st.bindLong(5, state.snapshotRowCount)
            st.bindText(6, state.sourceRecoveryReason)
            st.bindText(7, state.sourceRecoverySourceId)
            st.bindLong(8, state.sourceRecoverySourceBundleId)
            st.bindText(9, state.sourceRecoveryIntentState)
            st.step()
        }
    }
}
