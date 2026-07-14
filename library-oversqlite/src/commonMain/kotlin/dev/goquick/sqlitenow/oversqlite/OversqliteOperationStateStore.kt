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

internal const val operationKindNone = "none"
internal const val operationKindRemoteReplace = "remote_replace"
internal const val operationKindSourceRecovery = "source_recovery"

internal data class OversqliteOperationState(
    val kind: String = operationKindNone,
    val targetUserId: String = "",
    val stagedSnapshotId: String = "",
    val snapshotBundleSeq: Long = 0,
    val snapshotRowCount: Long = 0,
    val snapshotByteCount: Long = 0,
    val snapshotStageComplete: Boolean = false,
    val reason: String = "",
    val replacementSourceId: String = "",
)

internal fun OversqliteOperationState.isSourceRecoveryRequired(): Boolean {
    return kind == operationKindSourceRecovery
}

internal fun OversqliteOperationState.sourceRecoveryReasonOrNull(): SourceRecoveryReason? {
    return when (reason) {
        "history_pruned" -> SourceRecoveryReason.HISTORY_PRUNED
        "source_sequence_out_of_order" -> SourceRecoveryReason.SOURCE_SEQUENCE_OUT_OF_ORDER
        "source_sequence_changed" -> SourceRecoveryReason.SOURCE_SEQUENCE_CHANGED
        "source_retired" -> SourceRecoveryReason.SOURCE_RETIRED
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
        SourceRecoveryReason.SOURCE_RETIRED -> "source_retired"
    }
}

internal class OversqliteOperationStateStore(
    private val db: SafeSQLiteConnection,
) {
    suspend fun loadState(): OversqliteOperationState {
        return db.queryRequiredSingle(
            sql = """
                SELECT kind, target_user_id, staged_snapshot_id, snapshot_bundle_seq, snapshot_row_count,
                       snapshot_byte_count, snapshot_stage_complete, reason, replacement_source_id
                FROM _sync_operation_state
                WHERE singleton_key = 1
            """.trimIndent(),
            missingMessage = "_sync_operation_state singleton row is missing",
        ) { st ->
            OversqliteOperationState(
                kind = st.getText(0),
                targetUserId = st.getText(1),
                stagedSnapshotId = st.getText(2),
                snapshotBundleSeq = st.getLong(3),
                snapshotRowCount = st.getLong(4),
                snapshotByteCount = st.getLong(5),
                snapshotStageComplete = st.getLong(6) == 1L,
                reason = st.getText(7),
                replacementSourceId = st.getText(8).let { sourceId ->
                    if (st.getText(0) == operationKindSourceRecovery) {
                        requireValidOversqliteSourceId(sourceId)
                    } else {
                        requireValidOptionalOversqliteSourceId(sourceId)
                    }
                },
            )
        }
    }

    suspend fun persistState(
        state: OversqliteOperationState,
        statementCache: StatementCache? = null,
    ) {
        if (state.kind == operationKindSourceRecovery) {
            requireValidOversqliteSourceId(state.replacementSourceId)
        } else {
            requireValidOptionalOversqliteSourceId(state.replacementSourceId)
        }
        db.withPreparedStatement(
            sql = """
                UPDATE _sync_operation_state
                SET kind = ?,
                    target_user_id = ?,
                    staged_snapshot_id = ?,
                    snapshot_bundle_seq = ?,
                    snapshot_row_count = ?,
                    snapshot_byte_count = ?,
                    snapshot_stage_complete = ?,
                    reason = ?,
                    replacement_source_id = ?
                WHERE singleton_key = 1
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            st.bindText(1, state.kind)
            st.bindText(2, state.targetUserId)
            st.bindText(3, state.stagedSnapshotId)
            st.bindLong(4, state.snapshotBundleSeq)
            st.bindLong(5, state.snapshotRowCount)
            st.bindLong(6, state.snapshotByteCount)
            st.bindLong(7, if (state.snapshotStageComplete) 1L else 0L)
            st.bindText(8, state.reason)
            st.bindText(9, state.replacementSourceId)
            st.step()
        }
    }
}
