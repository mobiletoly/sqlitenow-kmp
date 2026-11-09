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

internal const val attachmentBindingAnonymous = "anonymous"
internal const val attachmentBindingAttached = "attached"

internal data class OversqliteAttachmentState(
    val currentSourceId: String = "",
    val attachmentEpoch: Long = 1,
    val bindingState: String = attachmentBindingAnonymous,
    val attachedUserId: String = "",
    val schemaName: String = "",
    val lastBundleSeqSeen: Long = 0,
    val rebuildRequired: Boolean = false,
    val pendingInitializationId: String = "",
    val initializationReconnectRequired: Boolean = false,
)

internal class OversqliteAttachmentStateStore(
    internal val db: SafeSQLiteConnection,
) {
    suspend fun loadState(): OversqliteAttachmentState {
        return db.queryRequiredSingle(
            sql = """
                SELECT current_source_id, binding_state, attached_user_id, schema_name,
                       last_bundle_seq_seen, rebuild_required, pending_initialization_id, attachment_epoch, initialization_reconnect_required
                FROM _sync_attachment_state
                WHERE singleton_key = 1
            """.trimIndent(),
            missingMessage = "_sync_attachment_state singleton row is missing",
        ) { st ->
            OversqliteAttachmentState(
                currentSourceId = requireValidOptionalOversqliteSourceId(st.getText(0)),
                attachmentEpoch = st.getLong(7),
                initializationReconnectRequired = st.getLong(8)==1L,
                bindingState = st.getText(1),
                attachedUserId = st.getText(2),
                schemaName = st.getText(3),
                lastBundleSeqSeen = st.getLong(4),
                rebuildRequired = st.getLong(5) == 1L,
                pendingInitializationId = requireValidOptionalOversqliteSessionToken(st.getText(6)),
            )
        }
    }

    suspend fun persistState(
        state: OversqliteAttachmentState,
        statementCache: StatementCache? = null,
        epochAlreadyAdvanced: Boolean = false,
    ) {
        requireValidOptionalOversqliteSourceId(state.currentSourceId)
        requireValidOptionalOversqliteSessionToken(state.pendingInitializationId)
        db.withPreparedStatement(
            sql = """
                UPDATE _sync_attachment_state
                SET current_source_id = ?1,
                    binding_state = ?2,
                    attached_user_id = ?3,
                    schema_name = ?4,
                    last_bundle_seq_seen = ?5,
                    rebuild_required = ?6,
                    pending_initialization_id = ?7,
                    initialization_reconnect_required = ?8,
                    attachment_epoch = attachment_epoch + CASE WHEN ?9 = 0 AND (current_source_id <> ?1 OR binding_state <> ?2 OR attached_user_id <> ?3) THEN 1 ELSE 0 END
                WHERE singleton_key = 1
                    -- An identical state is not rewritten; a ready reopen must leave the file unchanged.
                    AND (current_source_id <> ?1 OR binding_state <> ?2 OR attached_user_id <> ?3 OR schema_name <> ?4
                        OR last_bundle_seq_seen <> ?5 OR rebuild_required <> ?6 OR pending_initialization_id <> ?7
                        OR initialization_reconnect_required <> ?8)
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            st.bindText(1, state.currentSourceId)
            st.bindText(2, state.bindingState)
            st.bindText(3, state.attachedUserId)
            st.bindText(4, state.schemaName)
            st.bindLong(5, state.lastBundleSeqSeen)
            st.bindLong(6, if (state.rebuildRequired) 1 else 0)
            st.bindText(7, state.pendingInitializationId)
            st.bindLong(8,if(state.initializationReconnectRequired)1 else 0)
            st.bindLong(9, if (epochAlreadyAdvanced) 1 else 0)
            st.step()
        }
        bindLocalBaselineContext(db)
    }

    suspend fun advanceAttachmentEpoch() {
        db.execSQL("UPDATE _sync_attachment_state SET attachment_epoch=attachment_epoch+1 WHERE singleton_key=1")
    }

    suspend fun persistAnonymousState(
        sourceId: String,
        statementCache: StatementCache? = null,
        epochAlreadyAdvanced: Boolean = false,
    ) {
        persistState(
            state = OversqliteAttachmentState(
                currentSourceId = sourceId,
                bindingState = attachmentBindingAnonymous,
            ),
            statementCache = statementCache,
            epochAlreadyAdvanced = epochAlreadyAdvanced,
        )
    }

    suspend fun persistAttachedState(
        sourceId: String,
        userId: String,
        schemaName: String,
        lastBundleSeqSeen: Long,
        rebuildRequired: Boolean,
        pendingInitializationId: String,
        statementCache: StatementCache? = null,
        epochAlreadyAdvanced: Boolean = false,
    ) {
        persistState(
            state = OversqliteAttachmentState(
                currentSourceId = sourceId,
                bindingState = attachmentBindingAttached,
                attachedUserId = userId,
                schemaName = schemaName,
                lastBundleSeqSeen = lastBundleSeqSeen,
                rebuildRequired = rebuildRequired,
                pendingInitializationId = pendingInitializationId,
            ),
            statementCache = statementCache,
            epochAlreadyAdvanced = epochAlreadyAdvanced,
        )
    }

    suspend fun isAttachedTo(
        userId: String,
        sourceId: String,
    ): Boolean {
        requireValidOversqliteSourceId(sourceId)
        val state = loadState()
        return state.bindingState == attachmentBindingAttached &&
            state.attachedUserId == userId &&
            state.currentSourceId == sourceId
    }

    suspend fun markBundleSeen(
        bundleSeq: Long,
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = """
                UPDATE _sync_attachment_state
                SET last_bundle_seq_seen = CASE
                      WHEN last_bundle_seq_seen < ? THEN ?
                      ELSE last_bundle_seq_seen
                    END
                WHERE singleton_key = 1
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            st.bindLong(1, bundleSeq)
            st.bindLong(2, bundleSeq)
            st.step()
        }
    }

    suspend fun advanceAfterCommittedPush(
        bundleSeq: Long,
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = """
                UPDATE _sync_attachment_state
                SET last_bundle_seq_seen = CASE
                      WHEN last_bundle_seq_seen + 1 = ? THEN ?
                      ELSE last_bundle_seq_seen
                    END,
                    pending_initialization_id='',initialization_reconnect_required=0
                WHERE singleton_key = 1
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            st.bindLong(1, bundleSeq)
            st.bindLong(2, bundleSeq)
            st.step()
        }
    }

    suspend fun setRebuildRequired(
        rebuildRequired: Boolean,
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = """
                UPDATE _sync_attachment_state
                SET rebuild_required = ?
                WHERE singleton_key = 1
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            st.bindLong(1, if (rebuildRequired) 1 else 0)
            st.step()
        }
    }

    suspend fun clearPendingInitializationId(
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = """
                UPDATE _sync_attachment_state
                SET pending_initialization_id = ''
                WHERE singleton_key = 1
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            st.step()
        }
    }
}
