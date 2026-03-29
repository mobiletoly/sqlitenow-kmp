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

internal const val attachmentBindingAnonymous = "anonymous"
internal const val attachmentBindingAttached = "attached"

internal data class OversqliteAttachmentState(
    val currentSourceId: String = "",
    val bindingState: String = attachmentBindingAnonymous,
    val attachedUserId: String = "",
    val schemaName: String = "",
    val lastBundleSeqSeen: Long = 0,
    val rebuildRequired: Boolean = false,
    val pendingInitializationId: String = "",
)

internal class OversqliteAttachmentStateStore(
    internal val db: SafeSQLiteConnection,
) {
    suspend fun loadState(): OversqliteAttachmentState {
        return db.prepare(
            """
            SELECT current_source_id, binding_state, attached_user_id, schema_name,
                   last_bundle_seq_seen, rebuild_required, pending_initialization_id
            FROM _sync_attachment_state
            WHERE singleton_key = 1
            """.trimIndent(),
        ).use { st ->
            check(st.step()) { "_sync_attachment_state singleton row is missing" }
            OversqliteAttachmentState(
                currentSourceId = st.getText(0),
                bindingState = st.getText(1),
                attachedUserId = st.getText(2),
                schemaName = st.getText(3),
                lastBundleSeqSeen = st.getLong(4),
                rebuildRequired = st.getLong(5) == 1L,
                pendingInitializationId = st.getText(6),
            )
        }
    }

    suspend fun persistState(
        state: OversqliteAttachmentState,
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = """
                UPDATE _sync_attachment_state
                SET current_source_id = ?,
                    binding_state = ?,
                    attached_user_id = ?,
                    schema_name = ?,
                    last_bundle_seq_seen = ?,
                    rebuild_required = ?,
                    pending_initialization_id = ?
                WHERE singleton_key = 1
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
            st.step()
        }
    }

    suspend fun persistAnonymousState(
        sourceId: String,
        statementCache: StatementCache? = null,
    ) {
        persistState(
            state = OversqliteAttachmentState(
                currentSourceId = sourceId,
                bindingState = attachmentBindingAnonymous,
            ),
            statementCache = statementCache,
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
        )
    }

    suspend fun isAttachedTo(
        userId: String,
        sourceId: String,
    ): Boolean {
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
