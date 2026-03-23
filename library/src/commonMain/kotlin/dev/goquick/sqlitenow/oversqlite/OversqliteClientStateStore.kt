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

internal class OversqliteClientStateStore(
    private val db: SafeSQLiteConnection,
) {
    suspend fun loadLastBundleSeqSeen(userId: String): Long {
        return db.prepare(
            "SELECT last_bundle_seq_seen FROM _sync_client_state WHERE user_id = ?"
        ).use { st ->
            st.bindText(1, userId)
            if (!st.step()) error("_sync_client_state missing for user $userId")
            st.getLong(0)
        }
    }

    suspend fun loadNextSourceBundleId(userId: String): Long {
        return db.prepare(
            """
            SELECT next_source_bundle_id
            FROM _sync_client_state
            WHERE user_id = ?
            """.trimIndent()
        ).use { st ->
            st.bindText(1, userId)
            if (!st.step()) error("_sync_client_state missing for user $userId")
            st.getLong(0)
        }
    }

    suspend fun pendingDirtyCount(): Int {
        return db.prepare("SELECT COUNT(*) FROM _sync_dirty_rows").use { st ->
            check(st.step())
            st.getLong(0).toInt()
        }
    }

    suspend fun pendingPushOutboundCount(): Int {
        return db.prepare("SELECT COUNT(*) FROM _sync_push_outbound").use { st ->
            check(st.step())
            st.getLong(0).toInt()
        }
    }

    suspend fun isRebuildRequired(userId: String): Boolean {
        return db.prepare("SELECT rebuild_required FROM _sync_client_state WHERE user_id = ?").use { st ->
            st.bindText(1, userId)
            if (!st.step()) error("_sync_client_state missing for user $userId")
            st.getLong(0) == 1L
        }
    }

    suspend fun clearSnapshotStage() {
        db.execSQL("DELETE FROM _sync_snapshot_stage")
    }

    suspend fun clearPushStage() {
        db.execSQL("DELETE FROM _sync_push_stage")
    }

    suspend fun setApplyMode(
        userId: String,
        enabled: Boolean,
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = "UPDATE _sync_client_state SET apply_mode = ? WHERE user_id = ?",
            statementCache = statementCache,
        ) { st ->
            st.bindLong(1, if (enabled) 1 else 0)
            st.bindText(2, userId)
            st.step()
        }
    }

    suspend fun advanceAfterCommittedPush(
        userId: String,
        bundleSeq: Long,
        sourceBundleId: Long,
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = """
                UPDATE _sync_client_state
                SET last_bundle_seq_seen = CASE
                      WHEN last_bundle_seq_seen + 1 = ? THEN ?
                      ELSE last_bundle_seq_seen
                    END,
                    next_source_bundle_id = CASE
                      WHEN next_source_bundle_id <= ? THEN ?
                      ELSE next_source_bundle_id
                    END
                WHERE user_id = ?
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            st.bindLong(1, bundleSeq)
            st.bindLong(2, bundleSeq)
            st.bindLong(3, sourceBundleId)
            st.bindLong(4, sourceBundleId + 1)
            st.bindText(5, userId)
            st.step()
        }
    }

    suspend fun markBundleSeen(
        userId: String,
        bundleSeq: Long,
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = """
                UPDATE _sync_client_state
                SET last_bundle_seq_seen = CASE
                      WHEN last_bundle_seq_seen < ? THEN ?
                      ELSE last_bundle_seq_seen
                    END
                WHERE user_id = ?
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            st.bindLong(1, bundleSeq)
            st.bindLong(2, bundleSeq)
            st.bindText(3, userId)
            st.step()
        }
    }

    suspend fun markSnapshotApplied(
        userId: String,
        snapshotBundleSeq: Long,
        newSourceId: String?,
        statementCache: StatementCache? = null,
    ) {
        if (newSourceId != null) {
            db.withPreparedStatement(
                sql = """
                    UPDATE _sync_client_state
                    SET source_id = ?, next_source_bundle_id = 1, last_bundle_seq_seen = ?, rebuild_required = 0
                    WHERE user_id = ?
                """.trimIndent(),
                statementCache = statementCache,
            ) { st ->
                st.bindText(1, newSourceId)
                st.bindLong(2, snapshotBundleSeq)
                st.bindText(3, userId)
                st.step()
            }
            return
        }

        db.withPreparedStatement(
            sql = """
                UPDATE _sync_client_state
                SET last_bundle_seq_seen = ?, rebuild_required = 0
                WHERE user_id = ?
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            st.bindLong(1, snapshotBundleSeq)
            st.bindText(2, userId)
            st.step()
        }
    }
}
