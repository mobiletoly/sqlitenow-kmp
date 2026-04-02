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

internal data class OversqliteSourceState(
    val sourceId: String,
    val nextSourceBundleId: Long,
    val replacedBySourceId: String,
    val createdAt: String,
)

internal class OversqliteSourceStateStore(
    internal val db: SafeSQLiteConnection,
) {
    suspend fun loadState(sourceId: String): OversqliteSourceState? {
        return db.withExclusiveAccess {
            db.prepare(
                """
                SELECT source_id, next_source_bundle_id, replaced_by_source_id, created_at
                FROM _sync_source_state
                WHERE source_id = ?
                """.trimIndent(),
            ).use { st ->
                st.bindText(1, sourceId)
                if (!st.step()) {
                    null
                } else {
                    OversqliteSourceState(
                        sourceId = st.getText(0),
                        nextSourceBundleId = st.getLong(1),
                        replacedBySourceId = st.getText(2),
                        createdAt = st.getText(3),
                    )
                }
            }
        }
    }

    suspend fun ensureSource(sourceId: String, initialNextSourceBundleId: Long = 1L) {
        db.withExclusiveAccess {
            db.prepare(
                """
                INSERT INTO _sync_source_state(source_id, next_source_bundle_id, replaced_by_source_id)
                VALUES(?, ?, '')
                ON CONFLICT(source_id) DO NOTHING
                """.trimIndent(),
            ).use { st ->
                st.bindText(1, sourceId)
                st.bindLong(2, initialNextSourceBundleId)
                st.step()
            }
        }
    }

    suspend fun upsertNextSourceBundleIdFloor(sourceId: String, nextSourceBundleId: Long) {
        db.withExclusiveAccess {
            db.prepare(
                """
                INSERT INTO _sync_source_state(source_id, next_source_bundle_id, replaced_by_source_id)
                VALUES(?, ?, '')
                ON CONFLICT(source_id) DO UPDATE SET
                  next_source_bundle_id = CASE
                    WHEN _sync_source_state.next_source_bundle_id < excluded.next_source_bundle_id
                    THEN excluded.next_source_bundle_id
                    ELSE _sync_source_state.next_source_bundle_id
                  END
                """.trimIndent(),
            ).use { st ->
                st.bindText(1, sourceId)
                st.bindLong(2, nextSourceBundleId)
                st.step()
            }
        }
    }

    suspend fun markRotated(
        previousSourceId: String,
        replacementSourceId: String,
    ) {
        db.withExclusiveAccess {
            db.prepare(
                """
                UPDATE _sync_source_state
                SET replaced_by_source_id = ?
                WHERE source_id = ?
                """.trimIndent(),
            ).use { st ->
                st.bindText(1, replacementSourceId)
                st.bindText(2, previousSourceId)
                st.step()
            }
        }
    }

    suspend fun loadNextSourceBundleId(sourceId: String): Long {
        ensureSource(sourceId)
        return loadState(sourceId)?.nextSourceBundleId
            ?: error("_sync_source_state missing for source $sourceId")
    }

    suspend fun advanceAfterCommittedPush(
        sourceId: String,
        sourceBundleId: Long,
    ) {
        upsertNextSourceBundleIdFloor(sourceId, sourceBundleId + 1)
    }
}
