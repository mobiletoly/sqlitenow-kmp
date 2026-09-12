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

internal suspend fun initializeOversqliteControlTables(db: SafeSQLiteConnection) {
    db.execSQL("PRAGMA foreign_keys = ON")
    db.execSQL(pushPlanDdl)
    db.execSQL(pushNodesDdl)
    db.execSQL(pushEdgesDdl)
    pushIndexDdl.values.forEach { db.execSQL(it) }
    db.execSQL(applyPlanDdl)
    db.execSQL(applyRowsDdl)
    db.execSQL(applyEdgesDdl)
    db.execSQL(applyPendingIndexDdl)
    db.execSQL(applyRoleIndexDdl)
    db.execSQL(createOversqliteOpeningTableSql)
    db.execSQL(
        oversqliteRetainedTableDdl.getValue("_sync_apply_state")
    )
    db.execSQL(
        """
        INSERT INTO _sync_apply_state(singleton_key, apply_mode)
        VALUES(1, 0)
        ON CONFLICT(singleton_key) DO NOTHING
        """.trimIndent(),
    )
    db.execSQL(
        oversqliteRetainedTableDdl.getValue("_sync_row_state")
    )
    db.execSQL(
        oversqliteRetainedTableDdl.getValue("_sync_dirty_rows")
    )
    db.execSQL(relationshipProjectionDdl)
    baselineIndexDdl.values.forEach { db.execSQL(it) }
    db.execSQL(
        oversqliteRetainedTableDdl.getValue("_sync_conflict_state")
    )
    db.execSQL(
        """
        INSERT INTO _sync_conflict_state(singleton_key)
        VALUES(1)
        ON CONFLICT(singleton_key) DO NOTHING
        """.trimIndent()
    )
    for (operation in listOf("insert", "update", "delete")) {
        db.execSQL(expectedConflictRevisionTriggerSql(operation))
        db.execSQL(expectedBaselineRevisionTriggerSql(operation))
    }
    db.execSQL(
        oversqliteRetainedTableDdl.getValue("_sync_snapshot_stage")
    )
    db.execSQL(
        oversqliteRetainedTableDdl.getValue("_sync_source_state")
    )
    db.execSQL(
        oversqliteRetainedTableDdl.getValue("_sync_attachment_state")
    )
    db.execSQL(
        """
        INSERT INTO _sync_attachment_state(
          singleton_key,
          current_source_id,
          binding_state,
          attached_user_id,
          schema_name,
          last_bundle_seq_seen,
          rebuild_required,
          pending_initialization_id
        )
        VALUES(1, '', 'anonymous', '', '', 0, 0, '')
        ON CONFLICT(singleton_key) DO NOTHING
        """.trimIndent()
    )
    db.execSQL(
        oversqliteRetainedTableDdl.getValue("_sync_operation_state")
    )
    db.execSQL(
        """
        INSERT INTO _sync_operation_state(
          singleton_key,
          kind,
          target_user_id,
          staged_snapshot_id,
          snapshot_bundle_seq,
          snapshot_row_count,
          snapshot_byte_count,
          snapshot_stage_complete,
          reason,
          replacement_source_id
        )
        VALUES(1, 'none', '', '', 0, 0, 0, 0, '', '')
        ON CONFLICT(singleton_key) DO NOTHING
        """.trimIndent()
    )
    snapshotStageAccountingDdl.values.forEach { db.execSQL(it) }
    db.execSQL(
        oversqliteRetainedTableDdl.getValue("_sync_outbox_bundle")
    )
    db.execSQL(
        """
        INSERT INTO _sync_outbox_bundle(
          singleton_key,
          canonical_json_contract,
          state,
          source_id,
          source_bundle_id,
          initialization_id,
          canonical_request_hash,
          row_count
        )
        VALUES(1, 'jcs_uniform_numeric_strings_v1', 'none', '', 0, '', '', 0)
        ON CONFLICT(singleton_key) DO NOTHING
        """.trimIndent()
    )
    db.execSQL(
        oversqliteRetainedTableDdl.getValue("_sync_outbox_rows")
    )
    db.execSQL(
        oversqliteRetainedTableDdl.getValue("_sync_managed_tables"),
    )
    db.execSQL("UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1")
}
