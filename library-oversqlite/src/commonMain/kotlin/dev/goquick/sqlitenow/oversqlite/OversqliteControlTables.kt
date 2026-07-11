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
    db.execSQL(
        """
        CREATE TABLE IF NOT EXISTS _sync_apply_state (
          singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
          apply_mode INTEGER NOT NULL DEFAULT 0
        )
        """.trimIndent()
    )
    db.execSQL(
        """
        INSERT INTO _sync_apply_state(singleton_key, apply_mode)
        VALUES(1, 0)
        ON CONFLICT(singleton_key) DO NOTHING
        """.trimIndent(),
    )
    db.execSQL(
        """
        CREATE TABLE IF NOT EXISTS _sync_row_state (
          schema_name TEXT NOT NULL,
          table_name TEXT NOT NULL,
          key_json TEXT NOT NULL,
          row_version INTEGER NOT NULL DEFAULT 0,
          deleted INTEGER NOT NULL DEFAULT 0,
          updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
          PRIMARY KEY (schema_name, table_name, key_json)
        )
        """.trimIndent()
    )
    db.execSQL(
        """
        CREATE TABLE IF NOT EXISTS _sync_dirty_rows (
          schema_name TEXT NOT NULL,
          table_name TEXT NOT NULL,
          key_json TEXT NOT NULL,
          op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
          base_row_version INTEGER NOT NULL DEFAULT 0,
          payload TEXT,
          dirty_ordinal INTEGER NOT NULL DEFAULT 0,
          updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
          PRIMARY KEY (schema_name, table_name, key_json)
        )
        """.trimIndent()
    )
    db.execSQL("CREATE INDEX IF NOT EXISTS idx_sync_dirty_rows_dirty_ordinal ON _sync_dirty_rows(dirty_ordinal)")
    db.execSQL(
        """
        CREATE TABLE IF NOT EXISTS _sync_snapshot_stage (
          snapshot_id TEXT NOT NULL,
          row_ordinal INTEGER NOT NULL,
          schema_name TEXT NOT NULL,
          table_name TEXT NOT NULL,
          key_json TEXT NOT NULL,
          row_version INTEGER NOT NULL,
          payload TEXT NOT NULL,
          PRIMARY KEY (snapshot_id, row_ordinal)
        )
        """.trimIndent()
    )
    db.execSQL(
        """
        CREATE TABLE IF NOT EXISTS _sync_source_state (
          source_id TEXT NOT NULL PRIMARY KEY,
          next_source_bundle_id INTEGER NOT NULL DEFAULT 1,
          replaced_by_source_id TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        )
        """.trimIndent()
    )
    db.execSQL(
        """
        CREATE TABLE IF NOT EXISTS _sync_attachment_state (
          singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
          current_source_id TEXT NOT NULL DEFAULT '',
          binding_state TEXT NOT NULL DEFAULT 'anonymous' CHECK (binding_state IN ('anonymous', 'attached')),
          attached_user_id TEXT NOT NULL DEFAULT '',
          schema_name TEXT NOT NULL DEFAULT '',
          last_bundle_seq_seen INTEGER NOT NULL DEFAULT 0,
          rebuild_required INTEGER NOT NULL DEFAULT 0,
          pending_initialization_id TEXT NOT NULL DEFAULT ''
        )
        """.trimIndent()
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
        """
        CREATE TABLE IF NOT EXISTS _sync_operation_state (
          singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
          kind TEXT NOT NULL DEFAULT 'none' CHECK (kind IN ('none', 'remote_replace', 'source_recovery')),
          target_user_id TEXT NOT NULL DEFAULT '',
          staged_snapshot_id TEXT NOT NULL DEFAULT '',
          snapshot_bundle_seq INTEGER NOT NULL DEFAULT 0,
          snapshot_row_count INTEGER NOT NULL DEFAULT 0,
          reason TEXT NOT NULL DEFAULT '',
          replacement_source_id TEXT NOT NULL DEFAULT ''
        )
        """.trimIndent()
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
          reason,
          replacement_source_id
        )
        VALUES(1, 'none', '', '', 0, 0, '', '')
        ON CONFLICT(singleton_key) DO NOTHING
        """.trimIndent()
    )
    db.execSQL(
        """
        CREATE TABLE IF NOT EXISTS _sync_outbox_bundle (
          singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
          canonical_json_contract TEXT NOT NULL CHECK (canonical_json_contract = 'jcs_typed_numeric_strings_v0'),
          state TEXT NOT NULL DEFAULT 'none' CHECK (state IN ('none', 'prepared', 'committed_remote')),
          source_id TEXT NOT NULL DEFAULT '',
          source_bundle_id INTEGER NOT NULL DEFAULT 0,
          initialization_id TEXT NOT NULL DEFAULT '',
          canonical_request_hash TEXT NOT NULL DEFAULT '',
          row_count INTEGER NOT NULL DEFAULT 0,
          remote_bundle_hash TEXT NOT NULL DEFAULT '',
          remote_bundle_seq INTEGER NOT NULL DEFAULT 0
        )
        """.trimIndent()
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
          row_count,
          remote_bundle_hash,
          remote_bundle_seq
        )
        VALUES(1, 'jcs_typed_numeric_strings_v0', 'none', '', 0, '', '', 0, '', 0)
        ON CONFLICT(singleton_key) DO NOTHING
        """.trimIndent()
    )
    db.execSQL(
        """
        CREATE TABLE IF NOT EXISTS _sync_outbox_rows (
          source_bundle_id INTEGER NOT NULL,
          row_ordinal INTEGER NOT NULL,
          schema_name TEXT NOT NULL,
          table_name TEXT NOT NULL,
          key_json TEXT NOT NULL,
          wire_key_json TEXT NOT NULL,
          op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
          base_row_version INTEGER NOT NULL DEFAULT 0,
          local_payload TEXT,
          wire_payload TEXT,
          PRIMARY KEY (source_bundle_id, row_ordinal)
        )
        """.trimIndent()
    )
    db.execSQL(
        """
        CREATE TABLE IF NOT EXISTS _sync_managed_tables (
          schema_name TEXT NOT NULL,
          table_name TEXT NOT NULL,
          PRIMARY KEY (schema_name, table_name)
        )
        """.trimIndent(),
    )
    db.execSQL("UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1")
}
