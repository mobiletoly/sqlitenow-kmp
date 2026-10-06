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

// Canonical DDL is shared by initialization and retained-state admission.
internal val oversqliteRetainedTableDdl = mapOf(
    "_sync_push_plan" to pushPlanDdl,
    "_sync_push_nodes" to pushNodesDdl,
    "_sync_push_edges" to pushEdgesDdl,
    "_sync_apply_plan" to applyPlanDdl,
    "_sync_apply_rows" to applyRowsDdl,
    "_sync_apply_edges" to applyEdgesDdl,
    "_sync_apply_state" to """
        CREATE TABLE IF NOT EXISTS _sync_apply_state (
          singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
          apply_mode INTEGER NOT NULL DEFAULT 0
        )
    """.trimIndent(),
    "_sync_row_state" to authoritativeRowStateDdl,
    "_sync_relationships" to relationshipProjectionDdl,
    "_sync_dirty_rows" to """
        CREATE TABLE IF NOT EXISTS _sync_dirty_rows (
          schema_name TEXT NOT NULL,
          table_name TEXT NOT NULL,
          key_json TEXT NOT NULL,
          op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
          base_row_version INTEGER NOT NULL DEFAULT 0,
          base_kind TEXT NOT NULL CHECK(base_kind IN ('present','deleted','absent','unavailable')),
          base_payload TEXT,
          base_provenance TEXT NOT NULL CHECK(json_valid(base_provenance) AND json_type(base_provenance)='object'),
          payload TEXT,
          dirty_ordinal INTEGER NOT NULL DEFAULT 0,
          updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
          PRIMARY KEY (schema_name, table_name, key_json),
 CHECK(typeof(base_row_version)='integer' AND CASE base_kind WHEN 'present' THEN base_row_version>0 AND base_payload IS NOT NULL AND json_valid(base_payload) AND json_type(base_payload)='object' WHEN 'deleted' THEN base_row_version>0 AND base_payload IS NULL ELSE base_row_version=0 AND base_payload IS NULL END),
 CHECK((op='DELETE' AND payload IS NULL) OR (op!='DELETE' AND payload IS NOT NULL AND json_valid(payload) AND json_type(payload)='object'))
        )
    """.trimIndent(),
    "_sync_conflict_state" to """
        CREATE TABLE IF NOT EXISTS _sync_conflict_state (
          singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
          dirty_revision INTEGER NOT NULL DEFAULT 0 CHECK(typeof(dirty_revision)='integer' AND dirty_revision>=0 AND dirty_revision<9223372036854775807),
 baseline_revision INTEGER NOT NULL DEFAULT 0 CHECK(typeof(baseline_revision)='integer' AND baseline_revision>=0 AND baseline_revision<9223372036854775807),
          decision_state TEXT NOT NULL DEFAULT 'none' CHECK (decision_state IN ('none','pending')),
          source_id TEXT NOT NULL DEFAULT '',
          source_bundle_id INTEGER NOT NULL DEFAULT 0,
          canonical_request_hash TEXT NOT NULL DEFAULT '',
          generation_id INTEGER NOT NULL DEFAULT 0,
          generation_fingerprint TEXT NOT NULL DEFAULT '',
          conflict_json TEXT NOT NULL DEFAULT '',
          local_payload_json TEXT
        )
    """.trimIndent(),
    "_sync_snapshot_stage" to """
        CREATE TABLE IF NOT EXISTS _sync_snapshot_stage (
          snapshot_id TEXT NOT NULL,
          row_ordinal INTEGER NOT NULL,
          schema_name TEXT NOT NULL,
          table_name TEXT NOT NULL,
          key_json TEXT NOT NULL,
          row_version INTEGER NOT NULL,
          payload TEXT NOT NULL,
          charge INTEGER NOT NULL DEFAULT 0 CHECK(typeof(charge)='integer' AND charge>=0),
          PRIMARY KEY (snapshot_id, row_ordinal),
          UNIQUE (snapshot_id, schema_name, table_name, key_json)
        )
    """.trimIndent(),
    "_sync_source_state" to """
        CREATE TABLE IF NOT EXISTS _sync_source_state (
          source_id TEXT NOT NULL PRIMARY KEY,
          next_source_bundle_id INTEGER NOT NULL DEFAULT 1,
          replaced_by_source_id TEXT NOT NULL DEFAULT '',
          created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        )
    """.trimIndent(),
    "_sync_attachment_state" to """
        CREATE TABLE IF NOT EXISTS _sync_attachment_state (
          singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
          attachment_epoch INTEGER NOT NULL DEFAULT 1 CHECK(typeof(attachment_epoch)='integer' AND attachment_epoch > 0),
          current_source_id TEXT NOT NULL DEFAULT '',
          binding_state TEXT NOT NULL DEFAULT 'anonymous' CHECK (binding_state IN ('anonymous', 'attached')),
          attached_user_id TEXT NOT NULL DEFAULT '',
          schema_name TEXT NOT NULL DEFAULT '',
          last_bundle_seq_seen INTEGER NOT NULL DEFAULT 0,
          rebuild_required INTEGER NOT NULL DEFAULT 0,
          pending_initialization_id TEXT NOT NULL DEFAULT ''
        )
    """.trimIndent(),
    "_sync_operation_state" to """
        CREATE TABLE IF NOT EXISTS _sync_operation_state (
          singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
          kind TEXT NOT NULL DEFAULT 'none' CHECK (kind IN ('none', 'remote_replace', 'source_recovery')),
          target_user_id TEXT NOT NULL DEFAULT '',
          staged_snapshot_id TEXT NOT NULL DEFAULT '',
          snapshot_bundle_seq INTEGER NOT NULL DEFAULT 0,
          snapshot_row_count INTEGER NOT NULL DEFAULT 0,
          snapshot_byte_count INTEGER NOT NULL DEFAULT 0,
          snapshot_stage_complete INTEGER NOT NULL DEFAULT 0,
          snapshot_staged_row_count INTEGER NOT NULL DEFAULT 0 CHECK(typeof(snapshot_staged_row_count)='integer' AND snapshot_staged_row_count>=0),
          snapshot_staged_byte_count INTEGER NOT NULL DEFAULT 0 CHECK(typeof(snapshot_staged_byte_count)='integer' AND snapshot_staged_byte_count>=0),
          snapshot_stage_guard TEXT NOT NULL DEFAULT '',
          reason TEXT NOT NULL DEFAULT '',
          replacement_source_id TEXT NOT NULL DEFAULT '',
          workspace_context TEXT NOT NULL DEFAULT '',
          reserved_entries INTEGER NOT NULL DEFAULT 0 CHECK(typeof(reserved_entries)='integer' AND reserved_entries>=0),
          reserved_bytes INTEGER NOT NULL DEFAULT 0 CHECK(typeof(reserved_bytes)='integer' AND reserved_bytes>=0),
          snapshot_stage_entries INTEGER NOT NULL DEFAULT 0 CHECK(typeof(snapshot_stage_entries)='integer' AND snapshot_stage_entries>=0),
          snapshot_stage_bytes INTEGER NOT NULL DEFAULT 0 CHECK(typeof(snapshot_stage_bytes)='integer' AND snapshot_stage_bytes>=0),
          snapshot_cleanup_ordinal INTEGER NOT NULL DEFAULT 0 CHECK(typeof(snapshot_cleanup_ordinal)='integer' AND snapshot_cleanup_ordinal>=0)
        )
    """.trimIndent(),
    "_sync_outbox_bundle" to """
        CREATE TABLE IF NOT EXISTS _sync_outbox_bundle (
          singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
          canonical_json_contract TEXT NOT NULL CHECK (canonical_json_contract = 'jcs_uniform_numeric_strings_v1'),
          state TEXT NOT NULL DEFAULT 'none' CHECK (state IN ('none', 'prepared')),
          source_id TEXT NOT NULL DEFAULT '',
          source_bundle_id INTEGER NOT NULL DEFAULT 0,
          initialization_id TEXT NOT NULL DEFAULT '',
          canonical_request_hash TEXT NOT NULL DEFAULT '',
          row_count INTEGER NOT NULL DEFAULT 0
        )
    """.trimIndent(),
    "_sync_outbox_rows" to """
        CREATE TABLE IF NOT EXISTS _sync_outbox_rows (
          source_bundle_id INTEGER NOT NULL,
          row_ordinal INTEGER NOT NULL,
          schema_name TEXT NOT NULL,
          table_name TEXT NOT NULL,
          key_json TEXT NOT NULL,
          wire_key_json TEXT NOT NULL,
          op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
          base_row_version INTEGER NOT NULL DEFAULT 0,
          base_kind TEXT NOT NULL CHECK(base_kind IN ('present','deleted','absent','unavailable')),
          base_payload TEXT,
          base_provenance TEXT NOT NULL CHECK(json_valid(base_provenance) AND json_type(base_provenance)='object'),
          local_payload TEXT,
          wire_payload TEXT,
          PRIMARY KEY (source_bundle_id, row_ordinal),
 UNIQUE(source_bundle_id,schema_name,table_name,key_json),
 CHECK(typeof(base_row_version)='integer' AND CASE base_kind WHEN 'present' THEN base_row_version>0 AND base_payload IS NOT NULL AND json_valid(base_payload) AND json_type(base_payload)='object' WHEN 'deleted' THEN base_row_version>0 AND base_payload IS NULL ELSE base_row_version=0 AND base_payload IS NULL END),
 CHECK((op='DELETE' AND local_payload IS NULL) OR (op!='DELETE' AND local_payload IS NOT NULL AND json_valid(local_payload) AND json_type(local_payload)='object'))
        )
    """.trimIndent(),
    "_sync_managed_tables" to """
        CREATE TABLE IF NOT EXISTS _sync_managed_tables (
          schema_name TEXT NOT NULL,
          table_name TEXT NOT NULL,
          PRIMARY KEY (schema_name, table_name)
        )
    """.trimIndent(),
    "_sync_contract_identity" to """
            CREATE TABLE _sync_contract_identity (
                singleton_key INTEGER PRIMARY KEY CHECK(singleton_key=1),
                database_id TEXT NOT NULL, service_id TEXT NOT NULL,
                generation_id INTEGER NOT NULL, local_version INTEGER NOT NULL,
                physical_shape TEXT NOT NULL, fingerprint TEXT NOT NULL DEFAULT ''
            )
    """.trimIndent(),
    "_sync_admitted_contracts" to """
            CREATE TABLE _sync_admitted_contracts (
                generation_id INTEGER PRIMARY KEY, fingerprint TEXT NOT NULL,
                descriptor TEXT NOT NULL, descriptor_hash TEXT NOT NULL
            )
    """.trimIndent(),
)
