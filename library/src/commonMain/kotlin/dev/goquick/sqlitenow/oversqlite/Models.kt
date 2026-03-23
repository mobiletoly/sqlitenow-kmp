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

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement

typealias SyncKey = Map<String, String>

@Serializable
data class BundleRow(
    val schema: String,
    val table: String,
    val key: SyncKey,
    val op: String,
    @SerialName("row_version") val rowVersion: Long,
    val payload: JsonElement? = null,
)

@Serializable
data class Bundle(
    @SerialName("bundle_seq") val bundleSeq: Long,
    @SerialName("source_id") val sourceId: String,
    @SerialName("source_bundle_id") val sourceBundleId: Long,
    val rows: List<BundleRow>,
)

@Serializable
data class PushRequestRow(
    val schema: String,
    val table: String,
    val key: SyncKey,
    val op: String,
    @SerialName("base_row_version") val baseRowVersion: Long,
    val payload: JsonElement? = null,
)

@Serializable
data class PushSessionCreateRequest(
    @SerialName("source_id") val sourceId: String,
    @SerialName("source_bundle_id") val sourceBundleId: Long,
    @SerialName("planned_row_count") val plannedRowCount: Long,
)

@Serializable
data class PushSessionCreateResponse(
    @SerialName("push_id") val pushId: String = "",
    val status: String,
    @SerialName("planned_row_count") val plannedRowCount: Long = 0,
    @SerialName("next_expected_row_ordinal") val nextExpectedRowOrdinal: Long = 0,
    @SerialName("bundle_seq") val bundleSeq: Long = 0,
    @SerialName("source_id") val sourceId: String = "",
    @SerialName("source_bundle_id") val sourceBundleId: Long = 0,
    @SerialName("row_count") val rowCount: Long = 0,
    @SerialName("bundle_hash") val bundleHash: String = "",
)

@Serializable
data class PushSessionChunkRequest(
    @SerialName("start_row_ordinal") val startRowOrdinal: Long,
    val rows: List<PushRequestRow>,
)

@Serializable
data class PushSessionChunkResponse(
    @SerialName("push_id") val pushId: String,
    @SerialName("next_expected_row_ordinal") val nextExpectedRowOrdinal: Long,
)

@Serializable
data class PushSessionCommitResponse(
    @SerialName("bundle_seq") val bundleSeq: Long,
    @SerialName("source_id") val sourceId: String,
    @SerialName("source_bundle_id") val sourceBundleId: Long,
    @SerialName("row_count") val rowCount: Long,
    @SerialName("bundle_hash") val bundleHash: String,
)

@Serializable
data class CommittedBundleRowsResponse(
    @SerialName("bundle_seq") val bundleSeq: Long,
    @SerialName("source_id") val sourceId: String,
    @SerialName("source_bundle_id") val sourceBundleId: Long,
    @SerialName("row_count") val rowCount: Long,
    @SerialName("bundle_hash") val bundleHash: String,
    val rows: List<BundleRow>,
    @SerialName("next_row_ordinal") val nextRowOrdinal: Long,
    @SerialName("has_more") val hasMore: Boolean,
)

@Serializable
data class PullResponse(
    @SerialName("stable_bundle_seq") val stableBundleSeq: Long,
    val bundles: List<Bundle>,
    @SerialName("has_more") val hasMore: Boolean,
)

@Serializable
data class SnapshotRow(
    val schema: String,
    val table: String,
    val key: SyncKey,
    @SerialName("row_version") val rowVersion: Long,
    val payload: JsonElement,
)

@Serializable
data class SnapshotSession(
    @SerialName("snapshot_id") val snapshotId: String,
    @SerialName("snapshot_bundle_seq") val snapshotBundleSeq: Long,
    @SerialName("row_count") val rowCount: Long,
    @SerialName("byte_count") val byteCount: Long = 0,
    @SerialName("expires_at") val expiresAt: String,
)

@Serializable
data class SnapshotChunkResponse(
    @SerialName("snapshot_id") val snapshotId: String,
    @SerialName("snapshot_bundle_seq") val snapshotBundleSeq: Long,
    val rows: List<SnapshotRow>,
    @SerialName("next_row_ordinal") val nextRowOrdinal: Long,
    @SerialName("has_more") val hasMore: Boolean,
)

@Serializable
data class ErrorResponse(
    val error: String,
    val message: String,
)

@Serializable
data class PushConflictDetails(
    val schema: String,
    val table: String,
    val key: SyncKey,
    val op: String,
    @SerialName("base_row_version") val baseRowVersion: Long,
    @SerialName("server_row_version") val serverRowVersion: Long,
    @SerialName("server_row_deleted") val serverRowDeleted: Boolean,
    @SerialName("server_row") val serverRow: JsonElement? = null,
)

@Serializable
data class PushConflictResponse(
    val error: String,
    val message: String,
    val conflict: PushConflictDetails? = null,
)

data class OversqliteConfig(
    val schema: String,
    val syncTables: List<SyncTable>,
    val uploadLimit: Int = 200,
    val downloadLimit: Int = 1000,
    val snapshotChunkRows: Int = 1000,
    val verboseLogs: Boolean = false,
)

data class SyncTable(
    val tableName: String,
    val syncKeyColumnName: String? = null,
    val syncKeyColumns: List<String> = emptyList(),
)
