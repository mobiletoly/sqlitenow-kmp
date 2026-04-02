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
    @SerialName("source_bundle_id") val sourceBundleId: Long,
    @SerialName("planned_row_count") val plannedRowCount: Long,
    @SerialName("initialization_id") val initializationId: String? = null,
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
data class ConnectRequest(
    @SerialName("has_local_pending_rows") val hasLocalPendingRows: Boolean,
)

@Serializable
data class ConnectResponse(
    val resolution: String,
    @SerialName("initialization_id") val initializationId: String = "",
    @SerialName("lease_expires_at") val leaseExpiresAt: String = "",
    @SerialName("retry_after_seconds") val retryAfterSeconds: Int = 0,
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
data class CapabilitiesResponse(
    @SerialName("protocol_version") val protocolVersion: String = "",
    @SerialName("schema_version") val schemaVersion: Int = 0,
    val features: Map<String, Boolean> = emptyMap(),
    @SerialName("bundle_limits") val bundleLimits: BundleCapabilitiesLimits? = null,
)

@Serializable
data class BundleCapabilitiesLimits(
    @SerialName("max_rows_per_bundle") val maxRowsPerBundle: Int = 0,
    @SerialName("max_bytes_per_bundle") val maxBytesPerBundle: Int = 0,
    @SerialName("max_bundles_per_pull") val maxBundlesPerPull: Int = 0,
    @SerialName("default_rows_per_push_chunk") val defaultRowsPerPushChunk: Int = 0,
    @SerialName("max_rows_per_push_chunk") val maxRowsPerPushChunk: Int = 0,
    @SerialName("push_session_ttl_seconds") val pushSessionTtlSeconds: Int = 0,
    @SerialName("default_rows_per_committed_bundle_chunk") val defaultRowsPerCommittedBundleChunk: Int = 0,
    @SerialName("max_rows_per_committed_bundle_chunk") val maxRowsPerCommittedBundleChunk: Int = 0,
    @SerialName("default_rows_per_snapshot_chunk") val defaultRowsPerSnapshotChunk: Int = 0,
    @SerialName("max_rows_per_snapshot_chunk") val maxRowsPerSnapshotChunk: Int = 0,
    @SerialName("snapshot_session_ttl_seconds") val snapshotSessionTtlSeconds: Int = 0,
    @SerialName("max_rows_per_snapshot_session") val maxRowsPerSnapshotSession: Long = 0,
    @SerialName("max_bytes_per_snapshot_session") val maxBytesPerSnapshotSession: Long = 0,
    @SerialName("initialization_lease_ttl_seconds") val initializationLeaseTtlSeconds: Int = 0,
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

/**
 * Static oversqlite client configuration.
 *
 * This config is lifecycle-neutral: it describes the local schema and runtime behavior, but it
 * does not bind a user or source identity up front.
 */
data class OversqliteConfig(
    val schema: String,
    val syncTables: List<SyncTable>,
    val uploadLimit: Int = 200,
    val downloadLimit: Int = 1000,
    val snapshotChunkRows: Int = 1000,
    val verboseLogs: Boolean = false,
    val transientRetryPolicy: OversqliteTransientRetryPolicy = OversqliteTransientRetryPolicy(),
)

/**
 * Declares one sync-managed local table.
 *
 * Exactly one visible local primary-key column must be exposed as the sync key, either via
 * [syncKeyColumnName] or [syncKeyColumns]. The runtime currently expects exactly one key column.
 */
data class SyncTable(
    val tableName: String,
    val syncKeyColumnName: String? = null,
    val syncKeyColumns: List<String> = emptyList(),
)

/** Bounded transient retry policy for transport/availability failures. */
data class OversqliteTransientRetryPolicy(
    val maxAttempts: Int = 3,
    val initialBackoffMillis: Long = 150,
    val maxBackoffMillis: Long = 1_500,
    val jitterRatio: Double = 0.2,
)
