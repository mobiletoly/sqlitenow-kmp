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
    @SerialName("bundle_hash") val bundleHash: String,
    val rows: List<BundleRow>,
)

@Serializable
data class BundleChangeEvent(
    @SerialName("bundle_seq") val bundleSeq: Long,
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
internal data class CompletePushRequest(
    @SerialName("source_bundle_id") val sourceBundleId: Long,
    @SerialName("canonical_request_hash") val canonicalRequestHash: String,
    @SerialName("initialization_id") val initializationId: String? = null,
    val rows: JsonElement,
)

@Serializable
internal data class CompletePushResponseWire(
    @SerialName("bundle_seq") val bundleSeq: Long,
    @SerialName("bundle_hash") val bundleHash: String,
    val rows: JsonElement,
)

internal data class CompletePushResult(
    val bundleSeq: Long,
    val bundleHash: String,
    val rows: List<BundleRow>,
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
data class PullResponse(
    @SerialName("stable_bundle_seq") val stableBundleSeq: Long,
    val bundles: List<Bundle>,
    @SerialName("has_more") val hasMore: Boolean,
)

@Serializable
internal data class AdmittedOversqliteContract(
    @SerialName("protocol_version") val protocolVersion: String,
    @SerialName("schema_generation") val generation: SyncGeneration,
    val limits: OversqliteWireLimits,
    @SerialName("watch_enabled") val watchEnabled: Boolean,
)

/** One server-advertised sync table and its ordered wire key-column contract. */
@Serializable
internal data class RegisteredTableSpec(
    val schema: String,
    val table: String,
    @SerialName("sync_key_columns") val syncKeyColumns: List<String>,
)

@Serializable
internal data class OversqliteWireLimits(
    @SerialName("max_push_rows") val maxPushRows: Int,
    @SerialName("max_push_decoded_bytes") val maxPushDecodedBytes: Long,
    @SerialName("max_pull_bundles") val maxPullBundles: Int,
    @SerialName("max_pull_decoded_bytes") val maxPullDecodedBytes: Long,
    @SerialName("max_snapshot_rows_per_chunk") val maxSnapshotRowsPerChunk: Int,
    @SerialName("max_snapshot_decoded_bytes_per_chunk") val maxSnapshotDecodedBytesPerChunk: Long,
    @SerialName("max_snapshot_rows") val maxSnapshotRows: Long,
    @SerialName("max_snapshot_logical_bytes") val maxSnapshotLogicalBytes: Long,
    @SerialName("max_row_decoded_bytes") val maxRowDecodedBytes: Long,
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
    @SerialName("logical_byte_count") val logicalByteCount: Long,
    @SerialName("expires_at") val expiresAt: String,
)

@Serializable
data class SnapshotSessionCreateRequest(
    @SerialName("source_replacement") val sourceReplacement: SnapshotSourceReplacement? = null,
)

@Serializable
data class SnapshotSourceReplacement(
    @SerialName("previous_source_id") val previousSourceId: String,
    @SerialName("new_source_id") val newSourceId: String,
    val reason: String,
)

@Serializable
data class SnapshotChunkResponse(
    val rows: List<SnapshotRow>,
    @SerialName("next_row_ordinal") val nextRowOrdinal: Long,
    @SerialName("has_more") val hasMore: Boolean,
    @SerialName("logical_byte_count") val logicalByteCount: Long,
)

@Serializable
data class ErrorResponse(
    val error: String,
    val message: String,
    @SerialName("required_decoded_body_bytes") val requiredDecodedBodyBytes: Long? = null,
)

@Serializable
data class SourceRetiredResponse(
    val error: String,
    val message: String,
    @SerialName("source_id") val sourceId: String,
    @SerialName("replaced_by_source_id") val replacedBySourceId: String? = null,
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

/** A durable conflict observation bound to one frozen outbound request. */
data class PendingConflict(
    val revision: Long,
    val baselineRevision: Long = 0,
    val sourceId: String,
    val sourceBundleId: Long,
    val canonicalRequestHash: String,
    val generationId: Long,
    val generationFingerprint: String,
    val conflict: ConflictContext,
)

/**
 * Static oversqlite client configuration.
 *
 * This config is lifecycle-neutral: it describes the local schema and runtime behavior, but it
 * does not bind a user or source identity up front.
 */
data class OversqliteConfig(
    val generationId: Long,
    val syncTables: List<SyncTable>,
    val databaseId: String,
    val serviceId: String,
    val maxSyncWorkspaceEntries: Long = 131_072,
    val maxSyncWorkspaceBytes: Long = 256L * 1024 * 1024,
    val maxSyncWorkUnitsPerInvocation: Long = 1_000_000,
    val transferTargets: OversqliteTransferTargets = OversqliteTransferTargets(),
    val snapshotApplyBatchRows: Int = 256,
    val snapshotApplyBatchBytes: Long = 4L * 1024L * 1024L,
    val verboseLogs: Boolean = false,
    val transientRetryPolicy: OversqliteTransientRetryPolicy = OversqliteTransientRetryPolicy(),
    val snapshotCapacityRetryPolicy: OversqliteSnapshotCapacityRetryPolicy =
        OversqliteSnapshotCapacityRetryPolicy(),
) {
    init {
        require(maxSyncWorkspaceEntries > 0 && maxSyncWorkspaceBytes > 0 && maxSyncWorkUnitsPerInvocation > 0) {
            "sync workspace limits must be positive"
        }
        require(generationId in 1L..Int.MAX_VALUE.toLong()) { "generationId must be in 1..2147483647" }
        require(databaseId.isNotBlank() && serviceId.isNotBlank()) { "databaseId and serviceId must be provided" }
        require(snapshotApplyBatchRows > 0) { "snapshotApplyBatchRows must be positive" }
        require(snapshotApplyBatchBytes > 0) { "snapshotApplyBatchBytes must be positive" }
    }
}

/** Local push row and byte targets are soft; complete FK groups may cross them within admitted hard limits. */
data class OversqliteTransferTargets(
    val maxPushRows: Int = 200,
    val maxPushDecodedBytes: Long = 1L * 1024L * 1024L,
    val maxPullBundles: Int = 1000,
    val maxSnapshotChunkRows: Int = 1000,
    val maxSnapshotChunkDecodedBytes: Long = 4L * 1024L * 1024L,
) {
    init {
        require(maxPushRows > 0) { "maxPushRows must be positive" }
        require(maxPushDecodedBytes > 0) { "maxPushDecodedBytes must be positive" }
        require(maxPullBundles > 0) { "maxPullBundles must be positive" }
        require(maxSnapshotChunkRows > 0) { "maxSnapshotChunkRows must be positive" }
        require(maxSnapshotChunkDecodedBytes > 0) { "maxSnapshotChunkDecodedBytes must be positive" }
    }
}

enum class BundleChangeWatchMode {
    OFF,
    AUTO,
}

/**
 * Opt-in automatic download worker configuration.
 *
 * This is separate from [OversqliteConfig] so existing constructor, copy, and component call sites
 * remain source and binary compatible.
 */
data class OversqliteAutomaticDownloadConfig(
    val automaticDownloadIntervalMillis: Long = 60_000,
    val bundleChangeWatchMode: BundleChangeWatchMode = BundleChangeWatchMode.OFF,
    val bundleChangeWatchReconnectMinMillis: Long = 1_000,
    val bundleChangeWatchReconnectMaxMillis: Long = 60_000,
) {
    init {
        require(automaticDownloadIntervalMillis > 0) {
            "automaticDownloadIntervalMillis must be positive"
        }
        require(bundleChangeWatchReconnectMinMillis > 0) {
            "bundleChangeWatchReconnectMinMillis must be positive"
        }
        require(bundleChangeWatchReconnectMaxMillis > 0) {
            "bundleChangeWatchReconnectMaxMillis must be positive"
        }
        require(bundleChangeWatchReconnectMaxMillis >= bundleChangeWatchReconnectMinMillis) {
            "bundleChangeWatchReconnectMaxMillis must be greater than or equal to bundleChangeWatchReconnectMinMillis"
        }
    }
}

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

/** Elapsed-time retry policy for snapshot build/chunk admission capacity. */
data class OversqliteSnapshotCapacityRetryPolicy(
    val enabled: Boolean = true,
    val maxWaitMillis: Long = 30_000,
    val fallbackDelayMillis: Long = 1_000,
    val jitterRatio: Double = 1.0,
) {
    init {
        require(maxWaitMillis > 0) { "maxWaitMillis must be positive" }
        require(fallbackDelayMillis > 0) { "fallbackDelayMillis must be positive" }
        require(jitterRatio in 0.0..1.0) { "jitterRatio must be between 0 and 1" }
    }
}
