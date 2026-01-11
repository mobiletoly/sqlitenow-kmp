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

/**
 * UploadRequest mirrors the server’s POST /sync/upload shape.
 */
@Serializable
data class UploadRequest(
    @SerialName("last_server_seq_seen") val lastServerSeqSeen: Long,
    val changes: List<ChangeUpload>
)

/**
 * ChangeUpload represents a single upsert/delete operation with optimistic concurrency (serverVersion).
 * Use schema to target the business schema registered on the server.
 */
@Serializable
data class ChangeUpload(
    @SerialName("source_change_id") val sourceChangeId: Long,
    val schema: String? = null,
    val table: String,
    val op: String, // INSERT, UPDATE, DELETE
    val pk: String,
    @SerialName("server_version") val serverVersion: Long,
    val payload: JsonElement? = null
)

/**
 * UploadResponse reports per‑change outcomes: applied, conflict, invalid, or materialize_error.
 */
@Serializable
data class UploadResponse(
    val accepted: Boolean,
    @SerialName("highest_server_seq") val highestServerSeq: Long, // per-user watermark (do not use as download cursor)
    val statuses: List<ChangeUploadStatus>
)

/**
 * ChangeUploadStatus contains the per‑item status and optional details (serverRow/invalid).
 */
@Serializable
data class ChangeUploadStatus(
    @SerialName("source_change_id") val sourceChangeId: Long,
    val status: String,
    @SerialName("new_server_version") val newServerVersion: Long? = null,
    @SerialName("server_row") val serverRow: JsonElement? = null,
    val message: String? = null,
    val invalid: JsonElement? = null
)

/**
 * UploadSummary aggregates the outcome of an upload batch.
 */
@Serializable
data class UploadSummary(
    val total: Int,
    val applied: Int,
    val conflict: Int,
    val invalid: Int,
    @SerialName("materialize_error") val materializeError: Int,
    @SerialName("invalid_reasons") val invalidReasons: Map<String, Int> = emptyMap(),
    val firstErrorMessage: String? = null,
)

/**
 * Canonical string constants for invalid reasons returned by the server in ChangeUploadStatus.invalid.reason.
 * Use these to branch on specific categories without relying on ad-hoc string comparisons.
 */
object InvalidReasons {
    const val FK_MISSING: String = "fk_missing"
    const val BAD_PAYLOAD: String = "bad_payload"
    const val PRECHECK_ERROR: String = "precheck_error"
    const val INTERNAL_ERROR: String = "internal_error"
    const val UNREGISTERED_TABLE: String = "unregistered_table"
    const val BATCH_TOO_LARGE: String = "batch_too_large"
}

/**
 * DownloadResponse mirrors the server’s GET /sync/download result.
 * Clients apply changes in `server_id` order and advance `next_after` atomically.
 */
@Serializable
data class DownloadResponse(
    val changes: List<ChangeDownloadResponse>,
    @SerialName("has_more") val hasMore: Boolean,
    @SerialName("next_after") val nextAfter: Long,
    @SerialName("window_until") val windowUntil: Long
)

/**
 * ChangeDownloadResponse is an item in the ordered stream for a user.
 * payload is the after‑image for INSERT/UPDATE; null for DELETE.
 */
@Serializable
data class ChangeDownloadResponse(
    @SerialName("server_id") val serverId: Long,
    val schema: String,
    @SerialName("table") val tableName: String,
    val op: String,
    val pk: String,
    val payload: JsonElement? = null,
    @SerialName("server_version") val serverVersion: Long,
    val deleted: Boolean,
    @SerialName("source_id") val sourceId: String,
    @SerialName("source_change_id") val sourceChangeId: Long,
    val ts: String
)

/**
 * OversqliteConfig defines the client’s schema and tables, and the upload/download page sizes.
 * The schema must match the server’s registered business schema and is required by design.
 */
data class OversqliteConfig(
    val schema: String, // mandatory; no default to avoid business-specific assumptions
    val syncTables: List<SyncTable>,
    val uploadLimit: Int = 200,
    val downloadLimit: Int = 1000,
    val syncWindowLookback: Long = 100L,
    val lookbackMaxPasses: Int = 20,
    val uploadPath: String = "/sync/upload",
    val downloadPath: String = "/sync/download",
    val verboseLogs: Boolean = false, // Enable detailed debug logging for troubleshooting
)

/**
 * Represents a table to be synchronized with its primary key column.
 *
 * @param tableName The name of the table to synchronize
 * @param syncKeyColumnName The primary key column name. "id" by default.
 */
data class SyncTable(
    val tableName: String,
    val syncKeyColumnName: String? = "id"
)
