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

import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

internal const val hiddenSyncScopeColumnName = "_sync_scope_id"

private val canonicalOversqliteSessionToken =
    Regex("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")

internal fun requireValidOversqliteSessionToken(value: String): String {
    require(canonicalOversqliteSessionToken.matches(value)) {
        "oversqlite session token must be a canonical lowercase dashed UUID"
    }
    return value
}

internal fun requireValidOptionalOversqliteSessionToken(value: String): String =
    if (value.isEmpty()) value else requireValidOversqliteSessionToken(value)

internal fun validateConnectResponse(response: ConnectResponse) {
    if (response.resolution == "initialize_local") {
        requireValidOversqliteSessionToken(response.initializationId)
    } else {
        require(response.initializationId.isEmpty()) {
            "connect response initialization_id must be absent"
        }
    }
}

internal fun requireValidOversqliteSourceId(value: String): String {
    if (value.isEmpty() || value.any { it !in '!'..'~' }) {
        throw InvalidOversqliteSourceIdException()
    }
    return value
}

internal fun requireValidOptionalOversqliteSourceId(value: String): String =
    if (value.isEmpty()) value else requireValidOversqliteSourceId(value)

internal fun validateSourceRetiredResponse(response: SourceRetiredResponse): String? {
    requireValidOversqliteSourceId(response.sourceId)
    return response.replacedBySourceId?.let(::requireValidOversqliteSourceId)
}

internal fun validatePullResponse(
    response: PullResponse,
    afterBundleSeq: Long,
) {
    require(response.stableBundleSeq >= 0) {
        "pull response stable_bundle_seq ${response.stableBundleSeq} must be non-negative"
    }
    require(response.stableBundleSeq >= afterBundleSeq) {
        "pull response stable_bundle_seq ${response.stableBundleSeq} is behind requested after_bundle_seq $afterBundleSeq"
    }
    require(response.bundles.isEmpty() || response.stableBundleSeq > 0) {
        "pull response missing stable_bundle_seq for non-empty bundle list"
    }

    var previous = afterBundleSeq
    response.bundles.forEachIndexed { index, bundle ->
        validateBundle(bundle)
        require(bundle.bundleSeq > previous) {
            "pull response bundle_seq ${bundle.bundleSeq} is not strictly greater than previous $previous"
        }
        require(bundle.bundleSeq <= response.stableBundleSeq) {
            "pull response bundle_seq ${bundle.bundleSeq} exceeds stable_bundle_seq ${response.stableBundleSeq}"
        }
        previous = bundle.bundleSeq
        if (index > 0) {
            require(response.bundles[index - 1].bundleSeq < bundle.bundleSeq) {
                "pull response bundle order is not strictly increasing"
            }
        }
    }
}

internal fun validateBundle(bundle: Bundle) {
    require(bundle.bundleSeq > 0) { "bundle_seq ${bundle.bundleSeq} must be positive" }
    requireValidOversqliteSourceId(bundle.sourceId)
    require(bundle.sourceBundleId > 0) { "bundle source_bundle_id ${bundle.sourceBundleId} must be positive" }
    validateIndexedRows(bundle.rows, "bundle", ::validateBundleRow)
}

internal fun validateBundleRow(row: BundleRow) {
    require(row.schema.isNotBlank()) { "bundle row schema must be non-empty" }
    require(row.table.isNotBlank()) { "bundle row table must be non-empty" }
    validateVisibleWireKey(row.key, "bundle row key")
    require(row.rowVersion > 0) { "bundle row row_version ${row.rowVersion} must be positive" }
    require(row.op in setOf("INSERT", "UPDATE", "DELETE")) { "bundle row op ${row.op} is unsupported" }
    if (row.op != "DELETE") {
        require(row.payload != null) { "bundle row payload must be present for ${row.op}" }
        validateVisibleWirePayload(row.payload, "bundle row payload")
    }
}

internal fun validateSnapshotRow(row: SnapshotRow) {
    try {
        require(row.schema.isNotBlank())
        require(row.table.isNotBlank())
        validateVisibleWireKey(row.key, "snapshot row key")
        require(row.rowVersion > 0)
        require(row.payload !is JsonNull)
        validateVisibleWirePayload(row.payload, "snapshot row payload")
    } catch (error: IllegalArgumentException) {
        if (error is SnapshotSemanticException) throw error
        throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_ROW)
    } catch (_: IllegalStateException) {
        throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_ROW)
    }
}

@OptIn(ExperimentalTime::class)
internal fun validateSnapshotSession(session: SnapshotSession) {
    try {
        require(session.snapshotId.isNotBlank())
        require(session.snapshotBundleSeq >= 0)
        require(session.rowCount >= 0)
        require(session.rowCount == 0L || session.snapshotBundleSeq > 0)
        require(session.byteCount >= 0)
        require(session.rowCount != 0L || session.byteCount == 0L)
        require(session.rowCount == 0L || session.byteCount > 0L)
        require(session.expiresAt.isNotBlank())
        parseOversqliteRfc3339Instant(session.expiresAt)
    } catch (error: IllegalArgumentException) {
        if (error is SnapshotSemanticException) throw error
        throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_SESSION)
    }
}

internal fun validateSnapshotChunkResponse(
    chunk: SnapshotChunkResponse,
    snapshotId: String,
    snapshotBundleSeq: Long,
    afterRowOrdinal: Long,
    maxRows: Int,
    maxBytes: Long,
) {
    try {
        require(chunk.snapshotId == snapshotId)
        require(chunk.snapshotBundleSeq == snapshotBundleSeq)
        require(chunk.rows.isEmpty() || chunk.snapshotBundleSeq > 0)
        require(chunk.rows.size <= maxRows)
        require(chunk.byteCount in 0..maxBytes)
        require(chunk.rows.isNotEmpty() || chunk.byteCount == 0L)
        require(chunk.rows.isEmpty() || chunk.byteCount > 0L)
        val expectedOrdinal = checkedAddSnapshotLong(afterRowOrdinal, chunk.rows.size.toLong()) {
            "snapshot chunk next ordinal overflow"
        }
        require(chunk.nextRowOrdinal == expectedOrdinal)
        require(!chunk.hasMore || chunk.rows.isNotEmpty())
        chunk.rows.forEach(::validateSnapshotRow)
    } catch (error: IllegalArgumentException) {
        if (error is SnapshotSemanticException) throw error
        throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK)
    } catch (_: IllegalStateException) {
        throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK)
    }
}

internal fun negotiateSnapshotLimits(
    capabilities: CapabilitiesResponse,
    config: OversqliteConfig,
): SnapshotNegotiation {
    capabilities.requireSupportedProtocol()
    val limits = capabilities.bundleLimits
    if (limits.defaultRowsPerSnapshotChunk <= 0 || limits.maxRowsPerSnapshotChunk <= 0) {
        throw SnapshotCapabilitiesException(
            "snapshot capabilities require positive default_rows_per_snapshot_chunk and max_rows_per_snapshot_chunk",
        )
    }
    if (limits.defaultRowsPerSnapshotChunk > limits.maxRowsPerSnapshotChunk) {
        throw SnapshotCapabilitiesException(
            "snapshot capability default_rows_per_snapshot_chunk exceeds max_rows_per_snapshot_chunk",
        )
    }
    if (
        limits.defaultBytesPerSnapshotChunk <= 0L ||
        limits.maxBytesPerSnapshotChunk <= 0L ||
        limits.maxBytesPerSnapshotRow <= 0L
    ) {
        throw SnapshotCapabilitiesException(
            "snapshot capabilities require positive default/max chunk byte and max row byte limits",
        )
    }
    if (limits.defaultBytesPerSnapshotChunk > limits.maxBytesPerSnapshotChunk) {
        throw SnapshotCapabilitiesException(
            "snapshot capability default_bytes_per_snapshot_chunk exceeds max_bytes_per_snapshot_chunk",
        )
    }
    if (limits.maxBytesPerSnapshotRow > limits.maxBytesPerSnapshotChunk) {
        throw SnapshotCapabilitiesException(
            "snapshot capability max_bytes_per_snapshot_row exceeds max_bytes_per_snapshot_chunk",
        )
    }
    if (limits.maxConcurrentSnapshotBuilds <= 0 || limits.maxConcurrentSnapshotChunkRequests <= 0) {
        throw SnapshotCapabilitiesException(
            "snapshot capabilities require positive max_concurrent_snapshot_builds and " +
                "max_concurrent_snapshot_chunk_requests",
        )
    }
    val effectiveRows = minOf(config.snapshotChunkRows, limits.maxRowsPerSnapshotChunk)
    val effectiveBytes = minOf(config.snapshotChunkBytes, limits.maxBytesPerSnapshotChunk)
    if (effectiveBytes < limits.maxBytesPerSnapshotRow) {
        throw SnapshotCapabilitiesException(
            "effective snapshot chunk byte budget is below server max_bytes_per_snapshot_row; " +
                "increase snapshotChunkBytes",
        )
    }
    if (config.snapshotApplyBatchBytes < limits.maxBytesPerSnapshotRow) {
        throw SnapshotCapabilitiesException(
            "snapshot apply byte budget is below server max_bytes_per_snapshot_row; " +
                "increase snapshotApplyBatchBytes",
        )
    }
    return SnapshotNegotiation(
        maxRows = effectiveRows,
        maxBytes = effectiveBytes,
        maxRowBytes = limits.maxBytesPerSnapshotRow,
    )
}

internal inline fun checkedAddSnapshotLong(
    left: Long,
    right: Long,
    message: () -> String,
): Long {
    if (right > 0L && left > Long.MAX_VALUE - right) {
        throw IllegalArgumentException(message())
    }
    if (right < 0L && left < Long.MIN_VALUE - right) {
        throw IllegalArgumentException(message())
    }
    return left + right
}

internal fun validatePushSessionCreateResponse(
    response: PushSessionCreateResponse,
    sourceBundleId: Long,
    plannedRowCount: Long,
    sourceId: String,
    canonicalRequestHash: String,
) {
    val expectedSourceId = requireValidOversqliteSourceId(sourceId)
    val responseSourceId = requireValidOptionalOversqliteSourceId(response.sourceId)
    when (response.status) {
        "staging" -> {
            requireValidOversqliteSessionToken(response.pushId)
            require(response.plannedRowCount == plannedRowCount) {
                "push session response planned_row_count ${response.plannedRowCount} does not match requested $plannedRowCount"
            }
            require(response.nextExpectedRowOrdinal == 0L) {
                "push session response next_expected_row_ordinal ${response.nextExpectedRowOrdinal} must be 0"
            }
            if (response.canonicalRequestHash != canonicalRequestHash) {
                throw SourceSequenceMismatchException(
                    "push session response canonical_request_hash ${response.canonicalRequestHash} does not match prepared hash $canonicalRequestHash",
                )
            }
        }

        "already_committed" -> {
            require(response.bundleSeq > 0) { "push session already_committed response missing bundle_seq" }
            requireValidOversqliteSourceId(responseSourceId)
            require(responseSourceId == expectedSourceId) {
                "push session already_committed response source_id does not match expected source"
            }
            require(response.sourceBundleId == sourceBundleId) {
                "push session already_committed response source_bundle_id ${response.sourceBundleId} does not match requested $sourceBundleId"
            }
            require(response.rowCount >= 0) {
                "push session already_committed response missing row_count"
            }
            require(response.bundleHash.isNotBlank()) {
                "push session already_committed response missing bundle_hash"
            }
            if (response.canonicalRequestHash != canonicalRequestHash) {
                throw SourceSequenceMismatchException(
                    "push session already_committed response canonical_request_hash ${response.canonicalRequestHash} does not match prepared hash $canonicalRequestHash",
                )
            }
        }

        else -> error("push session response returned unsupported status ${response.status}")
    }
}

internal fun validatePushSessionChunkResponse(
    response: PushSessionChunkResponse,
    requestedPushId: String,
) {
    val expected = requireValidOversqliteSessionToken(requestedPushId)
    val actual = requireValidOversqliteSessionToken(response.pushId)
    require(actual == expected) {
        "push chunk response push_id does not match requested session"
    }
}

internal fun committedPushBundleFromCreateResponse(
    response: PushSessionCreateResponse,
    sourceId: String,
    sourceBundleId: Long,
): CommittedPushBundle {
    validatePushSessionCreateResponse(response, sourceBundleId, response.rowCount, sourceId, response.canonicalRequestHash)
    require(response.status == "already_committed") {
        "unexpected push session status ${response.status}"
    }
    val committedSourceId = requireValidOversqliteSourceId(response.sourceId)
    return CommittedPushBundle(
        bundleSeq = response.bundleSeq,
        sourceId = committedSourceId,
        sourceBundleId = response.sourceBundleId,
        rowCount = response.rowCount,
		bundleHash = response.bundleHash,
		canonicalRequestHash = response.canonicalRequestHash,
    )
}

internal fun committedPushBundleFromCommitResponse(
    response: PushSessionCommitResponse,
    sourceId: String,
    sourceBundleId: Long,
): CommittedPushBundle {
    val expectedSourceId = requireValidOversqliteSourceId(sourceId)
    val committedSourceId = requireValidOversqliteSourceId(response.sourceId)
    require(response.bundleSeq > 0) { "push commit response bundle_seq must be positive" }
    require(committedSourceId == expectedSourceId) {
        "push commit response source_id does not match expected source"
    }
    require(response.sourceBundleId == sourceBundleId) {
        "push commit response source_bundle_id ${response.sourceBundleId} does not match requested $sourceBundleId"
    }
    require(response.rowCount >= 0) { "push commit response row_count must be non-negative" }
	require(response.bundleHash.isNotBlank()) { "push commit response bundle_hash must be non-empty" }
	require(isCanonicalSha256(response.canonicalRequestHash)) { "push commit response canonical_request_hash must be 64 lowercase hexadecimal characters" }
    return CommittedPushBundle(
        bundleSeq = response.bundleSeq,
        sourceId = committedSourceId,
        sourceBundleId = response.sourceBundleId,
        rowCount = response.rowCount,
		bundleHash = response.bundleHash,
		canonicalRequestHash = response.canonicalRequestHash,
    )
}

internal fun validatePushConflictDetails(details: PushConflictDetails) {
    require(details.schema.isNotBlank()) { "push conflict schema must be non-empty" }
    require(details.table.isNotBlank()) { "push conflict table must be non-empty" }
    validateVisibleWireKey(details.key, "push conflict key")
    require(details.op in setOf("INSERT", "UPDATE", "DELETE")) {
        "push conflict op ${details.op} is unsupported"
    }
    require(details.baseRowVersion >= 0) {
        "push conflict base_row_version ${details.baseRowVersion} must be non-negative"
    }
    require(details.serverRowVersion >= 0) {
        "push conflict server_row_version ${details.serverRowVersion} must be non-negative"
    }
    if (details.serverRow != null && details.serverRow !is JsonNull) {
        validateVisibleWirePayload(details.serverRow, "push conflict server_row")
    }
}

internal fun validateCommittedBundleRowsResponse(
    response: CommittedBundleRowsResponse,
    committed: CommittedPushBundle,
    afterRowOrdinal: Long?,
) {
    val expectedSourceId = requireValidOversqliteSourceId(committed.sourceId)
    val responseSourceId = requireValidOversqliteSourceId(response.sourceId)
    require(response.bundleSeq == committed.bundleSeq) {
        "committed bundle chunk response bundle_seq ${response.bundleSeq} does not match expected ${committed.bundleSeq}"
    }
    require(responseSourceId == expectedSourceId) {
        "committed bundle chunk response source_id does not match expected source"
    }
    require(response.sourceBundleId == committed.sourceBundleId) {
        "committed bundle chunk response source_bundle_id ${response.sourceBundleId} does not match expected ${committed.sourceBundleId}"
    }
    require(response.rowCount == committed.rowCount) {
        "committed bundle chunk response row_count ${response.rowCount} does not match expected ${committed.rowCount}"
    }
	require(response.bundleHash == committed.bundleHash) {
        "committed bundle chunk response bundle_hash ${response.bundleHash} does not match expected ${committed.bundleHash}"
	}
	if (response.canonicalRequestHash != committed.canonicalRequestHash) {
        throw SourceSequenceMismatchException(
            "committed bundle chunk response canonical_request_hash ${response.canonicalRequestHash} does not match expected ${committed.canonicalRequestHash}",
        )
	}

    val logicalAfter = afterRowOrdinal ?: -1L
    val expectedNext = if (response.rows.isEmpty()) logicalAfter else logicalAfter + response.rows.size
    require(response.nextRowOrdinal == expectedNext) {
        "committed bundle chunk response next_row_ordinal ${response.nextRowOrdinal} does not match expected $expectedNext"
    }
    require(!response.hasMore || response.rows.isNotEmpty()) {
        "committed bundle chunk response with has_more=true must include at least one row"
    }
    validateIndexedRows(response.rows, "committed bundle", ::validateBundleRow)
}

internal fun isCanonicalSha256(value: String): Boolean =
	value.length == 64 && value.all { it in '0'..'9' || it in 'a'..'f' }

private inline fun <T> validateIndexedRows(
    rows: List<T>,
    label: String,
    validate: (T) -> Unit,
) {
    rows.forEachIndexed { index, row ->
        try {
            validate(row)
        } catch (e: Throwable) {
            throw IllegalArgumentException("invalid $label row $index: ${e.message}", e)
        }
    }
}

@OptIn(ExperimentalTime::class)
private fun parseOversqliteRfc3339Instant(value: String): Instant {
    return try {
        Instant.parse(value)
    } catch (_: Exception) {
        throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_SESSION)
    }
}

private fun validateVisibleWireKey(
    key: SyncKey,
    label: String,
) {
    require(key.isNotEmpty()) { "$label must be non-empty" }
    require(hiddenSyncScopeColumnName !in key.keys) {
        "$label must not include hidden server column $hiddenSyncScopeColumnName"
    }
    require(key.values.all { it.isNotBlank() }) { "$label values must be non-empty strings" }
}

private fun validateVisibleWirePayload(
    payload: JsonElement?,
    label: String,
) {
    val payloadObject = payload as? JsonObject
        ?: error("$label must be a JSON object")
    require(hiddenSyncScopeColumnName !in payloadObject.keys) {
        "$label must not include hidden server column $hiddenSyncScopeColumnName"
    }
}
