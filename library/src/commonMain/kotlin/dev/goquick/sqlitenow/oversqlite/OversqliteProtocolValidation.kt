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
    require(bundle.sourceId.isNotBlank()) { "bundle source_id must be non-empty" }
    require(bundle.sourceBundleId > 0) { "bundle source_bundle_id ${bundle.sourceBundleId} must be positive" }
    bundle.rows.forEachIndexed { index, row ->
        try {
            validateBundleRow(row)
        } catch (e: Throwable) {
            throw IllegalArgumentException("invalid bundle row $index: ${e.message}", e)
        }
    }
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
    require(row.schema.isNotBlank()) { "snapshot row schema must be non-empty" }
    require(row.table.isNotBlank()) { "snapshot row table must be non-empty" }
    validateVisibleWireKey(row.key, "snapshot row key")
    require(row.rowVersion > 0) { "snapshot row row_version ${row.rowVersion} must be positive" }
    require(row.payload !is JsonNull) { "snapshot row payload must be present" }
    validateVisibleWirePayload(row.payload, "snapshot row payload")
}

@OptIn(ExperimentalTime::class)
internal fun validateSnapshotSession(session: SnapshotSession) {
    require(session.snapshotId.isNotBlank()) { "snapshot session response missing snapshot_id" }
    require(session.snapshotBundleSeq >= 0) {
        "snapshot session snapshot_bundle_seq ${session.snapshotBundleSeq} must be non-negative"
    }
    require(session.rowCount >= 0) { "snapshot session row_count ${session.rowCount} must be non-negative" }
    require(session.rowCount == 0L || session.snapshotBundleSeq > 0) {
        "snapshot session missing snapshot_bundle_seq for non-empty row set"
    }
    require(session.byteCount >= 0) { "snapshot session byte_count ${session.byteCount} must be non-negative" }
    require(session.expiresAt.isNotBlank()) { "snapshot session response missing expires_at" }
    parseOversqliteRfc3339Instant(session.expiresAt)
}

internal fun validateSnapshotChunkResponse(
    chunk: SnapshotChunkResponse,
    snapshotId: String,
    snapshotBundleSeq: Long,
    afterRowOrdinal: Long,
) {
    require(chunk.snapshotId == snapshotId) {
        "snapshot chunk response snapshot_id ${chunk.snapshotId} does not match requested $snapshotId"
    }
    require(chunk.snapshotBundleSeq == snapshotBundleSeq) {
        "snapshot chunk response snapshot_bundle_seq ${chunk.snapshotBundleSeq} does not match session $snapshotBundleSeq"
    }
    require(chunk.rows.isEmpty() || chunk.snapshotBundleSeq > 0) {
        "snapshot chunk response missing snapshot_bundle_seq for non-empty row set"
    }
    require(chunk.nextRowOrdinal == afterRowOrdinal + chunk.rows.size) {
        "snapshot chunk response next_row_ordinal ${chunk.nextRowOrdinal} does not match expected ${afterRowOrdinal + chunk.rows.size}"
    }
    require(!chunk.hasMore || chunk.rows.isNotEmpty()) {
        "snapshot chunk response with has_more=true must include at least one row"
    }
    chunk.rows.forEachIndexed { index, row ->
        try {
            validateSnapshotRow(row)
        } catch (e: Throwable) {
            throw IllegalArgumentException("invalid snapshot row $index: ${e.message}", e)
        }
    }
}

internal fun validatePushSessionCreateResponse(
    response: PushSessionCreateResponse,
    sourceBundleId: Long,
    plannedRowCount: Long,
    sourceId: String,
) {
    when (response.status) {
        "staging" -> {
            require(response.pushId.isNotBlank()) { "push session response missing push_id" }
            require(response.plannedRowCount == plannedRowCount) {
                "push session response planned_row_count ${response.plannedRowCount} does not match requested $plannedRowCount"
            }
            require(response.nextExpectedRowOrdinal == 0L) {
                "push session response next_expected_row_ordinal ${response.nextExpectedRowOrdinal} must be 0"
            }
        }

        "already_committed" -> {
            require(response.bundleSeq > 0) { "push session already_committed response missing bundle_seq" }
            require(response.sourceId == sourceId) {
                "push session already_committed response source_id ${response.sourceId} does not match client $sourceId"
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
        }

        else -> error("push session response returned unsupported status ${response.status}")
    }
}

internal fun committedPushBundleFromCreateResponse(
    response: PushSessionCreateResponse,
    sourceId: String,
    sourceBundleId: Long,
): CommittedPushBundle {
    validatePushSessionCreateResponse(response, sourceBundleId, response.rowCount, sourceId)
    require(response.status == "already_committed") {
        "unexpected push session status ${response.status}"
    }
    return CommittedPushBundle(
        bundleSeq = response.bundleSeq,
        sourceId = response.sourceId,
        sourceBundleId = response.sourceBundleId,
        rowCount = response.rowCount,
        bundleHash = response.bundleHash,
    )
}

internal fun committedPushBundleFromCommitResponse(
    response: PushSessionCommitResponse,
    sourceId: String,
    sourceBundleId: Long,
): CommittedPushBundle {
    require(response.bundleSeq > 0) { "push commit response bundle_seq must be positive" }
    require(response.sourceId == sourceId) {
        "push commit response source_id ${response.sourceId} does not match client $sourceId"
    }
    require(response.sourceBundleId == sourceBundleId) {
        "push commit response source_bundle_id ${response.sourceBundleId} does not match requested $sourceBundleId"
    }
    require(response.rowCount >= 0) { "push commit response row_count must be non-negative" }
    require(response.bundleHash.isNotBlank()) { "push commit response bundle_hash must be non-empty" }
    return CommittedPushBundle(
        bundleSeq = response.bundleSeq,
        sourceId = response.sourceId,
        sourceBundleId = response.sourceBundleId,
        rowCount = response.rowCount,
        bundleHash = response.bundleHash,
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
    require(response.bundleSeq == committed.bundleSeq) {
        "committed bundle chunk response bundle_seq ${response.bundleSeq} does not match expected ${committed.bundleSeq}"
    }
    require(response.sourceId == committed.sourceId) {
        "committed bundle chunk response source_id ${response.sourceId} does not match expected ${committed.sourceId}"
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

    val logicalAfter = afterRowOrdinal ?: -1L
    val expectedNext = if (response.rows.isEmpty()) logicalAfter else logicalAfter + response.rows.size
    require(response.nextRowOrdinal == expectedNext) {
        "committed bundle chunk response next_row_ordinal ${response.nextRowOrdinal} does not match expected $expectedNext"
    }
    require(!response.hasMore || response.rows.isNotEmpty()) {
        "committed bundle chunk response with has_more=true must include at least one row"
    }
    response.rows.forEachIndexed { index, row ->
        try {
            validateBundleRow(row)
        } catch (e: Throwable) {
            throw IllegalArgumentException("invalid committed bundle row $index: ${e.message}", e)
        }
    }
}

@OptIn(ExperimentalTime::class)
private fun parseOversqliteRfc3339Instant(value: String): Instant {
    return try {
        Instant.parse(value)
    } catch (e: Exception) {
        throw IllegalArgumentException("oversqlite timestamp must be RFC3339/ISO-8601 instant: '$value'", e)
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
