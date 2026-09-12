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

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
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
	require(response.resolution in setOf("remote_authoritative", "initialize_local", "initialize_empty", "retry_later")) {
		"connect response resolution is unsupported"
	}
	when (response.resolution) {
		"initialize_local" -> {
			requireValidOversqliteSessionToken(response.initializationId)
			require(response.leaseExpiresAt.isNotBlank()) { "connect response lease_expires_at is required" }
			parseOversqliteRfc3339Instant(response.leaseExpiresAt)
			require(response.retryAfterSeconds == 0) { "connect response retry_after_seconds must be absent" }
		}
		"retry_later" -> {
			require(response.retryAfterSeconds >= 1) { "connect response retry_after_seconds must be positive" }
			require(response.leaseExpiresAt.isNotBlank()) { "connect response lease_expires_at is required" }
			parseOversqliteRfc3339Instant(response.leaseExpiresAt)
			require(response.initializationId.isEmpty()) { "connect response initialization_id must be absent" }
		}
		else -> {
			require(response.initializationId.isEmpty()) { "connect response initialization_id must be absent" }
			require(response.leaseExpiresAt.isEmpty()) { "connect response lease_expires_at must be absent" }
			require(response.retryAfterSeconds == 0) { "connect response retry_after_seconds must be absent" }
		}
	}
}

internal fun validateConnectResponseWire(raw: String) {
	try {
		requireUniqueSnapshotJsonObjectMembers(raw)
		val root = Json.parseToJsonElement(raw).jsonObject
		val allowed = setOf("resolution", "initialization_id", "lease_expires_at", "retry_after_seconds")
		require(root.keys.all(allowed::contains)) { "connect response contains unknown fields" }
		val resolution = root["resolution"] as? JsonPrimitive
		require(resolution != null && resolution.isString)
		val initializationId = optionalConnectString(root, "initialization_id")
		val leaseExpiresAt = optionalConnectString(root, "lease_expires_at")
		val retryAfterSeconds = optionalConnectPositiveInt(root, "retry_after_seconds")
		when (resolution.content) {
			"initialize_local" -> {
				require(initializationId != null)
				requireValidOversqliteSessionToken(initializationId)
				require(leaseExpiresAt != null && leaseExpiresAt.isNotBlank())
				parseOversqliteRfc3339Instant(leaseExpiresAt)
				require(retryAfterSeconds == null)
			}
			"retry_later" -> {
				require(initializationId == null)
				require(leaseExpiresAt != null && leaseExpiresAt.isNotBlank())
				parseOversqliteRfc3339Instant(leaseExpiresAt)
				require(retryAfterSeconds != null && retryAfterSeconds >= 1)
			}
			"initialize_empty", "remote_authoritative" -> {
				require(initializationId == null && leaseExpiresAt == null && retryAfterSeconds == null)
			}
			else -> error("unsupported connect response resolution")
		}
	} catch (_: Throwable) {
		throw RemoteResponseSemanticException("connect request")
	}
}

private fun optionalConnectString(root: JsonObject, name: String): String? {
		val value = root[name] ?: return null
		val primitive = value as? JsonPrimitive
		require(primitive != null && primitive.isString)
		return primitive.content
}

private fun optionalConnectPositiveInt(root: JsonObject, name: String): Int? {
		val value = root[name] ?: return null
		val primitive = value as? JsonPrimitive
		require(primitive != null && !primitive.isString)
		return primitive.content.toIntOrNull() ?: error("connect response integer is invalid")
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
    validateIndexedRows(bundle.rows, "bundle") { row ->
        validateBundleRow(row)
        require(row.rowVersion == bundle.bundleSeq) {
            "bundle row row_version ${row.rowVersion} does not match bundle_seq ${bundle.bundleSeq}"
        }
    }
    require(isCanonicalSha256(bundle.bundleHash)) { "bundle_hash is invalid" }
    require(canonicalCommittedBundleHash(bundle.rows) == bundle.bundleHash) { "bundle_hash does not match rows" }
}

internal fun canonicalCommittedBundleHash(rows: List<BundleRow>): String {
    val digest = Sha256Accumulator()
    digest.appendAscii('[')
    rows.forEachIndexed { ordinal, row ->
        if (ordinal > 0) digest.appendAscii(',')
        val canonicalRow = buildJsonObject {
            put("key", buildJsonObject {
                row.key.entries.sortedBy { it.key }.forEach { (name, value) -> put(name, JsonPrimitive(value)) }
            })
            put("op", JsonPrimitive(row.op))
            put("payload", if (row.op == "DELETE") JsonNull else row.payload ?: error("bundle row payload is required"))
            put("row_ordinal", JsonPrimitive(ordinal.toString()))
            put("row_version", JsonPrimitive(row.rowVersion.toString()))
            put("schema", JsonPrimitive(row.schema))
            put("table", JsonPrimitive(row.table))
        }
        digest.append(canonicalizeJsonElement(canonicalRow).encodeToByteArray())
    }
    digest.appendAscii(']')
    return digest.finishHex()
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
        require(session.logicalByteCount >= 0)
        require(session.rowCount != 0L || session.logicalByteCount == 0L)
        require(session.rowCount == 0L || session.logicalByteCount > 0L)
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
        require(snapshotId.isNotBlank())
        require(snapshotBundleSeq >= 0L)
        require(chunk.rows.isEmpty() || snapshotBundleSeq > 0)
        require(chunk.rows.size <= maxRows)
        require(chunk.logicalByteCount >= 0L)
        require(chunk.rows.isNotEmpty() || chunk.logicalByteCount == 0L)
        require(chunk.rows.isEmpty() || chunk.logicalByteCount > 0L)
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
    contract: AdmittedOversqliteContract,
    config: OversqliteConfig,
): SnapshotNegotiation {
    contract.requireSupportedProtocol()
    val limits = contract.limits
    val effectiveRows = minOf(config.transferTargets.maxSnapshotChunkRows, limits.maxSnapshotRowsPerChunk)
    val effectiveBytes = minOf(
        config.transferTargets.maxSnapshotChunkDecodedBytes,
        limits.maxSnapshotDecodedBytesPerChunk,
    )
    if (config.snapshotApplyBatchBytes < limits.maxRowDecodedBytes) {
        throw SnapshotTransferLimitException(
            "snapshot apply byte budget is below server max_row_decoded_bytes; " +
                "increase snapshotApplyBatchBytes",
        )
    }
    return SnapshotNegotiation(
        maxRows = effectiveRows,
        maxBytes = effectiveBytes,
        maxRowBytes = limits.maxRowDecodedBytes,
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
    val rowIsNull = details.serverRow == null || details.serverRow is JsonNull
    when {
        details.serverRowVersion == 0L -> require(rowIsNull && !details.serverRowDeleted) {
            "absent push conflict state must have a null server_row and deleted=false"
        }
        details.serverRowDeleted -> require(rowIsNull) {
            "deleted push conflict state must have a null server_row"
        }
        else -> require(!rowIsNull) { "live push conflict state must include server_row" }
    }
    if (!rowIsNull) {
        validateVisibleWirePayload(checkNotNull(details.serverRow), "push conflict server_row")
    }
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
    require(key.keys.none { it == hiddenSyncScopeColumnName || it == "__oversync_owner" }) {
        "$label must not include a hidden server column"
    }
    require(key.values.all { it.isNotBlank() }) { "$label values must be non-empty strings" }
}

private fun validateVisibleWirePayload(
    payload: JsonElement?,
    label: String,
) {
    val payloadObject = payload as? JsonObject
        ?: error("$label must be a JSON object")
    require(payloadObject.keys.none { it == hiddenSyncScopeColumnName || it == "__oversync_owner" }) {
        "$label must not include a hidden server column"
    }
}
