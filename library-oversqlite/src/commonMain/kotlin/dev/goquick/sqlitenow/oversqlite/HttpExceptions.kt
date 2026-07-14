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

import io.ktor.http.HttpStatusCode
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

private val httpErrorJson = Json { ignoreUnknownKeys = true }

open class UploadHttpException(
    val status: HttpStatusCode,
    val rawBody: String,
    cause: Throwable? = null,
) : RuntimeException("Upload failed: HTTP $status", cause), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory
        get() = if (status.value in AUTH_HTTP_STATUSES) {
            OversqliteErrorCategory.AUTH
        } else {
            OversqliteErrorCategory.NETWORK
        }
}

open class DownloadHttpException(
    val status: HttpStatusCode,
    val rawBody: String,
    cause: Throwable? = null,
) : RuntimeException("Download failed: HTTP $status", cause), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory
        get() = if (status.value in AUTH_HTTP_STATUSES) {
            OversqliteErrorCategory.AUTH
        } else {
            OversqliteErrorCategory.NETWORK
        }
}

class HistoryPrunedException(
    message: String,
) : RuntimeException(message), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.NETWORK
}

class CheckpointAheadException(
    message: String,
) : RuntimeException(message), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

internal class SnapshotHttpException(
    val status: HttpStatusCode,
    val errorCode: String,
) : RuntimeException("snapshot request failed: HTTP ${status.value} error=$errorCode"), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory
        get() = if (status.value in AUTH_HTTP_STATUSES) {
            OversqliteErrorCategory.AUTH
        } else {
            OversqliteErrorCategory.NETWORK
    }
}

class SnapshotSessionLimitExceededException(
    val dimension: String,
    val actual: Long,
    val limit: Long,
) : RuntimeException(
    "snapshot session exceeds server $dimension limit: actual=$actual limit=$limit",
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.NETWORK
}

internal class SnapshotResponseDecodeException(
    val operation: String,
) : RuntimeException("$operation response could not be decoded"), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.PROTOCOL
}

internal class SnapshotCapacityException(
    val status: HttpStatusCode,
    val errorCode: String,
    val retryAfterMillis: Long?,
) : RuntimeException("snapshot request temporarily unavailable: HTTP ${status.value} error=$errorCode"),
    OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.NETWORK
}

class SnapshotCapacityRetryExhaustedException(
    val operation: String,
    val errorCode: String,
    val waitedMillis: Long,
) : RuntimeException("oversqlite snapshot capacity retry exhausted for $operation"),
    OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.NETWORK
}

internal class TransientRetryExhaustedException(
    operation: String,
    attempts: Int,
) : RuntimeException("oversqlite transient retry exhausted for $operation after $attempts attempts"),
    OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.NETWORK
}

internal class SnapshotSourceRecoveryRequiredException(
    val reason: SourceRecoveryReason,
    val replacementSourceId: String? = null,
) : RuntimeException("snapshot source recovery is required"), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

class CommittedReplayPrunedException(
    rawBody: String,
) : DownloadHttpException(HttpStatusCode.Conflict, rawBody)

class SourceRecoveryRequiredHttpException(
    status: HttpStatusCode,
    rawBody: String,
    val reason: SourceRecoveryReason,
    val replacementSourceId: String? = null,
) : UploadHttpException(status, rawBody)

class SourceReplacementInvalidHttpException(
    val status: HttpStatusCode,
) : RuntimeException(
    "snapshot session request failed: HTTP ${status.value}; server rejected the source replacement request",
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory
        get() = if (status.value in AUTH_HTTP_STATUSES) {
            OversqliteErrorCategory.AUTH
        } else {
            OversqliteErrorCategory.NETWORK
        }
}

class CommittedBundleNotFoundException(
    rawBody: String,
) : DownloadHttpException(HttpStatusCode.NotFound, rawBody)

class PushConflictException(
    rawBody: String,
    val response: PushConflictResponse,
) : UploadHttpException(HttpStatusCode.Conflict, rawBody) {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.CONFLICT

    val conflict: PushConflictDetails
        get() = response.conflict ?: error("push conflict response is missing conflict details")
}

private val AUTH_HTTP_STATUSES = setOf(401, 403)

internal fun DownloadHttpException.toHistoryPrunedOrNull(): HistoryPrunedException? {
    val error = decodeErrorResponseOrNull(
        status = status,
        rawBody = rawBody,
        expectedStatus = HttpStatusCode.Conflict,
        expectedError = "history_pruned",
    ) ?: return null
    return HistoryPrunedException("server history required for incremental sync has been pruned")
}

internal fun decodePushConflictExceptionOrNull(
    status: HttpStatusCode,
    rawBody: String,
): PushConflictException? {
    if (status != HttpStatusCode.Conflict) return null
    val response = runCatching {
        httpErrorJson.decodeFromString<PushConflictResponse>(rawBody)
    }.getOrNull() ?: return null
    if (response.error != "push_conflict" || response.conflict == null) return null
    validatePushConflictDetails(response.conflict)
    return PushConflictException(rawBody = "", response = response.copy(message = ""))
}

internal fun decodeCommittedBundleNotFoundExceptionOrNull(
    status: HttpStatusCode,
    rawBody: String,
): CommittedBundleNotFoundException? {
    decodeErrorResponseOrNull(
        status = status,
        rawBody = rawBody,
        expectedStatus = HttpStatusCode.NotFound,
        expectedError = "committed_bundle_not_found",
    ) ?: return null
    return CommittedBundleNotFoundException(rawBody = "")
}

internal fun decodeCommittedReplayPrunedExceptionOrNull(
    status: HttpStatusCode,
    rawBody: String,
): CommittedReplayPrunedException? {
    decodeErrorResponseOrNull(
        status = status,
        rawBody = rawBody,
        expectedStatus = HttpStatusCode.Conflict,
        expectedError = "history_pruned",
    ) ?: return null
    return CommittedReplayPrunedException(rawBody = "")
}

internal data class ValidatedSourceRetiredWire(
    val replacementSourceId: String?,
)

internal fun decodeValidatedSourceRetiredWireOrNull(
    rawBody: String,
    expectedSourceId: String?,
): ValidatedSourceRetiredWire? {
    val document = runCatching { httpErrorJson.parseToJsonElement(rawBody) }.getOrNull() as? JsonObject
        ?: return null
    val error = document["error"] as? JsonPrimitive ?: return null
    if (!error.isString || error.content != "source_retired") return null

    fun malformed(): Nothing = throw RemoteResponseSemanticException("source retired response")

    val message = document["message"] as? JsonPrimitive ?: malformed()
    if (!message.isString) malformed()
    val source = document["source_id"] as? JsonPrimitive ?: malformed()
    if (!source.isString) malformed()
    val sourceId = try {
        requireValidOversqliteSourceId(source.content)
    } catch (_: RuntimeException) {
        malformed()
    }
    if (expectedSourceId != null) {
        val expected = try {
            requireValidOversqliteSourceId(expectedSourceId)
        } catch (_: RuntimeException) {
            malformed()
        }
        if (sourceId != expected) malformed()
    }

    val replacement = if (document.containsKey("replaced_by_source_id")) {
        val value = document["replaced_by_source_id"]
        if (value == null || value is JsonNull) malformed()
        val primitive = value as? JsonPrimitive ?: malformed()
        if (!primitive.isString) malformed()
        try {
            requireValidOversqliteSourceId(primitive.content)
        } catch (_: RuntimeException) {
            malformed()
        }
    } else {
        null
    }
    return ValidatedSourceRetiredWire(replacement)
}

internal fun decodeSourceRecoveryRequiredExceptionOrNull(
    status: HttpStatusCode,
    rawBody: String,
    expectedSourceId: String? = null,
): SourceRecoveryRequiredHttpException? {
    if (status != HttpStatusCode.Conflict) return null
    val retired = decodeValidatedSourceRetiredWireOrNull(rawBody, expectedSourceId)
    if (retired != null) {
        return SourceRecoveryRequiredHttpException(
            status = status,
            rawBody = "",
            reason = SourceRecoveryReason.SOURCE_RETIRED,
            replacementSourceId = retired.replacementSourceId,
        )
    }
    val error = decodeErrorResponseOrNull(
        status = status,
        rawBody = rawBody,
        expectedStatus = HttpStatusCode.Conflict,
    ) ?: return null
    val reason = when (error.error) {
        "history_pruned" -> SourceRecoveryReason.HISTORY_PRUNED
        "source_sequence_out_of_order" -> SourceRecoveryReason.SOURCE_SEQUENCE_OUT_OF_ORDER
        "source_sequence_changed" -> SourceRecoveryReason.SOURCE_SEQUENCE_CHANGED
        else -> return null
    }
    return SourceRecoveryRequiredHttpException(
        status = status,
        rawBody = "",
        reason = reason,
    )
}

internal fun decodeSourceReplacementInvalidExceptionOrNull(
    status: HttpStatusCode,
    rawBody: String,
): SourceReplacementInvalidHttpException? {
    decodeErrorResponseOrNull(
        status = status,
        rawBody = rawBody,
        expectedStatus = HttpStatusCode.Conflict,
        expectedError = "source_replacement_invalid",
    ) ?: return null
    return SourceReplacementInvalidHttpException(status)
}

private fun decodeErrorResponseOrNull(
    status: HttpStatusCode,
    rawBody: String,
    expectedStatus: HttpStatusCode,
): ErrorResponse? {
    if (status != expectedStatus) return null
    return runCatching {
        httpErrorJson.decodeFromString<ErrorResponse>(rawBody)
    }.getOrNull()
}

private fun decodeErrorResponseOrNull(
    status: HttpStatusCode,
    rawBody: String,
    expectedStatus: HttpStatusCode,
    expectedError: String,
): ErrorResponse? {
    val error = decodeErrorResponseOrNull(
        status = status,
        rawBody = rawBody,
        expectedStatus = expectedStatus,
    ) ?: return null
    if (error.error != expectedError) return null
    return error
}
