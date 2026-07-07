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

private val httpErrorJson = Json { ignoreUnknownKeys = true }

open class UploadHttpException(
    val status: HttpStatusCode,
    val rawBody: String,
    cause: Throwable? = null,
) : RuntimeException("Upload failed: HTTP $status - $rawBody", cause), OversqliteCategorizedException {
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
) : RuntimeException("Download failed: HTTP $status - $rawBody", cause), OversqliteCategorizedException {
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
    val rawBody: String,
    val errorMessage: String,
) : RuntimeException("Snapshot session request failed: HTTP $status - $errorMessage"), OversqliteCategorizedException {
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
    return HistoryPrunedException(error.message)
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
    return PushConflictException(rawBody = rawBody, response = response)
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
    return CommittedBundleNotFoundException(rawBody = rawBody)
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
    return CommittedReplayPrunedException(rawBody = rawBody)
}

internal fun decodeSourceRecoveryRequiredExceptionOrNull(
    status: HttpStatusCode,
    rawBody: String,
): SourceRecoveryRequiredHttpException? {
    if (status != HttpStatusCode.Conflict) return null
    val retired = runCatching {
        httpErrorJson.decodeFromString<SourceRetiredResponse>(rawBody)
    }.getOrNull()
    if (retired?.error == "source_retired") {
        return SourceRecoveryRequiredHttpException(
            status = status,
            rawBody = rawBody,
            reason = SourceRecoveryReason.SOURCE_RETIRED,
            replacementSourceId = retired.replacedBySourceId?.takeIf { it.isNotBlank() },
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
        rawBody = rawBody,
        reason = reason,
    )
}

internal fun decodeSourceReplacementInvalidExceptionOrNull(
    status: HttpStatusCode,
    rawBody: String,
): SourceReplacementInvalidHttpException? {
    val error = decodeErrorResponseOrNull(
        status = status,
        rawBody = rawBody,
        expectedStatus = HttpStatusCode.Conflict,
        expectedError = "source_replacement_invalid",
    ) ?: return null
    return SourceReplacementInvalidHttpException(
        status = status,
        rawBody = rawBody,
        errorMessage = error.message,
    )
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
