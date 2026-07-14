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

import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.plugins.HttpTimeoutConfig
import io.ktor.client.plugins.timeout
import io.ktor.client.plugins.compression.UnsupportedContentEncodingException
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.prepareDelete
import io.ktor.client.request.prepareGet
import io.ktor.client.request.preparePost
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.HttpStatement
import io.ktor.client.statement.bodyAsChannel
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.utils.io.readBuffer
import io.ktor.utils.io.readLineStrict
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.io.EOFException
import kotlinx.io.IOException
import kotlinx.io.readByteArray
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.text.CharacterCodingException

@Serializable
private data class SnapshotSessionWire(
    @SerialName("snapshot_id") val snapshotId: String,
    @SerialName("snapshot_bundle_seq") val snapshotBundleSeq: Long,
    @SerialName("row_count") val rowCount: Long,
    @SerialName("byte_count") val byteCount: Long? = null,
    @SerialName("expires_at") val expiresAt: String,
)

@Serializable
private data class SnapshotSessionLimitResponseWire(
    val error: String,
    val message: String,
    val dimension: String,
    val actual: Long,
    val limit: Long,
)

private data class BoundedJsonBody(
    val raw: String,
    val byteCount: Long,
)

internal const val defaultSnapshotRetirementTimeoutMillis = 5_000L

internal fun parseSnapshotRetryAfterMillis(value: String?): Long? {
    val seconds = value?.trim()?.toLongOrNull() ?: return null
    if (seconds <= 0 || seconds > Long.MAX_VALUE / 1_000L) return null
    return seconds * 1_000L
}

internal class OversqliteRemoteApi(
    private val http: HttpClient,
    private val json: Json,
    private val snapshotDiagnostics: () -> SnapshotDiagnosticsRecorder? = { null },
    private val snapshotRetirementTimeoutMillis: Long = defaultSnapshotRetirementTimeoutMillis,
    private val log: ((() -> String)) -> Unit,
) {
    private companion object {
        const val sourceIdHeaderName = "Oversync-Source-ID"
        const val snapshotCapabilitiesBodyLimit = 4L * 1024L * 1024L
        const val snapshotControlBodyLimit = 64L * 1024L
        const val snapshotBodyEnvelopeBytes = 64L * 1024L
        val snapshotSessionLimitDimensions = setOf("row_count", "byte_count", "row_byte_count")
        val snapshotCapacityErrorCodes = setOf("snapshot_build_capacity", "snapshot_chunk_capacity")
        val knownSnapshotErrorCodes = setOf(
            "history_pruned",
            "initialization_expired",
            "initialization_stale",
            "snapshot_build_capacity",
            "snapshot_chunk_capacity",
            "snapshot_chunk_too_small",
            "snapshot_session_limit_exceeded",
            "source_replacement_invalid",
            "source_retired",
            "source_sequence_changed",
            "source_sequence_out_of_order",
        )
    }

    suspend fun fetchCapabilities(sourceId: String): CapabilitiesResponse {
        return executeLoggedStreamingCall(
            operation = "capabilities request",
            method = "GET",
            path = "/sync/capabilities",
            prepareCall = {
                http.prepareGet("sync/capabilities") {
                    header(sourceIdHeaderName, requireValidOversqliteSourceId(sourceId))
                }
            },
            consume = { response ->
                requireOkBoundedJson<CapabilitiesResponse>(
                    operation = "capabilities request",
                    method = "GET",
                    path = "/sync/capabilities",
                    call = response,
                    successLimit = snapshotCapabilitiesBodyLimit,
                    errorLimit = snapshotControlBodyLimit,
                )
            },
        ).requireSupportedProtocol()
    }

    suspend fun watchBundleChanges(
        sourceId: String,
        afterBundleSeq: Long,
        onEvent: suspend (BundleChangeEvent) -> Unit,
    ) {
        val normalizedAfterBundleSeq = afterBundleSeq.coerceAtLeast(0L)
        val path = "/sync/watch?after_bundle_seq=$normalizedAfterBundleSeq"
        log { "oversqlite http start op=bundle change watch method=GET path=$path" }
        try {
            http.prepareGet("sync/watch") {
                    header(sourceIdHeaderName, requireValidOversqliteSourceId(sourceId))
                timeout {
                    requestTimeoutMillis = HttpTimeoutConfig.INFINITE_TIMEOUT_MS
                }
                url {
                    parameters.append("after_bundle_seq", normalizedAfterBundleSeq.toString())
                }
            }.execute { response ->
                log {
                    "oversqlite http response op=bundle change watch method=GET path=$path " +
                        "status=${response.status.value}"
                }
                if (response.status != HttpStatusCode.OK) {
                    readBoundedJsonBody(
                        operation = "bundle change watch",
                        response = response,
                        limit = snapshotControlBodyLimit,
                        invalidUtf8 = { RemoteResponseDecodeException("bundle change watch") },
                    )
                    log {
                        "oversqlite http non-ok op=bundle change watch method=GET path=$path " +
                            "status=${response.status.value}"
                    }
                    throw DownloadHttpException(response.status, "")
                }
                val parser = BundleChangeWatchSseParser(json)
                val channel = response.bodyAsChannel()
                while (!channel.isClosedForRead) {
                    val line = try {
                        channel.readLineStrict(limit = bundleChangeWatchMaxLineBytes.toLong() + 1L)
                    } catch (_: EOFException) {
                        break
                    } ?: break
                    for (event in parser.accept(line)) {
                        onEvent(event)
                    }
                }
                parser.finish()
            }
        } catch (error: Throwable) {
            if (error is CancellationException) {
                throw error
            }
            log {
                "oversqlite http failure op=bundle change watch method=GET path=$path " +
                    "error=${error.logSummary()}"
            }
            throw when (error) {
                is DownloadHttpException,
                is SnapshotResponseBodyTooLargeException,
                is SnapshotUnsupportedContentEncodingException,
                is IOException,
                -> error
                else -> RemoteResponseDecodeException("bundle change watch")
            }
        }
    }

    suspend fun connect(
        sourceId: String,
        hasLocalPendingRows: Boolean,
    ): ConnectResponse {
        log {
            "oversqlite connect hasLocalPendingRows=$hasLocalPendingRows"
        }
        val response = requireOkJson<ConnectResponse>(
            operation = "connect request",
            method = "POST",
            path = "/sync/connect",
            call = executeLoggedCall(
                operation = "connect request",
                method = "POST",
                path = "/sync/connect",
            ) {
                http.post("sync/connect") {
                    header(sourceIdHeaderName, requireValidOversqliteSourceId(sourceId))
                    contentType(ContentType.Application.Json)
                    setBody(
                        ConnectRequest(
                            hasLocalPendingRows = hasLocalPendingRows,
                        ),
                    )
                }
            },
        )
        try {
            validateConnectResponse(response)
        } catch (_: RuntimeException) {
            throw RemoteResponseSemanticException("connect request")
        }
        return response
    }

    suspend fun createPushSession(
        sourceBundleId: Long,
        plannedRowCount: Long,
        canonicalRequestHash: String,
        sourceId: String,
        initializationId: String? = null,
    ): PushSessionCreateResponse {
        log {
            "oversqlite createPushSession sourceBundleId=$sourceBundleId plannedRowCount=$plannedRowCount"
        }
        val validatedInitializationId = initializationId?.let(::requireValidOversqliteSessionToken)
        val response = requireOkJson<PushSessionCreateResponse>(
            operation = "push session request",
            method = "POST",
            path = "/sync/push-sessions",
            call = executeLoggedCall(
                operation = "push session request",
                method = "POST",
                path = "/sync/push-sessions",
            ) {
                http.post("sync/push-sessions") {
                    header(sourceIdHeaderName, requireValidOversqliteSourceId(sourceId))
                    contentType(ContentType.Application.Json)
                    setBody(
                        PushSessionCreateRequest(
                            sourceBundleId = sourceBundleId,
                            plannedRowCount = plannedRowCount,
                            canonicalRequestHash = canonicalRequestHash,
                            initializationId = validatedInitializationId,
                        ),
                    )
                }
            },
        ) { status, raw ->
            decodeSourceRecoveryRequiredExceptionOrNull(status, raw, sourceId)
                ?: decodeInitializationLeaseExceptionOrNull(status, raw)
        }
        try {
            validatePushSessionCreateResponse(response, sourceBundleId, plannedRowCount, sourceId, canonicalRequestHash)
        } catch (error: SourceSequenceMismatchException) {
            throw error
        } catch (_: RuntimeException) {
            throw RemoteResponseSemanticException("push session request")
        }
        log {
            "oversqlite createPushSession response nextExpected=${response.nextExpectedRowOrdinal} " +
                "bundleSeq=${response.bundleSeq}"
        }
        return response
    }

    suspend fun uploadPushChunk(
        pushId: String,
        sourceId: String,
        request: PushSessionChunkRequest,
    ): PushSessionChunkResponse {
        val validatedPushId = requireValidOversqliteSessionToken(pushId)
        val encodedPushId = encodeOversqliteSessionIdPathSegment(validatedPushId)
        log {
            "oversqlite uploadPushChunk start=${request.startRowOrdinal} rows=${request.rows.size}"
        }
        val response = requireOkJson<PushSessionChunkResponse>(
            operation = "push chunk request",
            method = "POST",
            path = "/sync/push-sessions/{push_id}/chunks",
            call = executeLoggedCall(
                operation = "push chunk request",
                method = "POST",
                path = "/sync/push-sessions/{push_id}/chunks",
            ) {
                http.post("sync/push-sessions/$encodedPushId/chunks") {
                    header(sourceIdHeaderName, requireValidOversqliteSourceId(sourceId))
                    contentType(ContentType.Application.Json)
                    setBody(request)
                }
            },
        ) { status, raw ->
            decodeInitializationLeaseExceptionOrNull(status, raw)
        }
        try {
            validatePushSessionChunkResponse(response, validatedPushId)
        } catch (_: RuntimeException) {
            throw RemoteResponseSemanticException("push chunk request")
        }
        log {
            "oversqlite uploadPushChunk response nextExpected=${response.nextExpectedRowOrdinal}"
        }
        return response
    }

    suspend fun commitPushSession(
        pushId: String,
        sourceId: String,
    ): PushSessionCommitResponse {
        val encodedPushId = encodeOversqliteSessionIdPathSegment(requireValidOversqliteSessionToken(pushId))
        log { "oversqlite commitPushSession" }
        val response = requireOkJson<PushSessionCommitResponse>(
            operation = "push commit request",
            method = "POST",
            path = "/sync/push-sessions/{push_id}/commit",
            call = executeLoggedCall(
                operation = "push commit request",
                method = "POST",
                path = "/sync/push-sessions/{push_id}/commit",
            ) {
                http.post("sync/push-sessions/$encodedPushId/commit") {
                    header(sourceIdHeaderName, requireValidOversqliteSourceId(sourceId))
                }
            },
        ) { status, raw ->
            decodePushConflictExceptionOrNull(status, raw)
                ?: decodeSourceRecoveryRequiredExceptionOrNull(status, raw, sourceId)
                ?: decodeInitializationLeaseExceptionOrNull(status, raw)
        }
        log {
            "oversqlite commitPushSession response bundleSeq=${response.bundleSeq} rowCount=${response.rowCount} " +
                "sourceBundleId=${response.sourceBundleId}"
        }
        return response
    }

    suspend fun fetchCommittedBundleChunk(
        bundleSeq: Long,
        sourceId: String,
        afterRowOrdinal: Long?,
        maxRows: Int,
    ): CommittedBundleRowsResponse {
        val path = buildString {
            append("/sync/committed-bundles/")
            append(bundleSeq)
            append("/rows?after_row_ordinal=")
            append(afterRowOrdinal?.toString() ?: "<none>")
            append("&max_rows=")
            append(maxRows)
        }
        return requireOkJson(
            operation = "committed bundle chunk request",
            method = "GET",
            path = path,
            call = executeLoggedCall(
                operation = "committed bundle chunk request",
                method = "GET",
                path = path,
            ) {
                http.get("sync/committed-bundles/$bundleSeq/rows") {
                    header(sourceIdHeaderName, requireValidOversqliteSourceId(sourceId))
                    url {
                        if (afterRowOrdinal != null) {
                            parameters.append("after_row_ordinal", afterRowOrdinal.toString())
                        }
                        parameters.append("max_rows", maxRows.toString())
                    }
                }
            },
        ) { status, raw ->
            decodeCommittedBundleNotFoundExceptionOrNull(status, raw)
                ?: decodeCommittedReplayPrunedExceptionOrNull(status, raw)
        }
    }

    suspend fun deletePushSessionBestEffort(pushId: String, sourceId: String) {
        val validatedPushId = try {
            requireValidOversqliteSessionToken(pushId)
        } catch (_: IllegalArgumentException) {
            return
        }
        deleteSessionBestEffort(
            sessionId = validatedPushId,
            sourceId = sourceId,
            operation = "delete push session",
            pathPrefix = "sync/push-sessions",
        )
    }

    suspend fun createSnapshotSession(
        sourceId: String,
        request: SnapshotSessionCreateRequest? = null,
    ): SnapshotSession {
        request?.sourceReplacement?.let { replacement ->
            requireValidOversqliteSourceId(replacement.previousSourceId)
            requireValidOversqliteSourceId(replacement.newSourceId)
        }
        var createdSnapshotId: String? = null
        val wire = try {
            executeLoggedStreamingCall(
                operation = "snapshot session request",
                method = "POST",
                path = "/sync/snapshot-sessions",
                prepareCall = {
                    http.preparePost("sync/snapshot-sessions") {
                    header(sourceIdHeaderName, requireValidOversqliteSourceId(sourceId))
                        if (request != null) {
                            contentType(ContentType.Application.Json)
                            setBody(request)
                        }
                    }
                },
                consume = { response ->
                    requireOkBoundedJson<SnapshotSessionWire>(
                        operation = "snapshot session request",
                        method = "POST",
                        path = "/sync/snapshot-sessions",
                        call = response,
                        successLimit = snapshotControlBodyLimit,
                        errorLimit = snapshotControlBodyLimit,
                        onSuccessRawBody = { raw ->
                            createdSnapshotId = runCatching {
                                json.parseToJsonElement(raw)
                                    .jsonObject["snapshot_id"]
                                    ?.jsonPrimitive
                                    ?.content
                            }.getOrNull()?.takeIf { it.isNotEmpty() }
                        },
                        customError = { status: HttpStatusCode, raw: String ->
                            decodeSnapshotSessionLimitExceededOrNull(status, raw)
                                ?: decodeSnapshotSourceRecoveryRequiredOrNull(status, raw, sourceId)
                                ?: decodeSnapshotSourceReplacementInvalidOrNull(status, raw)
                        },
                    )
                },
            )
        } catch (error: Throwable) {
            createdSnapshotId?.let { deleteSnapshotSessionBestEffort(it, sourceId) }
            throw error
        }
        val requiredByteCount = wire.byteCount
        if (requiredByteCount == null) {
            deleteSnapshotSessionBestEffort(wire.snapshotId, sourceId)
            throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_SESSION)
        }
        val session = SnapshotSession(
            snapshotId = wire.snapshotId,
            snapshotBundleSeq = wire.snapshotBundleSeq,
            rowCount = wire.rowCount,
            byteCount = requiredByteCount,
            expiresAt = wire.expiresAt,
        )
        try {
            validateSnapshotSession(session)
        } catch (error: Throwable) {
            deleteSnapshotSessionBestEffort(session.snapshotId, sourceId)
            throw error
        }
        snapshotDiagnostics()?.recordSession()
        return session
    }

    suspend fun fetchSnapshotChunk(
        snapshotId: String,
        sourceId: String,
        snapshotBundleSeq: Long,
        afterRowOrdinal: Long,
        maxRows: Int,
        maxBytes: Long,
    ): SnapshotChunkResponse {
        val encodedSnapshotId = encodeOversqliteSessionIdPathSegment(snapshotId)
        val path = buildString {
            append("/sync/snapshot-sessions/{snapshot_id}")
            append("?after_row_ordinal=")
            append(afterRowOrdinal)
            append("&max_rows=")
            append(maxRows)
            append("&max_bytes=")
            append(maxBytes)
        }
        val bodyLimit = checkedSnapshotChunkBodyLimit(maxBytes, maxRows)
        val chunk = executeLoggedStreamingCall(
            operation = "snapshot chunk request",
            method = "GET",
            path = path,
            prepareCall = {
                http.prepareGet("sync/snapshot-sessions/$encodedSnapshotId") {
                    header(sourceIdHeaderName, requireValidOversqliteSourceId(sourceId))
                    url {
                        parameters.append("after_row_ordinal", afterRowOrdinal.toString())
                        parameters.append("max_rows", maxRows.toString())
                        parameters.append("max_bytes", maxBytes.toString())
                    }
                }
            },
            consume = { response ->
                requireOkBoundedJson<SnapshotChunkResponse>(
                    operation = "snapshot chunk request",
                    method = "GET",
                    path = path,
                    call = response,
                    successLimit = bodyLimit,
                    errorLimit = snapshotControlBodyLimit,
                    scanAllObjectMembers = false,
                    successDecoder = ::decodeSnapshotChunkResponse,
                    onCompleteBody = { decodedBytes ->
                        snapshotDiagnostics()?.recordCompletelyDecodedChunkBody(decodedBytes)
                    },
                    customError = { status, raw ->
                        decodeSnapshotChunkTooSmallOrNull(status, raw, maxBytes)
                    },
                )
            },
        )
        validateSnapshotChunkResponse(chunk, snapshotId, snapshotBundleSeq, afterRowOrdinal, maxRows, maxBytes)
        snapshotDiagnostics()?.recordValidatedChunk(chunk.rows.size, chunk.byteCount)
        return chunk
    }

    suspend fun deleteSnapshotSessionBestEffort(snapshotId: String, sourceId: String) {
        deleteSessionBestEffort(
            sessionId = snapshotId,
            sourceId = sourceId,
            operation = "delete snapshot session",
            pathPrefix = "sync/snapshot-sessions",
        )
    }

    private suspend fun deleteSessionBestEffort(
        sessionId: String,
        sourceId: String,
        operation: String,
        pathPrefix: String,
    ) {
        if (sessionId.isEmpty()) return
        val relativePath = "$pathPrefix/${encodeOversqliteSessionIdPathSegment(sessionId)}"
        val loggedPath = "/$pathPrefix/{session_id}"
        withContext(NonCancellable) {
            runCatching {
                withTimeout(snapshotRetirementTimeoutMillis) {
                    executeLoggedStreamingCall(
                        operation = operation,
                        method = "DELETE",
                        path = loggedPath,
                        prepareCall = {
                            http.prepareDelete(relativePath) {
                    header(sourceIdHeaderName, requireValidOversqliteSourceId(sourceId))
                            }
                        },
                        consume = { response ->
                            response.bodyAsChannel().cancel(null)
                        },
                    )
                }
            }.onFailure { error ->
                log {
                    "oversqlite http best-effort failure op=$operation method=DELETE " +
                        "path=$loggedPath error=${error.logSummary()}"
                }
            }
        }
    }

    private suspend inline fun <reified T> requireOkBoundedJson(
        operation: String,
        method: String,
        path: String,
        call: HttpResponse,
        successLimit: Long,
        errorLimit: Long,
        scanAllObjectMembers: Boolean = true,
        noinline onCompleteBody: ((Long) -> Unit)? = null,
        noinline onSuccessRawBody: ((String) -> Unit)? = null,
        noinline successDecoder: ((String) -> T)? = null,
        noinline customError: ((HttpStatusCode, String) -> Throwable?)? = null,
    ): T {
        val limit = if (call.status == HttpStatusCode.OK) successLimit else errorLimit
        val body = readBoundedJsonBody(
            operation = operation,
            response = call,
            limit = limit,
            invalidUtf8 = { SnapshotResponseDecodeException(operation) },
        )
        val raw = body.raw
        if (call.status != HttpStatusCode.OK) {
            val error = runCatching { json.decodeFromString<ErrorResponse>(raw) }.getOrNull()
            if (error != null) {
                onCompleteBody?.invoke(body.byteCount)
            }
            val safeErrorCode = error?.error
                ?.takeIf(knownSnapshotErrorCodes::contains)
                ?: "invalid_error_response"
            log {
                "oversqlite http non-ok op=$operation method=$method path=$path " +
                    "status=${call.status.value} error=$safeErrorCode"
            }
            customError?.invoke(call.status, raw)?.let { throw it }
            when {
                call.status == HttpStatusCode.TooManyRequests &&
                    safeErrorCode in snapshotCapacityErrorCodes ->
                    throw SnapshotCapacityException(
                        status = call.status,
                        errorCode = safeErrorCode,
                        retryAfterMillis = parseSnapshotRetryAfterMillis(call.headers[HttpHeaders.RetryAfter]),
                    )
            }
            throw SnapshotHttpException(call.status, safeErrorCode)
        }
        return try {
            if (scanAllObjectMembers) {
                requireUniqueSnapshotJsonObjectMembers(raw)
            }
            onSuccessRawBody?.invoke(raw)
            (successDecoder?.invoke(raw) ?: json.decodeFromString<T>(raw)).also {
                onCompleteBody?.invoke(body.byteCount)
            }
        } catch (error: Throwable) {
            if (error is CancellationException) throw error
            if (error is SnapshotSemanticException) throw error
            log {
                "oversqlite http decode failure op=$operation method=$method path=$path " +
                    "status=${call.status.value} error=${error::class.simpleName ?: "Throwable"}"
            }
            throw SnapshotResponseDecodeException(operation)
        }
    }

    private suspend fun readBoundedJsonBody(
        operation: String,
        response: HttpResponse,
        limit: Long,
        invalidUtf8: () -> Throwable,
    ): BoundedJsonBody {
        val body = readDecodedBodyBounded(operation, response, limit)
        val raw = try {
            body.decodeToString(throwOnInvalidSequence = true)
        } catch (_: CharacterCodingException) {
            throw invalidUtf8()
        }
        return BoundedJsonBody(raw = raw, byteCount = body.size.toLong())
    }

    private suspend fun readDecodedBodyBounded(
        operation: String,
        response: HttpResponse,
        limit: Long,
    ): ByteArray {
        require(limit >= 0L && limit < Int.MAX_VALUE.toLong()) {
            "$operation response body limit is invalid"
        }
        val encoding = response.headers[HttpHeaders.ContentEncoding]
            ?.trim()
            ?.lowercase()
            .orEmpty()
        if (encoding !in setOf("", "identity", "gzip")) {
            response.bodyAsChannel().cancel(null)
            throw SnapshotUnsupportedContentEncodingException(operation, "unsupported")
        }
        val channel = response.bodyAsChannel()
        return try {
            val body = channel.readBuffer((limit + 1L).toInt()).readByteArray()
            channel.closedCause?.let { throw it }
            if (body.size.toLong() > limit) {
                throw SnapshotResponseBodyTooLargeException(operation, limit)
            }
            body
        } finally {
            if (!channel.isClosedForRead) {
                channel.cancel(null)
            }
        }
    }

    private fun checkedSnapshotChunkBodyLimit(maxBytes: Long, maxRows: Int): Long {
        require(maxBytes > 0L && maxRows > 0) {
            "snapshot chunk body budget requires positive row and byte limits"
        }
        val withRows = checkedAddSnapshotLong(maxBytes, maxRows.toLong()) {
            "snapshot chunk body budget overflow"
        }
        return checkedAddSnapshotLong(withRows, snapshotBodyEnvelopeBytes) {
            "snapshot chunk body budget overflow"
        }
    }

    private fun decodeSnapshotChunkTooSmallOrNull(
        status: HttpStatusCode,
        raw: String,
        configuredBytes: Long,
    ): Throwable? {
        if (status != HttpStatusCode.BadRequest) return null
        val error = runCatching { json.decodeFromString<ErrorResponse>(raw) }.getOrNull() ?: return null
        if (error.error != "snapshot_chunk_too_small") return null
        val required = error.requiredByteCount
        if (required == null || required <= configuredBytes) {
            return SnapshotHttpException(status, "snapshot_chunk_too_small_invalid")
        }
        return SnapshotChunkTooSmallException(configuredBytes, required)
    }

    private fun decodeSnapshotSessionLimitExceededOrNull(
        status: HttpStatusCode,
        raw: String,
    ): Throwable? {
        if (status != HttpStatusCode.Conflict) return null
        val error = runCatching { json.decodeFromString<ErrorResponse>(raw) }.getOrNull() ?: return null
        if (error.error != "snapshot_session_limit_exceeded") return null
        val response = runCatching {
            json.decodeFromString<SnapshotSessionLimitResponseWire>(raw)
        }.getOrNull()
        if (
            response == null ||
            response.error != "snapshot_session_limit_exceeded" ||
            response.dimension !in snapshotSessionLimitDimensions ||
            response.limit <= 0L ||
            response.actual <= response.limit
        ) {
            return SnapshotHttpException(status, "invalid_error_response")
        }
        return SnapshotSessionLimitExceededException(
            dimension = response.dimension,
            actual = response.actual,
            limit = response.limit,
        )
    }

    private fun decodeSnapshotSourceRecoveryRequiredOrNull(
        status: HttpStatusCode,
        raw: String,
        expectedSourceId: String,
    ): Throwable? {
        if (status != HttpStatusCode.Conflict) return null
        val retired = decodeValidatedSourceRetiredWireOrNull(raw, expectedSourceId)
        if (retired != null) {
            return SnapshotSourceRecoveryRequiredException(
                reason = SourceRecoveryReason.SOURCE_RETIRED,
                replacementSourceId = retired.replacementSourceId,
            )
        }
        val error = runCatching { json.decodeFromString<ErrorResponse>(raw) }.getOrNull() ?: return null
        val reason = when (error.error) {
            "history_pruned" -> SourceRecoveryReason.HISTORY_PRUNED
            "source_sequence_out_of_order" -> SourceRecoveryReason.SOURCE_SEQUENCE_OUT_OF_ORDER
            "source_sequence_changed" -> SourceRecoveryReason.SOURCE_SEQUENCE_CHANGED
            else -> return null
        }
        return SnapshotSourceRecoveryRequiredException(reason)
    }

    private fun decodeSnapshotSourceReplacementInvalidOrNull(
        status: HttpStatusCode,
        raw: String,
    ): Throwable? {
        if (status != HttpStatusCode.Conflict) return null
        val error = runCatching { json.decodeFromString<ErrorResponse>(raw) }.getOrNull() ?: return null
        if (error.error != "source_replacement_invalid") return null
        return SourceReplacementInvalidException()
    }

    suspend fun sendPullRequest(
        afterBundleSeq: Long,
        maxBundles: Int,
        targetBundleSeq: Long,
        sourceId: String,
    ): PullResponse {
        val path = buildString {
            append("/sync/pull?after_bundle_seq=")
            append(afterBundleSeq)
            append("&max_bundles=")
            append(maxBundles)
            if (targetBundleSeq > 0) {
                append("&target_bundle_seq=")
                append(targetBundleSeq)
            }
        }
        return requireOkJson(
            operation = "pull",
            method = "GET",
            path = path,
            call = executeLoggedCall(
                operation = "pull",
                method = "GET",
                path = path,
            ) {
                http.get("sync/pull") {
                    header(sourceIdHeaderName, requireValidOversqliteSourceId(sourceId))
                    url {
                        parameters.append("after_bundle_seq", afterBundleSeq.toString())
                        parameters.append("max_bundles", maxBundles.toString())
                        if (targetBundleSeq > 0) {
                            parameters.append("target_bundle_seq", targetBundleSeq.toString())
                        }
                    }
                }
            },
        ) { status, raw ->
            if (status != HttpStatusCode.Conflict) {
                return@requireOkJson null
            }
            val error = runCatching { json.decodeFromString(ErrorResponse.serializer(), raw) }.getOrNull()
            when (error?.error) {
                "history_pruned" -> HistoryPrunedException("remote history is no longer available")
                "checkpoint_ahead" -> CheckpointAheadException("local checkpoint is ahead of the server")
                else -> null
            }
        }
    }

    private suspend inline fun <reified T> requireOkJson(
        operation: String,
        method: String,
        path: String,
        call: HttpResponse,
        noinline customError: ((HttpStatusCode, String) -> Throwable?)? = null,
    ): T {
        if (call.status != HttpStatusCode.OK) {
            val raw = readBoundedJsonBody(
                operation = operation,
                response = call,
                limit = snapshotControlBodyLimit,
                invalidUtf8 = { RemoteResponseDecodeException(operation) },
            ).raw
            log {
                "oversqlite http non-ok op=$operation method=$method path=$path " +
                    "status=${call.status.value}"
            }
            val typed = try {
                customError?.invoke(call.status, raw)
            } catch (_: RuntimeException) {
                throw RemoteResponseSemanticException(operation)
            }
            typed?.let { throw it.withRedactedRemotePayload() }
            if (path.startsWith("/sync/push-sessions")) {
                throw UploadHttpException(call.status, "")
            }
            throw DownloadHttpException(call.status, "")
        }
        return try {
            call.body()
        } catch (error: Throwable) {
            if (error is CancellationException) {
                throw error
            }
            if (
                error is UnsupportedContentEncodingException &&
                (operation == "capabilities request" || operation.startsWith("snapshot"))
            ) {
                throw SnapshotUnsupportedContentEncodingException(operation, "unsupported")
            }
            log {
                "oversqlite http decode failure op=$operation method=$method path=$path " +
                    "status=${call.status.value} error=${error.logSummary()}"
            }
            throw RemoteResponseDecodeException(operation)
        }
    }

    private suspend inline fun executeLoggedCall(
        operation: String,
        method: String,
        path: String,
        crossinline block: suspend () -> HttpResponse,
    ): HttpResponse {
        log { "oversqlite http start op=$operation method=$method path=$path" }
        return try {
            val response = block()
            log {
                "oversqlite http response op=$operation method=$method path=$path " +
                    "status=${response.status.value}"
            }
            response
        } catch (error: Throwable) {
            if (error is CancellationException) {
                throw error
            }
            if (
                error is UnsupportedContentEncodingException &&
                (operation == "capabilities request" || operation.startsWith("snapshot"))
            ) {
                throw SnapshotUnsupportedContentEncodingException(operation, "unsupported")
            }
            log {
                "oversqlite http failure op=$operation method=$method path=$path " +
                    "error=${error.logSummary()}"
            }
            throw error
        }
    }

    private suspend inline fun <T> executeLoggedStreamingCall(
        operation: String,
        method: String,
        path: String,
        crossinline prepareCall: suspend () -> HttpStatement,
        crossinline consume: suspend (HttpResponse) -> T,
    ): T {
        log { "oversqlite http start op=$operation method=$method path=$path" }
        return try {
            prepareCall().execute { response ->
                log {
                    "oversqlite http response op=$operation method=$method path=$path " +
                        "status=${response.status.value}"
                }
                consume(response)
            }
        } catch (error: Throwable) {
            if (error is CancellationException) {
                throw error
            }
            if (error is UnsupportedContentEncodingException) {
                throw SnapshotUnsupportedContentEncodingException(operation, "unsupported")
            }
            log {
                "oversqlite http failure op=$operation method=$method path=$path " +
                    "error=${error.logSummary()}"
            }
            throw error
        }
    }

    private fun decodeInitializationLeaseExceptionOrNull(
        status: HttpStatusCode,
        raw: String,
    ): Throwable? {
        if (status != HttpStatusCode.Conflict && status != HttpStatusCode.Gone) {
            return null
        }
        val error = runCatching { json.decodeFromString(ErrorResponse.serializer(), raw) }.getOrNull()
            ?: return null
        return when (error.error) {
            "initialization_stale", "initialization_expired" -> InitializationLeaseInvalidException(error.error)
            else -> null
        }
    }
}

internal fun encodeOversqliteSessionIdPathSegment(value: String): String = buildString {
    val hexadecimal = "0123456789ABCDEF"
    for (byte in value.encodeToByteArray()) {
        val unsigned = byte.toInt() and 0xff
        val unescaped = unsigned in 'a'.code..'z'.code ||
            unsigned in 'A'.code..'Z'.code ||
            unsigned in '0'.code..'9'.code ||
            unsigned == '-'.code || unsigned == '_'.code || unsigned == '~'.code
        if (unescaped) {
            append(unsigned.toChar())
        } else {
            append('%')
            append(hexadecimal[unsigned ushr 4])
            append(hexadecimal[unsigned and 0x0f])
        }
    }
}

private fun Throwable.withRedactedRemotePayload(): Throwable = when (this) {
    is PushConflictException -> PushConflictException(rawBody = "", response = response.copy(message = ""))
    is SourceRecoveryRequiredHttpException -> SourceRecoveryRequiredHttpException(
        status = status,
        rawBody = "",
        reason = reason,
        replacementSourceId = replacementSourceId?.let(::requireValidOversqliteSourceId),
    )
    is SourceReplacementInvalidHttpException -> SourceReplacementInvalidHttpException(status)
    is CommittedReplayPrunedException -> CommittedReplayPrunedException(rawBody = "")
    is CommittedBundleNotFoundException -> CommittedBundleNotFoundException(rawBody = "")
    else -> this
}

private fun Throwable.logSummary(): String = this::class.simpleName ?: "Throwable"
