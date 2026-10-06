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
import io.ktor.client.plugins.HttpTimeoutConfig
import io.ktor.client.plugins.timeout
import io.ktor.client.request.prepareDelete
import io.ktor.client.request.prepareGet
import io.ktor.client.request.preparePost
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.HttpStatement
import io.ktor.client.statement.bodyAsChannel
import io.ktor.http.ContentType
import io.ktor.http.HeadersBuilder
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.util.AttributeKey
import io.ktor.utils.io.readBuffer
import io.ktor.utils.io.readLineStrict
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.io.EOFException
import kotlinx.io.IOException
import kotlinx.io.readByteArray
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import io.ktor.client.request.HttpRequestBuilder
import io.ktor.client.request.HttpRequestPipeline
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

@Serializable
private data class SnapshotSessionWire(
    @SerialName("snapshot_id") val snapshotId: String,
    @SerialName("snapshot_bundle_seq") val snapshotBundleSeq: Long,
    @SerialName("row_count") val rowCount: Long,
    @SerialName("logical_byte_count") val logicalByteCount: Long? = null,
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

@Serializable
private data class CompactBundleWire(
    @SerialName("bundle_seq") val bundleSeq: Long,
    @SerialName("source_id") val sourceId: String,
    @SerialName("source_bundle_id") val sourceBundleId: Long,
    @SerialName("bundle_hash") val bundleHash: String,
    val rows: JsonElement,
)

@Serializable
private data class CompactPullResponseWire(
    @SerialName("stable_bundle_seq") val stableBundleSeq: Long,
    val bundles: List<CompactBundleWire>,
    @SerialName("has_more") val hasMore: Boolean,
)

@Serializable
private data class CompactSnapshotChunkWire(
    val rows: JsonElement,
    @SerialName("next_row_ordinal") val nextRowOrdinal: Long,
    @SerialName("has_more") val hasMore: Boolean,
    @SerialName("logical_byte_count") val logicalByteCount: Long,
)

private data class BoundedJsonBody(
    val raw: String,
    val byteCount: Long,
)

private fun ByteArray.isWellFormedUtf8(): Boolean {
    var index = 0
    while (index < size) {
        val first = this[index].toInt() and 0xff
        if (first < 0x80) {
            index++
            continue
        }
        val count = when (first) {
            in 0xc2..0xdf -> 2
            in 0xe0..0xef -> 3
            in 0xf0..0xf4 -> 4
            else -> return false
        }
        if (index > size - count) return false
        val second = this[index + 1].toInt() and 0xff
        if (second !in 0x80..0xbf) return false
        if (first == 0xe0 && second < 0xa0) return false
        if (first == 0xed && second > 0x9f) return false
        if (first == 0xf0 && second < 0x90) return false
        if (first == 0xf4 && second > 0x8f) return false
        if (count >= 3 && (this[index + 2].toInt() and 0xff) !in 0x80..0xbf) return false
        if (count == 4 && (this[index + 3].toInt() and 0xff) !in 0x80..0xbf) return false
        index += count
    }
    return true
}

private data class SyncRequestIdentity(val sourceId: String, val generation: SyncGeneration?)
private val syncRequestIdentityKey = AttributeKey<SyncRequestIdentity>("OversqliteRequestIdentity")
private val syncHeadersInstalledKey = AttributeKey<Unit>("OversqliteIdentityHeadersInstalled")

private val responseBodyAbandonedCancellation =
    CancellationException("Oversqlite response body is no longer needed")

private val streamingCallOwnerKey =
    AttributeKey<StreamingCallOwner>("OversqliteStreamingCallOwner")
private class PushBufferGrowth(val grow: suspend (Long,Long) -> Unit)
private val pushBufferGrowthKey = AttributeKey<PushBufferGrowth>("OversqlitePushBufferGrowth")

// Count compact row arrays and recursion depth without creating a JSON tree.
// This is allocation admission, not validation; the exact codec still validates
// the complete response, fields, ordering, values and hash before settlement.
private suspend fun compactResponseAllocationShape(raw: String): Long {
    var quoted=false; var escaped=false; var depth=0L; var maximum=0L; var containers=0L
    for(index in raw.indices) {
        if(index%256==0) currentCoroutineContext().ensureActive()
        val ch=raw[index]
        if(quoted) {
            if(escaped) escaped=false else if(ch=='\\') escaped=true else if(ch=='"') quoted=false
            continue
        }
        when(ch) {
            '"' -> quoted=true
            '[' -> { containers++; depth++; maximum=maxOf(maximum,depth) }
            '{' -> { containers++; depth++; maximum=maxOf(maximum,depth) }
            ']', '}' -> depth--
        }
    }
    return containers*32+maximum+64
}

private class StreamingCallOwner(private val callerJob: Job) {
    var abandoned: Boolean = false
        private set

    fun cancelWithoutDraining(response: HttpResponse) {
        abandoned = true
        response.cancel(responseBodyAbandonedCancellation)
        callerJob.cancel(responseBodyAbandonedCancellation)
    }
}

private fun HttpResponse.cancelWithoutDraining() {
    call.attributes[streamingCallOwnerKey].cancelWithoutDraining(this)
}

private fun redirectException(path: String, status: HttpStatusCode): Throwable = when {
    path.startsWith("/sync/snapshot") -> SnapshotHttpException(status, "redirect_refused")
    path == "/sync/push" -> UploadHttpException(status, "")
    else -> DownloadHttpException(status, "")
}

private fun HttpResponse.rejectRedirect(operation: String, method: String, path: String, log: ((() -> String) -> Unit)) {
    if (status.value !in 300..399) return
    cancelWithoutDraining()
    log {
        "oversqlite http redirect refused op=$operation method=$method path=$path status=${status.value}"
    }
    throw redirectException(path, status)
}

private suspend fun <T> executeStreamingCall(
    prepareCall: suspend () -> HttpStatement,
    consume: suspend (HttpResponse) -> T,
): T = supervisorScope {
    var owner: StreamingCallOwner? = null
    var consumeOutcome: Result<T>? = null
    val request = async {
        val callOwner = StreamingCallOwner(
            requireNotNull(currentCoroutineContext()[Job]) {
                "Oversqlite streaming request requires a coroutine job"
            },
        )
        owner = callOwner
        prepareCall().execute { response ->
            response.call.attributes.put(streamingCallOwnerKey, callOwner)
            try {
                consume(response).also { consumeOutcome = Result.success(it) }
            } catch (error: Throwable) {
                consumeOutcome = Result.failure(error)
                throw error
            }
        }
    }
    try {
        request.await()
    } catch (error: Throwable) {
        val outcome = consumeOutcome
        if (owner?.abandoned == true && outcome != null) {
            return@supervisorScope outcome.getOrThrow()
        }
        throw error
    }
}

internal const val defaultSnapshotRetirementTimeoutMillis = 5_000L
internal const val oversqlitePullResponseBodyHardLimit = 154_701_824L
internal const val oversqlitePushResponseBodyHardLimit = 67_112_960L

internal fun oversqliteResponseBodyHardLimit(path: String, status: Int): Long = when {
    status == HttpStatusCode.OK.value && (path == "/sync/pull" || path.startsWith("/sync/pull?")) ->
        oversqlitePullResponseBodyHardLimit
    status == HttpStatusCode.OK.value && path == "/sync/push" ->
        oversqlitePushResponseBodyHardLimit
    status in setOf(HttpStatusCode.Conflict.value, HttpStatusCode.UnprocessableEntity.value) && path == "/sync/push" ->
        oversqlitePushResponseBodyHardLimit
    else -> generationControlBodyLimit
}

private fun HeadersBuilder.requireExactSdkHeader(name: String, expected: String?) {
    val effective = getAll(name).orEmpty()
    if (expected == null) {
        if (effective.isNotEmpty()) throw OversqliteRequestIdentityHeaderException(name)
        return
    }
    when {
        effective.isEmpty() -> append(name, expected)
        effective.size == 1 && effective.single() == expected -> Unit
        else -> throw OversqliteRequestIdentityHeaderException(name)
    }
}

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
    private val generation: () -> SyncGeneration = { error("generation contract is unconfirmed") },
    private val requestGzipEngine: OversqliteRequestGzipEngine = OversqliteRequestGzipEngine(::compressOversqliteRequestGzip),
    private val log: ((() -> String)) -> Unit,
) {
    private val strictJson = Json(json) { ignoreUnknownKeys = false }

    init {
        // DefaultRequest merges headers in Before. Apply the exact request identity afterward.
        // Production passes the SDK-owned derived client; unrelated requests carry no attribute.
        http.attributes.computeIfAbsent(syncHeadersInstalledKey) {
            http.requestPipeline.intercept(HttpRequestPipeline.State) {
                context.attributes.getOrNull(syncRequestIdentityKey)?.let { identity ->
                    context.headers.requireExactSdkHeader(sourceIdHeaderName, identity.sourceId)
                    context.headers.requireExactSdkHeader(
                        "Oversync-Schema-Generation-ID",
                        identity.generation?.id,
                    )
                    context.headers.requireExactSdkHeader(
                        "Oversync-Schema-Generation-Fingerprint",
                        identity.generation?.fingerprint,
                    )
                }
            }
        }
    }

    private companion object {
        const val sourceIdHeaderName = "Oversync-Source-ID"
        const val snapshotControlBodyLimit = 64L * 1024L
        val snapshotSessionLimitDimensions = setOf("row_count", "logical_byte_count", "row_decoded_body_bytes")
        val snapshotCapacityErrorCodes = setOf("snapshot_build_capacity", "snapshot_chunk_capacity")
        val knownSnapshotErrorCodes = setOf(
            "history_pruned",
            "initialization_expired",
            "initialization_stale",
            "snapshot_build_capacity",
            "snapshot_chunk_capacity",
            "snapshot_chunk_too_small",
            "snapshot_session_expired",
            "snapshot_session_limit_exceeded",
            "snapshot_session_not_found",
            "source_replacement_invalid",
            "source_retired",
            "source_sequence_changed",
            "source_sequence_out_of_order",
        )
    }

    private fun HttpRequestBuilder.generationHeaders(sourceId: String, proof: SyncGeneration? = null) {
        val source = requireValidOversqliteSourceId(sourceId)
        val selected = proof ?: generation()
        attributes.put(syncRequestIdentityKey, SyncRequestIdentity(source, selected))
    }

    private fun HttpRequestBuilder.acceptBoundedJsonResponse() {
        if (!oversqliteFetchManagesResponseCompression) {
            headers.append(HttpHeaders.AcceptEncoding, "gzip")
        }
    }

    private fun HttpRequestBuilder.acceptIdentityEventStream() {
        if (!oversqliteFetchManagesResponseCompression) {
            headers.append(HttpHeaders.AcceptEncoding, "identity")
        }
    }

    private suspend fun prepareBoundedJsonBody(raw: String, decodedLimit: Long): OversqliteEncodedBody =
        encodeOversqliteRequestBody(raw.encodeToByteArray(), decodedLimit, decodedLimit, requestGzipEngine)

    internal suspend fun prepareConnectBody(raw: String): OversqliteEncodedBody =
        prepareBoundedJsonBody(raw, snapshotControlBodyLimit)

    private fun HttpRequestBuilder.setSelectedJsonBody(selected: OversqliteEncodedBody) {
        contentType(ContentType.Application.Json)
        selected.contentEncoding?.let { headers.append(HttpHeaders.ContentEncoding, it) }
        setBody(selected.bytes)
    }

    suspend fun fetchGeneration(sourceId: String, generationId: Long): AdmittedOversqliteContract {
        require(generationId in 1L..Int.MAX_VALUE.toLong()) { "generationId must be in 1..2147483647" }
        val source = requireValidOversqliteSourceId(sourceId)
        val path = "sync/generations/$generationId"
        return executeLoggedStreamingCall(
            operation = "generation discovery",
            method = "GET",
            path = "/$path",
            prepareCall = {
                http.prepareGet(path) {
                    attributes.put(syncRequestIdentityKey, SyncRequestIdentity(source, null))
                    acceptBoundedJsonResponse()
                }
            },
            consume = { response ->
                requireOkBoundedJson<AdmittedOversqliteContract>(
                    operation = "generation discovery",
                    method = "GET",
                    path = "/$path",
                    call = response,
                    successLimit = generationControlBodyLimit,
                    errorLimit = generationControlBodyLimit,
                    scanAllObjectMembers = false,
                    successDecoder = { decodeGenerationDiscovery(it, generationId) },
                    customError = { status, raw -> decodeGenerationDiscoveryError(status.value, raw, generationId) },
                )
            },
        )
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
            executeStreamingCall(
                prepareCall = {
                    http.prepareGet("sync/watch") {
                        generationHeaders(sourceId)
                        acceptIdentityEventStream()
                        timeout {
                            requestTimeoutMillis = HttpTimeoutConfig.INFINITE_TIMEOUT_MS
                        }
                        url {
                            parameters.append("after_bundle_seq", normalizedAfterBundleSeq.toString())
                        }
                    }
                },
                consume = { response ->
                    log {
                        "oversqlite http response op=bundle change watch method=GET path=$path " +
                            "status=${response.status.value}"
                    }
                    response.rejectRedirect("bundle change watch", "GET", path, log)
                    if (response.status != HttpStatusCode.OK) {
                        val raw = readBoundedJsonBody(
                            operation = "bundle change watch",
                            response = response,
                            limit = snapshotControlBodyLimit,
                            invalidUtf8 = { RemoteResponseDecodeException("bundle change watch") },
                        ).raw
                        log {
                            "oversqlite http non-ok op=bundle change watch method=GET path=$path " +
                                "status=${response.status.value}"
                        }
                        decodeRemoteContractErrorOrNull(
                            status = response.status.value,
                            raw = raw,
                            operation = "bundle change watch",
                        )?.let { throw it }
                        throw DownloadHttpException(response.status, "")
                    }
                    val eventEncoding = response.headers.getAll(HttpHeaders.ContentEncoding).orEmpty()
                    if (eventEncoding.size > 1 || eventEncoding.singleOrNull()?.trim()?.lowercase() !in setOf(null, "identity")) {
                        response.cancelWithoutDraining()
                        throw SnapshotUnsupportedContentEncodingException("bundle change watch", "unsupported")
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
                },
            )
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
                is OversqliteRemoteContractException,
                is OversqliteRequestIdentityHeaderException,
                is RemoteResponseSemanticException,
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
        val selectedBody = prepareConnectBody(
            json.encodeToString(ConnectRequest(hasLocalPendingRows = hasLocalPendingRows)),
        )
        val response = requestJson<ConnectResponse>(
            operation = "connect request",
            method = "POST",
            path = "/sync/connect",
            prepareCall = {
                http.preparePost("sync/connect") {
                    generationHeaders(sourceId)
                    acceptBoundedJsonResponse()
                    setSelectedJsonBody(selectedBody)
                }
            },
            successValidator = ::validateConnectResponseWire,
        )
        try {
            validateConnectResponse(response)
        } catch (_: RuntimeException) {
            throw RemoteResponseSemanticException("connect request")
        }
        return response
    }

    suspend fun push(
        sourceId: String,
        sourceBundleId: Long,
        canonicalRequestHash: String,
        initializationId: String?,
        rows: List<PushRequestRow>,
        maxRequestDecodedBytes: Long,
        maxResponseDecodedBytes: Long,
        onBufferGrowth: (suspend (Long,Long) -> Unit)? = null,
    ): CompletePushResult {
        require(sourceBundleId > 0L) { "source_bundle_id must be positive" }
        require(isCanonicalSha256(canonicalRequestHash)) { "canonical_request_hash must be canonical SHA-256" }
        val selectedGeneration = generation()
        val body = canonicalizeJsonElement(buildJsonObject {
            put("source_bundle_id", JsonPrimitive(sourceBundleId))
            put("canonical_request_hash", JsonPrimitive(canonicalRequestHash))
            initializationId?.let { put("initialization_id", JsonPrimitive(requireValidOversqliteSessionToken(it))) }
            put("rows", OversqliteCompactCodec.encodePushRows(selectedGeneration, rows))
        })
        val selectedBody = prepareBoundedJsonBody(body, maxRequestDecodedBytes)
        val response = executeLoggedStreamingCall(
            operation = "push",
            method = "POST",
            path = "/sync/push",
            prepareCall = {
                http.preparePost("sync/push") {
                    generationHeaders(sourceId)
                    acceptBoundedJsonResponse()
                    setSelectedJsonBody(selectedBody)
                }
            },
            consume = { call ->
                onBufferGrowth?.let { call.call.attributes.put(pushBufferGrowthKey,PushBufferGrowth(it)) }
                decodeJsonResponse<CompletePushResponseWire>(
                    operation = "push",
                    method = "POST",
                    path = "/sync/push",
                    call = call,
                    successLimit = maxResponseDecodedBytes,
                    successValidator = { raw ->
                        require(json.parseToJsonElement(raw).jsonObject.keys == setOf("bundle_seq", "bundle_hash", "rows")) {
                            "push response fields are invalid"
                        }
                    },
                    customError = { status, raw ->
                        decodePushConflictExceptionOrNull(status, raw)
                            ?: decodePushReplayHistoryPrunedExceptionOrNull(status, raw)
                            ?: decodeSourceRecoveryRequiredExceptionOrNull(status, raw, sourceId)
                            ?: decodeInitializationLeaseExceptionOrNull(status, raw)
                    },
                )
            },
        )
        val expanded = OversqliteCompactCodec.decodeBundleRows(selectedGeneration, response.rows)
        require(response.bundleSeq > 0L) { "push response bundle_seq must be positive" }
        require(isCanonicalSha256(response.bundleHash)) { "push response bundle_hash is invalid" }
        expanded.forEach(::validateBundleRow)
        return CompletePushResult(response.bundleSeq, response.bundleHash, expanded)
    }

    suspend fun createSnapshotSession(
        sourceId: String,
        request: SnapshotSessionCreateRequest? = null,
    ): SnapshotSession {
        request?.sourceReplacement?.let { replacement ->
            requireValidOversqliteSourceId(replacement.previousSourceId)
            requireValidOversqliteSourceId(replacement.newSourceId)
        }
        val snapshotSourceId = request?.sourceReplacement?.newSourceId ?: sourceId
        val selectedBody = request?.let {
            prepareBoundedJsonBody(json.encodeToString(it), snapshotControlBodyLimit)
        }
        var createdSnapshotId: String? = null
        val wire = try {
            executeLoggedStreamingCall(
                operation = "snapshot session request",
                method = "POST",
                path = "/sync/snapshot-sessions",
                prepareCall = {
                    http.preparePost("sync/snapshot-sessions") {
                        // Replacement is reserved by the old authenticated source. The new
                        // source owns the resulting snapshot only after creation succeeds.
                        generationHeaders(sourceId)
                        acceptBoundedJsonResponse()
                        if (selectedBody == null) {
                            contentType(ContentType.Application.Json)
                        } else {
                            setSelectedJsonBody(selectedBody)
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
            createdSnapshotId?.let { deleteSnapshotSessionBestEffort(it, snapshotSourceId) }
            throw error
        }
        val requiredLogicalByteCount = wire.logicalByteCount
        if (requiredLogicalByteCount == null) {
            deleteSnapshotSessionBestEffort(wire.snapshotId, snapshotSourceId)
            throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_SESSION)
        }
        val session = SnapshotSession(
            snapshotId = wire.snapshotId,
            snapshotBundleSeq = wire.snapshotBundleSeq,
            rowCount = wire.rowCount,
            logicalByteCount = requiredLogicalByteCount,
            expiresAt = wire.expiresAt,
        )
        try {
            validateSnapshotSession(session)
        } catch (error: Throwable) {
            deleteSnapshotSessionBestEffort(session.snapshotId, snapshotSourceId)
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
        admittedMaxBytes: Long,
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
        require(maxBytes > 0L && admittedMaxBytes > 0L)
        val bodyLimit = minOf(maxBytes, admittedMaxBytes, oversqlitePullResponseBodyHardLimit)
        val wire = executeLoggedStreamingCall(
            operation = "snapshot chunk request",
            method = "GET",
            path = path,
            prepareCall = {
                http.prepareGet("sync/snapshot-sessions/$encodedSnapshotId") {
                    generationHeaders(sourceId)
                    acceptBoundedJsonResponse()
                    url {
                        parameters.append("after_row_ordinal", afterRowOrdinal.toString())
                        parameters.append("max_rows", maxRows.toString())
                        parameters.append("max_bytes", maxBytes.toString())
                    }
                }
            },
            consume = { response ->
                requireOkBoundedJson<CompactSnapshotChunkWire>(
                    operation = "snapshot chunk request",
                    method = "GET",
                    path = path,
                    call = response,
                    successLimit = bodyLimit,
                    errorLimit = snapshotControlBodyLimit,
                    onSuccessRawBody = { raw ->
                        require(json.parseToJsonElement(raw).jsonObject.keys ==
                            setOf("rows", "next_row_ordinal", "has_more", "logical_byte_count")) {
                            "snapshot chunk fields are invalid"
                        }
                    },
                    onCompleteBody = { decodedBytes ->
                        snapshotDiagnostics()?.recordCompletelyDecodedChunkBody(decodedBytes)
                    },
                    customError = { status, raw ->
                        decodeSnapshotChunkTooSmallOrNull(status, raw, maxBytes)
                    },
                )
            },
        )
        val rows = OversqliteCompactCodec.decodeBundleRows(
            generation = generation(),
            encoded = wire.rows,
            expectedFirstOrdinal = afterRowOrdinal + 1L,
            allowEmpty = true,
            snapshot = true,
        ).map { row ->
            SnapshotRow(row.schema, row.table, row.key, row.rowVersion, checkNotNull(row.payload))
        }
        val logicalByteCount = OversqliteCompactCodec.snapshotLogicalByteCount(
            generation(),
            rows.map { BundleRow(it.schema, it.table, it.key, "INSERT", it.rowVersion, it.payload) },
        )
        require(logicalByteCount == wire.logicalByteCount) { "snapshot logical_byte_count mismatch" }
        val chunk = SnapshotChunkResponse(
            rows = rows,
            nextRowOrdinal = wire.nextRowOrdinal,
            hasMore = wire.hasMore,
            logicalByteCount = wire.logicalByteCount,
        )
        validateSnapshotChunkResponse(chunk, snapshotId, snapshotBundleSeq, afterRowOrdinal, maxRows, maxBytes)
        snapshotDiagnostics()?.recordValidatedChunk(chunk.rows.size, chunk.logicalByteCount)
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
                                generationHeaders(sourceId)
                                acceptBoundedJsonResponse()
                            }
                        },
                        consume = { response ->
                            response.rejectRedirect(operation, "DELETE", loggedPath, log)
                            if (response.status.value !in 200..299) {
                                response.cancelWithoutDraining()
                                throw SnapshotHttpException(response.status, "invalid_error_response")
                            }
                            val channel = response.bodyAsChannel()
                            channel.readBuffer(snapshotControlBodyLimit + 1L)
                            if (!channel.isClosedForRead) {
                                response.cancelWithoutDraining()
                            }
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
        call.rejectRedirect(operation, method, path, log)
        val limit = if (call.status == HttpStatusCode.OK) successLimit else errorLimit
        val body = readBoundedJsonBody(
            operation = operation,
            response = call,
            limit = limit,
            invalidUtf8 = { SnapshotResponseDecodeException(operation) },
        )
        val raw = body.raw
        if (call.status != HttpStatusCode.OK) {
            if (!isJsonShapedErrorBody(raw)) {
                throw SnapshotHttpException(call.status, "invalid_error_response")
            }
            try {
                requireUniqueSnapshotJsonObjectMembers(raw)
                json.parseToJsonElement(raw).jsonObject
            } catch (error: SnapshotSemanticException) {
                throw error
            } catch (error: CancellationException) {
                throw error
            } catch (_: RuntimeException) {
                throw SnapshotResponseDecodeException(operation)
            }
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
            decodeRemoteContractErrorOrNull(
                status = call.status.value,
                raw = raw,
                operation = operation,
            )?.let { throw it }
            decodeRemoteOperationErrorOrNull(
                status = call.status.value,
                raw = raw,
                operation = operation,
            )?.let { throw it }
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
            if (error is Error) throw error
            if (error is CancellationException) throw error
            if (error is SnapshotSemanticException || error.isTerminalSyncCompatibilityFailure()) throw error
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
        if (!body.isWellFormedUtf8()) {
            throw invalidUtf8()
        }
        val raw = body.decodeToString()
        response.call.attributes.getOrNull(pushBufferGrowthKey)?.grow?.let { growth ->
            val shape=compactResponseAllocationShape(raw)
            growth(body.size.toLong()*2+65536,shape)
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
        val encodingValues = response.headers.getAll(HttpHeaders.ContentEncoding).orEmpty()
        if (encodingValues.size > 1) {
            response.cancelWithoutDraining()
            throw SnapshotUnsupportedContentEncodingException(operation, "multiple")
        }
        val encoding = encodingValues.singleOrNull()?.trim()?.lowercase()
        if (encoding != null && (encoding.isEmpty() || ',' in encoding || encoding !in setOf("identity", "gzip"))) {
            response.cancelWithoutDraining()
            throw SnapshotUnsupportedContentEncodingException(operation, "unsupported")
        }
        val channel = response.bodyAsChannel()
        return try {
            val growth=response.call.attributes.getOrNull(pushBufferGrowthKey)?.grow
            val encoded = if(growth==null) channel.readBuffer(limit + 1L).readByteArray() else {
                var size=0L
                val pages=ArrayList<ByteArray>()
                while(!channel.isClosedForRead && size<=limit) {
                    val next=minOf(65536L,limit+1-size)
                    growth(size+next+65536,0)
                    val page=channel.readBuffer(next).readByteArray()
                    size+=page.size
                    if(page.isNotEmpty()) pages+=page
                }
                growth(size*2+65536,0)
                ByteArray(size.toInt()).also { bytes ->
                    var offset=0
                    for(page in pages) {page.copyInto(bytes,offset);offset+=page.size}
                }
            }
            channel.closedCause?.let { throw it }
            if (encoded.size.toLong() > limit) {
                throw SnapshotResponseBodyTooLargeException(operation, limit)
            }
            when {
                oversqliteFetchManagesResponseCompression -> encoded
                encoding == "gzip" -> try {
                    var decodedBound=limit
                    if(growth!=null && encoded.size>=4) {
                        var declared=0L
                        for(i in 0..3) declared=declared or ((encoded[encoded.size-4+i].toLong() and 255) shl (8*i))
                        if(declared>limit) throw OversqliteDecodedGzipTooLargeException()
                        growth(encoded.size.toLong()+declared+65536,0)
                        decodedBound=minOf(limit,declared+1)
                    }
                    decodeOversqliteSingleMemberGzip(encoded, decodedBound)
                } catch (error: Throwable) {
                    if (error is CancellationException) throw error
                    if (error is SyncWorkspaceLimitException || error is SyncWorkLimitException) throw error
                    if (error is OversqliteDecodedGzipTooLargeException) {
                        throw SnapshotResponseBodyTooLargeException(operation, limit)
                    }
                    throw RemoteResponseDecodeException(operation)
                }
                else -> encoded
            }
        } finally {
            if (!channel.isClosedForRead) {
                response.cancelWithoutDraining()
            }
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
        val required = error.requiredDecodedBodyBytes
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
        maxResponseDecodedBytes: Long,
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
        val wire = requestJson<CompactPullResponseWire>(
            operation = "pull",
            method = "GET",
            path = path,
            successLimit = maxResponseDecodedBytes,
            prepareCall = {
                http.prepareGet("sync/pull") {
                    generationHeaders(sourceId)
                    acceptBoundedJsonResponse()
                    url {
                        parameters.append("after_bundle_seq", afterBundleSeq.toString())
                        parameters.append("max_bundles", maxBundles.toString())
                        if (targetBundleSeq > 0) {
                            parameters.append("target_bundle_seq", targetBundleSeq.toString())
                        }
                    }
                }
            },
            successDecoder = { raw -> strictJson.decodeFromString<CompactPullResponseWire>(raw) },
        ) { status, raw ->
            if (status != HttpStatusCode.Conflict) {
                return@requestJson null
            }
            val error = runCatching { json.decodeFromString(ErrorResponse.serializer(), raw) }.getOrNull()
            when (error?.error) {
                "history_pruned" -> HistoryPrunedException("remote history is no longer available")
                "checkpoint_ahead" -> CheckpointAheadException("local checkpoint is ahead of the server")
                else -> null
            }
        }
        val selectedGeneration = generation()
        return PullResponse(
            stableBundleSeq = wire.stableBundleSeq,
            bundles = wire.bundles.map { compact ->
                Bundle(
                    bundleSeq = compact.bundleSeq,
                    sourceId = compact.sourceId,
                    sourceBundleId = compact.sourceBundleId,
                    bundleHash = compact.bundleHash,
                    rows = OversqliteCompactCodec.decodeBundleRows(
                        generation = selectedGeneration,
                        encoded = compact.rows,
                        allowEmpty = true,
                    ),
                )
            },
            hasMore = wire.hasMore,
        )
    }

    private suspend inline fun <reified T> requestJson(
        operation: String,
        method: String,
        path: String,
        noinline prepareCall: suspend () -> HttpStatement,
        successLimit: Long? = null,
        noinline successValidator: ((String) -> Unit)? = null,
        noinline successDecoder: ((String) -> T)? = null,
        noinline customError: ((HttpStatusCode, String) -> Throwable?)? = null,
    ): T = executeLoggedStreamingCall(operation, method, path, prepareCall) { response ->
        decodeJsonResponse(operation, method, path, response, successLimit, successValidator, successDecoder, customError)
    }

    private suspend inline fun <reified T> decodeJsonResponse(
        operation: String,
        method: String,
        path: String,
        call: HttpResponse,
        successLimit: Long? = null,
        noinline successValidator: ((String) -> Unit)? = null,
        noinline successDecoder: ((String) -> T)? = null,
        noinline customError: ((HttpStatusCode, String) -> Throwable?)? = null,
    ): T {
        call.rejectRedirect(operation, method, path, log)
        if (call.status != HttpStatusCode.OK) {
            val raw = readBoundedJsonBody(
                operation = operation,
                response = call,
                limit = oversqliteResponseBodyHardLimit(path, call.status.value),
                invalidUtf8 = { RemoteResponseDecodeException(operation) },
            ).raw
            log {
                "oversqlite http non-ok op=$operation method=$method path=$path " +
                    "status=${call.status.value}"
            }
            if (isJsonShapedErrorBody(raw)) {
                try {
                    requireUniqueSnapshotJsonObjectMembers(raw)
                    json.parseToJsonElement(raw).jsonObject
                } catch (error: SnapshotSemanticException) {
                    throw error
                } catch (error: CancellationException) {
                    throw error
                } catch (_: RuntimeException) {
                    throw RemoteResponseDecodeException(operation)
                }
            } else {
                if (path == "/sync/push") throw UploadHttpException(call.status, "")
                throw DownloadHttpException(call.status, "")
            }
            val typed = try {
                customError?.invoke(call.status, raw)
            } catch (_: RuntimeException) {
                throw RemoteResponseSemanticException(operation)
            }
            typed?.let { throw it.withRedactedRemotePayload() }
            decodeRemoteContractErrorOrNull(
                status = call.status.value,
                raw = raw,
                operation = operation,
            )?.let { throw it }
            decodeRemoteOperationErrorOrNull(
                status = call.status.value,
                raw = raw,
                operation = operation,
            )?.let { throw it }
            if (path == "/sync/push") {
                throw UploadHttpException(call.status, "")
            }
            throw DownloadHttpException(call.status, "")
        }
        return try {
            val raw = readBoundedJsonBody(
                operation,
                call,
                minOf(successLimit ?: Long.MAX_VALUE, oversqliteResponseBodyHardLimit(path, call.status.value)),
            ) { RemoteResponseDecodeException(operation) }.raw
            requireUniqueSnapshotJsonObjectMembers(raw)
            successValidator?.invoke(raw)
            successDecoder?.invoke(raw) ?: json.decodeFromString<T>(raw)
        } catch (error: Throwable) {
            if (error is Error) throw error
            if (error is SyncWorkspaceLimitException || error is SyncWorkLimitException) throw error
            if (error is SnapshotResponseBodyTooLargeException || error is SnapshotSemanticException || error is RemoteResponseSemanticException) throw error
            if (error is CancellationException) {
                throw error
            }
            log {
                "oversqlite http decode failure op=$operation method=$method path=$path " +
                    "status=${call.status.value} error=${error.logSummary()}"
            }
            throw RemoteResponseDecodeException(operation)
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
            executeStreamingCall(
                prepareCall = { prepareCall() },
                consume = { response ->
                    log {
                        "oversqlite http response op=$operation method=$method path=$path " +
                            "status=${response.status.value}"
                    }
                    consume(response)
                },
            )
        } catch (error: Throwable) {
            if (error is CancellationException) {
                throw error
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
    is PushReplayHistoryPrunedException -> PushReplayHistoryPrunedException(rawBody = "")
    else -> this
}

private fun Throwable.logSummary(): String = this::class.simpleName ?: "Throwable"

private fun isJsonShapedErrorBody(raw: String): Boolean {
    val trimmed = raw.trim()
    val first = trimmed.firstOrNull()
    return first == '{' || first == '[' || first == '"' || first == '-' ||
        (first != null && first in '0'..'9') || trimmed == "true" || trimmed == "false" || trimmed == "null"
}
