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
import io.ktor.client.request.delete
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.prepareGet
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.client.statement.bodyAsChannel
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.utils.io.readLine
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.Json

internal class OversqliteRemoteApi(
    private val http: HttpClient,
    private val json: Json,
    private val log: ((() -> String)) -> Unit,
) {
    private companion object {
        const val sourceIdHeaderName = "Oversync-Source-ID"
    }

    suspend fun fetchCapabilities(sourceId: String): CapabilitiesResponse {
        return requireOkJson<CapabilitiesResponse>(
            operation = "capabilities request",
            method = "GET",
            path = "/sync/capabilities",
            call = executeLoggedCall(
                operation = "capabilities request",
                method = "GET",
                path = "/sync/capabilities",
            ) {
                http.get("sync/capabilities") {
                    header(sourceIdHeaderName, sourceId)
                }
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
                header(sourceIdHeaderName, sourceId)
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
                    val raw = response.bodyAsTextOrEmptyPreservingCancellation()
                    log {
                        "oversqlite http non-ok op=bundle change watch method=GET path=$path " +
                            "status=${response.status.value} body=${raw.logExcerpt()}"
                    }
                    throw RuntimeException("bundle change watch failed: HTTP ${response.status} - $raw")
                }
                val parser = BundleChangeWatchSseParser(json)
                val channel = response.bodyAsChannel()
                while (!channel.isClosedForRead) {
                    val line = channel.readLine() ?: break
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
            throw error
        }
    }

    suspend fun connect(
        sourceId: String,
        hasLocalPendingRows: Boolean,
    ): ConnectResponse {
        log {
            "oversqlite connect sourceId=$sourceId hasLocalPendingRows=$hasLocalPendingRows"
        }
        return requireOkJson(
            operation = "connect request",
            method = "POST",
            path = "/sync/connect",
            call = executeLoggedCall(
                operation = "connect request",
                method = "POST",
                path = "/sync/connect",
            ) {
                http.post("sync/connect") {
                    header(sourceIdHeaderName, sourceId)
                    contentType(ContentType.Application.Json)
                    setBody(
                        ConnectRequest(
                            hasLocalPendingRows = hasLocalPendingRows,
                        ),
                    )
                }
            },
        )
    }

	suspend fun createPushSession(
        sourceBundleId: Long,
		plannedRowCount: Long,
		canonicalRequestHash: String,
        sourceId: String,
        initializationId: String? = null,
    ): PushSessionCreateResponse {
        log {
            "oversqlite createPushSession sourceBundleId=$sourceBundleId plannedRowCount=$plannedRowCount " +
                "sourceId=$sourceId initializationId=${initializationId ?: ""}"
        }
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
                    header(sourceIdHeaderName, sourceId)
                    contentType(ContentType.Application.Json)
                    setBody(
                        PushSessionCreateRequest(
                            sourceBundleId = sourceBundleId,
							plannedRowCount = plannedRowCount,
							canonicalRequestHash = canonicalRequestHash,
                            initializationId = initializationId?.takeIf { it.isNotBlank() },
                        ),
                    )
                }
            },
        ) { status, raw ->
            decodeSourceRecoveryRequiredExceptionOrNull(status, raw)
                ?: decodeInitializationLeaseExceptionOrNull(status, raw)
        }
		validatePushSessionCreateResponse(response, sourceBundleId, plannedRowCount, sourceId, canonicalRequestHash)
        log {
            "oversqlite createPushSession response status=${response.status} pushId=${response.pushId} " +
                "nextExpected=${response.nextExpectedRowOrdinal} bundleSeq=${response.bundleSeq}"
        }
        return response
    }

    suspend fun uploadPushChunk(
        pushId: String,
        sourceId: String,
        request: PushSessionChunkRequest,
    ): PushSessionChunkResponse {
        log {
            "oversqlite uploadPushChunk pushId=$pushId start=${request.startRowOrdinal} rows=${request.rows.size} " +
                request.rows.joinToString(" | ") { it.toVerboseSummary() }
        }
        val response = requireOkJson<PushSessionChunkResponse>(
            operation = "push chunk request",
            method = "POST",
            path = "/sync/push-sessions/$pushId/chunks",
            call = executeLoggedCall(
                operation = "push chunk request",
                method = "POST",
                path = "/sync/push-sessions/$pushId/chunks",
            ) {
                http.post("sync/push-sessions/$pushId/chunks") {
                    header(sourceIdHeaderName, sourceId)
                    contentType(ContentType.Application.Json)
                    setBody(request)
                }
            },
        ) { status, raw ->
            decodeInitializationLeaseExceptionOrNull(status, raw)
        }
        log {
            "oversqlite uploadPushChunk response pushId=${response.pushId} nextExpected=${response.nextExpectedRowOrdinal}"
        }
        return response
    }

    suspend fun commitPushSession(
        pushId: String,
        sourceId: String,
    ): PushSessionCommitResponse {
        log { "oversqlite commitPushSession pushId=$pushId" }
        val response = requireOkJson<PushSessionCommitResponse>(
            operation = "push commit request",
            method = "POST",
            path = "/sync/push-sessions/$pushId/commit",
            call = executeLoggedCall(
                operation = "push commit request",
                method = "POST",
                path = "/sync/push-sessions/$pushId/commit",
            ) {
                http.post("sync/push-sessions/$pushId/commit") {
                    header(sourceIdHeaderName, sourceId)
                }
            },
        ) { status, raw ->
            decodePushConflictExceptionOrNull(status, raw)
                ?: decodeSourceRecoveryRequiredExceptionOrNull(status, raw)
                ?: decodeInitializationLeaseExceptionOrNull(status, raw)
        }
        log {
            "oversqlite commitPushSession response bundleSeq=${response.bundleSeq} rowCount=${response.rowCount} " +
                "sourceBundleId=${response.sourceBundleId} bundleHash=${response.bundleHash}"
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
                    header(sourceIdHeaderName, sourceId)
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
        deleteSessionBestEffort(
            sessionId = pushId,
            sourceId = sourceId,
            operation = "delete push session",
            pathPrefix = "sync/push-sessions",
        )
    }

    suspend fun createSnapshotSession(
        sourceId: String,
        request: SnapshotSessionCreateRequest? = null,
    ): SnapshotSession {
        val response = executeLoggedCall(
            operation = "snapshot session request",
            method = "POST",
            path = "/sync/snapshot-sessions",
        ) {
            http.post("sync/snapshot-sessions") {
                header(sourceIdHeaderName, sourceId)
                if (request != null) {
                    contentType(ContentType.Application.Json)
                    setBody(request)
                }
            }
        }
        val session = requireOkJson<SnapshotSession>(
            operation = "snapshot session request",
            method = "POST",
            path = "/sync/snapshot-sessions",
            call = response,
            customError = { status: HttpStatusCode, raw: String ->
                decodeSourceRecoveryRequiredExceptionOrNull(status, raw)
                    ?: decodeSourceReplacementInvalidExceptionOrNull(status, raw)
            },
        )
        validateSnapshotSession(session)
        return session
    }

    suspend fun fetchSnapshotChunk(
        snapshotId: String,
        sourceId: String,
        snapshotBundleSeq: Long,
        afterRowOrdinal: Long,
        maxRows: Int,
    ): SnapshotChunkResponse {
        val path = buildString {
            append("/sync/snapshot-sessions/")
            append(snapshotId)
            append("?after_row_ordinal=")
            append(afterRowOrdinal)
            append("&max_rows=")
            append(maxRows)
        }
        val chunk = requireOkJson<SnapshotChunkResponse>(
            operation = "snapshot chunk request",
            method = "GET",
            path = path,
            call = executeLoggedCall(
                operation = "snapshot chunk request",
                method = "GET",
                path = path,
            ) {
                http.get("sync/snapshot-sessions/$snapshotId") {
                    header(sourceIdHeaderName, sourceId)
                    url {
                        parameters.append("after_row_ordinal", afterRowOrdinal.toString())
                        parameters.append("max_rows", maxRows.toString())
                    }
                }
            },
        )
        validateSnapshotChunkResponse(chunk, snapshotId, snapshotBundleSeq, afterRowOrdinal)
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
        if (sessionId.isBlank()) return
        val relativePath = "$pathPrefix/$sessionId"
        val loggedPath = "/$relativePath"
        runCatching {
            executeLoggedCall(
                operation = operation,
                method = "DELETE",
                path = loggedPath,
            ) {
                http.delete(relativePath) {
                    header(sourceIdHeaderName, sourceId)
                }
            }
        }.onFailure { error ->
            log {
                "oversqlite http best-effort failure op=$operation method=DELETE " +
                    "path=$loggedPath error=${error.logSummary()}"
            }
        }
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
                    header(sourceIdHeaderName, sourceId)
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
                "history_pruned" -> HistoryPrunedException(error.message)
                "checkpoint_ahead" -> CheckpointAheadException(error.message)
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
            val raw = call.bodyAsTextOrEmptyPreservingCancellation()
            log {
                "oversqlite http non-ok op=$operation method=$method path=$path " +
                    "status=${call.status.value} body=${raw.logExcerpt()}"
            }
            customError?.invoke(call.status, raw)?.let { throw it }
            if (path.startsWith("/sync/push-sessions")) {
                throw UploadHttpException(call.status, raw)
            }
            throw DownloadHttpException(call.status, raw)
        }
        return try {
            call.body()
        } catch (error: Throwable) {
            if (error is CancellationException) {
                throw error
            }
            log {
                "oversqlite http decode failure op=$operation method=$method path=$path " +
                    "status=${call.status.value} error=${error.logSummary()}"
            }
            throw error
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

private suspend fun HttpResponse.bodyAsTextOrEmptyPreservingCancellation(): String {
    return try {
        bodyAsText()
    } catch (error: CancellationException) {
        throw error
    } catch (_: Throwable) {
        ""
    }
}

private fun Throwable.logSummary(): String = buildString {
    append(this@logSummary::class.simpleName ?: "Throwable")
    this@logSummary.message
        ?.replace('\n', ' ')
        ?.replace('\r', ' ')
        ?.trim()
        ?.takeIf { it.isNotEmpty() }
        ?.let {
            append(": ")
            append(it)
        }
}

private fun String.logExcerpt(limit: Int = 512): String {
    val normalized = replace('\n', ' ').replace('\r', ' ').trim()
    return if (normalized.length <= limit) {
        normalized
    } else {
        normalized.take(limit) + "...[truncated]"
    }
}
