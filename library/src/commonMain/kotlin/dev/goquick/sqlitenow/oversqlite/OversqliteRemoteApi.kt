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
import io.ktor.client.request.delete
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import kotlinx.serialization.json.Json

internal class OversqliteRemoteApi(
    private val http: HttpClient,
    private val json: Json,
    private val log: ((() -> String)) -> Unit,
) {
    suspend fun fetchCapabilities(): CapabilitiesResponse {
        return requireOkJson(
            operation = "capabilities request",
            method = "GET",
            path = "/sync/capabilities",
            call = executeLoggedCall(
                operation = "capabilities request",
                method = "GET",
                path = "/sync/capabilities",
            ) {
                http.get("sync/capabilities")
            },
        )
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
                    contentType(ContentType.Application.Json)
                    setBody(
                        ConnectRequest(
                            sourceId = sourceId,
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
                    contentType(ContentType.Application.Json)
                    setBody(
                        PushSessionCreateRequest(
                            sourceId = sourceId,
                            sourceBundleId = sourceBundleId,
                            plannedRowCount = plannedRowCount,
                            initializationId = initializationId?.takeIf { it.isNotBlank() },
                        ),
                    )
                }
            },
        ) { status, raw ->
            decodeInitializationLeaseExceptionOrNull(status, raw)
        }
        validatePushSessionCreateResponse(response, sourceBundleId, plannedRowCount, sourceId)
        log {
            "oversqlite createPushSession response status=${response.status} pushId=${response.pushId} " +
                "nextExpected=${response.nextExpectedRowOrdinal} bundleSeq=${response.bundleSeq}"
        }
        return response
    }

    suspend fun uploadPushChunk(
        pushId: String,
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

    suspend fun commitPushSession(pushId: String): PushSessionCommitResponse {
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
                http.post("sync/push-sessions/$pushId/commit")
            },
        ) { status, raw ->
            decodePushConflictExceptionOrNull(status, raw)
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
        }
    }

    suspend fun deletePushSessionBestEffort(pushId: String) {
        if (pushId.isBlank()) return
        runCatching {
            executeLoggedCall(
                operation = "delete push session",
                method = "DELETE",
                path = "/sync/push-sessions/$pushId",
            ) {
                http.delete("sync/push-sessions/$pushId")
            }
        }.onFailure { error ->
            log {
                "oversqlite http best-effort failure op=delete push session method=DELETE " +
                    "path=/sync/push-sessions/$pushId error=${error.logSummary()}"
            }
        }
    }

    suspend fun createSnapshotSession(): SnapshotSession {
        val session = requireOkJson<SnapshotSession>(
            operation = "snapshot session request",
            method = "POST",
            path = "/sync/snapshot-sessions",
            call = executeLoggedCall(
                operation = "snapshot session request",
                method = "POST",
                path = "/sync/snapshot-sessions",
            ) {
                http.post("sync/snapshot-sessions")
            },
        )
        validateSnapshotSession(session)
        return session
    }

    suspend fun fetchSnapshotChunk(
        snapshotId: String,
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

    suspend fun deleteSnapshotSessionBestEffort(snapshotId: String) {
        if (snapshotId.isBlank()) return
        runCatching {
            executeLoggedCall(
                operation = "delete snapshot session",
                method = "DELETE",
                path = "/sync/snapshot-sessions/$snapshotId",
            ) {
                http.delete("sync/snapshot-sessions/$snapshotId")
            }
        }.onFailure { error ->
            log {
                "oversqlite http best-effort failure op=delete snapshot session method=DELETE " +
                    "path=/sync/snapshot-sessions/$snapshotId error=${error.logSummary()}"
            }
        }
    }

    suspend fun sendPullRequest(
        afterBundleSeq: Long,
        maxBundles: Int,
        targetBundleSeq: Long,
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
            if (error?.error == "history_pruned") {
                HistoryPrunedException(error.message)
            } else {
                null
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
            val raw = runCatching { call.bodyAsText() }.getOrDefault("")
            log {
                "oversqlite http non-ok op=$operation method=$method path=$path " +
                    "status=${call.status.value} body=${raw.logExcerpt()}"
            }
            customError?.invoke(call.status, raw)?.let { throw it }
            throw RuntimeException("$operation failed: HTTP ${call.status} - $raw")
        }
        return try {
            call.body()
        } catch (error: Throwable) {
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
