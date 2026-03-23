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
    suspend fun createPushSession(
        sourceBundleId: Long,
        plannedRowCount: Long,
        sourceId: String,
    ): PushSessionCreateResponse {
        log {
            "oversqlite createPushSession sourceBundleId=$sourceBundleId plannedRowCount=$plannedRowCount sourceId=$sourceId"
        }
        val response = requireOkJson<PushSessionCreateResponse>(
            operation = "push session request",
            call = http.post("/sync/push-sessions") {
                contentType(ContentType.Application.Json)
                setBody(
                    PushSessionCreateRequest(
                        sourceId = sourceId,
                        sourceBundleId = sourceBundleId,
                        plannedRowCount = plannedRowCount,
                    )
                )
            },
        )
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
            call = http.post("/sync/push-sessions/$pushId/chunks") {
                contentType(ContentType.Application.Json)
                setBody(request)
            },
        )
        log {
            "oversqlite uploadPushChunk response pushId=${response.pushId} nextExpected=${response.nextExpectedRowOrdinal}"
        }
        return response
    }

    suspend fun commitPushSession(pushId: String): PushSessionCommitResponse {
        log { "oversqlite commitPushSession pushId=$pushId" }
        val response = requireOkJson<PushSessionCommitResponse>(
            operation = "push commit request",
            call = http.post("/sync/push-sessions/$pushId/commit"),
        ) { status, raw ->
            decodePushConflictExceptionOrNull(status, raw)
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
        return requireOkJson(
            operation = "committed bundle chunk request",
            call = http.get("/sync/committed-bundles/$bundleSeq/rows") {
                url {
                    if (afterRowOrdinal != null) {
                        parameters.append("after_row_ordinal", afterRowOrdinal.toString())
                    }
                    parameters.append("max_rows", maxRows.toString())
                }
            },
        )
    }

    suspend fun deletePushSessionBestEffort(pushId: String) {
        if (pushId.isBlank()) return
        runCatching { http.delete("/sync/push-sessions/$pushId") }
    }

    suspend fun createSnapshotSession(): SnapshotSession {
        val session = requireOkJson<SnapshotSession>(
            operation = "snapshot session request",
            call = http.post("/sync/snapshot-sessions"),
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
        val chunk = requireOkJson<SnapshotChunkResponse>(
            operation = "snapshot chunk request",
            call = http.get("/sync/snapshot-sessions/$snapshotId") {
                url {
                    parameters.append("after_row_ordinal", afterRowOrdinal.toString())
                    parameters.append("max_rows", maxRows.toString())
                }
            },
        )
        validateSnapshotChunkResponse(chunk, snapshotId, snapshotBundleSeq, afterRowOrdinal)
        return chunk
    }

    suspend fun deleteSnapshotSessionBestEffort(snapshotId: String) {
        if (snapshotId.isBlank()) return
        runCatching { http.delete("/sync/snapshot-sessions/$snapshotId") }
    }

    suspend fun sendPullRequest(
        afterBundleSeq: Long,
        maxBundles: Int,
        targetBundleSeq: Long,
    ): PullResponse {
        return requireOkJson(
            operation = "pull",
            call = http.get("/sync/pull") {
                url {
                    parameters.append("after_bundle_seq", afterBundleSeq.toString())
                    parameters.append("max_bundles", maxBundles.toString())
                    if (targetBundleSeq > 0) {
                        parameters.append("target_bundle_seq", targetBundleSeq.toString())
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
        call: HttpResponse,
        noinline customError: ((HttpStatusCode, String) -> Throwable?)? = null,
    ): T {
        if (call.status != HttpStatusCode.OK) {
            val raw = runCatching { call.bodyAsText() }.getOrDefault("")
            customError?.invoke(call.status, raw)?.let { throw it }
            throw RuntimeException("$operation failed: HTTP ${call.status} - $raw")
        }
        return call.body()
    }
}
