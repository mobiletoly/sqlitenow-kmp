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
package dev.goquick.sqlitenow.swift.runtime.sync

import dev.goquick.sqlitenow.oversqlite.AttachOutcome
import dev.goquick.sqlitenow.oversqlite.AttachResult
import dev.goquick.sqlitenow.oversqlite.BundleChangeWatchMode
import dev.goquick.sqlitenow.oversqlite.ConflictContext
import dev.goquick.sqlitenow.oversqlite.DefaultOversqliteClient
import dev.goquick.sqlitenow.oversqlite.DetachOutcome
import dev.goquick.sqlitenow.oversqlite.MergeResult
import dev.goquick.sqlitenow.oversqlite.OversqliteAutomaticDownloadConfig
import dev.goquick.sqlitenow.oversqlite.OversqliteConfig
import dev.goquick.sqlitenow.oversqlite.OversqliteProgress
import dev.goquick.sqlitenow.oversqlite.PendingSyncStatus
import dev.goquick.sqlitenow.oversqlite.PushReport
import dev.goquick.sqlitenow.oversqlite.RemoteSyncReport
import dev.goquick.sqlitenow.oversqlite.Resolver
import dev.goquick.sqlitenow.oversqlite.RestoreSummary
import dev.goquick.sqlitenow.oversqlite.ServerWinsResolver
import dev.goquick.sqlitenow.oversqlite.SourceInfo
import dev.goquick.sqlitenow.oversqlite.SyncReport
import dev.goquick.sqlitenow.oversqlite.SyncStatus
import dev.goquick.sqlitenow.oversqlite.SyncTable
import dev.goquick.sqlitenow.oversqlite.SyncThenDetachResult
import dev.goquick.sqlitenow.swift.runtime.core.SQLiteNowCoreRuntimeDatabase
import io.ktor.client.HttpClient
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.plugins.auth.Auth
import io.ktor.client.plugins.auth.providers.BearerTokens
import io.ktor.client.plugins.auth.providers.bearer
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.coroutines.cancellation.CancellationException as KotlinCancellationException

class SQLiteNowSyncRuntimeTableSpec(
    val tableName: String,
    val syncKeyColumnName: String,
)

class SQLiteNowSyncRuntimeConfig(
    val schema: String,
    val syncTables: List<SQLiteNowSyncRuntimeTableSpec>,
    val uploadLimit: Int,
    val downloadLimit: Int,
    val verboseLogs: Boolean,
)

class SQLiteNowSyncRuntimeAuth(
    private val accessTokenProvider: () -> String,
    private val refreshedAccessTokenProvider: (() -> String?)?,
) {
    internal fun bearerTokens(): BearerTokens = BearerTokens(accessTokenProvider(), null)

    internal fun refreshTokens(): BearerTokens? {
        val token = refreshedAccessTokenProvider?.invoke() ?: accessTokenProvider()
        return BearerTokens(token, null)
    }
}

class SQLiteNowSyncRuntimeAutomaticDownloadConfig(
    val automaticDownloadIntervalMillis: Long,
    val bundleChangeWatchMode: String,
    val bundleChangeWatchReconnectMinMillis: Long,
    val bundleChangeWatchReconnectMaxMillis: Long,
)

class SQLiteNowSyncRuntimePendingSyncStatus(
    val hasPendingSyncData: Boolean,
    val pendingRowCount: Long,
    val blocksDetach: Boolean,
)

class SQLiteNowSyncRuntimeSyncStatus(
    val authority: String,
    val pending: SQLiteNowSyncRuntimePendingSyncStatus,
    val lastBundleSeqSeen: Long,
)

class SQLiteNowSyncRuntimeSourceInfo(
    val currentSourceId: String,
    val rebuildRequired: Boolean,
    val sourceRecoveryRequired: Boolean,
    val sourceRecoveryReason: String?,
)

class SQLiteNowSyncRuntimeRestoreSummary(
    val bundleSeq: Long,
    val rowCount: Long,
)

class SQLiteNowSyncRuntimeAttachResult(
    val kind: String,
    val outcome: String?,
    val status: SQLiteNowSyncRuntimeSyncStatus?,
    val retryAfterSeconds: Long,
    val restore: SQLiteNowSyncRuntimeRestoreSummary?,
)

class SQLiteNowSyncRuntimeDetachOutcome(
    val outcome: String,
)

class SQLiteNowSyncRuntimePushReport(
    val outcome: String,
    val status: SQLiteNowSyncRuntimeSyncStatus,
)

class SQLiteNowSyncRuntimeRemoteSyncReport(
    val outcome: String,
    val status: SQLiteNowSyncRuntimeSyncStatus,
    val restore: SQLiteNowSyncRuntimeRestoreSummary?,
)

class SQLiteNowSyncRuntimeSyncReport(
    val pushOutcome: String,
    val remoteOutcome: String,
    val status: SQLiteNowSyncRuntimeSyncStatus,
    val restore: SQLiteNowSyncRuntimeRestoreSummary?,
)

class SQLiteNowSyncRuntimeSyncThenDetachResult(
    val lastSync: SQLiteNowSyncRuntimeSyncReport,
    val detach: SQLiteNowSyncRuntimeDetachOutcome,
    val syncRounds: Int,
    val remainingPendingRowCount: Long,
    val success: Boolean,
)

class SQLiteNowSyncRuntimeProgress(
    val kind: String,
    val operation: String?,
    val phase: String?,
)

class SQLiteNowSyncRuntimeConflict(
    val schema: String,
    val table: String,
    val keyJson: String,
    val localOp: String,
    val localPayloadJson: String?,
    val baseRowVersion: Long,
    val serverRowVersion: Long,
    val serverRowDeleted: Boolean,
    val serverRowJson: String?,
)

class SQLiteNowSyncRuntimeResolverResult(
    val kind: String,
    val mergedPayloadJson: String?,
)

class SQLiteNowSyncRuntimeResolver(
    private val resolveBlock: (SQLiteNowSyncRuntimeConflict) -> SQLiteNowSyncRuntimeResolverResult,
) {
    fun resolveForRuntimeSmoke(conflict: SQLiteNowSyncRuntimeConflict): SQLiteNowSyncRuntimeResolverResult =
        resolveBlock(conflict)

    internal fun toOversqliteResolver(json: Json): Resolver =
        Resolver { conflict ->
            resolveBlock(conflict.toRuntimeConflict()).toMergeResult(json)
        }
}

interface SQLiteNowSyncRuntimeProgressObserver {
    fun onProgress(progress: SQLiteNowSyncRuntimeProgress)
    fun onError(payload: SQLiteNowSyncRuntimeErrorPayload)
    fun onComplete()
}

interface SQLiteNowSyncRuntimeErrorObserver {
    fun onError(payload: SQLiteNowSyncRuntimeErrorPayload)
    fun onComplete()
}

class SQLiteNowSyncRuntimeCancelHandle internal constructor(
    private val cancelBlock: () -> Unit,
) {
    fun cancel() {
        cancelBlock()
    }
}

class SQLiteNowSyncRuntimeErrorPayload(
    val category: String,
    val code: String,
    val message: String,
)

class SQLiteNowSyncRuntimeException(
    val payload: SQLiteNowSyncRuntimeErrorPayload,
) : RuntimeException(payload.message)

class SQLiteNowSyncRuntimeClient(
    coreDatabase: SQLiteNowCoreRuntimeDatabase,
    baseUrl: String,
    auth: SQLiteNowSyncRuntimeAuth,
    config: SQLiteNowSyncRuntimeConfig,
    resolver: SQLiteNowSyncRuntimeResolver?,
) {
    private val json = Json { ignoreUnknownKeys = true }
    private val runtimeResolver = resolver
    private val httpClient = sqliteNowSyncRuntimeHttpClient(baseUrl, auth, json)
    private val client = DefaultOversqliteClient(
        db = coreDatabase.connectionForSQLiteNowSyncRuntime(),
        config = config.toOversqliteConfig(),
        http = httpClient,
        resolver = resolver?.toOversqliteResolver(json) ?: ServerWinsResolver,
    )
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun open() = mapSyncRuntimeErrors {
        client.open().runtimeGet()
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun attach(userId: String): SQLiteNowSyncRuntimeAttachResult = mapSyncRuntimeErrors {
        client.attach(userId).runtimeGet().toRuntimeAttachResult()
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun sourceInfo(): SQLiteNowSyncRuntimeSourceInfo = mapSyncRuntimeErrors {
        client.sourceInfo().runtimeGet().toRuntimeSourceInfo()
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun syncStatus(): SQLiteNowSyncRuntimeSyncStatus = mapSyncRuntimeErrors {
        client.syncStatus().runtimeGet().toRuntimeSyncStatus()
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun detach(): SQLiteNowSyncRuntimeDetachOutcome = mapSyncRuntimeErrors {
        client.detach().runtimeGet().toRuntimeDetachOutcome()
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun pushPending(): SQLiteNowSyncRuntimePushReport = mapSyncRuntimeErrors {
        client.pushPending().runtimeGet().toRuntimePushReport()
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun pullToStable(): SQLiteNowSyncRuntimeRemoteSyncReport = mapSyncRuntimeErrors {
        client.pullToStable().runtimeGet().toRuntimeRemoteSyncReport()
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun sync(): SQLiteNowSyncRuntimeSyncReport = mapSyncRuntimeErrors {
        client.sync().runtimeGet().toRuntimeSyncReport()
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun syncThenDetach(): SQLiteNowSyncRuntimeSyncThenDetachResult = mapSyncRuntimeErrors {
        client.syncThenDetach().runtimeGet().toRuntimeSyncThenDetachResult()
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun rebuild(): SQLiteNowSyncRuntimeRemoteSyncReport = mapSyncRuntimeErrors {
        client.rebuild().runtimeGet().toRuntimeRemoteSyncReport()
    }

    fun observeProgress(observer: SQLiteNowSyncRuntimeProgressObserver): SQLiteNowSyncRuntimeCancelHandle {
        val job = scope.launch {
            try {
                client.progress.collect { progress ->
                    observer.onProgress(progress.toRuntimeProgress())
                }
                observer.onComplete()
            } catch (error: Throwable) {
                if (error is KotlinCancellationException) {
                    observer.onComplete()
                    return@launch
                }
                observer.onError(syncErrorPayload(error))
            }
        }
        return SQLiteNowSyncRuntimeCancelHandle {
            job.cancel()
        }
    }

    fun startAutomaticDownloads(
        config: SQLiteNowSyncRuntimeAutomaticDownloadConfig,
        observer: SQLiteNowSyncRuntimeErrorObserver,
    ): SQLiteNowSyncRuntimeCancelHandle {
        val job = scope.launch {
            try {
                client.runAutomaticDownloads(config.toOversqliteAutomaticDownloadConfig())
            } catch (error: Throwable) {
                if (error is KotlinCancellationException) {
                    observer.onComplete()
                    return@launch
                }
                observer.onError(syncErrorPayload(error))
            }
        }
        return SQLiteNowSyncRuntimeCancelHandle {
            job.cancel()
        }
    }

    fun resolveForRuntimeSmoke(conflict: SQLiteNowSyncRuntimeConflict): SQLiteNowSyncRuntimeResolverResult =
        runtimeResolver?.resolveForRuntimeSmoke(conflict)
            ?: SQLiteNowSyncRuntimeResolverResult(kind = "acceptServer", mergedPayloadJson = null)

    fun close() {
        scope.cancel()
        client.close()
        httpClient.close()
    }
}

private fun sqliteNowSyncRuntimeHttpClient(
    baseUrl: String,
    auth: SQLiteNowSyncRuntimeAuth,
    json: Json,
): HttpClient =
    HttpClient {
        install(HttpTimeout) {
            requestTimeoutMillis = 30_000L
            connectTimeoutMillis = 10_000L
            socketTimeoutMillis = 90_000L
        }
        install(ContentNegotiation) {
            json(json)
        }
        install(Auth) {
            bearer {
                loadTokens {
                    auth.bearerTokens()
                }
                refreshTokens {
                    auth.refreshTokens()
                }
                sendWithoutRequest { true }
                cacheTokens = false
                nonCancellableRefresh = true
            }
        }
        defaultRequest {
            url(baseUrl)
        }
    }

private fun SQLiteNowSyncRuntimeConfig.toOversqliteConfig(): OversqliteConfig =
    OversqliteConfig(
        schema = schema,
        syncTables = syncTables.map {
            SyncTable(
                tableName = it.tableName,
                syncKeyColumnName = it.syncKeyColumnName,
            )
        },
        uploadLimit = uploadLimit,
        downloadLimit = downloadLimit,
        verboseLogs = verboseLogs,
    )

private fun SQLiteNowSyncRuntimeAutomaticDownloadConfig.toOversqliteAutomaticDownloadConfig() =
    OversqliteAutomaticDownloadConfig(
        automaticDownloadIntervalMillis = automaticDownloadIntervalMillis,
        bundleChangeWatchMode = when (bundleChangeWatchMode.uppercase()) {
            "AUTO" -> BundleChangeWatchMode.AUTO
            else -> BundleChangeWatchMode.OFF
        },
        bundleChangeWatchReconnectMinMillis = bundleChangeWatchReconnectMinMillis,
        bundleChangeWatchReconnectMaxMillis = bundleChangeWatchReconnectMaxMillis,
    )

private fun PendingSyncStatus.toRuntimePendingSyncStatus(): SQLiteNowSyncRuntimePendingSyncStatus =
    SQLiteNowSyncRuntimePendingSyncStatus(
        hasPendingSyncData = hasPendingSyncData,
        pendingRowCount = pendingRowCount,
        blocksDetach = blocksDetach,
    )

private fun SyncStatus.toRuntimeSyncStatus(): SQLiteNowSyncRuntimeSyncStatus =
    SQLiteNowSyncRuntimeSyncStatus(
        authority = authority.name,
        pending = pending.toRuntimePendingSyncStatus(),
        lastBundleSeqSeen = lastBundleSeqSeen,
    )

private fun RestoreSummary.toRuntimeRestoreSummary(): SQLiteNowSyncRuntimeRestoreSummary =
    SQLiteNowSyncRuntimeRestoreSummary(
        bundleSeq = bundleSeq,
        rowCount = rowCount,
    )

private fun AttachResult.toRuntimeAttachResult(): SQLiteNowSyncRuntimeAttachResult =
    when (this) {
        is AttachResult.Connected -> SQLiteNowSyncRuntimeAttachResult(
            kind = "connected",
            outcome = outcome.toRuntimeOutcome(),
            status = status.toRuntimeSyncStatus(),
            retryAfterSeconds = 0,
            restore = restore?.toRuntimeRestoreSummary(),
        )
        is AttachResult.RetryLater -> SQLiteNowSyncRuntimeAttachResult(
            kind = "retryLater",
            outcome = null,
            status = null,
            retryAfterSeconds = retryAfterSeconds,
            restore = null,
        )
    }

private fun AttachOutcome.toRuntimeOutcome(): String = name

private fun SourceInfo.toRuntimeSourceInfo(): SQLiteNowSyncRuntimeSourceInfo =
    SQLiteNowSyncRuntimeSourceInfo(
        currentSourceId = currentSourceId,
        rebuildRequired = rebuildRequired,
        sourceRecoveryRequired = sourceRecoveryRequired,
        sourceRecoveryReason = sourceRecoveryReason?.name,
    )

private fun DetachOutcome.toRuntimeDetachOutcome(): SQLiteNowSyncRuntimeDetachOutcome =
    when (this) {
        DetachOutcome.DETACHED -> SQLiteNowSyncRuntimeDetachOutcome(outcome = "DETACHED")
        DetachOutcome.BLOCKED_UNSYNCED_DATA -> SQLiteNowSyncRuntimeDetachOutcome(outcome = "BLOCKED_UNSYNCED_DATA")
    }

private fun PushReport.toRuntimePushReport(): SQLiteNowSyncRuntimePushReport =
    SQLiteNowSyncRuntimePushReport(
        outcome = outcome.name,
        status = status.toRuntimeSyncStatus(),
    )

private fun RemoteSyncReport.toRuntimeRemoteSyncReport(): SQLiteNowSyncRuntimeRemoteSyncReport =
    SQLiteNowSyncRuntimeRemoteSyncReport(
        outcome = outcome.name,
        status = status.toRuntimeSyncStatus(),
        restore = restore?.toRuntimeRestoreSummary(),
    )

private fun SyncReport.toRuntimeSyncReport(): SQLiteNowSyncRuntimeSyncReport =
    SQLiteNowSyncRuntimeSyncReport(
        pushOutcome = pushOutcome.name,
        remoteOutcome = remoteOutcome.name,
        status = status.toRuntimeSyncStatus(),
        restore = restore?.toRuntimeRestoreSummary(),
    )

private fun SyncThenDetachResult.toRuntimeSyncThenDetachResult(): SQLiteNowSyncRuntimeSyncThenDetachResult =
    SQLiteNowSyncRuntimeSyncThenDetachResult(
        lastSync = lastSync.toRuntimeSyncReport(),
        detach = detach.toRuntimeDetachOutcome(),
        syncRounds = syncRounds,
        remainingPendingRowCount = remainingPendingRowCount,
        success = isSuccess(),
    )

private fun OversqliteProgress.toRuntimeProgress(): SQLiteNowSyncRuntimeProgress =
    when (this) {
        OversqliteProgress.Idle -> SQLiteNowSyncRuntimeProgress(
            kind = "idle",
            operation = null,
            phase = null,
        )
        is OversqliteProgress.Active -> SQLiteNowSyncRuntimeProgress(
            kind = "active",
            operation = operation.name,
            phase = phase.name,
        )
    }

private fun ConflictContext.toRuntimeConflict(): SQLiteNowSyncRuntimeConflict =
    SQLiteNowSyncRuntimeConflict(
        schema = schema,
        table = table,
        keyJson = key.toJsonString(),
        localOp = localOp,
        localPayloadJson = localPayload?.jsonString(),
        baseRowVersion = baseRowVersion,
        serverRowVersion = serverRowVersion,
        serverRowDeleted = serverRowDeleted,
        serverRowJson = serverRow?.jsonString(),
    )

private fun Map<String, String>.toJsonString(): String =
    buildJsonObject {
        forEach { (key, value) ->
            put(key, value)
        }
    }.toString()

private fun JsonElement.jsonString(): String = toString()

private fun SQLiteNowSyncRuntimeResolverResult.toMergeResult(json: Json): MergeResult =
    when (kind) {
        "keepLocal" -> MergeResult.KeepLocal
        "keepMerged" -> {
            val payload = requireNotNull(mergedPayloadJson) {
                "keepMerged resolver result requires mergedPayloadJson"
            }
            MergeResult.KeepMerged(json.parseToJsonElement(payload))
        }
        else -> MergeResult.AcceptServer
    }

private suspend inline fun <T> mapSyncRuntimeErrors(
    crossinline block: suspend () -> T,
): T {
    try {
        return block()
    } catch (error: KotlinCancellationException) {
        throw error
    } catch (error: SQLiteNowSyncRuntimeException) {
        throw error
    } catch (error: Throwable) {
        throw SQLiteNowSyncRuntimeException(syncErrorPayload(error))
    }
}

private fun <T> Result<T>.runtimeGet(): T =
    getOrElse { error ->
        if (error is KotlinCancellationException) throw error
        throw SQLiteNowSyncRuntimeException(syncErrorPayload(error))
    }

private fun syncErrorPayload(error: Throwable): SQLiteNowSyncRuntimeErrorPayload {
    val message = error.message ?: error.toString()
    val code = error::class.simpleName ?: "Throwable"
    val lowerCode = code.lowercase()
    val lowerMessage = message.lowercase()
    val category = when {
        lowerCode.contains("cancel") || lowerMessage.contains("cancel") -> "cancelled"
        lowerCode.contains("auth") || lowerMessage.contains("unauthorized") || lowerMessage.contains("forbidden") -> "auth"
        lowerCode.contains("conflict") || lowerMessage.contains("conflict") -> "conflict"
        lowerCode.contains("network") || lowerCode.contains("http") || lowerMessage.contains("connect") -> "network"
        lowerCode.contains("state") || lowerMessage.contains("open") || lowerMessage.contains("attach") -> "state"
        else -> "unknown"
    }
    return SQLiteNowSyncRuntimeErrorPayload(
        category = category,
        code = code,
        message = message,
    )
}
