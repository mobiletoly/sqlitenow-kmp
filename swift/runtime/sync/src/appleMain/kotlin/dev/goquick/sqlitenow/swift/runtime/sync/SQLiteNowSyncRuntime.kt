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

import dev.goquick.sqlitenow.oversqlite.PushDependencyGroupExceedsAdmittedLimitException
import dev.goquick.sqlitenow.oversqlite.FKReconciliationRequiredException
import dev.goquick.sqlitenow.oversqlite.ResolutionBaseUnavailableException
import dev.goquick.sqlitenow.oversqlite.SyncWorkspaceLimitException
import dev.goquick.sqlitenow.oversqlite.SyncWorkLimitException
import dev.goquick.sqlitenow.oversqlite.StalePushDependencyDiagnosticException
import dev.goquick.sqlitenow.oversqlite.SyncProgress
import dev.goquick.sqlitenow.oversqlite.AttachOutcome
import dev.goquick.sqlitenow.oversqlite.AttachResult
import dev.goquick.sqlitenow.oversqlite.BundleChangeWatchMode
import dev.goquick.sqlitenow.oversqlite.ConflictContext
import dev.goquick.sqlitenow.oversqlite.DefaultOversqliteClient
import dev.goquick.sqlitenow.oversqlite.DetachOutcome
import dev.goquick.sqlitenow.oversqlite.DeferConflictResolver
import dev.goquick.sqlitenow.oversqlite.MergeResult
import dev.goquick.sqlitenow.oversqlite.OversqliteAutomaticDownloadConfig
import dev.goquick.sqlitenow.oversqlite.OversqliteCategorizedException
import dev.goquick.sqlitenow.oversqlite.OversqliteConfig
import dev.goquick.sqlitenow.oversqlite.OversqliteProgress
import dev.goquick.sqlitenow.oversqlite.OversqliteOpening
import dev.goquick.sqlitenow.oversqlite.OversqliteOpeningStatus
import dev.goquick.sqlitenow.oversqlite.OversqlitePendingRejection
import dev.goquick.sqlitenow.oversqlite.OversqliteRemoteContractException
import dev.goquick.sqlitenow.oversqlite.OversqliteSnapshotCapacityRetryPolicy
import dev.goquick.sqlitenow.oversqlite.OversqliteTransientRetryPolicy
import dev.goquick.sqlitenow.oversqlite.OversqliteTransferTargets
import dev.goquick.sqlitenow.oversqlite.OversqliteUpgradePlan
import dev.goquick.sqlitenow.oversqlite.PendingSyncStatus
import dev.goquick.sqlitenow.oversqlite.PendingConflict
import dev.goquick.sqlitenow.oversqlite.PushReport
import dev.goquick.sqlitenow.oversqlite.RemoteSyncReport
import dev.goquick.sqlitenow.oversqlite.Resolver
import dev.goquick.sqlitenow.oversqlite.RestoreSummary
import dev.goquick.sqlitenow.oversqlite.SourceInfo
import dev.goquick.sqlitenow.oversqlite.SyncReport
import dev.goquick.sqlitenow.oversqlite.SyncStatus
import dev.goquick.sqlitenow.oversqlite.SyncTable
import dev.goquick.sqlitenow.oversqlite.SyncThenDetachResult
import dev.goquick.sqlitenow.oversqlite.SyncIncompleteException
import dev.goquick.sqlitenow.swift.runtime.core.SQLiteNowCoreRuntimeDatabase
import dev.goquick.sqlitenow.oversqlite.openForSync
import dev.goquick.sqlitenow.oversqlite.runAutomaticDownloads
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
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlin.concurrent.atomics.AtomicBoolean
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.coroutines.cancellation.CancellationException as KotlinCancellationException

class SQLiteNowSyncRuntimeTableSpec(
    val tableName: String,
    val syncKeyColumnName: String,
)

class SQLiteNowSyncRuntimeTransferTargets(
    val maxPushRows: Int,
    val maxPushDecodedBytes: Long,
    val maxPullBundles: Int,
    val maxSnapshotChunkRows: Int,
    val maxSnapshotChunkDecodedBytes: Long,
)

class SQLiteNowSyncRuntimeConfig(
    val generationId: Long,
    val databaseId: String,
    val serviceId: String,
    val maxSyncWorkspaceEntries: Long,
    val maxSyncWorkspaceBytes: Long,
    val maxSyncWorkUnitsPerInvocation: Long,
    val syncTables: List<SQLiteNowSyncRuntimeTableSpec>,
    val transferTargets: SQLiteNowSyncRuntimeTransferTargets,
    val snapshotApplyBatchRows: Int,
    val snapshotApplyBatchBytes: Long,
    val verboseLogs: Boolean,
    val transientRetryPolicy: SQLiteNowSyncRuntimeTransientRetryPolicy,
    val snapshotCapacityRetryPolicy: SQLiteNowSyncRuntimeSnapshotCapacityRetryPolicy,
)

class SQLiteNowSyncRuntimeUpgradePlan(
    val migrationId: String,
    val fromGenerationId: Long,
    val toGenerationId: Long,
    val fromLocalVersion: Int,
    val toLocalVersion: Int,
    val preservedLocalTables: List<String>,
)

class SQLiteNowSyncRuntimeOpeningStatus(
    val mode: String?,
    val outcome: String?,
    val phase: String?,
    val revision: Long,
    val installedGenerationId: Long?,
    val targetGenerationId: Long,
    val installedLocalVersion: Int?,
    val targetLocalVersion: Int,
    val contractConfirmed: Boolean?,
    val pendingDirtyCount: Long?,
    val pendingFrozenCount: Long?,
    val pendingConflictCount: Long?,
    val drainComplete: Boolean?,
    val replacementState: String?,
    val stagedRowCount: Long?,
    val stagedByteCount: Long?,
    val targetReadsAvailable: Boolean?,
    val writesAvailable: Boolean?,
    val nextAction: String,
    val diagnosticCode: String?,
    val diagnosticMessage: String?,
)

class SQLiteNowSyncRuntimePendingRejection(
    val revision: Long,
    val sourceId: String,
    val sourceBundleId: Long,
    val canonicalRequestHash: String,
    val generationId: Long,
    val rejectionCode: String,
) {
    internal fun toOversqlite(): OversqlitePendingRejection = OversqlitePendingRejection(
        revision = revision,
        sourceId = sourceId,
        sourceBundleId = sourceBundleId,
        canonicalRequestHash = canonicalRequestHash,
        generationId = generationId,
        rejectionCode = rejectionCode,
    )
}

class SQLiteNowSyncRuntimeTransientRetryPolicy(
    val maxAttempts: Int,
    val initialBackoffMillis: Long,
    val maxBackoffMillis: Long,
    val jitterRatio: Double,
)

class SQLiteNowSyncRuntimeSnapshotCapacityRetryPolicy(
    val enabled: Boolean,
    val maxWaitMillis: Long,
    val fallbackDelayMillis: Long,
    val jitterRatio: Double,
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
    val generationId: Long,
    val contractConfirmed: Boolean,
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
    val lastSync: SQLiteNowSyncRuntimeSyncReport?,
    val detach: SQLiteNowSyncRuntimeDetachOutcome,
    val syncRounds: Int,
    val remainingPendingRowCount: Long,
    val blockedReason: String?,
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

class SQLiteNowSyncRuntimePendingConflict(
    val revision: Long,
    val sourceId: String,
    val sourceBundleId: Long,
    val canonicalRequestHash: String,
    val generationId: Long,
    val generationFingerprint: String,
    val conflict: SQLiteNowSyncRuntimeConflict,
    val baselineRevision: Long = 0,
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

class SQLiteNowSyncRuntimeOpeningOperationResult(
    val opening: SQLiteNowSyncRuntimeOpening? = null,
    val status: SQLiteNowSyncRuntimeOpeningStatus? = null,
    val conflict: SQLiteNowSyncRuntimePendingConflict? = null,
    val rejection: SQLiteNowSyncRuntimePendingRejection? = null,
    val syncReport: SQLiteNowSyncRuntimeSyncReport? = null,
    val syncThenDetachResult: SQLiteNowSyncRuntimeSyncThenDetachResult? = null,
    val pushReport: SQLiteNowSyncRuntimePushReport? = null,
    val remoteReport: SQLiteNowSyncRuntimeRemoteSyncReport? = null,
)

interface SQLiteNowSyncRuntimeOpeningOperationCompletion {
    fun onSuccess(result: SQLiteNowSyncRuntimeOpeningOperationResult)
    fun onFailure(payload: SQLiteNowSyncRuntimeErrorPayload)
}

class SQLiteNowSyncRuntimeErrorPayload(
    val category: String,
    val code: String,
    val message: String,
    val remoteContract: Boolean,
    val httpStatus: Int,
    val operation: String,
    val remediation: String?,
    val retryable: Boolean,
    val generationId: String?,
    val incompleteReason: String? = null,
    val dirtyCount: Long = 0,
    val outboundCount: Long = 0,
    val confirmedBundleCount: Int = 0,
    val pushAttempts: Int = 0,
    val localPlanningJson: String? = null,
)

class SQLiteNowSyncRuntimePushDependencyDiagnosticPage(
    val diagnosticId: String,
    val identitiesJson: String,
    val nextCursor: String?,
)

class SQLiteNowSyncRuntimeException(
    val payload: SQLiteNowSyncRuntimeErrorPayload,
) : RuntimeException(payload.message)

@OptIn(ExperimentalAtomicApi::class)
class SQLiteNowSyncRuntimeOpening internal constructor(
    private val opening: OversqliteOpening<*>,
    private val coreDatabase: SQLiteNowCoreRuntimeDatabase,
    private val httpClient: HttpClient,
    private val runtimeResolver: SQLiteNowSyncRuntimeResolver?,
) {
    private val json = Json { ignoreUnknownKeys = true }
    private val readyClient = AtomicReference<SQLiteNowSyncRuntimeClient?>(null)
    private val closed = AtomicBoolean(false)

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun status(): SQLiteNowSyncRuntimeOpeningStatus = mapSyncRuntimeErrors {
        opening.status().toRuntimeOpeningStatus()
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun advance(): SQLiteNowSyncRuntimeOpeningStatus = mapSyncRuntimeErrors {
        opening.advance().also { bindReadyClientIfNeeded() }.toRuntimeOpeningStatus()
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun pendingConflict(): SQLiteNowSyncRuntimePendingConflict? = mapSyncRuntimeErrors {
        opening.pendingConflict().runtimeGet()?.toRuntimePendingConflict()
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun resolveConflict(
        selected: SQLiteNowSyncRuntimePendingConflict,
        result: SQLiteNowSyncRuntimeResolverResult,
    ) = mapSyncRuntimeErrors {
        opening.resolveConflict(selected.toPendingConflict(json), result.toMergeResult(json)).runtimeGet()
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun retryConflict(selected: SQLiteNowSyncRuntimePendingConflict) = mapSyncRuntimeErrors {
        opening.retryConflict(selected.toPendingConflict(json)).runtimeGet()
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun pendingRejection(): SQLiteNowSyncRuntimePendingRejection? = mapSyncRuntimeErrors {
        opening.pendingRejection()?.toRuntimePendingRejection()
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun discardRejected(selected: SQLiteNowSyncRuntimePendingRejection): SQLiteNowSyncRuntimeOpeningStatus =
        mapSyncRuntimeErrors { opening.discardRejected(selected.toOversqlite()).toRuntimeOpeningStatus() }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun retryRejected(selected: SQLiteNowSyncRuntimePendingRejection): SQLiteNowSyncRuntimeOpeningStatus =
        mapSyncRuntimeErrors { opening.retryRejected(selected.toOversqlite()).toRuntimeOpeningStatus() }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun cancel() = mapSyncRuntimeErrors { opening.cancel() }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun close() = mapSyncRuntimeErrors {
        closed.store(true)
        readyClient.store(null)
        coreDatabase.closeTableObserversForSQLiteNowSyncRuntime()
        try {
            opening.close()
        } finally {
            httpClient.close()
        }
    }

    fun startStatus(completion: SQLiteNowSyncRuntimeOpeningOperationCompletion) =
        startOpeningOperation(completion) {
            SQLiteNowSyncRuntimeOpeningOperationResult(status = status())
        }

    fun startAdvance(completion: SQLiteNowSyncRuntimeOpeningOperationCompletion) =
        startOpeningOperation(completion) {
            SQLiteNowSyncRuntimeOpeningOperationResult(status = advance())
        }

    fun startPendingConflict(completion: SQLiteNowSyncRuntimeOpeningOperationCompletion) =
        startOpeningOperation(completion) {
            SQLiteNowSyncRuntimeOpeningOperationResult(conflict = pendingConflict())
        }

    fun startResolveConflict(
        selected: SQLiteNowSyncRuntimePendingConflict,
        result: SQLiteNowSyncRuntimeResolverResult,
        completion: SQLiteNowSyncRuntimeOpeningOperationCompletion,
    ) = startOpeningOperation(completion) {
        resolveConflict(selected, result)
        SQLiteNowSyncRuntimeOpeningOperationResult()
    }

    fun startRetryConflict(
        selected: SQLiteNowSyncRuntimePendingConflict,
        completion: SQLiteNowSyncRuntimeOpeningOperationCompletion,
    ) = startOpeningOperation(completion) {
        retryConflict(selected)
        SQLiteNowSyncRuntimeOpeningOperationResult()
    }

    fun startPendingRejection(completion: SQLiteNowSyncRuntimeOpeningOperationCompletion) =
        startOpeningOperation(completion) {
            SQLiteNowSyncRuntimeOpeningOperationResult(rejection = pendingRejection())
        }

    fun startDiscardRejected(
        selected: SQLiteNowSyncRuntimePendingRejection,
        completion: SQLiteNowSyncRuntimeOpeningOperationCompletion,
    ) = startOpeningOperation(completion) {
        SQLiteNowSyncRuntimeOpeningOperationResult(status = discardRejected(selected))
    }

    fun startRetryRejected(
        selected: SQLiteNowSyncRuntimePendingRejection,
        completion: SQLiteNowSyncRuntimeOpeningOperationCompletion,
    ) = startOpeningOperation(completion) {
        SQLiteNowSyncRuntimeOpeningOperationResult(status = retryRejected(selected))
    }

    fun startCancel(completion: SQLiteNowSyncRuntimeOpeningOperationCompletion) =
        startOpeningOperation(completion) {
            cancel()
            SQLiteNowSyncRuntimeOpeningOperationResult()
        }

    fun startClose(completion: SQLiteNowSyncRuntimeOpeningOperationCompletion) =
        startOpeningOperation(completion) {
            close()
            SQLiteNowSyncRuntimeOpeningOperationResult()
        }

    fun readyClientOrNull(): SQLiteNowSyncRuntimeClient? {
        return bindReadyClientIfNeeded()
    }

    private fun bindReadyClientIfNeeded(): SQLiteNowSyncRuntimeClient? {
        if (closed.load()) return null
        if (opening.readyOrNull() == null) {
            readyClient.store(null)
            return null
        }
        readyClient.load()?.let { return it }
        val runtime = opening.readyOrNull()?.client?.let { client ->
            SQLiteNowSyncRuntimeClient(client, runtimeResolver, coreDatabase)
        } ?: return null
        if (!readyClient.compareAndSet(null, runtime)) return readyClient.load()
        if (closed.load() || opening.readyOrNull() == null) {
            readyClient.compareAndSet(runtime, null)
            return null
        }
        return runtime
    }
}

@Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
suspend fun sqliteNowSyncRuntimeOpenForSync(
    coreDatabase: SQLiteNowCoreRuntimeDatabase,
    baseUrl: String,
    auth: SQLiteNowSyncRuntimeAuth,
    config: SQLiteNowSyncRuntimeConfig,
    resolver: SQLiteNowSyncRuntimeResolver?,
    upgradePlan: SQLiteNowSyncRuntimeUpgradePlan?,
): SQLiteNowSyncRuntimeOpening = mapSyncRuntimeErrors {
    val json = Json { ignoreUnknownKeys = true }
    val httpClient = sqliteNowSyncRuntimeHttpClient(baseUrl, auth, json)
    try {
        SQLiteNowSyncRuntimeOpening(
            opening = coreDatabase.databaseForSQLiteNowSyncRuntime().openForSync(
                config = config.toOversqliteConfig(),
                httpClient = httpClient,
                resolver = resolver?.toOversqliteResolver(json) ?: DeferConflictResolver,
                upgradePlan = upgradePlan?.toOversqlite(),
            ),
            coreDatabase = coreDatabase,
            httpClient = httpClient,
            runtimeResolver = resolver,
        )
    } catch (failure: Throwable) {
        httpClient.close()
        throw failure
    }
}

/** Swift-friendly entry point that avoids exposing Kotlin file-facade names in generated products. */
class SQLiteNowSyncRuntimeFactory {
    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun openForSync(
        coreDatabase: SQLiteNowCoreRuntimeDatabase,
        baseUrl: String,
        auth: SQLiteNowSyncRuntimeAuth,
        config: SQLiteNowSyncRuntimeConfig,
        resolver: SQLiteNowSyncRuntimeResolver?,
        upgradePlan: SQLiteNowSyncRuntimeUpgradePlan?,
    ): SQLiteNowSyncRuntimeOpening = sqliteNowSyncRuntimeOpenForSync(
        coreDatabase = coreDatabase,
        baseUrl = baseUrl,
        auth = auth,
        config = config,
        resolver = resolver,
        upgradePlan = upgradePlan,
    )

    fun startOpenForSync(
        coreDatabase: SQLiteNowCoreRuntimeDatabase,
        baseUrl: String,
        auth: SQLiteNowSyncRuntimeAuth,
        config: SQLiteNowSyncRuntimeConfig,
        resolver: SQLiteNowSyncRuntimeResolver?,
        upgradePlan: SQLiteNowSyncRuntimeUpgradePlan?,
        completion: SQLiteNowSyncRuntimeOpeningOperationCompletion,
    ) = startOpeningOperation(completion) {
        SQLiteNowSyncRuntimeOpeningOperationResult(
            opening = openForSync(coreDatabase, baseUrl, auth, config, resolver, upgradePlan),
        )
    }
}

private class SQLiteNowSyncRuntimeClientResources(
    val client: dev.goquick.sqlitenow.oversqlite.OversqliteClient,
    val httpClient: HttpClient?,
)

class SQLiteNowSyncRuntimeClient private constructor(
    resources: SQLiteNowSyncRuntimeClientResources,
    resolver: SQLiteNowSyncRuntimeResolver?,
    private val coordinatedCoreDatabase: SQLiteNowCoreRuntimeDatabase? = null,
) {
    private val json = Json { ignoreUnknownKeys = true }
    private val runtimeResolver = resolver
    private val httpClient = resources.httpClient
    private val client = resources.client
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    constructor(
        coreDatabase: SQLiteNowCoreRuntimeDatabase,
        baseUrl: String,
        auth: SQLiteNowSyncRuntimeAuth,
        config: SQLiteNowSyncRuntimeConfig,
        resolver: SQLiteNowSyncRuntimeResolver?,
    ) : this(createRuntimeClientResources(coreDatabase, baseUrl, auth, config, resolver), resolver)

    internal constructor(
        client: dev.goquick.sqlitenow.oversqlite.OversqliteClient,
        resolver: SQLiteNowSyncRuntimeResolver?,
        coreDatabase: SQLiteNowCoreRuntimeDatabase,
    ) : this(SQLiteNowSyncRuntimeClientResources(client, null), resolver, coreDatabase)

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun open() = mapSyncRuntimeErrors {
        inRuntimeScope { client.open().runtimeGet() }
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun attach(userId: String): SQLiteNowSyncRuntimeAttachResult = mapSyncRuntimeErrors {
        inRuntimeScope { client.attach(userId).runtimeGet().toRuntimeAttachResult() }
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun pauseUploads() = mapSyncRuntimeErrors {
        inRuntimeScope { client.pauseUploads() }
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun resumeUploads() = mapSyncRuntimeErrors {
        inRuntimeScope { client.resumeUploads() }
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun pauseDownloads() = mapSyncRuntimeErrors {
        inRuntimeScope { client.pauseDownloads() }
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun resumeDownloads() = mapSyncRuntimeErrors {
        inRuntimeScope { client.resumeDownloads() }
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun sourceInfo(): SQLiteNowSyncRuntimeSourceInfo = mapSyncRuntimeErrors {
        inRuntimeScope { client.sourceInfo().runtimeGet().toRuntimeSourceInfo() }
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun syncStatus(): SQLiteNowSyncRuntimeSyncStatus = mapSyncRuntimeErrors {
        inRuntimeScope { client.syncStatus().runtimeGet().toRuntimeSyncStatus() }
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun detach(): SQLiteNowSyncRuntimeDetachOutcome = mapSyncRuntimeErrors {
        inRuntimeScope { client.detach().runtimeGet().toRuntimeDetachOutcome() }
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun pushPending(): SQLiteNowSyncRuntimePushReport = mapSyncRuntimeErrors {
        inRuntimeScope { client.pushPending().runtimeGet().toRuntimePushReport() }
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun readPushDependencyDiagnostic(diagnosticId: String,cursor: String?,pageSize: Int): SQLiteNowSyncRuntimePushDependencyDiagnosticPage = mapSyncRuntimeErrors {
        inRuntimeScope {
            val page=client.readPushDependencyDiagnostic(diagnosticId,cursor,pageSize).runtimeGet()
            SQLiteNowSyncRuntimePushDependencyDiagnosticPage(page.diagnosticId,Json.encodeToJsonElement(page.identities).toString(),page.nextCursor)
        }
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun releasePushDependencyDiagnostic(diagnosticId: String) = mapSyncRuntimeErrors {
        inRuntimeScope {client.releasePushDependencyDiagnostic(diagnosticId).runtimeGet()}
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun pendingConflict(): SQLiteNowSyncRuntimePendingConflict? = mapSyncRuntimeErrors {
        inRuntimeScope { client.pendingConflict().runtimeGet()?.toRuntimePendingConflict() }
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun resolveConflict(
        selected: SQLiteNowSyncRuntimePendingConflict,
        result: SQLiteNowSyncRuntimeResolverResult,
    ) = mapSyncRuntimeErrors {
        SwiftA3ConflictApplyProbe.aroundResolution(selected.canonicalRequestHash, client) {
            inRuntimeScope {
                client.resolveConflict(selected.toPendingConflict(json), result.toMergeResult(json)).runtimeGet()
            }
        }
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun retryConflict(selected: SQLiteNowSyncRuntimePendingConflict) = mapSyncRuntimeErrors {
        inRuntimeScope { client.retryConflict(selected.toPendingConflict(json)).runtimeGet() }
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun pullToStable(): SQLiteNowSyncRuntimeRemoteSyncReport = mapSyncRuntimeErrors {
        inRuntimeScope { client.pullToStable().runtimeGet().toRuntimeRemoteSyncReport() }
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun sync(): SQLiteNowSyncRuntimeSyncReport = mapSyncRuntimeErrors {
        inRuntimeScope { client.sync().runtimeGet().toRuntimeSyncReport() }
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun syncThenDetach(): SQLiteNowSyncRuntimeSyncThenDetachResult = mapSyncRuntimeErrors {
        inRuntimeScope { client.syncThenDetach().runtimeGet().toRuntimeSyncThenDetachResult() }
    }

    fun startSync(completion: SQLiteNowSyncRuntimeOpeningOperationCompletion) =
        startOpeningOperation(completion) {
            SQLiteNowSyncRuntimeOpeningOperationResult(syncReport = sync())
        }

    fun startSyncThenDetach(completion: SQLiteNowSyncRuntimeOpeningOperationCompletion) =
        startOpeningOperation(completion) {
            SQLiteNowSyncRuntimeOpeningOperationResult(syncThenDetachResult = syncThenDetach())
        }

    fun startPushPending(completion: SQLiteNowSyncRuntimeOpeningOperationCompletion) =
        startOpeningOperation(completion) {
            SQLiteNowSyncRuntimeOpeningOperationResult(pushReport = pushPending())
        }

    fun startPullToStable(completion: SQLiteNowSyncRuntimeOpeningOperationCompletion) =
        startOpeningOperation(completion) {
            SQLiteNowSyncRuntimeOpeningOperationResult(remoteReport = pullToStable())
        }

    fun startRebuild(completion: SQLiteNowSyncRuntimeOpeningOperationCompletion) =
        startOpeningOperation(completion) {
            SQLiteNowSyncRuntimeOpeningOperationResult(remoteReport = rebuild())
        }

    fun startResolveConflict(
        selected: SQLiteNowSyncRuntimePendingConflict,
        result: SQLiteNowSyncRuntimeResolverResult,
        completion: SQLiteNowSyncRuntimeOpeningOperationCompletion,
    ) = startOpeningOperation(completion) {
        resolveConflict(selected, result)
        SQLiteNowSyncRuntimeOpeningOperationResult()
    }

    fun startRetryConflict(
        selected: SQLiteNowSyncRuntimePendingConflict,
        completion: SQLiteNowSyncRuntimeOpeningOperationCompletion,
    ) = startOpeningOperation(completion) {
        retryConflict(selected)
        SQLiteNowSyncRuntimeOpeningOperationResult()
    }

    @Throws(SQLiteNowSyncRuntimeException::class, CancellationException::class)
    suspend fun rebuild(): SQLiteNowSyncRuntimeRemoteSyncReport = mapSyncRuntimeErrors {
        inRuntimeScope { client.rebuild().runtimeGet().toRuntimeRemoteSyncReport() }
    }

    private suspend fun <T> inRuntimeScope(block: suspend () -> T): T {
        currentCoroutineContext().ensureActive()
        val operation = scope.async(start = CoroutineStart.LAZY) { block() }
        try {
            operation.start()
            return operation.await()
        } catch (cancelled: CancellationException) {
            // The client operation is owned by this caller until its cleanup is complete.
            withContext(NonCancellable) { operation.cancelAndJoin() }
            throw cancelled
        }
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
            ?: SQLiteNowSyncRuntimeResolverResult(kind = "deferConflict", mergedPayloadJson = null)

    fun close() {
        coordinatedCoreDatabase?.closeTableObserversForSQLiteNowSyncRuntime()
        scope.cancel()
        client.close()
        httpClient?.close()
    }
}

private fun createRuntimeClientResources(
    coreDatabase: SQLiteNowCoreRuntimeDatabase,
    baseUrl: String,
    auth: SQLiteNowSyncRuntimeAuth,
    config: SQLiteNowSyncRuntimeConfig,
    resolver: SQLiteNowSyncRuntimeResolver?,
): SQLiteNowSyncRuntimeClientResources {
    val json = Json { ignoreUnknownKeys = true }
    val httpClient = sqliteNowSyncRuntimeHttpClient(baseUrl, auth, json)
    return SQLiteNowSyncRuntimeClientResources(
        client = DefaultOversqliteClient(
            db = coreDatabase.connectionForSQLiteNowSyncRuntime(),
            config = config.toOversqliteConfig(),
            http = httpClient,
            resolver = resolver?.toOversqliteResolver(json) ?: DeferConflictResolver,
        ),
        httpClient = httpClient,
    )
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
        generationId = generationId,
        databaseId = databaseId,
        serviceId = serviceId,
        maxSyncWorkspaceEntries = maxSyncWorkspaceEntries,
        maxSyncWorkspaceBytes = maxSyncWorkspaceBytes,
        maxSyncWorkUnitsPerInvocation = maxSyncWorkUnitsPerInvocation,
        syncTables = syncTables.map {
            SyncTable(
                tableName = it.tableName,
                syncKeyColumnName = it.syncKeyColumnName,
            )
        },
        transferTargets = OversqliteTransferTargets(
            maxPushRows = transferTargets.maxPushRows,
            maxPushDecodedBytes = transferTargets.maxPushDecodedBytes,
            maxPullBundles = transferTargets.maxPullBundles,
            maxSnapshotChunkRows = transferTargets.maxSnapshotChunkRows,
            maxSnapshotChunkDecodedBytes = transferTargets.maxSnapshotChunkDecodedBytes,
        ),
        snapshotApplyBatchRows = snapshotApplyBatchRows,
        snapshotApplyBatchBytes = snapshotApplyBatchBytes,
        verboseLogs = verboseLogs,
        transientRetryPolicy = OversqliteTransientRetryPolicy(
            maxAttempts = transientRetryPolicy.maxAttempts,
            initialBackoffMillis = transientRetryPolicy.initialBackoffMillis,
            maxBackoffMillis = transientRetryPolicy.maxBackoffMillis,
            jitterRatio = transientRetryPolicy.jitterRatio,
        ),
        snapshotCapacityRetryPolicy = OversqliteSnapshotCapacityRetryPolicy(
            enabled = snapshotCapacityRetryPolicy.enabled,
            maxWaitMillis = snapshotCapacityRetryPolicy.maxWaitMillis,
            fallbackDelayMillis = snapshotCapacityRetryPolicy.fallbackDelayMillis,
            jitterRatio = snapshotCapacityRetryPolicy.jitterRatio,
        ),
    )

private fun SQLiteNowSyncRuntimeUpgradePlan.toOversqlite(): OversqliteUpgradePlan =
    OversqliteUpgradePlan(
        migrationId = migrationId,
        fromGenerationId = fromGenerationId,
        toGenerationId = toGenerationId,
        fromLocalVersion = fromLocalVersion,
        toLocalVersion = toLocalVersion,
        preservedLocalTables = preservedLocalTables.toSet(),
    )

private fun OversqliteOpeningStatus.toRuntimeOpeningStatus(): SQLiteNowSyncRuntimeOpeningStatus =
    SQLiteNowSyncRuntimeOpeningStatus(
        mode = mode?.name?.lowercase(),
        outcome = outcome?.name?.lowercase(),
        phase = phase,
        revision = revision,
        installedGenerationId = installedGenerationId,
        targetGenerationId = targetGenerationId,
        installedLocalVersion = installedLocalVersion,
        targetLocalVersion = targetLocalVersion,
        contractConfirmed = contractConfirmed,
        pendingDirtyCount = pendingDirtyCount,
        pendingFrozenCount = pendingFrozenCount,
        pendingConflictCount = pendingConflictCount,
        drainComplete = drainComplete,
        replacementState = replacementState?.name?.lowercase(),
        stagedRowCount = stagedRowCount,
        stagedByteCount = stagedByteCount,
        targetReadsAvailable = targetReadsAvailable,
        writesAvailable = writesAvailable,
        nextAction = nextAction.name.lowercase(),
        diagnosticCode = diagnosticCode,
        diagnosticMessage = diagnosticMessage,
    )

private fun OversqlitePendingRejection.toRuntimePendingRejection(): SQLiteNowSyncRuntimePendingRejection =
    SQLiteNowSyncRuntimePendingRejection(
        revision = revision,
        sourceId = sourceId,
        sourceBundleId = sourceBundleId,
        canonicalRequestHash = canonicalRequestHash,
        generationId = generationId,
        rejectionCode = rejectionCode,
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
        generationId = generationId,
        contractConfirmed = contractConfirmed,
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
        lastSync = lastSync?.toRuntimeSyncReport(),
        detach = detach.toRuntimeDetachOutcome(),
        syncRounds = syncRounds,
        remainingPendingRowCount = remainingPendingRowCount,
        blockedReason = blockedReason?.name,
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
        "deferConflict" -> MergeResult.DeferConflict
        "acceptServer" -> MergeResult.AcceptServer
        "keepLocal" -> MergeResult.KeepLocal
        "keepMerged" -> {
            val payload = requireNotNull(mergedPayloadJson) {
                "keepMerged resolver result requires mergedPayloadJson"
            }
            MergeResult.KeepMerged(json.parseToJsonElement(payload))
        }
        else -> throw IllegalArgumentException("unsupported resolver result kind: $kind")
    }

private fun PendingConflict.toRuntimePendingConflict(): SQLiteNowSyncRuntimePendingConflict =
    SQLiteNowSyncRuntimePendingConflict(
        revision, sourceId, sourceBundleId, canonicalRequestHash,
        generationId, generationFingerprint, conflict.toRuntimeConflict(), baselineRevision,
    )

private fun SQLiteNowSyncRuntimePendingConflict.toPendingConflict(json: Json): PendingConflict =
    PendingConflict(
        revision = revision,
        baselineRevision = baselineRevision,
        sourceId = sourceId,
        sourceBundleId = sourceBundleId,
        canonicalRequestHash = canonicalRequestHash,
        generationId = generationId,
        generationFingerprint = generationFingerprint,
        conflict = ConflictContext(
            schema = conflict.schema,
            table = conflict.table,
            key = json.parseToJsonElement(conflict.keyJson).let { element ->
                (element as kotlinx.serialization.json.JsonObject).mapValues { (_, value) ->
                    (value as JsonPrimitive).content
                }
            },
            localOp = conflict.localOp,
            localPayload = conflict.localPayloadJson?.let(json::parseToJsonElement),
            baseRowVersion = conflict.baseRowVersion,
            serverRowVersion = conflict.serverRowVersion,
            serverRowDeleted = conflict.serverRowDeleted,
            serverRow = conflict.serverRowJson?.let(json::parseToJsonElement),
        ),
    )

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

internal fun startOpeningOperation(
    completion: SQLiteNowSyncRuntimeOpeningOperationCompletion,
    context: kotlin.coroutines.CoroutineContext = Dispatchers.Default,
    block: suspend () -> SQLiteNowSyncRuntimeOpeningOperationResult,
): SQLiteNowSyncRuntimeCancelHandle {
    val gate = OpeningOperationCompletionGate(completion)
    val owner = SupervisorJob(context[kotlinx.coroutines.Job])
    val job = CoroutineScope(context + owner).launch(start = CoroutineStart.LAZY) {
        try {
            gate.success(block())
        } catch (error: Throwable) {
            gate.failure(error)
        }
    }
    job.invokeOnCompletion { cause ->
        owner.complete()
        if (cause != null) gate.failure(cause)
    }
    job.start()
    return SQLiteNowSyncRuntimeCancelHandle(job::cancel)
}

@OptIn(ExperimentalAtomicApi::class)
private class OpeningOperationCompletionGate(
    private val completion: SQLiteNowSyncRuntimeOpeningOperationCompletion,
) {
    private val completed = AtomicBoolean(false)

    fun success(result: SQLiteNowSyncRuntimeOpeningOperationResult) {
        if (completed.compareAndSet(false, true)) completion.onSuccess(result)
    }

    fun failure(error: Throwable) {
        if (completed.compareAndSet(false, true)) {
            completion.onFailure(
                if (error is SQLiteNowSyncRuntimeException) error.payload else syncErrorPayload(error),
            )
        }
    }
}

private fun <T> Result<T>.runtimeGet(): T =
    getOrElse { error ->
        if (error is KotlinCancellationException) throw error
        throw SQLiteNowSyncRuntimeException(syncErrorPayload(error))
    }

private fun syncErrorPayload(error: Throwable): SQLiteNowSyncRuntimeErrorPayload {
    val cancellationCause = error.firstCauseMatching { it is KotlinCancellationException }
    val categorizedCause = error.firstCauseMatching { it is OversqliteCategorizedException }
    val incomplete = error.firstCauseMatching { it is SyncIncompleteException } as? SyncIncompleteException
    val remoteContractCause = error.firstCauseMatching { it is OversqliteRemoteContractException }
        as? OversqliteRemoteContractException
    val message = remoteContractCause?.message ?: error.message ?: error.toString()
    val code = remoteContractCause?.code
        ?: (cancellationCause ?: categorizedCause)?.let { it::class.simpleName }
        ?: (error::class.simpleName ?: "Throwable")
    val category = when {
        cancellationCause != null -> "cancelled"
        categorizedCause is OversqliteCategorizedException -> categorizedCause.category.payloadCategory
        else -> "unknown"
    }
    return SQLiteNowSyncRuntimeErrorPayload(
        category = category,
        code = code,
        message = message,
        remoteContract = remoteContractCause != null,
        httpStatus = remoteContractCause?.httpStatus ?: 0,
        operation = remoteContractCause?.operation.orEmpty(),
        remediation = remoteContractCause?.remediation,
        retryable = remoteContractCause?.retryable ?: false,
        generationId = remoteContractCause?.generationId?.toString(),
        incompleteReason = incomplete?.reason?.name,
        dirtyCount = incomplete?.dirtyCount ?: 0,
        outboundCount = incomplete?.outboundCount ?: 0,
        confirmedBundleCount = incomplete?.confirmedBundleCount ?: 0,
        pushAttempts = incomplete?.pushAttempts ?: 0,
        localPlanningJson = localPlanningErrorJson(categorizedCause),
    )
}

// This bridge is local facade data. It adds no transport fields or hash input.
private fun localPlanningErrorJson(error: Throwable?): String? {
    val progress=when(error) {
        is PushDependencyGroupExceedsAdmittedLimitException -> error.progress
        is FKReconciliationRequiredException -> error.progress
        is ResolutionBaseUnavailableException -> error.progress
        is SyncWorkLimitException -> error.progress
        is SyncWorkspaceLimitException -> error.progress
        is StalePushDependencyDiagnosticException -> null
        else -> return null
    }
    return buildJsonObject {
        when(error) {
            is PushDependencyGroupExceedsAdmittedLimitException -> {
                put("reason",error.reason);put("dimension",error.dimension);put("limit",error.limit);put("actual",error.actual)
                put("componentRows",error.componentRows);put("componentDecodedBytes",error.componentDecodedBytes)
                put("diagnosticId",error.diagnosticId);put("sample",Json.encodeToJsonElement(error.sample));put("hasMore",error.hasMore)
            }
            is FKReconciliationRequiredException -> {
                put("reason",error.reason);put("detail",error.detail);put("unit",error.unit)
                put("sample",Json.encodeToJsonElement(error.sample));put("hasMore",error.hasMore)
            }
            is ResolutionBaseUnavailableException -> {
                put("reason",error.reason);put("unit",error.unit);put("sample",Json.encodeToJsonElement(error.sample));put("hasMore",error.hasMore)
            }
            is SyncWorkLimitException -> {put("reason","sync_work_limit");put("unit",error.unit);put("limit",error.limit);put("used",error.used)}
            is SyncWorkspaceLimitException -> {
                put("reason","sync_workspace_limit");put("unit",error.unit);put("dimension",error.dimension)
                put("limit",error.limit);put("used",error.used);put("reserved",error.reserved);put("resumable",error.resumable)
            }
            is StalePushDependencyDiagnosticException -> {put("reason",error.reason);put("diagnosticId",error.diagnosticId)}
        }
        if(progress!=null) put("progress",buildJsonObject {
            put("dirtyRows",progress.dirtyRows);put("outboundRows",progress.outboundRows);put("committedBundles",progress.committedBundles)
        })
    }.toString()
}

private inline fun Throwable.firstCauseMatching(predicate: (Throwable) -> Boolean): Throwable? {
    var current: Throwable? = this
    while (current != null) {
        if (predicate(current)) return current
        current = current.cause
    }
    return null
}
