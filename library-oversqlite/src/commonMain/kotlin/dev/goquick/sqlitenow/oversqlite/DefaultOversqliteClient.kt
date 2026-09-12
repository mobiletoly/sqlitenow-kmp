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

import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowInternalApi
import io.ktor.client.HttpClient
import kotlinx.coroutines.flow.StateFlow
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.Json

class DefaultOversqliteClient private constructor(
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
    private val http: HttpClient,
    private val resolver: Resolver,
    private val requestGzipEngine: OversqliteRequestGzipEngine,
) : OversqliteClient {
    constructor(
        db: SafeSQLiteConnection,
        config: OversqliteConfig,
        http: HttpClient,
        resolver: Resolver = DeferConflictResolver,
    ) : this(db, config, http, resolver, OversqliteRequestGzipEngine(::compressOversqliteRequestGzip))

    companion object {
        internal fun forOpening(
            db: SafeSQLiteConnection,
            config: OversqliteConfig,
            http: HttpClient,
            resolver: Resolver,
            leaseGroup: Any,
            mutationAuthority: OversqliteOpeningMutationAuthority? = null,
            requestGzipEngine: OversqliteRequestGzipEngine = OversqliteRequestGzipEngine(::compressOversqliteRequestGzip),
        ): DefaultOversqliteClient = DefaultOversqliteClient(db, config, http, resolver, requestGzipEngine).also {
            it.resourceOwner.activeClientLeaseGroup = leaseGroup
            it.sessionState.coordinatorAuthorized = true
            it.sessionState.openingMutationAuthority = mutationAuthority
        }

        internal fun forTesting(
            db: SafeSQLiteConnection,
            config: OversqliteConfig,
            http: HttpClient,
            resolver: Resolver = DeferConflictResolver,
            requestGzipEngine: OversqliteRequestGzipEngine = OversqliteRequestGzipEngine(::compressOversqliteRequestGzip),
        ): DefaultOversqliteClient = forOpening(db, config, http, resolver, Any(), requestGzipEngine = requestGzipEngine)
    }

    private val tableInfoCache = TableInfoCache()
    private val runtimeInitializer = SyncRuntimeInitializer(config, tableInfoCache)

    @OptIn(ExperimentalSerializationApi::class)
    private val json = Json {
        ignoreUnknownKeys = true
        exceptionsWithDebugInfo = false
    }

    private val managedTableStore = OversqliteManagedTableStore(db)
    private val applyStateStore = OversqliteApplyStateStore(db)
    private val sourceStateStore = OversqliteSourceStateStore(db)
    private val attachmentStateStore = OversqliteAttachmentStateStore(db)
    private val operationStateStore = OversqliteOperationStateStore(db)
    private val syncStateStore: OversqliteSyncStateStore = OversqliteSyncStateStore(db) { localStore }
    private val outboxStateStore = OversqliteOutboxStateStore(db)
    private val conflictStateStore = OversqliteConflictStateStore(db, json)
    private val snapshotDiagnosticsState = SnapshotDiagnosticsState()
    private val resourceOwner = OversqliteClientResourceOwner(db, http)
    private val operationController = OversqliteClientOperationController(
        config = config,
        resources = resourceOwner,
        snapshotDiagnosticsState = snapshotDiagnosticsState,
        log = ::verboseLog,
    )
    internal fun workspaceDiagnostics(): A3AccountingStats = operationController.a3Stats.copy(work=operationController.usedSyncWork())
    private val sessionState = OversqliteClientSessionState(
        db = db,
        config = config,
        tableInfoCache = tableInfoCache,
        runtimeInitializer = runtimeInitializer,
        managedTableStore = managedTableStore,
        applyStateStore = applyStateStore,
        sourceStateStore = sourceStateStore,
        attachmentStateStore = attachmentStateStore,
        operationStateStore = operationStateStore,
        syncStateStore = syncStateStore,
        outboxStateStore = outboxStateStore,
    )
    private val remoteApi = OversqliteRemoteApi(
        http = resourceOwner.http,
        json = json,
        requestGzipEngine = requestGzipEngine,
        log = { message -> verboseLog(message) },
        snapshotDiagnostics = snapshotDiagnosticsState::current,
        generation = {
            checkNotNull(sessionState.requireValidatedConfig().generation) {
                "generation contract is unconfirmed"
            }
        },
    )
    private val contractGateway = OversqliteContractGateway(
        db = db,
        config = config,
        json = json,
        tableInfoCache = tableInfoCache,
        runtimeInitializer = runtimeInitializer,
        remoteApi = remoteApi,
        sessionState = sessionState,
        operationController = operationController,
    )
    private val localStore: OversqliteLocalStore = OversqliteLocalStore(db, json, sessionState::requireValidatedConfig)
    private val bundleApplier = OversqliteBundleApplier(localStore, syncStateStore,
        OversqliteApplySurvivors(db, localStore, config, operationController, json))
    private val stageStore = OversqliteStageStore(db, localStore, syncStateStore, json, config, operationController)
    private val applyExecutor = OversqliteApplyExecutor(db, applyStateStore)
    private val snapshotGate = OversqliteSnapshotGate(
        db = db,
        syncStateStore = syncStateStore,
        outboxStateStore = outboxStateStore,
        attachmentStateStore = attachmentStateStore,
        operationStateStore = operationStateStore,
    )
    private val pushWorkflow = OversqlitePushWorkflow(
        config = config,
        resolver = resolver,
        db = db,
        tableInfoCache = tableInfoCache,
        remoteApi = remoteApi,
        sourceStateStore = sourceStateStore,
        attachmentStateStore = attachmentStateStore,
        outboxStateStore = outboxStateStore,
        conflictStateStore = conflictStateStore,
        syncStateStore = syncStateStore,
        localStore = localStore,
        bundleApplier = bundleApplier,
        stageStore = stageStore,
        applyExecutor = applyExecutor,
        openingMutationAuthority = { sessionState.openingMutationAuthority },
        json = json,
    ) { message -> verboseLog(message) }
    private val downloadWorkflow = OversqliteDownloadWorkflow(
        db = db,
        config = config,
        remoteApi = remoteApi,
        sourceStateStore = sourceStateStore,
        outboxStateStore = outboxStateStore,
        attachmentStateStore = attachmentStateStore,
        operationStateStore = operationStateStore,
        syncStateStore = syncStateStore,
        localStore = localStore,
        bundleApplier = bundleApplier,
        stageStore = stageStore,
        applyExecutor = applyExecutor,
        snapshotGate = snapshotGate,
        snapshotDiagnostics = snapshotDiagnosticsState::current,
        json = json,
        notifyUpdatedTables = ::notifyUpdatedTables,
    ) { message -> verboseLog(message) }
    private val lifecycleCoordinator = OversqliteClientLifecycleCoordinator(
        db = db,
        config = config,
        resources = resourceOwner,
        operations = operationController,
        session = sessionState,
        contractGateway = contractGateway,
        remoteApi = remoteApi,
        applyStateStore = applyStateStore,
        sourceStateStore = sourceStateStore,
        attachmentStateStore = attachmentStateStore,
        operationStateStore = operationStateStore,
        syncStateStore = syncStateStore,
        outboxStateStore = outboxStateStore,
        stageStore = stageStore,
        snapshotDiagnosticsState = snapshotDiagnosticsState,
        snapshotGate = snapshotGate,
        downloadWorkflow = downloadWorkflow,
        notifyUpdatedTables = ::notifyUpdatedTables,
    )
    private val syncCoordinator = OversqliteClientSyncCoordinator(
        operations = operationController,
        session = sessionState,
        contractGateway = contractGateway,
        lifecycle = lifecycleCoordinator,
        pushWorkflow = pushWorkflow,
        downloadWorkflow = downloadWorkflow,
        attachmentStateStore = attachmentStateStore,
        operationStateStore = operationStateStore,
        syncStateStore = syncStateStore,
        outboxStateStore = outboxStateStore,
        conflictStateStore = conflictStateStore,
        notifyUpdatedTables = ::notifyUpdatedTables,
    )

    internal var cleanupObservationTimeoutMillis: Long
        get() = resourceOwner.cleanupObservationTimeoutMillis
        set(value) {
            resourceOwner.cleanupObservationTimeoutMillis = value
        }

    override val progress: StateFlow<OversqliteProgress> = operationController.progress

    internal fun snapshotRestoreDiagnosticsForTest(): SnapshotRestoreDiagnostics =
        snapshotDiagnosticsState.published()

    internal fun setSnapshotApplyFaultInjectorForTest(injector: SnapshotApplyFaultInjector?) {
        applyExecutor.faultInjector = injector
    }

    internal fun setBeforeDetachWriteReservationForTest(hook: (suspend () -> Unit)?) {
        lifecycleCoordinator.beforeDetachWriteReservationForTest = hook
    }

    internal fun setSnapshotStageStatementCloseForTest(close: ((SQLiteStatement) -> Unit)?) {
        stageStore.snapshotStatementCloseForTest = close
    }

    internal fun setSnapshotStageReusableStatementCleanupForTest(cleanup: ReusableStatementCleanup?) {
        stageStore.reusableStatementCleanupForTest = cleanup
    }

    internal fun resetSnapshotRestoreDiagnosticsForTest() {
        operationController.resetSnapshotRestoreDiagnosticsForTest()
    }

    private fun verboseLog(message: () -> String) {
        if (config.verboseLogs) {
            sqliteNowLogger.i(message = message)
        }
    }

    private fun notifyUpdatedTables(updatedTables: Set<String>) {
        db.reportExternalTableChanges(updatedTables)
    }

    override suspend fun pauseUploads() = operationController.pauseUploads()

    override suspend fun resumeUploads() = operationController.resumeUploads()

    override suspend fun pauseDownloads() = operationController.pauseDownloads()

    override suspend fun resumeDownloads() = operationController.resumeDownloads()

    suspend fun runAutomaticDownloads(
        automaticDownloadConfig: OversqliteAutomaticDownloadConfig = OversqliteAutomaticDownloadConfig(),
    ): Nothing = resourceOwner.trackOwnedOperation {
        OversqliteAutomaticDownloads(
            automaticDownloadConfig = automaticDownloadConfig,
            remoteApi = remoteApi,
            attachmentStateStore = attachmentStateStore,
            downloadsPaused = operationController::downloadsPaused,
            connectedRuntimeState = sessionState::requireConnectedRuntimeState,
            pullToStable = syncCoordinator::pullToStable,
            log = { message -> verboseLog(message) },
        ).run()
    }

    override suspend fun open(): Result<Unit> = lifecycleCoordinator.open()
    override suspend fun sourceInfo(): Result<SourceInfo> = lifecycleCoordinator.sourceInfo()
    override suspend fun attach(userId: String): Result<AttachResult> = lifecycleCoordinator.attach(userId)
    override suspend fun syncStatus(): Result<SyncStatus> = syncCoordinator.syncStatus()
    override suspend fun detach(): Result<DetachOutcome> = lifecycleCoordinator.detach()
    override suspend fun pushPending(): Result<PushReport> = syncCoordinator.pushPending()
    override suspend fun readPushDependencyDiagnostic(diagnosticId: String,cursor: String?,pageSize: Int): Result<PushDependencyDiagnosticPage> =
        operationController.runExclusiveOperation {
            val attachment=attachmentStateStore.loadState()
            val state=RuntimeState(sessionState.requireValidatedConfig(),sessionState.requireAdmittedContract(),attachment.attachedUserId,attachment.currentSourceId,attachment.pendingInitializationId)
            stageStore.readPushDependencyDiagnostic(state,diagnosticId,cursor,pageSize)
        }
    override suspend fun releasePushDependencyDiagnostic(diagnosticId: String): Result<Unit> =
        operationController.runExclusiveOperation {
            stageStore.releasePushDependencyDiagnostic(diagnosticId)
        }

    override suspend fun pendingConflict(): Result<PendingConflict?> = syncCoordinator.pendingConflict()
    override suspend fun retryConflict(selected: PendingConflict): Result<Unit> = syncCoordinator.retryConflict(selected)
    override suspend fun resolveConflict(selected: PendingConflict, result: MergeResult): Result<Unit> =
        syncCoordinator.resolveConflict(selected, result)
    override suspend fun pullToStable(): Result<RemoteSyncReport> = syncCoordinator.pullToStable()
    override suspend fun sync(): Result<SyncReport> = syncCoordinator.sync()
    override suspend fun syncThenDetach(): Result<SyncThenDetachResult> = syncCoordinator.syncThenDetach()
    override suspend fun rebuild(): Result<RemoteSyncReport> = syncCoordinator.rebuild()

    internal suspend fun rebuildForOpening(finalizer: suspend (String) -> Unit): Result<RemoteSyncReport> =
        syncCoordinator.rebuildForOpening(finalizer)

    @OptIn(SqliteNowInternalApi::class)
    override fun close() {
        resourceOwner.close()
    }

    internal fun bindCoordinatedClose(handler: () -> Unit) = resourceOwner.bindCoordinatedClose(handler)

    internal val cleanupPending: Boolean
        get() = resourceOwner.cleanupPending

    internal suspend fun closeFromCoordinatedOwner() = resourceOwner.closeFromCoordinatedOwner()

    internal suspend fun previewGenerationForUpgrade(
        sourceId: String,
        generationId: Long,
    ): SyncGeneration = lifecycleCoordinator.previewGenerationForUpgrade(sourceId, generationId)

    internal suspend fun admitAttachedGenerationOpening(userId: String) =
        lifecycleCoordinator.admitAttachedGenerationOpening(userId)

    internal suspend fun prepareGenerationReplacement(
        oldSourceId: String,
        newSourceId: String,
        userId: String,
        precondition: suspend () -> Unit,
        finalizer: suspend () -> Unit,
    ) = lifecycleCoordinator.prepareGenerationReplacement(
        oldSourceId,
        newSourceId,
        userId,
        precondition,
        finalizer,
    )

    internal suspend fun createGenerationReplacementSnapshot(
        oldSourceId: String,
        newSourceId: String,
        reason: String,
    ): SnapshotSession = lifecycleCoordinator.createGenerationReplacementSnapshot(
        oldSourceId,
        newSourceId,
        reason,
    )

    internal suspend fun createGenerationRecoverySnapshot(newSourceId: String): SnapshotSession =
        lifecycleCoordinator.createGenerationRecoverySnapshot(newSourceId)

    internal suspend fun stageGenerationSnapshot(
        session: SnapshotSession,
        newSourceId: String,
        userId: String,
        initialTotals: SnapshotTransferTotals = SnapshotTransferTotals(),
        onChunkStaged: suspend (SnapshotTransferTotals) -> Unit,
    ): SnapshotTransferTotals = lifecycleCoordinator.stageGenerationSnapshot(
        session,
        newSourceId,
        userId,
        initialTotals,
        onChunkStaged,
    )

    internal suspend fun applyGenerationSnapshot(
        session: SnapshotSession,
        oldSourceId: String,
        newSourceId: String,
        userId: String,
        finalizer: suspend () -> Unit,
    ) = lifecycleCoordinator.applyGenerationSnapshot(
        session,
        oldSourceId,
        newSourceId,
        userId,
        finalizer,
    )

    internal suspend fun deleteGenerationSnapshotBestEffort(snapshotId: String, sourceId: String) =
        lifecycleCoordinator.deleteGenerationSnapshotBestEffort(snapshotId, sourceId)
}
