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
    internal fun workspaceDiagnostics(): A3AccountingStats = operationController.a3Stats.copy(work=operationController.workSliceDiagnostics.totalWork)
    internal fun workSliceDiagnostics(): WorkSliceDiagnostics = operationController.workSliceDiagnostics.copy()
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
        beforeRecoveryExchange = operationController::chargeRecoveryExchange,
        diagnostics = config.diagnostics,
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
    private val applyExecutor = OversqliteApplyExecutor(db, applyStateStore, operationController)
    private val automaticRecovery = OversqliteAutomaticRecovery(db,config,operationController)
    private val snapshotGate = OversqliteSnapshotGate(
        automaticRecovery = automaticRecovery,
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
        automaticRecovery = automaticRecovery,
        operations = operationController,
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

    override suspend fun open(): Result<Unit> = diagnosticSyncResult(config, "sync.open", db) { lifecycleCoordinator.open() }
    override suspend fun sourceInfo(): Result<SourceInfo> = diagnosticSyncResult(config, "sync.sourceInfo", db) { lifecycleCoordinator.sourceInfo() }
    override suspend fun attach(userId: String): Result<AttachResult> = diagnosticSyncResult(config, "sync.attach", db) { lifecycleCoordinator.attach(userId) }
    override suspend fun syncStatus(): Result<SyncStatus> = diagnosticSyncResult(config, "sync.syncStatus", db) { syncCoordinator.syncStatus() }
    override suspend fun detach(): Result<DetachOutcome> = diagnosticSyncResult(config, "sync.detach", db) { lifecycleCoordinator.detach() }
    override suspend fun pushPending(): Result<PushReport> = diagnosticSyncResult(config, "sync.pushPending", db) { syncCoordinator.pushPending() }
    // Discharge only the already frozen operation before mutable U0 admission.
    internal suspend fun finishExistingUploadForOpening(): Result<Unit> =
        operationController.runSyncOperation("finish existing upload") {
            val journal=loadUploadJournal(db) ?: return@runSyncOperation
            val state=sessionState.requireConnectedRuntimeState("finish existing upload")
            if(journal.phase=="ack_pending") pushWorkflow.acknowledgePending(state)
            else try {
                notifyUpdatedTables(pushWorkflow.pushPending(state).updatedTables)
            } catch(failure:Throwable) {
                val outbox=outboxStateStore.loadBundleState()
                val original=journal.binding.operation
                val policyFailure=when(failure) {
                    is ConflictNeedsResolutionException -> conflictStateStore.load()!=null
                    is OversqliteRemoteContractException -> failure.httpStatus==200 && !failure.retryable && conflictStateStore.applicationRejection()!=null
                    else -> false
                }
                if(!policyFailure || failure.suppressedExceptions.isNotEmpty() || loadUploadJournal(db)!=null ||
                    outbox.state!=outboxStateRejected || outbox.sourceId!=original.sourceId ||
                    outbox.sourceBundleId!=original.sourceBundleId || outbox.canonicalRequestHash!=original.canonicalRequestHash) throw failure
                // Only protocol completion is consumed here. Existing U0 and
                // old-intent policy owners still decide whether work may continue.
            }
        }
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

    override suspend fun pendingConflict(): Result<PendingConflict?> = diagnosticSyncResult(config, "sync.pendingConflict", db) { syncCoordinator.pendingConflict() }
    override suspend fun retryConflict(selected: PendingConflict): Result<Unit> = diagnosticSyncResult(config, "sync.retryConflict", db) { syncCoordinator.retryConflict(selected) }
    override suspend fun resolveConflict(selected: PendingConflict, result: MergeResult): Result<Unit> =
        diagnosticSyncResult(config, "sync.resolveConflict", db) { syncCoordinator.resolveConflict(selected, result) }
    override suspend fun pullToStable(): Result<RemoteSyncReport> = diagnosticSyncResult(config, "sync.pullToStable", db) { syncCoordinator.pullToStable() }
    override suspend fun recoveryStatus(): Result<RecoveryStatus> = diagnosticSyncResult(config, "sync.recoveryStatus", db) { operationController.runRecoveryControl { automaticRecovery.status() } }
    override suspend fun setRecoveryMode(mode: RecoveryMode): Result<Unit> = diagnosticSyncResult(config, "sync.setRecoveryMode", db) { operationController.runRecoveryControl { automaticRecovery.setMode(mode) } }
    override suspend fun retryRecovery(): Result<Unit> = diagnosticSyncResult(config, "sync.retryRecovery", db) { operationController.runRecoveryControl { automaticRecovery.retry() } }
    override suspend fun sync(): Result<SyncReport> = diagnosticSyncResult(config, "sync.sync", db) { syncCoordinator.sync() }
    override suspend fun syncThenDetach(): Result<SyncThenDetachResult> = diagnosticSyncResult(config, "sync.syncThenDetach", db) { syncCoordinator.syncThenDetach() }
    override suspend fun rebuild(): Result<RemoteSyncReport> = diagnosticSyncResult(config, "sync.rebuild", db) { syncCoordinator.rebuild() }

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

    internal suspend fun retainGenerationSnapshot(snapshot: SnapshotSession, newSourceId: String) =
        lifecycleCoordinator.retainGenerationSnapshot(snapshot, newSourceId)

    internal suspend fun retainedGenerationSnapshot(newSourceId: String): SnapshotSession =
        lifecycleCoordinator.retainedGenerationSnapshot(newSourceId)

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
