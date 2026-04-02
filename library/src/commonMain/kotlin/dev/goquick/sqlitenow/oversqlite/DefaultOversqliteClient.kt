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

import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import dev.goquick.sqlitenow.core.sqlite.use
import io.ktor.client.HttpClient
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.serialization.json.Json
import kotlin.concurrent.Volatile
import kotlin.random.Random
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

class DefaultOversqliteClient(
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
    private val http: HttpClient,
    private val resolver: Resolver = ServerWinsResolver,
) : OversqliteClient {
    private companion object {
        const val MAX_SYNC_THEN_DETACH_ROUNDS = 3
    }

    private val progressState = MutableStateFlow<OversqliteProgress>(OversqliteProgress.Idle)
    private val syncGate = Mutex()
    private val tableInfoCache = TableInfoCache()
    private val runtimeInitializer = SyncRuntimeInitializer(config, tableInfoCache)
    private val json = Json { ignoreUnknownKeys = true }
    private val managedTableStore = OversqliteManagedTableStore(db)
    private val applyStateStore = OversqliteApplyStateStore(db)
    private val sourceStateStore = OversqliteSourceStateStore(db)
    private val attachmentStateStore = OversqliteAttachmentStateStore(db)
    private val operationStateStore = OversqliteOperationStateStore(db)
    private val syncStateStore = OversqliteSyncStateStore(db)
    private val outboxStateStore = OversqliteOutboxStateStore(db)
    private val remoteApi = OversqliteRemoteApi(http, json) { message -> verboseLog(message) }
    private val localStore = OversqliteLocalStore(db, tableInfoCache, json, ::requireConnectedStateForLocalStore)
    private val bundleApplier = OversqliteBundleApplier(localStore, syncStateStore)
    private val stageStore = OversqliteStageStore(db, localStore, syncStateStore, json)
    private val applyExecutor = OversqliteApplyExecutor(db, applyStateStore)
    private val pushWorkflow = OversqlitePushWorkflow(
        config = config,
        resolver = resolver,
        db = db,
        tableInfoCache = tableInfoCache,
        remoteApi = remoteApi,
        sourceStateStore = sourceStateStore,
        attachmentStateStore = attachmentStateStore,
        outboxStateStore = outboxStateStore,
        syncStateStore = syncStateStore,
        localStore = localStore,
        bundleApplier = bundleApplier,
        stageStore = stageStore,
        applyExecutor = applyExecutor,
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
        json = json,
    ) { message -> verboseLog(message) }

    @Volatile
    private var uploadsPaused = false

    @Volatile
    private var downloadsPaused = false

    @Volatile
    private var validatedConfig: ValidatedConfig? = null

    @Volatile
    private var currentSourceId: String? = null

    @Volatile
    private var currentUserId: String? = null

    @Volatile
    private var sessionConnected = false

    @Volatile
    private var pendingInitializationId: String = ""

    override val progress: StateFlow<OversqliteProgress> = progressState.asStateFlow()

    private fun verboseLog(message: () -> String) {
        if (config.verboseLogs) {
            sqliteNowLogger.i(message = message)
        }
    }

    private fun notifyUpdatedTables(updatedTables: Set<String>) {
        db.reportExternalTableChanges(updatedTables)
    }

    override suspend fun pauseUploads() {
        uploadsPaused = true
    }

    override suspend fun resumeUploads() {
        uploadsPaused = false
    }

    override suspend fun pauseDownloads() {
        downloadsPaused = true
    }

    override suspend fun resumeDownloads() {
        downloadsPaused = false
    }

    override suspend fun open(): Result<Unit> = runExclusiveOperation {
        openInternal()
        restoreInMemoryLifecycleState()
    }

    private suspend fun restoreInMemoryLifecycleState() {
        val state = loadDurableLifecycleView()
        if (state.isAttached) {
            currentUserId = state.attachedUserId
            pendingInitializationId = state.pendingInitializationId
            sessionConnected = false
            return
        }
        currentUserId = null
        pendingInitializationId = ""
        sessionConnected = false
    }

    override suspend fun sourceInfo(): Result<SourceInfo> = runExclusiveOperation {
        ensureOpened("sourceInfo()")
        val attachment = attachmentStateStore.loadState()
        val operation = operationStateStore.loadState()
        SourceInfo(
            currentSourceId = attachment.currentSourceId.ifBlank { requireCurrentSourceId() },
            rebuildRequired = attachment.rebuildRequired || operation.isSourceRecoveryRequired(),
            sourceRecoveryRequired = operation.isSourceRecoveryRequired(),
            sourceRecoveryReason = operation.sourceRecoveryReasonOrNull(),
        )
    }

    override suspend fun attach(userId: String): Result<AttachResult> = runExclusiveOperation {
        withOperationProgress(OversqliteOperation.ATTACH) {
            setProgress(OversqliteOperation.ATTACH, OversqlitePhase.ATTACHING)
            val requestedUserId = userId.trim()
            require(requestedUserId.isNotEmpty()) { "userId must be provided" }
            ensureOpened("attach(userId)")
            val validated = requireValidatedConfig()
            val state = loadDurableLifecycleView()
            val sourceId = state.sourceId.ifBlank {
                currentSourceId ?: throw OpenRequiredException("attach(userId)")
            }

            if (state.operation.kind == operationKindRemoteReplace) {
                if (state.operation.targetUserId != requestedUserId || state.sourceId != sourceId) {
                    throw ConnectLocalStateConflictException(
                        "pending remote_replace belongs to scope \"${state.operation.targetUserId}\" on source \"$sourceId\"",
                    )
                }
                currentUserId = requestedUserId
                val downloadResult = finalizeRemoteReplace(
                    validated = validated,
                    state = state,
                    operation = OversqliteOperation.ATTACH,
                )
                notifyUpdatedTables(downloadResult.updatedTables)
                persistConnectedLifecycleState(requestedUserId, initializationId = "")
                sessionConnected = true
                pendingInitializationId = ""
                val runtimeState = requireConnectedRuntimeState("attach(userId)")
                val status = syncStatusInternal(runtimeState)
                return@withOperationProgress AttachResult.Connected(
                    outcome = AttachOutcome.USED_REMOTE_STATE,
                    status = status,
                    restore = downloadResult.restore,
                )
            }

            if (state.isAttached &&
                state.attachedUserId == requestedUserId &&
                state.operation.kind == operationKindNone
            ) {
                currentUserId = requestedUserId
                pendingInitializationId = state.pendingInitializationId
                sessionConnected = true
                val status = syncStatusInternal(requireConnectedRuntimeState("attach(userId)"))
                return@withOperationProgress AttachResult.Connected(
                    outcome = AttachOutcome.RESUMED_ATTACHED_STATE,
                    status = status,
                )
            }

            if (state.isAttached && state.attachedUserId != requestedUserId) {
                throw ConnectBindingConflictException(
                    attachedUserId = state.attachedUserId,
                    requestedUserId = requestedUserId,
                )
            }

            verifyConnectLifecycleSupported()
            val hasLocalPendingRows = pendingLocalChangeCount() > 0
            val response = withTransientRetry("attach() transport") {
                remoteApi.connect(sourceId, hasLocalPendingRows)
            }
            return@withOperationProgress when (response.resolution) {
                "retry_later" -> {
                    sessionConnected = false
                    AttachResult.RetryLater(response.retryAfterSeconds.toLong())
                }

                "initialize_empty" -> {
                    persistConnectedLifecycleState(requestedUserId, initializationId = "")
                    currentUserId = requestedUserId
                    pendingInitializationId = ""
                    sessionConnected = true
                    val status = syncStatusInternal(requireConnectedRuntimeState("attach(userId)"))
                    AttachResult.Connected(
                        outcome = AttachOutcome.STARTED_EMPTY,
                        status = status,
                    )
                }

                "initialize_local" -> {
                    persistConnectedLifecycleState(requestedUserId, initializationId = response.initializationId)
                    currentUserId = requestedUserId
                    pendingInitializationId = response.initializationId
                    sessionConnected = true
                    val status = syncStatusInternal(requireConnectedRuntimeState("attach(userId)"))
                    AttachResult.Connected(
                        outcome = AttachOutcome.SEEDED_FROM_LOCAL,
                        status = status,
                    )
                }

                "remote_authoritative" -> {
                    currentUserId = requestedUserId
                    beginRemoteReplace(requestedUserId, sourceId)
                    val refreshedState = loadDurableLifecycleView()
                    val downloadResult = finalizeRemoteReplace(
                        validated = validated,
                        state = refreshedState,
                        operation = OversqliteOperation.ATTACH,
                    )
                    notifyUpdatedTables(downloadResult.updatedTables)
                    persistConnectedLifecycleState(requestedUserId, initializationId = "")
                    pendingInitializationId = ""
                    sessionConnected = true
                    val status = syncStatusInternal(requireConnectedRuntimeState("attach(userId)"))
                    AttachResult.Connected(
                        outcome = AttachOutcome.USED_REMOTE_STATE,
                        status = status,
                        restore = downloadResult.restore,
                    )
                }

                else -> error("unexpected connect resolution ${response.resolution}")
            }
        }
    }

    override suspend fun syncStatus(): Result<SyncStatus> = runSyncOperation("syncStatus()") {
        ensureNoDestructiveTransition()
        syncStatusInternal(requireConnectedRuntimeState("syncStatus()"))
    }

    override suspend fun detach(): Result<DetachOutcome> = runExclusiveOperation {
        val execution = executeDetach("detach()")
        notifyUpdatedTables(execution.updatedTables)
        execution.outcome
    }

    override suspend fun pushPending(): Result<PushReport> = runSyncOperation("pushPending()") {
        withOperationProgress(OversqliteOperation.PUSH_PENDING) {
            val execution = executePush(
                operationName = "pushPending()",
                operation = OversqliteOperation.PUSH_PENDING,
            )
            notifyUpdatedTables(execution.updatedTables)
            execution.report
        }
    }

    override suspend fun pullToStable(): Result<RemoteSyncReport> = runSyncOperation("pullToStable()") {
        withOperationProgress(OversqliteOperation.PULL_TO_STABLE) {
            val execution = executePullToStable(
                operationName = "pullToStable()",
                operation = OversqliteOperation.PULL_TO_STABLE,
            )
            notifyUpdatedTables(execution.updatedTables)
            execution.report
        }
    }

    override suspend fun sync(): Result<SyncReport> = runSyncOperation("sync()") {
        withOperationProgress(OversqliteOperation.SYNC) {
            val execution = executeSyncRound(
                operationName = "sync()",
                operation = OversqliteOperation.SYNC,
            )
            notifyUpdatedTables(execution.updatedTables)
            execution.report
        }
    }

    override suspend fun syncThenDetach(): Result<SyncThenDetachResult> = runSyncOperation("syncThenDetach()") {
        withOperationProgress(OversqliteOperation.SYNC) {
            var previousPendingRowCount = Long.MAX_VALUE
            var lastReport: SyncReport? = null
            repeat(MAX_SYNC_THEN_DETACH_ROUNDS) { roundIndex ->
                val syncExecution = executeSyncRound(
                    operationName = "syncThenDetach()",
                    operation = OversqliteOperation.SYNC,
                )
                lastReport = syncExecution.report
                notifyUpdatedTables(syncExecution.updatedTables)

                val detachExecution = executeDetach("syncThenDetach()")
                notifyUpdatedTables(detachExecution.updatedTables)
                if (detachExecution.outcome == DetachOutcome.DETACHED) {
                    return@withOperationProgress SyncThenDetachResult(
                        lastSync = syncExecution.report,
                        detach = DetachOutcome.DETACHED,
                        syncRounds = roundIndex + 1,
                        remainingPendingRowCount = 0L,
                    )
                }

                val pending = pendingSyncStatusInternal()
                if (pending.pendingRowCount >= previousPendingRowCount) {
                    return@withOperationProgress SyncThenDetachResult(
                        lastSync = syncExecution.report,
                        detach = DetachOutcome.BLOCKED_UNSYNCED_DATA,
                        syncRounds = roundIndex + 1,
                        remainingPendingRowCount = pending.pendingRowCount,
                    )
                }
                previousPendingRowCount = pending.pendingRowCount
            }

            val finalReport = lastReport ?: error("syncThenDetach() did not execute a sync round")
            val pending = pendingSyncStatusInternal()
            SyncThenDetachResult(
                lastSync = finalReport,
                detach = DetachOutcome.BLOCKED_UNSYNCED_DATA,
                syncRounds = MAX_SYNC_THEN_DETACH_ROUNDS,
                remainingPendingRowCount = pending.pendingRowCount,
            )
        }
    }

    override suspend fun rebuild(): Result<RemoteSyncReport> = runSyncOperation("rebuild()") {
        val plan = selectRebuildPlan()
        withOperationProgress(plan.operation) {
            val execution = executeSnapshotRebuild(
                operationName = "rebuild()",
                operation = plan.operation,
                rotatedSourceId = plan.rotatedSourceId,
                outboxMode = plan.outboxMode,
            )
            notifyUpdatedTables(execution.updatedTables)
            execution.report
        }
    }

    override fun close() {
    }

    private suspend fun openInternal(): ValidatedConfig {
        val validated = runtimeInitializer.prepareLocalRuntime(db, managedTableStore)
        var attachmentState: OversqliteAttachmentState? = null
        db.transaction(TransactionMode.IMMEDIATE) {
            val operation = operationStateStore.loadState()
            val currentAttachment = attachmentStateStore.loadState()
            val boundSourceId = currentAttachment.currentSourceId.ifBlank { generateFreshSourceId() }
            sourceStateStore.ensureSource(boundSourceId)
            if (currentAttachment.currentSourceId.isBlank()) {
                runtimeInitializer.capturePreexistingAnonymousRows(db, validated)
            }
            val normalizedAttachment = currentAttachment.copy(
                currentSourceId = boundSourceId,
                bindingState = if (
                    operation.kind == operationKindRemoteReplace ||
                    currentAttachment.bindingState != attachmentBindingAttached ||
                    currentAttachment.attachedUserId.isBlank()
                ) {
                    attachmentBindingAnonymous
                } else {
                    attachmentBindingAttached
                },
                attachedUserId = currentAttachment.attachedUserId.takeIf {
                    operation.kind != operationKindRemoteReplace &&
                        currentAttachment.bindingState == attachmentBindingAttached &&
                        it.isNotBlank()
                }.orEmpty(),
                schemaName = config.schema.trim().takeIf {
                    operation.kind != operationKindRemoteReplace &&
                        currentAttachment.bindingState == attachmentBindingAttached &&
                        currentAttachment.attachedUserId.isNotBlank()
                }.orEmpty(),
                lastBundleSeqSeen = currentAttachment.lastBundleSeqSeen.takeIf {
                    operation.kind != operationKindRemoteReplace &&
                        currentAttachment.bindingState == attachmentBindingAttached &&
                        currentAttachment.attachedUserId.isNotBlank()
                } ?: 0L,
                rebuildRequired = currentAttachment.rebuildRequired &&
                    operation.kind != operationKindRemoteReplace &&
                    currentAttachment.bindingState == attachmentBindingAttached &&
                    currentAttachment.attachedUserId.isNotBlank(),
                pendingInitializationId = currentAttachment.pendingInitializationId.takeIf {
                    operation.kind == operationKindNone &&
                        currentAttachment.bindingState == attachmentBindingAttached &&
                        currentAttachment.attachedUserId.isNotBlank()
                }.orEmpty(),
            )
            attachmentStateStore.persistState(normalizedAttachment)
            attachmentState = normalizedAttachment
        }

        validatedConfig = validated
        currentSourceId = attachmentState!!.currentSourceId
        pendingInitializationId = attachmentState.pendingInitializationId
        if (attachmentState.bindingState == attachmentBindingAttached && attachmentState.attachedUserId.isNotBlank()) {
            currentUserId = attachmentState.attachedUserId
        } else {
            currentUserId = null
        }
        return validated
    }

    private suspend fun verifyConnectLifecycleSupported() {
        val capabilities = withTransientRetry("attach() capability gate") {
            remoteApi.fetchCapabilities(requireCurrentSourceId())
        }
        if (!capabilities.features.getOrElse("connect_lifecycle") { false }) {
            throw ConnectLifecycleUnsupportedException("connect_lifecycle capability is absent")
        }
    }

    private suspend fun beginRemoteReplace(
        userId: String,
        sourceId: String,
    ) {
        val statementCache = StatementCache(db)
        try {
            attachmentStateStore.persistAnonymousState(sourceId, statementCache)
            operationStateStore.persistState(
                OversqliteOperationState(
                    kind = operationKindRemoteReplace,
                    targetUserId = userId,
                ),
                statementCache,
            )
        } finally {
            statementCache.close()
        }
    }

    private suspend fun finalizeRemoteReplace(
        validated: ValidatedConfig,
        state: DurableLifecycleView,
        operation: OversqliteOperation,
    ): DownloadWorkflowResult {
        require(state.operation.kind == operationKindRemoteReplace) {
            "cannot finalize durable operation ${state.operation.kind} as remote_replace"
        }
        val refreshedState = ensureRemoteReplaceSnapshotStaged(state, operation)
        val runtimeState = RuntimeState(
            validated = validated,
            userId = refreshedState.operation.targetUserId,
            sourceId = refreshedState.sourceId,
        )
        val session = SnapshotSession(
            snapshotId = refreshedState.operation.stagedSnapshotId,
            snapshotBundleSeq = refreshedState.operation.snapshotBundleSeq,
            rowCount = refreshedState.operation.snapshotRowCount,
            expiresAt = "",
        )
        setProgress(operation, OversqlitePhase.APPLYING_REMOTE_STATE)
        val result = downloadWorkflow.applyStagedSnapshot(
            state = runtimeState,
            session = session,
            rotatedSourceId = null,
            outboxMode = SnapshotRebuildOutboxMode.CLEAR_ALL,
        )
        applyDownloadResult(result)
        return result
    }

    private suspend fun ensureRemoteReplaceSnapshotStaged(
        state: DurableLifecycleView,
        operation: OversqliteOperation,
    ): DurableLifecycleView {
        var nextOperation = state.operation
        if (nextOperation.stagedSnapshotId.isNotBlank()) {
            val stagedCount = countSnapshotStageRows(nextOperation.stagedSnapshotId)
            if (stagedCount == nextOperation.snapshotRowCount) {
                return state.copy(operation = nextOperation)
            }
            stageStore.clearAllSnapshotStages()
            nextOperation = nextOperation.copy(
                stagedSnapshotId = "",
                snapshotBundleSeq = 0,
                snapshotRowCount = 0,
            )
            operationStateStore.persistState(nextOperation)
        }

        setProgress(operation, OversqlitePhase.STAGING_REMOTE_STATE)
        val session = withTransientRetry("remote_replace snapshot create") {
            remoteApi.createSnapshotSession(state.sourceId.ifBlank { requireCurrentSourceId() })
        }
        try {
            stageStore.clearAllSnapshotStages()
            nextOperation = nextOperation.copy(
                stagedSnapshotId = session.snapshotId,
                snapshotBundleSeq = session.snapshotBundleSeq,
                snapshotRowCount = session.rowCount,
            )
            operationStateStore.persistState(nextOperation)

            var afterRowOrdinal = 0L
            while (true) {
                val chunk = withTransientRetry("remote_replace snapshot fetch") {
                    remoteApi.fetchSnapshotChunk(
                        snapshotId = session.snapshotId,
                        sourceId = state.sourceId.ifBlank { requireCurrentSourceId() },
                        snapshotBundleSeq = session.snapshotBundleSeq,
                        afterRowOrdinal = afterRowOrdinal,
                        maxRows = config.snapshotChunkRows.takeIf { it > 0 } ?: 1000,
                    )
                }
                val runtimeState = RuntimeState(
                    validated = requireValidatedConfig(),
                    userId = nextOperation.targetUserId,
                    sourceId = state.sourceId.ifBlank { requireCurrentSourceId() },
                )
                stageStore.stageSnapshotChunk(runtimeState, session, chunk, afterRowOrdinal)
                if (!chunk.hasMore) {
                    break
                }
                afterRowOrdinal = chunk.nextRowOrdinal
            }
            return state.copy(operation = nextOperation)
        } finally {
            remoteApi.deleteSnapshotSessionBestEffort(
                snapshotId = session.snapshotId,
                sourceId = state.sourceId.ifBlank { requireCurrentSourceId() },
            )
        }
    }

    private suspend fun persistConnectedLifecycleState(
        userId: String,
        initializationId: String,
    ) {
        val existing = attachmentStateStore.loadState()
        val statementCache = StatementCache(db)
        try {
            attachmentStateStore.persistAttachedState(
                sourceId = existing.currentSourceId.ifBlank { requireCurrentSourceId() },
                userId = userId,
                schemaName = config.schema.trim(),
                lastBundleSeqSeen = existing.lastBundleSeqSeen,
                rebuildRequired = existing.rebuildRequired,
                pendingInitializationId = initializationId,
                statementCache = statementCache,
            )
            operationStateStore.persistState(OversqliteOperationState(), statementCache)
        } finally {
            statementCache.close()
        }
    }

    private suspend fun cancelPendingRemoteReplace(
        state: DurableLifecycleView,
    ) {
        if (state.operation.stagedSnapshotId.isNotBlank()) {
            stageStore.deleteSnapshotStage(state.operation.stagedSnapshotId)
        } else {
            stageStore.clearAllSnapshotStages()
        }
        val statementCache = StatementCache(db)
        try {
            attachmentStateStore.persistAnonymousState(
                sourceId = state.sourceId.ifBlank { requireCurrentSourceId() },
                statementCache = statementCache,
            )
            operationStateStore.persistState(OversqliteOperationState(), statementCache)
        } finally {
            statementCache.close()
        }
    }

    private suspend fun clearFullLocalSyncState(
        validated: ValidatedConfig,
    ) {
        val statementCache = StatementCache(db)
        try {
            // Destructive lifecycle cleanup may temporarily violate managed-table FK edges
            // until the whole graph is cleared within this transaction.
            db.execSQL("PRAGMA defer_foreign_keys = ON")
            applyStateStore.setApplyMode(true, statementCache)
            try {
                clearManagedTables(validated, statementCache)
                stageStore.clearAllSnapshotStages(statementCache)
            } finally {
                applyStateStore.setApplyMode(false, statementCache)
            }
        } finally {
            statementCache.close()
        }
    }

    private suspend fun clearManagedTables(
        validated: ValidatedConfig,
        statementCache: StatementCache? = null,
    ) {
        for (tableName in managedTableNames(validated)) {
            if (tableName.isBlank()) continue
            if (sqliteTableExists(tableName)) {
                db.execSQL("DELETE FROM ${quoteIdent(tableName)}")
            }
            syncStateStore.clearStructuredRowState(validated.schema, tableName, statementCache)
        }
        syncStateStore.clearDirtyRows()
        outboxStateStore.clearBundleAndRows(statementCache)
    }

    private suspend fun managedTableNames(validated: ValidatedConfig): Set<String> {
        val tables = linkedSetOf<String>()
        tables += managedTableStore.loadManagedTables(validated.schema).map { it.lowercase() }
        tables += validated.tables.map { it.tableName.lowercase() }
        return tables.filter { it.isNotBlank() }.toCollection(linkedSetOf())
    }

    private suspend fun sqliteTableExists(tableName: String): Boolean {
        return db.prepare(
            """
            SELECT COUNT(*)
            FROM sqlite_master
            WHERE type = 'table' AND name = ?
            """.trimIndent(),
        ).use { st ->
            st.bindText(1, tableName)
            check(st.step())
            st.getLong(0) > 0L
        }
    }

    private suspend fun countSnapshotStageRows(snapshotId: String): Long {
        return db.prepare(
            """
            SELECT COUNT(*)
            FROM _sync_snapshot_stage
            WHERE snapshot_id = ?
            """.trimIndent(),
        ).use { st ->
            st.bindText(1, snapshotId)
            check(st.step())
            st.getLong(0)
        }
    }

    private suspend fun pendingSyncStatusInternal(): PendingSyncStatus {
        val total = pendingLocalChangeCount().toLong()
        val state = attachmentStateStore.loadState()
        return PendingSyncStatus(
            hasPendingSyncData = total > 0,
            pendingRowCount = total,
            blocksDetach = state.bindingState == attachmentBindingAttached && total > 0,
        )
    }

    private suspend fun pendingLocalChangeCount(): Int {
        return syncStateStore.countDirtyRows() + outboxStateStore.countRows()
    }

    private suspend fun syncStatusInternal(
        state: RuntimeState,
    ): SyncStatus {
        val authority = when {
            state.pendingInitializationId.isNotBlank() -> AuthorityStatus.PENDING_LOCAL_SEED
            syncStateStore.countLiveStructuredRows() == 0L -> AuthorityStatus.AUTHORITATIVE_EMPTY
            else -> AuthorityStatus.AUTHORITATIVE_MATERIALIZED
        }
        return SyncStatus(
            authority = authority,
            pending = pendingSyncStatusInternal(),
            lastBundleSeqSeen = attachmentStateStore.loadState().lastBundleSeqSeen,
        )
    }

    private fun setProgress(
        operation: OversqliteOperation,
        phase: OversqlitePhase,
    ) {
        progressState.value = OversqliteProgress.Active(operation, phase)
    }

    private suspend fun <T> withOperationProgress(
        operation: OversqliteOperation,
        block: suspend () -> T,
    ): T {
        try {
            return block()
        } finally {
            progressState.value = OversqliteProgress.Idle
        }
    }

    private suspend fun executePush(
        operationName: String,
        operation: OversqliteOperation,
    ): PushExecution {
        val state = requireConnectedRuntimeState(operationName)
        val durableOperation = operationStateStore.loadState()
        if (durableOperation.isSourceRecoveryRequired()) {
            throw SourceRecoveryRequiredException(durableOperation.requireSourceRecoveryReason())
        }
        setProgress(
            operation = operation,
            phase = if (state.pendingInitializationId.isNotBlank()) {
                OversqlitePhase.SEEDING
            } else {
                OversqlitePhase.PUSHING
            },
        )
        val workflowResult = try {
            pushWorkflow.pushPending(state)
        } catch (e: SourceRecoveryRequiredHttpException) {
            persistSourceRecoveryRequiredState(state, e.reason)
            throw SourceRecoveryRequiredException(e.reason)
        } catch (e: CommittedReplayPrunedException) {
            attachmentStateStore.setRebuildRequired(true)
            throw RebuildRequiredException()
        } catch (e: InitializationLeaseInvalidException) {
            clearStaleInitializationState()
            throw e
        }
        if (state.pendingInitializationId.isNotBlank() && workflowResult.outcome == PushOutcome.COMMITTED) {
            attachmentStateStore.clearPendingInitializationId()
            pendingInitializationId = ""
        }
        val status = syncStatusInternal(requireConnectedRuntimeState(operationName))
        return PushExecution(
            report = PushReport(
                outcome = workflowResult.outcome,
                status = status,
            ),
            updatedTables = workflowResult.updatedTables,
        )
    }

    private suspend fun executeSyncRound(
        operationName: String,
        operation: OversqliteOperation,
    ): SyncExecution {
        ensureNoDestructiveTransition()
        val pushExecution = executePush(
            operationName = operationName,
            operation = operation,
        )
        val remoteExecution = executePullToStable(
            operationName = operationName,
            operation = operation,
        )
        val updatedTables = linkedSetOf<String>()
        updatedTables += pushExecution.updatedTables
        updatedTables += remoteExecution.updatedTables
        return SyncExecution(
            report = SyncReport(
                pushOutcome = pushExecution.report.outcome,
                remoteOutcome = remoteExecution.report.outcome,
                status = remoteExecution.report.status,
                restore = remoteExecution.report.restore,
            ),
            updatedTables = updatedTables,
        )
    }

    private suspend fun executePullToStable(
        operationName: String,
        operation: OversqliteOperation,
    ): RemoteExecution {
        ensureNoDestructiveTransition()
        val state = requireConnectedRuntimeState(operationName)
        val result = downloadWorkflow.pullToStable(state) { phase ->
            setProgress(operation, phase)
        }
        applyDownloadResult(result)
        val status = syncStatusInternal(requireConnectedRuntimeState(operationName))
        return RemoteExecution(
            report = RemoteSyncReport(
                outcome = result.outcome,
                status = status,
                restore = result.restore,
            ),
            updatedTables = result.updatedTables,
        )
    }

    private suspend fun executeDetach(operationName: String): DetachExecution {
        ensureOpened(operationName)
        val state = loadDurableLifecycleView()
        if (state.operation.kind == operationKindRemoteReplace) {
            cancelPendingRemoteReplace(state)
            currentUserId = null
            pendingInitializationId = ""
            sessionConnected = false
            return DetachExecution(
                outcome = DetachOutcome.DETACHED,
                updatedTables = emptySet(),
            )
        }

        val status = pendingSyncStatusInternal()
        if (status.blocksDetach) {
            return DetachExecution(
                outcome = DetachOutcome.BLOCKED_UNSYNCED_DATA,
                updatedTables = emptySet(),
            )
        }

        val validated = requireValidatedConfig()
        val managedTables = managedTableNames(validated)
        db.transaction(TransactionMode.IMMEDIATE) {
            val statementCache = StatementCache(db)
            try {
                clearFullLocalSyncState(validated)
                attachmentStateStore.persistAnonymousState(
                    sourceId = requireCurrentSourceId(),
                    statementCache = statementCache,
                )
                operationStateStore.persistState(OversqliteOperationState(), statementCache)
            } finally {
                statementCache.close()
            }
        }

        currentUserId = null
        pendingInitializationId = ""
        sessionConnected = false
        return DetachExecution(
            outcome = DetachOutcome.DETACHED,
            updatedTables = managedTables,
        )
    }

    private suspend fun executeSnapshotRebuild(
        operationName: String,
        operation: OversqliteOperation,
        rotatedSourceId: String?,
        outboxMode: SnapshotRebuildOutboxMode,
    ): RemoteExecution {
        ensureNoDestructiveTransition()
        val state = requireConnectedRuntimeState(operationName)
        val result = downloadWorkflow.rebuildFromSnapshot(
            state = state,
            rotatedSourceId = rotatedSourceId,
            outboxMode = outboxMode,
        ) { phase ->
            setProgress(operation, phase)
        }
        applyDownloadResult(result)
        val status = syncStatusInternal(requireConnectedRuntimeState(operationName))
        return RemoteExecution(
            report = RemoteSyncReport(
                outcome = result.outcome,
                status = status,
                restore = result.restore,
            ),
            updatedTables = result.updatedTables,
        )
    }

    private suspend fun clearStaleInitializationState() {
        val sourceId = currentSourceId ?: return
        val statementCache = StatementCache(db)
        try {
            attachmentStateStore.persistAnonymousState(sourceId, statementCache)
            operationStateStore.persistState(OversqliteOperationState(), statementCache)
        } finally {
            statementCache.close()
        }
        currentUserId = null
        pendingInitializationId = ""
        sessionConnected = false
    }

    private suspend fun ensureNoDestructiveTransition() {
        val operation = operationStateStore.loadState()
        if (
            operation.kind != operationKindNone &&
            operation.kind != operationKindRemoteReplace &&
            operation.kind != operationKindSourceRecovery
        ) {
            throw DestructiveTransitionInProgressException(operation.kind)
        }
    }

    private suspend fun loadDurableLifecycleView(): DurableLifecycleView {
        val attachment = attachmentStateStore.loadState()
        val operation = operationStateStore.loadState()
        return DurableLifecycleView(
            sourceId = attachment.currentSourceId,
            bindingState = attachment.bindingState,
            attachedUserId = attachment.attachedUserId,
            pendingInitializationId = attachment.pendingInitializationId,
            operation = operation,
        )
    }

    private suspend fun requireConnectedRuntimeState(operation: String): RuntimeState {
        val validated = requireValidatedConfig()
        val attachment = attachmentStateStore.loadState()
        val sourceId = attachment.currentSourceId.ifBlank {
            currentSourceId ?: throw OpenRequiredException(operation)
        }
        val userId = attachment.attachedUserId.takeIf {
            attachment.bindingState == attachmentBindingAttached && it.isNotBlank()
        } ?: throw ConnectRequiredException(operation)
        if (!sessionConnected) {
            throw ConnectRequiredException(operation)
        }
        if (currentUserId != null && currentUserId != userId) {
            throw ConnectRequiredException(operation)
        }
        if (sourceStateStore.loadState(sourceId) == null) {
            throw ConnectRequiredException(operation)
        }
        pendingInitializationId = attachment.pendingInitializationId
        return RuntimeState(
            validated = validated,
            userId = userId,
            sourceId = sourceId,
            pendingInitializationId = attachment.pendingInitializationId,
        )
    }

    private fun requireConnectedStateForLocalStore(): RuntimeState {
        val validated = validatedConfig ?: error("oversqlite runtime state is unavailable")
        val sourceId = currentSourceId ?: error("oversqlite source is not open")
        val userId = currentUserId ?: error("oversqlite user is not connected")
        return RuntimeState(
            validated = validated,
            userId = userId,
            sourceId = sourceId,
            pendingInitializationId = pendingInitializationId,
        )
    }

    private fun requireValidatedConfig(): ValidatedConfig {
        return validatedConfig ?: throw OpenRequiredException("oversqlite runtime access")
    }

    private fun requireCurrentSourceId(): String {
        return currentSourceId ?: throw OpenRequiredException("oversqlite runtime access")
    }

    private fun ensureOpened(operation: String) {
        if (currentSourceId == null || validatedConfig == null) {
            throw OpenRequiredException(operation)
        }
    }

    private fun applyDownloadResult(result: DownloadWorkflowResult) {
        result.rotatedSourceId?.let { currentSourceId = it }
    }

    private suspend fun selectRebuildPlan(): RebuildPlan {
        val operation = operationStateStore.loadState()
        if (operation.isSourceRecoveryRequired()) {
            return RebuildPlan(
                operation = OversqliteOperation.REBUILD_ROTATE_SOURCE,
                rotatedSourceId = generateFreshSourceId(),
                outboxMode = SnapshotRebuildOutboxMode.PRESERVE_SOURCE_RECOVERY,
            )
        }
        val outboxState = outboxStateStore.loadBundleState()
        return RebuildPlan(
            operation = OversqliteOperation.REBUILD_KEEP_SOURCE,
            rotatedSourceId = null,
            outboxMode = if (outboxState.state == outboxStateCommittedRemote) {
                SnapshotRebuildOutboxMode.PRESERVE_COMMITTED_REMOTE
            } else {
                SnapshotRebuildOutboxMode.CLEAR_ALL
            },
        )
    }

    private suspend fun persistSourceRecoveryRequiredState(
        state: RuntimeState,
        reason: SourceRecoveryReason,
    ) {
        val outboxState = outboxStateStore.loadBundleState()
        val intentState = if (outboxState.state != outboxStateNone && outboxState.rowCount > 0L) {
            sourceRecoveryIntentStateOutbox
        } else {
            sourceRecoveryIntentStateNone
        }
        operationStateStore.persistState(
            OversqliteOperationState(
                kind = operationKindSourceRecovery,
                sourceRecoveryReason = reason.toPersistedOperationReason(),
                sourceRecoverySourceId = state.sourceId,
                sourceRecoverySourceBundleId = outboxState.sourceBundleId,
                sourceRecoveryIntentState = intentState,
            ),
        )
    }

    @OptIn(ExperimentalUuidApi::class)
    private fun generateFreshSourceId(): String {
        return Uuid.random().toString()
    }

    private suspend fun maybeRetryTransportFailure(
        attempt: Int,
    ) {
        if (attempt <= 0) {
            return
        }
        val policy = config.transientRetryPolicy
        var backoff = policy.initialBackoffMillis
        repeat(attempt - 1) {
            backoff = minOf(backoff * 2, policy.maxBackoffMillis)
        }
        val jitterSpan = (backoff * policy.jitterRatio).toLong().coerceAtLeast(0L)
        val jitter = if (jitterSpan == 0L) 0L else Random.nextLong(0L, jitterSpan + 1)
        delay(backoff + jitter)
    }

    private suspend fun <T> withTransientRetry(
        operation: String,
        block: suspend () -> T,
    ): T {
        var lastError: Throwable? = null
        val maxAttempts = config.transientRetryPolicy.maxAttempts.coerceAtLeast(1)
        repeat(maxAttempts) { attemptIndex ->
            try {
                return block()
            } catch (error: Throwable) {
                val retryable = shouldRetryTransportFailure(error)
                verboseLog {
                    "oversqlite operation failure op=$operation attempt=${attemptIndex + 1}/$maxAttempts " +
                        "retryable=$retryable error=${error::class.simpleName ?: "Throwable"}: ${error.message.orEmpty()}"
                }
                if (!retryable) {
                    throw error
                }
                lastError = error
                if (attemptIndex == maxAttempts - 1) {
                    verboseLog {
                        "oversqlite transient retry exhausted op=$operation attempts=$maxAttempts " +
                            "error=${error::class.simpleName ?: "Throwable"}: ${error.message.orEmpty()}"
                    }
                    throw error
                }
                verboseLog {
                    "oversqlite transient retry op=$operation attempt=${attemptIndex + 1}/$maxAttempts error=${error.message}"
                }
                maybeRetryTransportFailure(attemptIndex + 1)
            }
        }
        throw lastError ?: error("unreachable $operation retry state")
    }

    private suspend fun <T> runExclusiveOperation(block: suspend () -> T): Result<T> = runCatching {
        if (!syncGate.tryLock()) {
            throw SyncOperationInProgressException()
        }
        try {
            block()
        } finally {
            syncGate.unlock()
        }
    }

    private suspend fun <T> runSyncOperation(
        operation: String,
        block: suspend () -> T,
    ): Result<T> = runExclusiveOperation {
        withTransientRetry(operation, block)
    }

    private fun shouldRetryTransportFailure(error: Throwable): Boolean {
        if (
            error is InitializationLeaseInvalidException ||
            error is ConnectLifecycleUnsupportedException ||
            error is SourceSequenceMismatchException ||
            error is OpenRequiredException ||
            error is ConnectRequiredException ||
            error is DestructiveTransitionInProgressException ||
            error is PushConflictException ||
            error is PushConflictRetryExhaustedException ||
            error is InvalidConflictResolutionException ||
            error is RebuildRequiredException ||
            error is SourceRecoveryRequiredException ||
            error is PendingPushReplayException ||
            error is DirtyStateRejectedException
        ) {
            return false
        }
        val message = error.message?.lowercase().orEmpty()
        return message.contains("http 5") ||
            message.contains("timeout") ||
            message.contains("temporar") ||
            message.contains("unavailable") ||
            message.contains("connect") ||
            message.contains("network")
    }
}

private data class PushExecution(
    val report: PushReport,
    val updatedTables: Set<String>,
)

private data class SyncExecution(
    val report: SyncReport,
    val updatedTables: Set<String>,
)

private data class DurableLifecycleView(
    val sourceId: String,
    val bindingState: String,
    val attachedUserId: String,
    val pendingInitializationId: String,
    val operation: OversqliteOperationState,
) {
    val isAttached: Boolean
        get() = bindingState == attachmentBindingAttached && attachedUserId.isNotBlank()
}

private data class RebuildPlan(
    val operation: OversqliteOperation,
    val rotatedSourceId: String?,
    val outboxMode: SnapshotRebuildOutboxMode,
)

private data class RemoteExecution(
    val report: RemoteSyncReport,
    val updatedTables: Set<String>,
)

private data class DetachExecution(
    val outcome: DetachOutcome,
    val updatedTables: Set<String>,
)
