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

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowSyncOpenRequiredException
import dev.goquick.sqlitenow.core.TransactionMode
import dev.goquick.sqlitenow.core.sqlite.use

internal class OversqliteClientLifecycleCoordinator(
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
    private val resources: OversqliteClientResourceOwner,
    private val operations: OversqliteClientOperationController,
    private val session: OversqliteClientSessionState,
    private val contractGateway: OversqliteContractGateway,
    private val remoteApi: OversqliteRemoteApi,
    private val applyStateStore: OversqliteApplyStateStore,
    private val sourceStateStore: OversqliteSourceStateStore,
    private val attachmentStateStore: OversqliteAttachmentStateStore,
    private val operationStateStore: OversqliteOperationStateStore,
    private val syncStateStore: OversqliteSyncStateStore,
    private val outboxStateStore: OversqliteOutboxStateStore,
    private val stageStore: OversqliteStageStore,
    private val snapshotDiagnosticsState: SnapshotDiagnosticsState,
    private val snapshotGate: OversqliteSnapshotGate,
    private val downloadWorkflow: OversqliteDownloadWorkflow,
    private val notifyUpdatedTables: (Set<String>) -> Unit,
) {
    internal var beforeDetachWriteReservationForTest: (suspend () -> Unit)? = null

    internal suspend fun open(): Result<Unit> {
        if (!session.coordinatorAuthorized) {
            return Result.failure(
                SqliteNowSyncOpenRequiredException(
                    "DefaultOversqliteClient requires coordinator-issued ownership; use openForSync()",
                ),
            )
        }
        return operations.runExclusiveOperation {
            resources.acquireActiveClientLease()
            try {
                session.openLocalRuntime()
                session.restoreInMemoryLifecycleState()
            } catch (failure: Throwable) {
                resources.releaseActiveClientLease()
                throw failure
            }
        }
    }

    internal suspend fun sourceInfo(): Result<SourceInfo> = operations.runExclusiveOperation {
        session.ensureOpened("sourceInfo()")
        val attachment = attachmentStateStore.loadState()
        val operation = operationStateStore.loadState()
        SourceInfo(
            generationId = config.generationId,
            contractConfirmed = session.requireValidatedConfig().generation != null,
            currentSourceId = session.resolveSourceIdOrCurrent(attachment.currentSourceId),
            rebuildRequired = attachment.rebuildRequired || operation.isSourceRecoveryRequired(),
            sourceRecoveryRequired = operation.isSourceRecoveryRequired(),
            sourceRecoveryReason = operation.sourceRecoveryReasonOrNull(),
        )
    }

    internal suspend fun attach(userId: String): Result<AttachResult> = operations.runExclusiveOperation {
        operations.withSnapshotDiagnosticsScope {
            operations.withOperationProgress(OversqliteOperation.ATTACH) {
                operations.setProgress(OversqliteOperation.ATTACH, OversqlitePhase.ATTACHING)
                val requestedUserId = userId.trim()
                require(requestedUserId.isNotEmpty()) { "userId must be provided" }
                session.ensureOpened("attach(userId)")
                var validated = session.requireValidatedConfig()
                val state = session.loadDurableLifecycleView()
                val sourceId = session.resolveSourceIdOrCurrent(state.sourceId)

                // Each opened client admits one immutable discovery contract before any attached
                // state can be published back to callers, including durable resume paths.
                contractGateway.verifyConnectLifecycleSupported()
                validated = session.requireValidatedConfig()

                if (state.operation.kind == operationKindRemoteReplace &&
                    (state.operation.targetUserId != requestedUserId || state.sourceId != sourceId)
                ) {
                    throw ConnectLocalStateConflictException(
                        "pending remote_replace belongs to a different local scope",
                    )
                }

                if (state.isAttached && state.attachedUserId != requestedUserId) {
                    throw ConnectBindingConflictException(state.attachedUserId, requestedUserId)
                }

                // Snapshot creation retires the old source remotely before local
                // atomic publication. Reattach through its reserved replacement
                // to resume its owned recovery stage after reopening the file.
                val connectSourceId = if (state.isAttached && state.operation.isSourceRecoveryRequired() &&
                    state.operation.stagedSnapshotId.isNotBlank()
                ) state.operation.replacementSourceId else sourceId
                val response = operations.withTransientRetry("attach() transport") {
                    remoteApi.connect(connectSourceId, session.pendingLocalChangeCount() > 0L)
                }
                if (response.resolution == "retry_later") {
                    session.sessionConnected = false
                    return@withOperationProgress AttachResult.RetryLater(response.retryAfterSeconds.toLong())
                }

                val resumingAttachedState = state.isAttached &&
                    state.attachedUserId == requestedUserId &&
                    state.operation.kind != operationKindRemoteReplace
                if (resumingAttachedState) {
                    val resumesRetainedInitialization = response.resolution == "initialize_local" &&
                        state.pendingInitializationId.isNotEmpty() &&
                        response.initializationId == state.pendingInitializationId
                    require(response.resolution == "remote_authoritative" || resumesRetainedInitialization) {
                        "resumed attached state requires remote_authoritative resolution or its retained initialization lease"
                    }
                    session.currentUserId = requestedUserId
                    session.pendingInitializationId = state.pendingInitializationId
                    session.sessionConnected = true
                    return@withOperationProgress AttachResult.Connected(
                        outcome = AttachOutcome.RESUMED_ATTACHED_STATE,
                        status = session.syncStatus(session.requireConnectedRuntimeState("attach(userId)")),
                    )
                }

                if (state.operation.kind == operationKindRemoteReplace) {
                    require(response.resolution == "remote_authoritative") {
                        "pending remote replacement requires remote_authoritative connect resolution"
                    }
                    val downloadResult = finalizeRemoteReplace(validated, state, OversqliteOperation.ATTACH)
                    notifyUpdatedTables(downloadResult.updatedTables)
                    session.currentUserId = requestedUserId
                    session.sessionConnected = true
                    session.pendingInitializationId = ""
                    return@withOperationProgress AttachResult.Connected(
                        outcome = AttachOutcome.USED_REMOTE_STATE,
                        status = session.syncStatus(session.requireConnectedRuntimeState("attach(userId)")),
                        restore = downloadResult.restore,
                    )
                }

                when (response.resolution) {
                    "initialize_empty" -> {
                        persistConnectedLifecycleState(requestedUserId, "")
                        session.currentUserId = requestedUserId
                        session.pendingInitializationId = ""
                        session.sessionConnected = true
                        AttachResult.Connected(
                            outcome = AttachOutcome.STARTED_EMPTY,
                            status = session.syncStatus(session.requireConnectedRuntimeState("attach(userId)")),
                        )
                    }

                    "initialize_local" -> {
                        persistConnectedLifecycleState(requestedUserId, response.initializationId)
                        session.currentUserId = requestedUserId
                        session.pendingInitializationId = response.initializationId
                        session.sessionConnected = true
                        AttachResult.Connected(
                            outcome = AttachOutcome.SEEDED_FROM_LOCAL,
                            status = session.syncStatus(session.requireConnectedRuntimeState("attach(userId)")),
                        )
                    }

                    "remote_authoritative" -> {
                        beginRemoteReplace(requestedUserId, sourceId)
                        val result = finalizeRemoteReplace(
                            validated,
                            session.loadDurableLifecycleView(),
                            OversqliteOperation.ATTACH,
                        )
                        notifyUpdatedTables(result.updatedTables)
                        session.currentUserId = requestedUserId
                        session.pendingInitializationId = ""
                        session.sessionConnected = true
                        AttachResult.Connected(
                            outcome = AttachOutcome.USED_REMOTE_STATE,
                            status = session.syncStatus(session.requireConnectedRuntimeState("attach(userId)")),
                            restore = result.restore,
                        )
                    }

                    else -> error("unexpected connect resolution ${response.resolution}")
                }
            }
        }
    }

    internal suspend fun detach(): Result<DetachOutcome> = operations.runExclusiveOperation {
        val execution = executeDetach("detach()")
        notifyUpdatedTables(execution.updatedTables)
        execution.outcome
    }

    internal suspend fun executeDetach(operationName: String): DetachExecution {
        session.ensureOpened(operationName)
        val state = session.loadDurableLifecycleView()
        if (state.operation.kind == operationKindRemoteReplace) {
            cancelPendingRemoteReplace(state)
            session.currentSourceId = session.resolveSourceIdOrCurrent(state.sourceId)
            session.currentUserId = null
            session.pendingInitializationId = ""
            session.sessionConnected = false
            return DetachExecution(
                outcome = DetachOutcome.DETACHED,
                updatedTables = emptySet(),
                currentSourceId = session.currentSourceId,
            )
        }

        val validated = session.requireValidatedConfig()
        val managedTables = managedTableNames(validated).toSet()
        val previousSourceId = session.resolveSourceIdOrCurrent(state.sourceId)
        val rotatedSourceId = session.generateFreshReplacementSourceId(previousSourceId)
        beforeDetachWriteReservationForTest?.invoke()
        if(!session.pendingSyncStatus().blocksDetach) stageStore.clearPushPlanForLifecycle()
        val detached = db.transaction(TransactionMode.IMMEDIATE) {
            if (session.pendingSyncStatus().blocksDetach) {
                false
            } else {
                db.withStatementCache { statementCache ->
                    clearFullLocalSyncState(validated, statementCache)
                    sourceStateStore.ensureSource(rotatedSourceId, 1L, statementCache)
                    sourceStateStore.markRotated(previousSourceId, rotatedSourceId, statementCache)
                    attachmentStateStore.persistAnonymousState(rotatedSourceId, statementCache, epochAlreadyAdvanced = true)
                    operationStateStore.persistState(OversqliteOperationState(), statementCache)
                }
                true
            }
        }
        if (!detached) {
            return DetachExecution(DetachOutcome.BLOCKED_UNSYNCED_DATA, emptySet())
        }
        session.currentSourceId = rotatedSourceId
        session.currentUserId = null
        session.pendingInitializationId = ""
        session.sessionConnected = false
        return DetachExecution(DetachOutcome.DETACHED, managedTables, rotatedSourceId)
    }

    internal suspend fun previewGenerationForUpgrade(sourceId: String, generationId: Long): SyncGeneration {
        val target = remoteApi.fetchGeneration(sourceId, generationId)
        return target.generation
    }

    internal suspend fun admitAttachedGenerationOpening(userId: String) {
        session.ensureOpened("generation opening admission")
        contractGateway.verifyConnectLifecycleSupported()
        val state = session.loadDurableLifecycleView()
        require(state.isAttached && state.attachedUserId == userId) {
            "generation opening admission requires the durable attached user"
        }
        val sourceId = session.resolveSourceIdOrCurrent(state.sourceId)
        val response = operations.withTransientRetry("generation opening connect") {
            remoteApi.connect(sourceId, session.pendingLocalChangeCount() > 0L)
        }
        require(response.resolution == "remote_authoritative") {
            "generation opening requires remote_authoritative connect resolution"
        }
        session.currentUserId = userId
        session.pendingInitializationId = state.pendingInitializationId
        session.sessionConnected = true
    }

    internal suspend fun prepareGenerationReplacement(
        oldSourceId: String,
        newSourceId: String,
        userId: String,
        precondition: suspend () -> Unit,
        finalizer: suspend () -> Unit,
    ) {
        val validated = session.requireValidatedConfig()
        stageStore.clearPushPlanForLifecycle()
        db.transaction(TransactionMode.IMMEDIATE) {
            val statementCache = StatementCache(db)
            try {
                precondition()
                applyStateStore.setApplyMode(true, statementCache)
                clearFullLocalSyncState(validated, statementCache)
                db.execSQL("DELETE FROM _sync_conflict_state")
                db.execSQL("INSERT INTO _sync_conflict_state(singleton_key) VALUES(1)")
                sourceStateStore.ensureSource(oldSourceId, statementCache = statementCache)
                sourceStateStore.ensureSource(newSourceId, statementCache = statementCache)
                attachmentStateStore.persistAttachedState(
                    sourceId = oldSourceId,
                    userId = userId,
                    schemaName = validated.generation!!.tables.first().schema,
                    lastBundleSeqSeen = 0,
                    rebuildRequired = true,
                    pendingInitializationId = "",
                    statementCache = statementCache,
                    epochAlreadyAdvanced = true,
                )
                operationStateStore.persistState(OversqliteOperationState(), statementCache)
                applyStateStore.setApplyMode(false, statementCache)
                check(!applyStateStore.isApplyMode()) {
                    "generation replacement apply bypass remained enabled"
                }
                finalizer()
            } finally {
                statementCache.close()
            }
        }
    }

    internal suspend fun createGenerationReplacementSnapshot(
        oldSourceId: String,
        newSourceId: String,
        reason: String,
    ): SnapshotSession {
        return remoteApi.createSnapshotSession(
            oldSourceId,
            SnapshotSessionCreateRequest(SnapshotSourceReplacement(oldSourceId, newSourceId, reason)),
        )
    }

    internal suspend fun createGenerationRecoverySnapshot(newSourceId: String): SnapshotSession {
        return remoteApi.createSnapshotSession(newSourceId, SnapshotSessionCreateRequest())
    }

    internal suspend fun stageGenerationSnapshot(
        snapshotSession: SnapshotSession,
        newSourceId: String,
        userId: String,
        initialTotals: SnapshotTransferTotals = SnapshotTransferTotals(),
        onChunkStaged: suspend (SnapshotTransferTotals) -> Unit,
    ): SnapshotTransferTotals {
        val contract = session.requireAdmittedContract()
        val limits = negotiateSnapshotLimits(contract, config)
        return downloadWorkflow.downloadSnapshotSession(
            state = RuntimeState(session.requireValidatedConfig(), contract, userId, newSourceId),
            session = snapshotSession,
            limits = limits,
            initialTotals = initialTotals,
            onChunkStaged = onChunkStaged,
        )
    }

    internal suspend fun applyGenerationSnapshot(
        snapshotSession: SnapshotSession,
        oldSourceId: String,
        newSourceId: String,
        userId: String,
        finalizer: suspend () -> Unit,
    ) {
        downloadWorkflow.applyStagedSnapshot(
            state = RuntimeState(
                session.requireValidatedConfig(), session.requireAdmittedContract(), userId, oldSourceId,
            ),
            session = snapshotSession,
            rotatedSourceId = newSourceId,
            outboxMode = SnapshotRebuildOutboxMode.CLEAR_ALL,
            finalizer = finalizer,
        )
        session.currentSourceId = newSourceId
        session.currentUserId = userId
        session.sessionConnected = true
    }

    internal suspend fun deleteGenerationSnapshotBestEffort(snapshotId: String, sourceId: String) {
        remoteApi.deleteSnapshotSessionBestEffort(snapshotId, sourceId)
    }

    private suspend fun beginRemoteReplace(userId: String, sourceId: String) {
        db.transaction(TransactionMode.IMMEDIATE) {
            val outboxState = outboxStateStore.loadBundleState()
            val pendingOutboundCount = outboxStateStore.countRows()
            if (outboxState.state != outboxStateNone || pendingOutboundCount != 0L) {
                throw PendingPushReplayException(pendingOutboundCount)
            }
            attachmentStateStore.persistAnonymousState(sourceId)
            operationStateStore.persistState(
                OversqliteOperationState(kind = operationKindRemoteReplace, targetUserId = userId),
            )
            syncStateStore.clearDirtyRows()
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
        val snapshotSession = SnapshotSession(
            snapshotId = refreshedState.operation.stagedSnapshotId,
            snapshotBundleSeq = refreshedState.operation.snapshotBundleSeq,
            rowCount = refreshedState.operation.snapshotRowCount,
            logicalByteCount = refreshedState.operation.snapshotByteCount,
            expiresAt = "",
        )
        operations.setProgress(operation, OversqlitePhase.APPLYING_REMOTE_STATE)
        val guard = snapshotGate.pin(
            mode = SnapshotRebuildOutboxMode.CLEAR_ALL,
            remoteReplace = true,
            remoteTargetUserId = refreshedState.operation.targetUserId,
        )
        val result = downloadWorkflow.applyStagedSnapshot(
            state = RuntimeState(
                validated,
                session.requireAdmittedContract(),
                refreshedState.operation.targetUserId,
                refreshedState.sourceId,
            ),
            session = snapshotSession,
            rotatedSourceId = null,
            outboxMode = SnapshotRebuildOutboxMode.CLEAR_ALL,
            pinnedGuard = guard,
        )
        session.applyDownloadResult(result)
        return result
    }

    private suspend fun ensureRemoteReplaceSnapshotStaged(
        state: DurableLifecycleView,
        operation: OversqliteOperation,
    ): DurableLifecycleView {
        val diagnostics = requireNotNull(snapshotDiagnosticsState.current()) {
            "snapshot diagnostics scope is missing for remote replacement"
        }
        diagnostics.markRestoreAttempt()
        operations.setProgress(operation, OversqlitePhase.STAGING_REMOTE_STATE)
        val source = session.resolveSourceIdOrCurrent(state.sourceId)
        val (snapshot, _) = downloadWorkflow.ensureOperationSnapshotStaged(
            RuntimeState(session.requireValidatedConfig(), session.requireAdmittedContract(),
                state.operation.targetUserId, source), SnapshotRebuildOutboxMode.CLEAR_ALL,
            remoteReplace = true)
        diagnostics.recordStagedRows(snapshot.rowCount)
        return state.copy(operation = operationStateStore.loadState())
    }

    private suspend fun persistConnectedLifecycleState(userId: String, initializationId: String) {
        val existing = attachmentStateStore.loadState()
        attachmentStateStore.persistAttachedState(
            sourceId = session.resolveSourceIdOrCurrent(existing.currentSourceId),
            userId = userId,
            schemaName = unconfirmedLocalSchema,
            lastBundleSeqSeen = existing.lastBundleSeqSeen,
            rebuildRequired = existing.rebuildRequired,
            pendingInitializationId = initializationId,
        )
        operationStateStore.persistState(OversqliteOperationState())
    }

    private suspend fun cancelPendingRemoteReplace(state: DurableLifecycleView) {
        if (state.operation.stagedSnapshotId.isNotBlank()) {
            stageStore.deleteSnapshotStage(state.operation.stagedSnapshotId)
        } else {
            stageStore.clearAllSnapshotStages()
        }
        attachmentStateStore.persistAnonymousState(session.resolveSourceIdOrCurrent(state.sourceId))
        operationStateStore.persistState(OversqliteOperationState())
    }

    private suspend fun clearFullLocalSyncState(
        validated: ValidatedConfig,
        statementCache: StatementCache,
    ) {
        attachmentStateStore.advanceAttachmentEpoch()
        db.execSQL("PRAGMA defer_foreign_keys = ON")
        applyStateStore.setApplyMode(true, statementCache)
        try {
            clearManagedTables(validated, statementCache)
            stageStore.clearAllSnapshotStages(statementCache)
        } finally {
            applyStateStore.setApplyMode(false, statementCache)
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
            syncStateStore.clearStructuredRowState(
                validated.schemaForTable(tableName),
                tableName,
                statementCache,
            )
        }
        syncStateStore.clearDirtyRows()
        outboxStateStore.clearBundleAndRows(statementCache)
    }

    private fun managedTableNames(validated: ValidatedConfig): List<String> = validated.tables
        .sortedByDescending { validated.tableOrder[it.tableName.lowercase()] ?: Int.MIN_VALUE }
        .map { it.tableName.lowercase() }
        .filter { it.isNotBlank() }
        .distinct()

    private suspend fun sqliteTableExists(tableName: String): Boolean = db.withExclusiveAccess {
        db.prepare(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        ).use { statement ->
            statement.bindText(1, tableName)
            check(statement.step())
            statement.getLong(0) > 0L
        }
    }

    private suspend fun countSnapshotStageRows(snapshotId: String): Long = db.withExclusiveAccess {
        db.prepare(
            "SELECT COUNT(*) FROM _sync_snapshot_stage WHERE snapshot_id = ?",
        ).use { statement ->
            statement.bindText(1, snapshotId)
            check(statement.step())
            statement.getLong(0)
        }
    }
}

internal data class DetachExecution(
    val outcome: DetachOutcome,
    val updatedTables: Set<String>,
    val currentSourceId: String? = null,
)
