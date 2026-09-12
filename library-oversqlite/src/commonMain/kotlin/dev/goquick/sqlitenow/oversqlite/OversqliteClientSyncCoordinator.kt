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

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import dev.goquick.sqlitenow.core.TransactionMode
import androidx.sqlite.async.step

private const val MAX_INTERACTIVE_PUSH_ATTEMPTS = 64

private class InteractiveSyncInvocation {
    var pushAttempts: Int = 0
    val confirmedBundles = linkedSetOf<Pair<String, Long>>()
}

internal class OversqliteClientSyncCoordinator(
    private val operations: OversqliteClientOperationController,
    private val session: OversqliteClientSessionState,
    private val contractGateway: OversqliteContractGateway,
    private val lifecycle: OversqliteClientLifecycleCoordinator,
    private val pushWorkflow: OversqlitePushWorkflow,
    private val downloadWorkflow: OversqliteDownloadWorkflow,
    private val attachmentStateStore: OversqliteAttachmentStateStore,
    private val operationStateStore: OversqliteOperationStateStore,
    private val syncStateStore: OversqliteSyncStateStore,
    private val outboxStateStore: OversqliteOutboxStateStore,
    private val conflictStateStore: OversqliteConflictStateStore,
    private val notifyUpdatedTables: (Set<String>) -> Unit,
) {
    private companion object {
        const val MAX_SYNC_THEN_DETACH_ROUNDS = 3
    }

    internal suspend fun syncStatus(): Result<SyncStatus> = operations.runSyncOperation("syncStatus()") {
        session.ensureNoDestructiveTransition()
        session.syncStatus(session.requireConnectedRuntimeState("syncStatus()"))
    }

    internal suspend fun pushPending(): Result<PushReport> = operations.runSyncOperation("pushPending()") {
        contractGateway.requireAdmittedRuntime("pushPending()")
        operations.withOperationProgress(OversqliteOperation.PUSH_PENDING) {
            val execution = executePush("pushPending()", OversqliteOperation.PUSH_PENDING,invocation=InteractiveSyncInvocation())
            execution.report
        }
    }

    internal suspend fun pendingConflict(): Result<PendingConflict?> =
        operations.runSyncOperation("pendingConflict()") {
            session.requireValidatedConfig()
            pushWorkflow.pendingConflict()
        }

    internal suspend fun retryConflict(selected: PendingConflict): Result<Unit> =
        operations.runSyncOperation("retryConflict()") {
            session.requireValidatedConfig()
            pushWorkflow.retryConflict(selected)
        }

    internal suspend fun resolveConflict(
        selected: PendingConflict,
        result: MergeResult,
    ): Result<Unit> = operations.runSyncOperation("resolveConflict()") {
        val updated = pushWorkflow.resolveConflict(
            session.requireConnectedRuntimeState("resolveConflict()"),
            selected,
            result,
        )
        notifyUpdatedTables(updated)
    }

    internal suspend fun pullToStable(): Result<RemoteSyncReport> =
        operations.runSyncOperation("pullToStable()") {
            contractGateway.requireAdmittedRuntime("pullToStable()")
            operations.withOperationProgress(OversqliteOperation.PULL_TO_STABLE) {
                val execution = executePullToStable(
                    "pullToStable()",
                    OversqliteOperation.PULL_TO_STABLE,
                )
                execution.report
            }
        }

    internal suspend fun sync(): Result<SyncReport> {
        val invocation = InteractiveSyncInvocation()
        return operations.runSyncOperation("sync()") {
            contractGateway.requireAdmittedRuntime("sync()")
            operations.withOperationProgress(OversqliteOperation.SYNC) {
                executeSyncRound("sync()", OversqliteOperation.SYNC, invocation).report
            }
        }
    }

    internal suspend fun syncThenDetach(): Result<SyncThenDetachResult> {
        val invocation = InteractiveSyncInvocation()
        return operations.runSyncOperation("syncThenDetach()") {
            contractGateway.requireAdmittedRuntime("syncThenDetach()")
            operations.withOperationProgress(OversqliteOperation.SYNC) {
                var lastReport: SyncReport? = null
                repeat(MAX_SYNC_THEN_DETACH_ROUNDS) { roundIndex ->
                    currentCoroutineContext().ensureActive()
                    val before = pendingWorkIdentity()
                    val syncExecution = try {
                        executeSyncRound("syncThenDetach()", OversqliteOperation.SYNC, invocation)
                    } catch (incomplete: SyncIncompleteException) {
                        return@withOperationProgress blockedDetachResult(
                            lastReport, roundIndex + 1,
                            when (incomplete.reason) {
                                SyncIncompleteReason.PUSH_ATTEMPT_LIMIT -> SyncThenDetachBlockedReason.PUSH_ATTEMPT_LIMIT
                                SyncIncompleteReason.NO_PROGRESS -> SyncThenDetachBlockedReason.NO_PROGRESS
                            },
                        )
                    }
                    lastReport = syncExecution.report
                    currentCoroutineContext().ensureActive()
                    val detachExecution = lifecycle.executeDetach("syncThenDetach()")
                    notifyUpdatedTables(detachExecution.updatedTables)
                    if (detachExecution.outcome == DetachOutcome.DETACHED) {
                        return@withOperationProgress SyncThenDetachResult(
                            lastSync = syncExecution.report,
                            detach = DetachOutcome.DETACHED,
                            syncRounds = roundIndex + 1,
                            remainingPendingRowCount = 0L,
                        )
                    }

                    if (before == pendingWorkIdentity()) {
                        return@withOperationProgress blockedDetachResult(
                            lastReport, roundIndex + 1, SyncThenDetachBlockedReason.NO_PROGRESS,
                        )
                    }
                }
                blockedDetachResult(lastReport, MAX_SYNC_THEN_DETACH_ROUNDS, SyncThenDetachBlockedReason.SYNC_ROUND_LIMIT)
            }
        }
    }

    private suspend fun blockedDetachResult(
        lastReport: SyncReport?,
        rounds: Int,
        reason: SyncThenDetachBlockedReason,
    ): SyncThenDetachResult = SyncThenDetachResult(
        lastSync = lastReport,
        detach = DetachOutcome.BLOCKED_UNSYNCED_DATA,
        syncRounds = rounds,
        remainingPendingRowCount = session.pendingSyncStatus().pendingRowCount,
        blockedReason = reason,
    )

    internal suspend fun rebuild(): Result<RemoteSyncReport> = executeRebuildOperation("rebuild()")

    internal suspend fun rebuildForOpening(
        finalizer: suspend (String) -> Unit,
    ): Result<RemoteSyncReport> = executeRebuildOperation("coordinated old-source recovery") {
        finalizer(attachmentStateStore.loadState().currentSourceId)
    }

    private suspend fun executeRebuildOperation(
        operationName: String,
        finalizer: suspend () -> Unit = {},
    ): Result<RemoteSyncReport> = operations.runSyncOperation(operationName) {
        contractGateway.requireAdmittedRuntime(operationName)
        val plan = selectRebuildPlan()
        operations.withOperationProgress(plan.operation) {
            val execution = executeSnapshotRebuild(
                operationName = operationName,
                operation = plan.operation,
                rotatedSourceId = plan.rotatedSourceId,
                outboxMode = plan.outboxMode,
                sourceReplacementReason = plan.sourceReplacementReason,
                finalizer = finalizer,
            )
            execution.report
        }
    }

    private suspend fun executePush(
        operationName: String,
        operation: OversqliteOperation,
        allowCheckpointRecovery: Boolean = false,
        invocation: InteractiveSyncInvocation? = null,
    ): ClientSyncPushExecution {
        val state = session.requireConnectedRuntimeState(operationName)
        val durableOperation = operationStateStore.loadState()
        if (durableOperation.isSourceRecoveryRequired()) {
            throw SourceRecoveryRequiredException(durableOperation.requireSourceRecoveryReason())
        }
        operations.setProgress(
            operation,
            if (state.pendingInitializationId.isNotEmpty()) {
                OversqlitePhase.SEEDING
            } else {
                OversqlitePhase.PUSHING
            },
        )
        val workflowResult = try {
            pushWorkflow.pushPending(state, allowCheckpointRecovery) {
                if (invocation != null) {
                    currentCoroutineContext().ensureActive()
                    if (invocation.pushAttempts == MAX_INTERACTIVE_PUSH_ATTEMPTS) {
                        throw incompleteSync(invocation, SyncIncompleteReason.PUSH_ATTEMPT_LIMIT)
                    }
                    invocation.pushAttempts++
                }
            }
        } catch (error: SourceRecoveryRequiredHttpException) {
            session.persistSourceRecoveryRequiredState(state, error.reason, error.replacementSourceId)
            throw SourceRecoveryRequiredException(error.reason)
        } catch (error: PushReplayHistoryPrunedException) {
            attachmentStateStore.setRebuildRequired(true)
            if (allowCheckpointRecovery) throw error
            throw RebuildRequiredException()
        } catch (error: InitializationLeaseInvalidException) {
            session.clearStaleInitializationState()
            throw error
        }
        invocation?.confirmedBundles?.addAll(workflowResult.confirmedBundles)
        pushWorkflow.completePushPlanCleanup()
        if (state.pendingInitializationId.isNotEmpty() && workflowResult.outcome == PushOutcome.COMMITTED) {
            attachmentStateStore.clearPendingInitializationId()
            session.pendingInitializationId = ""
        }
        return ClientSyncPushExecution(
            report = PushReport(
                outcome = workflowResult.outcome,
                status = session.syncStatus(session.requireConnectedRuntimeState(operationName)),
            ),
            updatedTables = workflowResult.updatedTables,
        )
    }

    private suspend fun executeSyncRound(
        operationName: String,
        operation: OversqliteOperation,
        invocation: InteractiveSyncInvocation,
    ): ClientSyncExecution {
        session.ensureNoDestructiveTransition()
        val updatedTables = linkedSetOf<String>()
        while (true) {
            currentCoroutineContext().ensureActive()
            val recovering = checkpointRecoveryRequired()
            val (dirty, outbound) = pendingCounts()
            if (dirty > 0 || outbound > 0) {
                val before = pendingWorkIdentity()
                val push = executePush(
                    operationName, operation,
                    allowCheckpointRecovery = recovering,
                    invocation = invocation,
                )
                updatedTables += push.updatedTables
                if (push.report.outcome == PushOutcome.NO_CHANGE) {
                    val (remainingDirty, remainingOutbound) = pendingCounts()
                    if ((remainingDirty > 0 || remainingOutbound > 0) && before == pendingWorkIdentity()) {
                        throw incompleteSync(invocation, SyncIncompleteReason.NO_PROGRESS)
                    }
                }
                continue
            }

            val remote = try {
                if (recovering) {
                    executeSnapshotRebuild(
                        operationName, operation, null, SnapshotRebuildOutboxMode.CLEAR_ALL, null,
                    )
                } else {
                    executePullToStable(operationName, operation)
                }
            } catch (error: Throwable) {
                if (error is DirtyStateRejectedException || error is PendingPushReplayException) {
                    continue
                }
                if (error is SnapshotFinalApplyGateException &&
                    (error.cause is DirtyStateRejectedException || error.cause is PendingPushReplayException) &&
                    checkpointRecoveryRequired()
                ) {
                    continue
                }
                throw error
            }
            updatedTables += remote.updatedTables
            val (remainingDirty, remainingOutbound) = pendingCounts()
            if (remainingDirty > 0 || remainingOutbound > 0) continue
            return ClientSyncExecution(
                report = SyncReport(
                    pushOutcome = if (invocation.confirmedBundles.isEmpty()) PushOutcome.NO_CHANGE else PushOutcome.COMMITTED,
                    remoteOutcome = remote.report.outcome,
                    status = remote.report.status,
                    restore = remote.report.restore,
                ),
                updatedTables = updatedTables,
            )
        }
    }

    private suspend fun executePullToStable(
        operationName: String,
        operation: OversqliteOperation,
    ): ClientRemoteExecution {
        if (checkpointRecoveryRequired()) {
            return executeCheckpointRecovery(operationName, operation)
        }
        return executeRemoteDownload(operationName, operation) { state, onPhaseChanged ->
            downloadWorkflow.pullToStable(state, onPhaseChanged)
        }
    }

    private suspend fun pendingCounts(): Pair<Long, Long> =
        syncStateStore.db.transaction(TransactionMode.IMMEDIATE) {
            syncStateStore.countDirtyRows() to outboxStateStore.countRows()
        }

    private suspend fun incompleteSync(
        invocation: InteractiveSyncInvocation,
        reason: SyncIncompleteReason,
    ): SyncIncompleteException {
        currentCoroutineContext().ensureActive()
        val (dirty, outbound) = pendingCounts()
        return SyncIncompleteException(
            reason, dirty, outbound, invocation.confirmedBundles.size, invocation.pushAttempts,
        )
    }

    private suspend fun pendingWorkIdentity(): String =
        syncStateStore.db.transaction(TransactionMode.IMMEDIATE) {
            val digest = Sha256Accumulator()
            fun append(value: String?) {
                if (value == null) {
                    digest.appendAscii('N')
                } else {
                    val bytes = value.encodeToByteArray()
                    digest.appendAscii('V')
                    digest.append(bytes.size.toString().encodeToByteArray())
                    digest.appendAscii(':')
                    digest.append(bytes)
                }
            }
            val queries = listOf(
                "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1" to 1,
                """SELECT state, source_id, CAST(source_bundle_id AS TEXT), canonical_request_hash,
                   CAST(row_count AS TEXT), initialization_id FROM _sync_outbox_bundle WHERE singleton_key = 1""" to 6,
                """SELECT source_id, CAST(next_source_bundle_id AS TEXT), replaced_by_source_id
                   FROM _sync_source_state WHERE source_id =
                   (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1)""" to 3,
                """SELECT schema_name, table_name, key_json, op, CAST(base_row_version AS TEXT),
                   payload, CAST(dirty_ordinal AS TEXT) FROM _sync_dirty_rows
                   ORDER BY schema_name, table_name, key_json""" to 7,
                """SELECT schema_name, table_name, key_json, op, CAST(base_row_version AS TEXT),
                   local_payload, CAST(row_ordinal AS TEXT), CAST(source_bundle_id AS TEXT)
                   FROM _sync_outbox_rows ORDER BY source_bundle_id, row_ordinal""" to 8,
            )
            queries.forEachIndexed { index, (sql, columns) ->
                append("query:$index")
                syncStateStore.db.withPreparedStatement(sql = sql) { statement ->
                    while (statement.step()) {
                        append("row")
                        repeat(columns) { column ->
                            append(if (statement.isNull(column)) null else statement.getText(column))
                        }
                    }
                }
            }
            digest.finishHex()
        }

    private suspend fun checkpointRecoveryRequired(): Boolean {
        val attachment = attachmentStateStore.loadState()
        if (!attachment.rebuildRequired) return false
        return !operationStateStore.loadState().isSourceRecoveryRequired()
    }

    private suspend fun executeCheckpointRecovery(
        operationName: String,
        operation: OversqliteOperation,
    ): ClientRemoteExecution {
        var dirtyCount = syncStateStore.countDirtyRows()
        var outboundCount = outboxStateStore.countRows()
        var replayState = outboxStateStore.loadBundleState().state
        if (dirtyCount > 0 || outboundCount > 0) {
            try {
                executePush(operationName, operation, allowCheckpointRecovery = true)
            } catch (error: SourceRecoveryRequiredException) {
                throw error
            } catch (_: PushReplayHistoryPrunedException) {
                return executeSnapshotRebuild(
                    operationName,
                    operation,
                    null,
                    SnapshotRebuildOutboxMode.PRESERVE_PREPARED,
                    null,
                )
            } catch (error: Throwable) {
                if (error is CancellationException) throw error
                if (operations.shouldRetryTransportFailure(error)) throw error
                if ((error as? OversqliteCategorizedException)?.category == OversqliteErrorCategory.AUTH) {
                    throw error
                }
                dirtyCount = syncStateStore.countDirtyRows()
                outboundCount = outboxStateStore.countRows()
                replayState = outboxStateStore.loadBundleState().state
                throw CheckpointRecoveryBlockedException(
                    CheckpointRecoveryBlockedReason.PUSH_FAILED,
                    dirtyCount,
                    outboundCount,
                    replayState,
                    error,
                )
            }
            dirtyCount = syncStateStore.countDirtyRows()
            outboundCount = outboxStateStore.countRows()
            replayState = outboxStateStore.loadBundleState().state
            if (dirtyCount > 0 || outboundCount > 0) {
                throw CheckpointRecoveryBlockedException(
                    CheckpointRecoveryBlockedReason.PENDING_WORK,
                    dirtyCount,
                    outboundCount,
                    replayState,
                )
            }
        }
        return executeSnapshotRebuild(
            operationName,
            operation,
            null,
            SnapshotRebuildOutboxMode.CLEAR_ALL,
            null,
        )
    }

    private suspend fun executeSnapshotRebuild(
        operationName: String,
        operation: OversqliteOperation,
        rotatedSourceId: String?,
        outboxMode: SnapshotRebuildOutboxMode,
        sourceReplacementReason: String?,
        finalizer: suspend () -> Unit = {},
    ): ClientRemoteExecution = executeRemoteDownload(operationName, operation) { state, onPhaseChanged ->
        downloadWorkflow.rebuildFromSnapshot(
            state,
            rotatedSourceId,
            sourceReplacementReason,
            outboxMode,
            onPhaseChanged,
            finalizer,
        )
    }

    private suspend fun executeRemoteDownload(
        operationName: String,
        operation: OversqliteOperation,
        download: suspend (RuntimeState, suspend (OversqlitePhase) -> Unit) -> DownloadWorkflowResult,
    ): ClientRemoteExecution {
        session.ensureNoDestructiveTransition()
        val state = session.requireConnectedRuntimeState(operationName)
        val result = try {
            download(state) { phase -> operations.setProgress(operation, phase) }
        } catch (error: SourceRecoveryRequiredHttpException) {
            session.persistSourceRecoveryRequiredState(state, error.reason, error.replacementSourceId)
            throw SourceRecoveryRequiredException(error.reason)
        } catch (error: SnapshotSourceRecoveryRequiredException) {
            session.persistSourceRecoveryRequiredState(state, error.reason, error.replacementSourceId)
            throw SourceRecoveryRequiredException(error.reason)
        } catch (_: SourceReplacementInvalidHttpException) {
            throw SourceReplacementInvalidException()
        }
        session.applyDownloadResult(result)
        return ClientRemoteExecution(
            report = RemoteSyncReport(
                outcome = result.outcome,
                status = session.syncStatus(session.requireConnectedRuntimeState(operationName)),
                restore = result.restore,
            ),
            updatedTables = result.updatedTables,
        )
    }

    private suspend fun selectRebuildPlan(): ClientRebuildPlan {
        conflictStateStore.load()?.let { throw ConflictNeedsResolutionException(it.pending) }
        val operation = operationStateStore.loadState()
        if (operation.isSourceRecoveryRequired()) {
            val reason = operation.reason.trim()
            check(reason.isNotBlank()) { "source recovery operation state is missing reason" }
            return ClientRebuildPlan(
                operation = OversqliteOperation.REBUILD_ROTATE_SOURCE,
                rotatedSourceId = requireValidOversqliteSourceId(operation.replacementSourceId),
                sourceReplacementReason = reason,
                outboxMode = SnapshotRebuildOutboxMode.PRESERVE_SOURCE_RECOVERY,
            )
        }
        return ClientRebuildPlan(
            operation = OversqliteOperation.REBUILD_KEEP_SOURCE,
            rotatedSourceId = null,
            sourceReplacementReason = null,
            outboxMode = if (outboxStateStore.loadBundleState().state == outboxStatePrepared) {
                SnapshotRebuildOutboxMode.PRESERVE_PREPARED
            } else {
                SnapshotRebuildOutboxMode.CLEAR_ALL
            },
        )
    }
}

private data class ClientSyncPushExecution(
    val report: PushReport,
    val updatedTables: Set<String>,
)

private data class ClientSyncExecution(
    val report: SyncReport,
    val updatedTables: Set<String>,
)

private data class ClientRebuildPlan(
    val operation: OversqliteOperation,
    val rotatedSourceId: String?,
    val sourceReplacementReason: String?,
    val outboxMode: SnapshotRebuildOutboxMode,
)

private data class ClientRemoteExecution(
    val report: RemoteSyncReport,
    val updatedTables: Set<String>,
)
