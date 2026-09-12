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

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject

internal data class DownloadWorkflowResult(
    val outcome: RemoteSyncOutcome,
    val updatedTables: Set<String>,
    val restore: RestoreSummary? = null,
    val rotatedSourceId: String? = null,
)

private data class StagedSnapshotPageProgress(
    val rowCount: Int,
    val lastRowOrdinal: Long?,
)

internal enum class SnapshotRebuildOutboxMode {
    CLEAR_ALL,
    PRESERVE_PREPARED,
    PRESERVE_SOURCE_RECOVERY,
}

internal class OversqliteDownloadWorkflow(
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
    private val remoteApi: OversqliteRemoteApi,
    private val sourceStateStore: OversqliteSourceStateStore,
    private val outboxStateStore: OversqliteOutboxStateStore,
    private val attachmentStateStore: OversqliteAttachmentStateStore,
    private val operationStateStore: OversqliteOperationStateStore,
    private val syncStateStore: OversqliteSyncStateStore,
    private val localStore: OversqliteLocalStore,
    private val bundleApplier: OversqliteBundleApplier,
    private val stageStore: OversqliteStageStore,
    private val applyExecutor: OversqliteApplyExecutor,
    private val snapshotGate: OversqliteSnapshotGate,
    private val snapshotDiagnostics: () -> SnapshotDiagnosticsRecorder?,
    private val json: Json,
    private val notifyUpdatedTables: (Set<String>) -> Unit = {},
    private val log: ((() -> String)) -> Unit,
) {
    suspend fun pullToStable(
        state: RuntimeState,
        onPhaseChanged: suspend (OversqlitePhase) -> Unit = {},
    ): DownloadWorkflowResult {
        requireOrdinarySyncAllowed()
        val initialDirtyCount = syncStateStore.countDirtyRows()
        val initialOutboundCount = outboxStateStore.countRows()
        log {
            "oversqlite pullToStable start pendingDirty=$initialDirtyCount pendingOutbound=$initialOutboundCount"
        }
        return try {
            onPhaseChanged(OversqlitePhase.PULLING)
            val pullResult = pullIncremental(state)
            val result = DownloadWorkflowResult(
                outcome = pullResult.outcome,
                updatedTables = pullResult.updatedTables,
            )
            log { "oversqlite pullToStable finished successfully" }
            result
        } catch (e: HistoryPrunedException) {
            markCheckpointRecoveryRequired("history_pruned")
            val result = rebuildFromSnapshot(
                state = state,
                rotatedSourceId = null,
                sourceReplacementReason = null,
                outboxMode = SnapshotRebuildOutboxMode.CLEAR_ALL,
                onPhaseChanged = onPhaseChanged,
            )
            log { "oversqlite pullToStable recovered via snapshot rebuild" }
            result
        } catch (e: CheckpointAheadException) {
            markCheckpointRecoveryRequired("checkpoint_ahead")
            val result = rebuildFromSnapshot(
                state = state,
                rotatedSourceId = null,
                sourceReplacementReason = null,
                outboxMode = SnapshotRebuildOutboxMode.CLEAR_ALL,
                onPhaseChanged = onPhaseChanged,
            )
            log { "oversqlite pullToStable recovered ahead checkpoint via snapshot rebuild" }
            result
        }
    }

    suspend fun rebuildFromSnapshot(
        state: RuntimeState,
        rotatedSourceId: String?,
        sourceReplacementReason: String?,
        outboxMode: SnapshotRebuildOutboxMode = SnapshotRebuildOutboxMode.CLEAR_ALL,
        onPhaseChanged: suspend (OversqlitePhase) -> Unit = {},
        finalizer: suspend () -> Unit = {},
    ): DownloadWorkflowResult {
        val diagnostics = requireNotNull(snapshotDiagnostics()) {
            "snapshot diagnostics scope is missing for rebuild"
        }
        diagnostics.markRestoreAttempt()
        val limits = negotiateSnapshotLimits(state.admittedContract, config)
        if (rotatedSourceId == null && outboxMode == SnapshotRebuildOutboxMode.CLEAR_ALL) {
            markCheckpointRecoveryRequired("explicit_rebuild")
        }
        requireSnapshotRebuildState(outboxMode)
        val guard = snapshotGate.pin(outboxMode)

        onPhaseChanged(OversqlitePhase.STAGING_REMOTE_STATE)
        stageStore.clearAllSnapshotStages()
        val sessionRequest = rotatedSourceId?.let { replacementSourceId ->
            val reason = sourceReplacementReason?.trim()
            check(!reason.isNullOrBlank()) {
                "rotated rebuild requires a non-blank source replacement reason"
            }
            SnapshotSessionCreateRequest(
                sourceReplacement = SnapshotSourceReplacement(
                    previousSourceId = state.sourceId,
                    newSourceId = replacementSourceId,
                    reason = reason,
                ),
            )
        }
        val session = remoteApi.createSnapshotSession(state.sourceId, sessionRequest)
        val snapshotSourceId = rotatedSourceId ?: state.sourceId
        try {
            val totals = downloadSnapshotSession(state.copy(sourceId = snapshotSourceId), session, limits)
            diagnostics.recordStagedRows(totals.rows)

            onPhaseChanged(OversqlitePhase.APPLYING_REMOTE_STATE)
            return applyStagedSnapshot(state, session, rotatedSourceId, outboxMode, guard, finalizer)
        } finally {
            remoteApi.deleteSnapshotSessionBestEffort(session.snapshotId, snapshotSourceId)
        }
    }

    internal suspend fun downloadSnapshotSession(
        state: RuntimeState,
        session: SnapshotSession,
        limits: SnapshotNegotiation,
        initialTotals: SnapshotTransferTotals = SnapshotTransferTotals(),
        fetchChunk: (suspend (afterRowOrdinal: Long) -> SnapshotChunkResponse)? = null,
        onChunkStaged: suspend (SnapshotTransferTotals) -> Unit = {},
    ): SnapshotTransferTotals {
        var totals = initialTotals
        var afterRowOrdinal = initialTotals.rows
        while (true) {
            val chunk = fetchChunk?.invoke(afterRowOrdinal) ?: fetchSnapshotChunkWithOneRowProgress(
                state, session, afterRowOrdinal, limits,
            )
            val nextTotals = SnapshotTransferTotals(
                rows = checkedAddSnapshotLong(totals.rows, chunk.rows.size.toLong()) {
                    "snapshot accumulated row count overflow"
                },
                bytes = checkedAddSnapshotLong(totals.bytes, chunk.logicalByteCount) {
                    "snapshot accumulated byte count overflow"
                },
            )
            stageStore.stageSnapshotChunk(state, session, chunk, afterRowOrdinal) {
                onChunkStaged(nextTotals)
            }
            totals = nextTotals
            if (totals.rows > session.rowCount || totals.bytes > session.logicalByteCount) {
                throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK)
            }
            if (chunk.hasMore) {
                if (totals.rows == session.rowCount || totals.bytes == session.logicalByteCount) {
                    throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK)
                }
                afterRowOrdinal = chunk.nextRowOrdinal
                continue
            }
            if (totals.rows != session.rowCount || totals.bytes != session.logicalByteCount) {
                throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK)
            }
            return totals
        }
    }

    private suspend fun pullIncremental(state: RuntimeState): IncrementalPullResult {
        requireIncrementalPullState(state)

        val initialBundleSeq = attachmentStateStore.loadState().lastBundleSeqSeen
        var afterBundleSeq = initialBundleSeq
        val maxBundles = minOf(config.transferTargets.maxPullBundles, state.admittedContract.limits.maxPullBundles)
        var targetBundleSeq = 0L
        val updatedTables = linkedSetOf<String>()

        while (true) {
            val response = remoteApi.sendPullRequest(
                afterBundleSeq, maxBundles, targetBundleSeq, state.sourceId,
                state.admittedContract.limits.maxPullDecodedBytes,
            )
            validatePullResponse(response, afterBundleSeq)
            response.bundles.forEach { bundle ->
                bundle.rows.forEach { row ->
                    require(row.schema == state.validated.schemaForTable(row.table)) {
                        "bundle row schema ${row.schema} does not match client schema ${state.validated.schemaForTable(row.table)}"
                    }
                }
            }

            if (targetBundleSeq == 0L) {
                targetBundleSeq = response.stableBundleSeq
            } else if (response.stableBundleSeq != targetBundleSeq) {
                error("pull response stable bundle seq changed from $targetBundleSeq to ${response.stableBundleSeq}")
            }

            for (bundle in response.bundles) {
                updatedTables += applyPulledBundle(state, bundle)
                afterBundleSeq = bundle.bundleSeq
            }

            if (afterBundleSeq >= response.stableBundleSeq) {
                return IncrementalPullResult(
                    updatedTables = updatedTables,
                    outcome = if (afterBundleSeq == initialBundleSeq && response.stableBundleSeq == initialBundleSeq) {
                        RemoteSyncOutcome.ALREADY_AT_TARGET
                    } else {
                        RemoteSyncOutcome.APPLIED_INCREMENTAL
                    },
                )
            }
            if (!response.hasMore && response.bundles.isEmpty()) {
                error("pull ended before reaching stable bundle seq ${response.stableBundleSeq}")
            }
            if (!response.hasMore && afterBundleSeq < response.stableBundleSeq) {
                error("pull ended early at bundle seq $afterBundleSeq before stable bundle seq ${response.stableBundleSeq}")
            }
        }
    }

    private suspend fun applyPulledBundle(
        state: RuntimeState,
        bundle: Bundle,
    ): Set<String> {
        val updatedTables = applyExecutor.inApplyModeTransaction(
            state,
            beforeApplyMode = {
                val outboundCount = outboxStateStore.countRows()
                if (outboundCount > 0 || outboxStateStore.loadBundleState().state != outboxStateNone) {
                    throw PendingPushReplayException(outboundCount)
                }
                val dirtyCount = syncStateStore.countDirtyRows()
                if (dirtyCount > 0) throw DirtyStateRejectedException(dirtyCount)
            },
        ) { statementCache ->
            val updatedTables = linkedSetOf<String>()
            bundleApplier.survivors.prepare(state, applyUnit("pull", bundle.bundleSeq, bundle.bundleHash), bundle.rows.size, inputSequence = bundle.bundleSeq) { index ->
                val row = bundle.rows[index]
                val (key, pk) = localStore.bundleRowKeyToLocalKey(state, row.table, row.key)
                val current = syncStateStore.loadStructuredRowState(state.validated.schemaForTable(row.table), row.table, key, statementCache)
                if (current.exists && current.rowVersion >= row.rowVersion) {
                    bundleApplier.survivors.capture(state, row.table, pk)
                } else bundleApplier.survivors.wire(row.table, pk, row.payload)
            }
            for (row in bundle.rows) {
                val schema = state.validated.schemaForTable(row.table)
                val (keyJson, localPk) = localStore.bundleRowKeyToLocalKey(state, row.table, row.key)
                val current = syncStateStore.loadStructuredRowState(schema, row.table, keyJson, statementCache)
                if (current.exists && current.rowVersion >= row.rowVersion) {
                    if(current.rowVersion==row.rowVersion) syncStateStore.installAuthoritativeBaseline(schema,row.table,keyJson,if(row.op=="DELETE") "deleted" else "live",row.rowVersion,(row.payload as? JsonObject)?.let { localStore.captureAuthoritativePayload(row.table,it,statementCache) },"pull",bundle.bundleSeq,statementCache)
                    continue
                }
                bundleApplier.applyAuthoritativeRow(state, row, localPk, keyJson, statementCache,"pull",bundle.bundleSeq)
                syncStateStore.deleteDirtyRow(schema, row.table, keyJson, statementCache)
                updatedTables += row.table.lowercase()
            }
            for (pass in 0 until 64) {
                val restored = bundleApplier.survivors.restore(state, statementCache)
                updatedTables += restored
                if (restored.isEmpty()) break
            }
            bundleApplier.survivors.restore(state, statementCache, verifyOnly = true)
            bundleApplier.survivors.finish()
            attachmentStateStore.markBundleSeen(bundle.bundleSeq, statementCache)
            updatedTables
        }
        notifyUpdatedTables(updatedTables)
        return updatedTables
    }

    internal suspend fun applyStagedSnapshot(
        state: RuntimeState,
        session: SnapshotSession,
        rotatedSourceId: String?,
        outboxMode: SnapshotRebuildOutboxMode,
        pinnedGuard: SnapshotApplyGuard? = null,
        finalizer: suspend () -> Unit = {},
    ): DownloadWorkflowResult {
        if (rotatedSourceId != null) {
            requireFreshRotatedSourceState(rotatedSourceId)
        }
        val guard = pinnedGuard ?: snapshotGate.pin(outboxMode)
        val result = applyExecutor.inApplyModeTransaction(state) { statementCache ->
            snapshotGate.validateFinal(guard, session)
            val lifecycle = OversqliteApplyLifecycle(db, config, localStore, bundleApplier.survivors, json)
            val preserve = outboxMode != SnapshotRebuildOutboxMode.CLEAR_ALL
            lifecycle.prepareSnapshot(state, session, preserve)
            var stagedRowCount = 0L
            var afterRowOrdinal = 0L
            var snapshotUpserts: SnapshotUpsertPlan? = null
            while (true) {
                val progress = consumeNextStagedSnapshotPage(
                    state = state,
                    session = session,
                    afterRowOrdinal = afterRowOrdinal,
                ) { rows ->
                    if (snapshotUpserts == null) {
                        require(session.rowCount == 0L || rows.isNotEmpty()) {
                            "staged snapshot is missing its first declared row"
                        }
                        snapshotUpserts = localStore.prepareSnapshotUpsertPlan(statementCache)
                        clearManagedTables(
                            state = state,
                            statementCache = statementCache,
                            clearOutbox = outboxMode == SnapshotRebuildOutboxMode.CLEAR_ALL,
                        )
                    }
                    val upserts = checkNotNull(snapshotUpserts)
                    for (row in rows) {
                        try {
                            val payload = json.parseToJsonElement(row.payload) as? JsonObject
                                ?: error(
                                    "bundle row payload must be a JSON object for " +
                                        "${row.schemaName}.${row.tableName}",
                                )
                            upserts.upsertAuthoritativeRow(row.tableName, payload)
                            syncStateStore.installAuthoritativeBaseline(
                                schemaName = row.schemaName,
                                tableName = row.tableName,
                                keyJson = row.keyJson,
                                rowVersion = row.rowVersion,
                                kind = "live", payload = localStore.captureAuthoritativePayload(row.tableName,payload,statementCache),
                                origin = "snapshot", bundleSeq = session.snapshotBundleSeq, statementCache = statementCache,
                            )
                            applyExecutor.afterAppliedRowForTest()
                        } catch (error: Throwable) {
                            if (error is kotlinx.coroutines.CancellationException || error is SyncWorkspaceLimitException ||
                                error is SyncWorkLimitException || error is FKReconciliationRequiredException || error is ResolutionBaseUnavailableException) throw error
                            throw SnapshotRowApplyException(
                                rowOrdinal = row.rowOrdinal,
                                schemaName = row.schemaName,
                                tableName = row.tableName,
                            )
                        }
                    }
                }
                stagedRowCount = checkedAddSnapshotLong(stagedRowCount, progress.rowCount.toLong()) {
                    "staged snapshot applied row count overflow"
                }
                val lastRowOrdinal = progress.lastRowOrdinal ?: break
                afterRowOrdinal = lastRowOrdinal
            }

            if (stagedRowCount != session.rowCount) {
                throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK)
            }
            snapshotUpserts?.finish()

            val preservedOutbox = outboxStateStore.loadBundleState()
            if (rotatedSourceId != null) {
                if (state.sourceId != rotatedSourceId) {
                    sourceStateStore.markRotated(state.sourceId, rotatedSourceId)
                }
            }
            val currentSourceId = rotatedSourceId ?: state.sourceId
            when (outboxMode) {
                SnapshotRebuildOutboxMode.CLEAR_ALL -> Unit

                SnapshotRebuildOutboxMode.PRESERVE_PREPARED -> {
                    if (preservedOutbox.state == outboxStatePrepared) {
                        db.withPreparedStatement("""INSERT INTO _sync_row_state(schema_name,table_name,key_json,row_version,server_kind,server_payload,server_provenance,relationships_known)
 SELECT o.schema_name,o.table_name,o.key_json,0,'absent',NULL,${baselineProvenanceSql("'snapshot'","0","?")},1 FROM _sync_outbox_rows o WHERE NOT EXISTS(SELECT 1 FROM _sync_row_state a WHERE a.schema_name=o.schema_name AND a.table_name=o.table_name AND a.key_json=o.key_json)""",statementCache) { st -> st.bindLong(1,session.snapshotBundleSeq); st.step() }

                        replayPreparedOutboxIntent(
                            state = state,
                            sourceBundleId = preservedOutbox.sourceBundleId,
                            expectedRowCount = preservedOutbox.rowCount,
                            statementCache = statementCache,
                        )
                    }
                }

                SnapshotRebuildOutboxMode.PRESERVE_SOURCE_RECOVERY -> {
                    if (preservedOutbox.state == outboxStatePrepared) {
                        db.withPreparedStatement("""INSERT INTO _sync_row_state(schema_name,table_name,key_json,row_version,server_kind,server_payload,server_provenance,relationships_known)
 SELECT o.schema_name,o.table_name,o.key_json,0,'absent',NULL,${baselineProvenanceSql("'snapshot'","0","?")},1 FROM _sync_outbox_rows o WHERE NOT EXISTS(SELECT 1 FROM _sync_row_state a WHERE a.schema_name=o.schema_name AND a.table_name=o.table_name AND a.key_json=o.key_json)""",statementCache) { st -> st.bindLong(1,session.snapshotBundleSeq); st.step() }

                        replayPreparedOutboxIntent(
                            state = state,
                            sourceBundleId = preservedOutbox.sourceBundleId,
                            expectedRowCount = preservedOutbox.rowCount,
                            statementCache = statementCache,
                        )
                        sourceStateStore.upsertNextSourceBundleIdFloor(currentSourceId, 2L)
                        outboxStateStore.rebindBundleSource(
                            sourceId = currentSourceId,
                            sourceBundleId = 1L,
                            statementCache = statementCache,
                        )
                    }
                }
            }
            lifecycle.restoreAndVerify(state, statementCache)
            lifecycle.verifySnapshot(state, session, preserve, statementCache)
            bundleApplier.survivors.finish()
            attachmentStateStore.persistAttachedState(
                sourceId = currentSourceId,
                userId = state.userId,
                schemaName = unconfirmedLocalSchema,
                lastBundleSeqSeen = session.snapshotBundleSeq,
                rebuildRequired = false,
                pendingInitializationId = "",
                statementCache = statementCache,
            )
            operationStateStore.persistState(OversqliteOperationState(), statementCache)

            finalizer()

            stageStore.deleteSnapshotStage(session.snapshotId, statementCache)
            DownloadWorkflowResult(
                outcome = RemoteSyncOutcome.APPLIED_SNAPSHOT,
                updatedTables = state.validated.tables.map { it.tableName.lowercase() }.toSet(),
                restore = RestoreSummary(
                    bundleSeq = session.snapshotBundleSeq,
                    rowCount = session.rowCount,
                ),
                rotatedSourceId = rotatedSourceId,
            )
        }
        snapshotDiagnostics()?.recordAppliedRows(session.rowCount)
        notifyUpdatedTables(result.updatedTables)
        return result
    }

    private suspend fun replayPreparedOutboxIntent(
        state: RuntimeState,
        sourceBundleId: Long,
        expectedRowCount: Long,
        statementCache: StatementCache,
    ) {
        val sourceRowCount = outboxStateStore.countRowsForSourceBundle(sourceBundleId)
        val totalRowCount = outboxStateStore.countRows()
        val counts = OversqliteCountEvaluation(
            dirtyRowCount = 0L,
            outboundRowCount = totalRowCount,
            sourceBundleRowCount = sourceRowCount,
            expectedOutboxRowCount = expectedRowCount,
        )
        require(counts.preparedOutboxMatches) {
            "prepared outbox row count $sourceRowCount does not match expected row_count $expectedRowCount"
        }
        var replayedRowCount = 0L
        var afterRowOrdinal = -1L
        while (true) {
            val row = outboxStateStore.loadReplayRowAfter(
                sourceBundleId = sourceBundleId,
                afterRowOrdinal = afterRowOrdinal,
                statementCache = statementCache,
            ) ?: break
            when (row.op) {
                "DELETE" -> {
                    val (localPk, _) = localStore.decodeDirtyKeyForPush(state, row.tableName, row.keyJson)
                    localStore.deleteLocalRow(state, row.tableName, localPk, statementCache)
                }
                "INSERT", "UPDATE" -> {
                    val payload = row.localPayload?.let(json::parseToJsonElement) as? JsonObject
                        ?: throw IllegalStateException("prepared outbox row is missing a valid local payload")
                    localStore.upsertRow(
                        tableName = row.tableName,
                        payload = payload,
                        payloadSource = PayloadSource.LOCAL_STATE,
                        statementCache = statementCache,
                    )
                }
                else -> throw IllegalStateException("prepared outbox row has an unsupported operation")
            }
            replayedRowCount = checkedAddSnapshotLong(replayedRowCount, 1L) {
                "prepared outbox replay row count overflow"
            }
            afterRowOrdinal = row.rowOrdinal
        }
        require(replayedRowCount == expectedRowCount) {
            "prepared outbox replayed row count $replayedRowCount does not match expected row_count $expectedRowCount"
        }
    }

    private suspend fun consumeNextStagedSnapshotPage(
        state: RuntimeState,
        session: SnapshotSession,
        afterRowOrdinal: Long,
        consume: suspend (List<StagedSnapshotRow>) -> Unit,
    ): StagedSnapshotPageProgress {
        val page = stageStore.loadStagedSnapshotPage(
            state = state,
            snapshotId = session.snapshotId,
            afterRowOrdinal = afterRowOrdinal,
            maxRows = config.snapshotApplyBatchRows,
            maxBytes = config.snapshotApplyBatchBytes,
        )
        if (page.rows.isNotEmpty()) {
            snapshotDiagnostics()?.recordApplyPage(page.rows.size, page.retainedTextBytes)
            applyExecutor.afterApplyPageLoadedForTest()
        }
        consume(page.rows)
        return StagedSnapshotPageProgress(
            rowCount = page.rows.size,
            lastRowOrdinal = page.lastRowOrdinal,
        )
    }

    private suspend fun clearManagedTables(
        state: RuntimeState,
        statementCache: StatementCache? = null,
        clearOutbox: Boolean = true,
    ) {
        val reverseDependencyOrder = state.validated.tables.sortedByDescending {
            state.validated.tableOrder[it.tableName.lowercase()] ?: Int.MIN_VALUE
        }
        for (table in reverseDependencyOrder) {
            db.execSQL("DELETE FROM ${quoteIdent(table.tableName)}")
            syncStateStore.clearStructuredRowState(state.validated.schemaForTable(table.tableName), table.tableName, statementCache)
        }
        syncStateStore.clearDirtyRows()
        if (clearOutbox) {
            outboxStateStore.clearBundleAndRows(statementCache)
        }
    }

    private suspend fun requireIncrementalPullState(state: RuntimeState) {
        requireSnapshotRebuildState(SnapshotRebuildOutboxMode.CLEAR_ALL)
        requireOrdinarySyncAllowed()
    }

    private suspend fun requireSnapshotRebuildState(outboxMode: SnapshotRebuildOutboxMode) {
        val outboxState = outboxStateStore.loadBundleState()
        val pendingOutboundCount = outboxStateStore.countRows()
        when (outboxMode) {
            SnapshotRebuildOutboxMode.CLEAR_ALL -> {
                if (pendingOutboundCount > 0) {
                    throw PendingPushReplayException(pendingOutboundCount)
                }
            }

            SnapshotRebuildOutboxMode.PRESERVE_PREPARED -> {
                if (pendingOutboundCount > 0 && outboxState.state != outboxStatePrepared) {
                    throw PendingPushReplayException(pendingOutboundCount)
                }
            }

            SnapshotRebuildOutboxMode.PRESERVE_SOURCE_RECOVERY -> {
                if (pendingOutboundCount > 0 && outboxState.state == outboxStateNone) {
                    throw PendingPushReplayException(pendingOutboundCount)
                }
            }
        }

        val pendingCount = syncStateStore.countDirtyRows()
        if (pendingCount > 0) {
            throw DirtyStateRejectedException(pendingCount)
        }
    }

    private suspend fun requireOrdinarySyncAllowed() {
        val operation = operationStateStore.loadState()
        if (operation.kind == operationKindSourceRecovery) {
            throw SourceRecoveryRequiredException(operation.requireSourceRecoveryReason())
        }
        if (attachmentStateStore.loadState().rebuildRequired) {
            throw RebuildRequiredException()
        }
    }

    private suspend fun markCheckpointRecoveryRequired(reason: String) {
        db.transaction(TransactionMode.IMMEDIATE) {
            attachmentStateStore.setRebuildRequired(true)
            val operation = operationStateStore.loadState()
            if (operation.kind == operationKindSourceRecovery) {
                return@transaction
            }
            check(operation.kind == operationKindNone) {
                "cannot begin checkpoint recovery while ${operation.kind} is in progress"
            }
            operationStateStore.persistState(
                operation.copy(
                    reason = operation.reason.takeIf(::isCheckpointRecoveryReason) ?: reason,
                ),
            )
        }
    }

    private suspend fun fetchSnapshotChunkWithOneRowProgress(
        state: RuntimeState,
        session: SnapshotSession,
        afterRowOrdinal: Long,
        limits: SnapshotNegotiation,
    ): SnapshotChunkResponse = try {
        remoteApi.fetchSnapshotChunk(
            snapshotId = session.snapshotId,
            sourceId = state.sourceId,
            snapshotBundleSeq = session.snapshotBundleSeq,
            afterRowOrdinal = afterRowOrdinal,
            maxRows = limits.maxRows,
            maxBytes = limits.maxBytes,
            admittedMaxBytes = state.admittedContract.limits.maxSnapshotDecodedBytesPerChunk,
        )
    } catch (tooSmall: SnapshotChunkTooSmallException) {
        val hardLimit = state.admittedContract.limits.maxSnapshotDecodedBytesPerChunk
        if (tooSmall.requiredBytes > hardLimit) {
            throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK)
        }
        try {
            remoteApi.fetchSnapshotChunk(
                snapshotId = session.snapshotId,
                sourceId = state.sourceId,
                snapshotBundleSeq = session.snapshotBundleSeq,
                afterRowOrdinal = afterRowOrdinal,
                maxRows = 1,
                maxBytes = tooSmall.requiredBytes,
                admittedMaxBytes = hardLimit,
            )
        } catch (_: SnapshotChunkTooSmallException) {
            throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK)
        }
    }

    private suspend fun requireFreshRotatedSourceState(rotatedSourceId: String) {
        val sourceState = sourceStateStore.loadState(rotatedSourceId)
            ?: error("_sync_source_state is missing for the rotated source")
        require(sourceState.nextSourceBundleId == 1L && sourceState.replacedBySourceId.isBlank()) {
            "rotated rebuild requires a fresh replacement source"
        }
    }
}

internal fun isCheckpointRecoveryReason(reason: String): Boolean =
    reason in setOf("history_pruned", "checkpoint_ahead", "explicit_rebuild", "resume")

internal data class IncrementalPullResult(
    val updatedTables: Set<String>,
    val outcome: RemoteSyncOutcome,
)
