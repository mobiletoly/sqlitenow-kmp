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
import kotlinx.serialization.json.Json

internal data class DownloadWorkflowResult(
    val outcome: RemoteSyncOutcome,
    val updatedTables: Set<String>,
    val restore: RestoreSummary? = null,
    val rotatedSourceId: String? = null,
)

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
    private val json: Json,
    private val log: ((() -> String)) -> Unit,
) {
    suspend fun pullToStable(
        state: RuntimeState,
        onPhaseChanged: suspend (OversqlitePhase) -> Unit = {},
    ): DownloadWorkflowResult {
        if (attachmentStateStore.loadState().rebuildRequired) {
            throw RebuildRequiredException()
        }
        val initialDirtyCount = syncStateStore.countDirtyRows()
        val initialOutboundCount = outboxStateStore.countRows()
        log {
            "oversqlite pullToStable start user=${state.userId} source=${state.sourceId} " +
                "pendingDirty=$initialDirtyCount pendingOutbound=$initialOutboundCount"
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
            val result = rebuildFromSnapshot(state, rotatedSourceId = null, onPhaseChanged = onPhaseChanged)
            log { "oversqlite pullToStable recovered via snapshot rebuild updatedTables=${result.updatedTables}" }
            result
        }
    }

    suspend fun rebuildFromSnapshot(
        state: RuntimeState,
        rotatedSourceId: String?,
        onPhaseChanged: suspend (OversqlitePhase) -> Unit = {},
    ): DownloadWorkflowResult {
        requireSnapshotRebuildState()

        onPhaseChanged(OversqlitePhase.STAGING_REMOTE_STATE)
        stageStore.clearAllSnapshotStages()
        val session = remoteApi.createSnapshotSession()
        try {
            var afterRowOrdinal = 0L
            while (true) {
                val chunk = remoteApi.fetchSnapshotChunk(
                    snapshotId = session.snapshotId,
                    snapshotBundleSeq = session.snapshotBundleSeq,
                    afterRowOrdinal = afterRowOrdinal,
                    maxRows = snapshotChunkRows(),
                )
                stageStore.stageSnapshotChunk(state, session, chunk, afterRowOrdinal)
                if (!chunk.hasMore) {
                    break
                }
                afterRowOrdinal = chunk.nextRowOrdinal
            }

            onPhaseChanged(OversqlitePhase.APPLYING_REMOTE_STATE)
            return applyStagedSnapshot(state, session, rotatedSourceId)
        } finally {
            remoteApi.deleteSnapshotSessionBestEffort(session.snapshotId)
        }
    }

    private suspend fun pullIncremental(state: RuntimeState): IncrementalPullResult {
        requireIncrementalPullState(state)

        val initialBundleSeq = attachmentStateStore.loadState().lastBundleSeqSeen
        var afterBundleSeq = initialBundleSeq
        val maxBundles = config.downloadLimit.takeIf { it > 0 } ?: 1000
        var targetBundleSeq = 0L
        val updatedTables = linkedSetOf<String>()

        while (true) {
            val response = remoteApi.sendPullRequest(afterBundleSeq, maxBundles, targetBundleSeq)
            validatePullResponse(response, afterBundleSeq)

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
        return applyExecutor.inApplyModeTransaction(state) { statementCache ->
            val updatedTables = linkedSetOf<String>()
            for (row in bundle.rows) {
                val (keyJson, localPk) = localStore.bundleRowKeyToLocalKey(state, row.table, row.key)
                val current = syncStateStore.loadStructuredRowState(row.schema, row.table, keyJson, statementCache)
                if (current.exists && current.rowVersion >= row.rowVersion) {
                    continue
                }
                bundleApplier.applyAuthoritativeRow(state, row, localPk, keyJson, statementCache)
                syncStateStore.deleteDirtyRow(row.schema, row.table, keyJson, statementCache)
                updatedTables += row.table.lowercase()
            }

            attachmentStateStore.markBundleSeen(bundle.bundleSeq, statementCache)
            updatedTables
        }
    }

    internal suspend fun applyStagedSnapshot(
        state: RuntimeState,
        session: SnapshotSession,
        rotatedSourceId: String?,
    ): DownloadWorkflowResult {
        return applyExecutor.inApplyModeTransaction(state) { statementCache ->
            clearManagedTables(state, statementCache)

            var stagedRowCount = 0L
            for (row in stageStore.loadStagedSnapshotRows(state, session.snapshotId)) {
                val bundleRow = BundleRow(
                    schema = row.schemaName,
                    table = row.tableName,
                    key = row.wireKey,
                    op = "INSERT",
                    rowVersion = row.rowVersion,
                    payload = json.parseToJsonElement(row.payload),
                )
                bundleApplier.applyAuthoritativeRow(state, bundleRow, row.localPk, row.keyJson, statementCache)
                stagedRowCount++
            }

            require(stagedRowCount == session.rowCount) {
                "staged snapshot row count $stagedRowCount does not match expected row_count ${session.rowCount}"
            }

            if (rotatedSourceId != null) {
                sourceStateStore.ensureSource(rotatedSourceId)
                if (state.sourceId != rotatedSourceId) {
                    sourceStateStore.markRotated(state.sourceId, rotatedSourceId)
                }
            }
            val currentSourceId = rotatedSourceId ?: state.sourceId
            attachmentStateStore.persistAttachedState(
                sourceId = currentSourceId,
                userId = state.userId,
                schemaName = state.validated.schema,
                lastBundleSeqSeen = session.snapshotBundleSeq,
                rebuildRequired = false,
                pendingInitializationId = "",
                statementCache = statementCache,
            )
            operationStateStore.persistState(OversqliteOperationState(), statementCache)

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
    }

    private suspend fun clearManagedTables(
        state: RuntimeState,
        statementCache: StatementCache? = null,
    ) {
        for (table in state.validated.tables) {
            db.execSQL("DELETE FROM ${quoteIdent(table.tableName)}")
            syncStateStore.clearStructuredRowState(state.validated.schema, table.tableName, statementCache)
        }
        syncStateStore.clearDirtyRows()
        outboxStateStore.clearBundleAndRows(statementCache)
    }

    private suspend fun requireIncrementalPullState(state: RuntimeState) {
        requireSnapshotRebuildState()
        if (attachmentStateStore.loadState().rebuildRequired) {
            throw RebuildRequiredException()
        }
    }

    private suspend fun requireSnapshotRebuildState() {
        val pendingOutboundCount = outboxStateStore.countRows()
        if (pendingOutboundCount > 0) {
            throw PendingPushReplayException(pendingOutboundCount)
        }

        val pendingCount = syncStateStore.countDirtyRows()
        if (pendingCount > 0) {
            throw DirtyStateRejectedException(pendingCount)
        }
    }

    private fun snapshotChunkRows(): Int = config.snapshotChunkRows.takeIf { it > 0 } ?: 1000
}

internal data class IncrementalPullResult(
    val updatedTables: Set<String>,
    val outcome: RemoteSyncOutcome,
)
