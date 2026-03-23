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
    val updatedTables: Set<String>,
    val rotatedSourceId: String? = null,
)

internal class OversqliteDownloadWorkflow(
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
    private val remoteApi: OversqliteRemoteApi,
    private val clientStateStore: OversqliteClientStateStore,
    private val syncStateStore: OversqliteSyncStateStore,
    private val localStore: OversqliteLocalStore,
    private val bundleApplier: OversqliteBundleApplier,
    private val stageStore: OversqliteStageStore,
    private val applyExecutor: OversqliteApplyExecutor,
    private val json: Json,
    private val log: ((() -> String)) -> Unit,
) {
    suspend fun pullToStable(state: RuntimeState): DownloadWorkflowResult {
        if (clientStateStore.isRebuildRequired(state.userId)) {
            throw RebuildRequiredException()
        }
        val initialDirtyCount = clientStateStore.pendingDirtyCount()
        val initialOutboundCount = clientStateStore.pendingPushOutboundCount()
        log {
            "oversqlite pullToStable start user=${state.userId} source=${state.sourceId} " +
                "pendingDirty=$initialDirtyCount pendingOutbound=$initialOutboundCount"
        }
        return try {
            val result = DownloadWorkflowResult(updatedTables = pullIncremental(state))
            log { "oversqlite pullToStable finished successfully" }
            result
        } catch (e: HistoryPrunedException) {
            val result = rebuildFromSnapshot(state, rotateSource = false)
            log { "oversqlite pullToStable recovered via snapshot rebuild updatedTables=${result.updatedTables}" }
            result
        }
    }

    suspend fun rebuildFromSnapshot(
        state: RuntimeState,
        rotateSource: Boolean,
    ): DownloadWorkflowResult {
        requireSnapshotRebuildState()

        clientStateStore.clearSnapshotStage()
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

            return applyStagedSnapshot(state, session, rotateSource)
        } finally {
            remoteApi.deleteSnapshotSessionBestEffort(session.snapshotId)
        }
    }

    private suspend fun pullIncremental(state: RuntimeState): Set<String> {
        requireIncrementalPullState(state)

        var afterBundleSeq = clientStateStore.loadLastBundleSeqSeen(state.userId)
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
                return updatedTables
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

            clientStateStore.markBundleSeen(
                userId = state.userId,
                bundleSeq = bundle.bundleSeq,
                statementCache = statementCache,
            )
            updatedTables
        }
    }

    private suspend fun applyStagedSnapshot(
        state: RuntimeState,
        session: SnapshotSession,
        rotateSource: Boolean,
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

            val rotatedSourceId = if (rotateSource) randomSourceId() else null
            clientStateStore.markSnapshotApplied(
                userId = state.userId,
                snapshotBundleSeq = session.snapshotBundleSeq,
                newSourceId = rotatedSourceId,
                statementCache = statementCache,
            )

            stageStore.deleteSnapshotStage(session.snapshotId, statementCache)
            DownloadWorkflowResult(
                updatedTables = state.validated.tables.map { it.tableName.lowercase() }.toSet(),
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
        db.execSQL("DELETE FROM _sync_push_outbound")
        db.execSQL("DELETE FROM _sync_push_stage")
    }

    private suspend fun requireIncrementalPullState(state: RuntimeState) {
        requireSnapshotRebuildState()
        if (clientStateStore.isRebuildRequired(state.userId)) {
            throw RebuildRequiredException()
        }
    }

    private suspend fun requireSnapshotRebuildState() {
        val pendingOutboundCount = clientStateStore.pendingPushOutboundCount()
        if (pendingOutboundCount > 0) {
            throw PendingPushReplayException(pendingOutboundCount)
        }

        val pendingCount = clientStateStore.pendingDirtyCount()
        if (pendingCount > 0) {
            throw DirtyStateRejectedException(pendingCount)
        }
    }

    private fun snapshotChunkRows(): Int = config.snapshotChunkRows.takeIf { it > 0 } ?: 1000
}
