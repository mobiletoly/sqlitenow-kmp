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

import kotlinx.serialization.json.Json

internal sealed class ReplayRowAction {
    data object AcceptAuthoritative : ReplayRowAction()

    data class PreserveLocal(
        val requeueOp: String,
        val requeuePayload: String?,
    ) : ReplayRowAction()
}

internal data class ReplayPlanDiagnostics(
    val pendingExists: Boolean,
    val pendingMatches: Boolean,
    val livePayload: String?,
    val liveMatches: Boolean,
    val currentDirtyPayload: String?,
)

internal data class UploadedReplayPlan(
    val rowRef: DirtyRowRef,
    val action: ReplayRowAction,
    val diagnostics: ReplayPlanDiagnostics,
)

internal class OversqliteReplayPlanner(
    private val json: Json,
) {
    fun plan(
        tableInfo: TableInfo,
        uploaded: DirtyRowCapture,
        pending: DirtyUploadState,
        livePayload: String?,
    ): UploadedReplayPlan {
        val pendingMatches = pending.exists &&
            pending.op == uploaded.op &&
            equivalentLocalPayload(tableInfo, pending.payload, uploaded.localPayload)
        val liveMatches = livePayloadMatchesUploadedIntent(tableInfo, uploaded, livePayload)
        val diagnostics = ReplayPlanDiagnostics(
            pendingExists = pending.exists,
            pendingMatches = pendingMatches,
            livePayload = livePayload,
            liveMatches = liveMatches,
            currentDirtyPayload = pending.payload,
        )
        return UploadedReplayPlan(
            rowRef = uploaded.toRowRef(),
            action = when (uploaded.op) {
                "DELETE" -> {
                    if (livePayload != null) {
                        ReplayRowAction.PreserveLocal(
                            requeueOp = "INSERT",
                            requeuePayload = livePayload,
                        )
                    } else {
                        ReplayRowAction.AcceptAuthoritative
                    }
                }

                else -> {
                    when {
                        livePayload == null -> ReplayRowAction.PreserveLocal(
                            requeueOp = "DELETE",
                            requeuePayload = null,
                        )

                        pending.exists && !pendingMatches -> ReplayRowAction.PreserveLocal(
                            requeueOp = "UPDATE",
                            requeuePayload = livePayload,
                        )

                        !liveMatches -> ReplayRowAction.PreserveLocal(
                            requeueOp = "UPDATE",
                            requeuePayload = livePayload,
                        )

                        else -> ReplayRowAction.AcceptAuthoritative
                    }
                }
            },
            diagnostics = diagnostics,
        )
    }

    private fun livePayloadMatchesUploadedIntent(
        tableInfo: TableInfo,
        uploaded: DirtyRowCapture,
        livePayload: String?,
    ): Boolean {
        return if (uploaded.op == "DELETE") {
            livePayload == null
        } else {
            livePayload != null && equivalentLocalPayload(tableInfo, livePayload, uploaded.localPayload)
        }
    }

    private fun equivalentLocalPayload(
        tableInfo: TableInfo,
        left: String?,
        right: String?,
    ): Boolean {
        return OversqliteValueCodec.equivalentPayloadTexts(
            tableInfo = tableInfo,
            left = left,
            right = right,
            leftSource = PayloadSource.LOCAL_STATE,
            rightSource = PayloadSource.LOCAL_STATE,
            json = json,
        )
    }
}

internal class OversqlitePushReplayExecutor(
    private val syncStateStore: OversqliteSyncStateStore,
    private val bundleApplier: OversqliteBundleApplier,
    private val localStore: OversqliteLocalStore,
    private val sourceStateStore: OversqliteSourceStateStore,
    private val attachmentStateStore: OversqliteAttachmentStateStore,
    private val outboxStateStore: OversqliteOutboxStateStore,
    private val json: Json,
    private val log: ((() -> String)) -> Unit,
) {
    private val planner = OversqliteReplayPlanner(json)

    suspend fun applyCommittedBundle(
        state: RuntimeState,
        uploadedRows: List<DirtyRowCapture>,
        committedRows: List<CommittedReplayRow>,
        committed: CommittedPushBundle,
        statementCache: StatementCache,
    ): Set<String> {
        require(committedRows.size.toLong() == committed.rowCount) {
            "committed push replay row count ${committedRows.size} does not match expected row_count ${committed.rowCount}"
        }

        val plans = buildReplayPlans(state, uploadedRows, statementCache)
        val updatedTables = linkedSetOf<String>()
        for (row in committedRows) {
            val bundleRow = BundleRow(
                schema = row.schemaName,
                table = row.tableName,
                key = row.wireKey,
                op = row.op,
                rowVersion = row.rowVersion,
                payload = row.payload?.let { json.parseToJsonElement(it) },
            )
            when (val action = plans[row.toRowRef()]?.action) {
                null -> bundleApplier.applyAuthoritativeRow(state, bundleRow, row.localPk, row.keyJson, statementCache)
                ReplayRowAction.AcceptAuthoritative -> {
                    bundleApplier.applyAuthoritativeRow(state, bundleRow, row.localPk, row.keyJson, statementCache)
                    syncStateStore.deleteDirtyRow(row.schemaName, row.tableName, row.keyJson, statementCache)
                }
                is ReplayRowAction.PreserveLocal -> {
                    syncStateStore.updateStructuredRowState(
                        schemaName = state.validated.schema,
                        tableName = row.tableName,
                        keyJson = row.keyJson,
                        rowVersion = row.rowVersion,
                        deleted = row.op == "DELETE",
                        statementCache = statementCache,
                    )
                    syncStateStore.requeueDirtyIntent(
                        schemaName = row.schemaName,
                        tableName = row.tableName,
                        keyJson = row.keyJson,
                        op = action.requeueOp,
                        baseRowVersion = row.rowVersion,
                        payload = action.requeuePayload,
                        statementCache = statementCache,
                    )
                }
            }
            updatedTables += row.tableName.lowercase()
        }

        sourceStateStore.advanceAfterCommittedPush(state.sourceId, committed.sourceBundleId)
        attachmentStateStore.advanceAfterCommittedPush(committed.bundleSeq, statementCache)
        outboxStateStore.clearBundleAndRows(statementCache)
        assertReplayResidualDirtyState(uploadedRows, plans, statementCache)
        return updatedTables
    }

    private suspend fun buildReplayPlans(
        state: RuntimeState,
        uploadedRows: List<DirtyRowCapture>,
        statementCache: StatementCache,
    ): Map<DirtyRowRef, UploadedReplayPlan> {
        return buildMap {
            for (uploaded in uploadedRows) {
                val tableInfo = state.validated.tableInfoByName[uploaded.tableName.lowercase()]
                    ?: error(
                        "missing validated table info for " +
                            safeSnapshotDiagnosticIdentifier(uploaded.tableName),
                    )
                val currentDirty = syncStateStore.loadDirtyUploadState(
                    schemaName = uploaded.schemaName,
                    tableName = uploaded.tableName,
                    keyJson = uploaded.keyJson,
                    statementCache = statementCache,
                )
                val livePayload = localStore.serializeExistingRow(
                    tableName = uploaded.tableName,
                    localPk = uploaded.localPk,
                    statementCache = statementCache,
                )
                val plan = planner.plan(tableInfo, uploaded, currentDirty, livePayload)
                if (plan.action is ReplayRowAction.PreserveLocal ||
                    plan.diagnostics.pendingExists ||
                    !plan.diagnostics.liveMatches
                ) {
                    log { replayDecisionLog(uploaded, plan) }
                }
                put(plan.rowRef, plan)
            }
        }
    }

    private suspend fun assertReplayResidualDirtyState(
        uploadedRows: List<DirtyRowCapture>,
        plans: Map<DirtyRowRef, UploadedReplayPlan>,
        statementCache: StatementCache,
    ) {
        for (uploaded in uploadedRows) {
            val plan = plans[uploaded.toRowRef()]
                ?: error(replayMissingPlanMessage)
            val dirty = syncStateStore.loadDirtyUploadState(
                schemaName = uploaded.schemaName,
                tableName = uploaded.tableName,
                keyJson = uploaded.keyJson,
                statementCache = statementCache,
            )
            when (plan.action) {
                ReplayRowAction.AcceptAuthoritative -> require(!dirty.exists) {
                    replayUnexpectedDirtyMessage
                }

                is ReplayRowAction.PreserveLocal -> require(dirty.exists) {
                    replayMissingPreservedDirtyMessage
                }
            }
        }
    }
}

internal const val replayMissingPlanMessage = "missing replay plan for uploaded row"
internal const val replayUnexpectedDirtyMessage = "successful push replay left an unexpected dirty row"
internal const val replayMissingPreservedDirtyMessage = "successful push replay lost a preserved dirty row"

internal fun replayDecisionLog(
    uploaded: DirtyRowCapture,
    plan: UploadedReplayPlan,
): String {
    val diagnostics = plan.diagnostics
    return buildString {
        append("oversqlite replay decision ")
        append("table=").append(safeSnapshotDiagnosticIdentifier(uploaded.tableName))
        append(" pendingExists=").append(diagnostics.pendingExists)
        append(" pendingMatches=").append(diagnostics.pendingMatches)
        append(" liveExists=").append(diagnostics.livePayload != null)
        append(" liveMatches=").append(diagnostics.liveMatches)
        when (val action = plan.action) {
            ReplayRowAction.AcceptAuthoritative -> {
                append(" needsRequeue=false")
                append(" requeueOp=<none>")
            }
            is ReplayRowAction.PreserveLocal -> {
                append(" needsRequeue=true")
                append(" requeueOp=").append(safeReplayOperation(action.requeueOp))
            }
        }
    }
}

private fun safeReplayOperation(value: String): String =
    value.takeIf { it == "INSERT" || it == "UPDATE" || it == "DELETE" } ?: "<invalid>"

internal fun DirtyRowCapture.toRowRef(): DirtyRowRef =
    DirtyRowRef(schemaName = schemaName, tableName = tableName, keyJson = keyJson)

private fun CommittedReplayRow.toRowRef(): DirtyRowRef =
    DirtyRowRef(schemaName = schemaName, tableName = tableName, keyJson = keyJson)
