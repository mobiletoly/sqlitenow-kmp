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
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject

internal class OversqlitePushWorkflow(
    private val config: OversqliteConfig,
    private val resolver: Resolver,
    private val db: SafeSQLiteConnection,
    private val tableInfoCache: TableInfoCache,
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
    private companion object {
        const val MAX_PUSH_CONFLICT_AUTO_RETRIES = 2
    }

    private val replayExecutor = OversqlitePushReplayExecutor(
        db = db,
        tableInfoCache = tableInfoCache,
        stageStore = stageStore,
        syncStateStore = syncStateStore,
        bundleApplier = bundleApplier,
        localStore = localStore,
        clientStateStore = clientStateStore,
        json = json,
        log = log,
    )
    private val conflictExecutor = OversqliteConflictExecutor(localStore, syncStateStore, stageStore)

    suspend fun pushPending(state: RuntimeState): Set<String> {
        return pushPending(state, conflictRetryCount = 0)
    }

    private suspend fun pushPending(
        state: RuntimeState,
        conflictRetryCount: Int,
    ): Set<String> {
        if (clientStateStore.isRebuildRequired(state.userId)) {
            throw RebuildRequiredException()
        }

        val initialDirtyCount = clientStateStore.pendingDirtyCount()
        val initialOutboundCount = clientStateStore.pendingPushOutboundCount()
        log {
            "oversqlite pushPending start user=${state.userId} source=${state.sourceId} " +
                "pendingDirty=$initialDirtyCount pendingOutbound=$initialOutboundCount"
        }
        val snapshot = ensurePushOutboundSnapshot(state)
        if (snapshot.rows.isEmpty()) {
            log { "oversqlite pushPending no-op: no rows ready for upload" }
            return emptySet()
        }

        log {
            "oversqlite pushPending snapshot sourceBundleId=${snapshot.sourceBundleId} " +
                "rows=${snapshot.rows.size} ${snapshot.rows.joinToString(" | ") { it.toVerboseSummary() }}"
        }
        clientStateStore.clearPushStage()
        val committed = try {
            commitPushOutboundSnapshot(state, snapshot)
        } catch (e: PushConflictException) {
            val updatedTables = resolvePushConflict(state, snapshot, e)
            val remainingDirtyCount = clientStateStore.pendingDirtyCount()
            if (remainingDirtyCount == 0) {
                return updatedTables
            }
            if (conflictRetryCount >= MAX_PUSH_CONFLICT_AUTO_RETRIES) {
                throw PushConflictRetryExhaustedException(
                    retryCount = MAX_PUSH_CONFLICT_AUTO_RETRIES,
                    remainingDirtyCount = remainingDirtyCount,
                )
            }
            return updatedTables + pushPending(state, conflictRetryCount = conflictRetryCount + 1)
        }

        log {
            "oversqlite pushPending committed bundleSeq=${committed.bundleSeq} rowCount=${committed.rowCount} " +
                "sourceBundleId=${committed.sourceBundleId}"
        }
        fetchCommittedPushBundle(state, committed)
        val updatedTables = applyExecutor.inApplyModeTransaction(state) { statementCache ->
            replayExecutor.applyCommittedBundle(
                state = state,
                uploadedRows = snapshot.rows,
                committed = committed,
                statementCache = statementCache,
            )
        }
        log { "oversqlite pushPending finished updatedTables=$updatedTables" }
        return updatedTables
    }

    private suspend fun ensurePushOutboundSnapshot(state: RuntimeState): PushOutboundSnapshot {
        val existing = stageStore.loadPushOutboundSnapshot(state)
        if (existing != null) {
            log {
                "oversqlite reusing frozen outbound snapshot sourceBundleId=${existing.sourceBundleId} rows=${existing.rows.size}"
            }
            return existing
        }

        val prepared = stageStore.preparePush(state, clientStateStore.loadNextSourceBundleId(state.userId))
        log {
            "oversqlite collectDirtyRows prepared=${prepared.rows.size} noOps=${prepared.discardedRows.size} " +
                prepared.rows.joinToString(" | ") { it.toVerboseSummary() }
        }
        stageStore.freezePushOutboundSnapshot(prepared)
        val frozen = PushOutboundSnapshot(
            sourceBundleId = prepared.sourceBundleId,
            rows = prepared.rows,
        )
        if (frozen.rows.isNotEmpty()) {
            log {
                "oversqlite froze outbound snapshot sourceBundleId=${frozen.sourceBundleId} rows=${frozen.rows.size}"
            }
        }
        return frozen
    }

    private suspend fun commitPushOutboundSnapshot(
        state: RuntimeState,
        snapshot: PushOutboundSnapshot,
    ): CommittedPushBundle {
        require(snapshot.rows.isNotEmpty()) { "outbound push snapshot is empty" }

        val createResponse = remoteApi.createPushSession(snapshot.sourceBundleId, snapshot.rows.size.toLong(), state.sourceId)
        return when (createResponse.status) {
            "already_committed" -> committedPushBundleFromCreateResponse(createResponse, state.sourceId, snapshot.sourceBundleId)
            "staging" -> {
                val pushId = createResponse.pushId.trim()
                require(pushId.isNotEmpty()) { "push session response missing push_id" }
                try {
                    val chunkSize = pushChunkRows()
                    var start = 0
                    while (start < snapshot.rows.size) {
                        val end = minOf(start + chunkSize, snapshot.rows.size)
                        val chunkResponse = remoteApi.uploadPushChunk(
                            pushId = pushId,
                            request = PushSessionChunkRequest(
                                startRowOrdinal = start.toLong(),
                                rows = buildPushRequestRows(snapshot.rows.subList(start, end)),
                            ),
                        )
                        require(chunkResponse.pushId == pushId) {
                            "push chunk response push_id ${chunkResponse.pushId} does not match requested $pushId"
                        }
                        require(chunkResponse.nextExpectedRowOrdinal == end.toLong()) {
                            "push chunk response next_expected_row_ordinal ${chunkResponse.nextExpectedRowOrdinal} does not match expected $end"
                        }
                        start = end
                    }
                    committedPushBundleFromCommitResponse(remoteApi.commitPushSession(pushId), state.sourceId, snapshot.sourceBundleId)
                } catch (t: Throwable) {
                    remoteApi.deletePushSessionBestEffort(pushId)
                    throw t
                }
            }
            else -> error("unexpected push session status ${createResponse.status}")
        }
    }

    private suspend fun buildPushRequestRows(rows: List<DirtyRowCapture>): List<PushRequestRow> {
        return rows.map { dirty ->
            PushRequestRow(
                schema = dirty.schemaName,
                table = dirty.tableName,
                key = dirty.wireKey,
                op = dirty.op,
                baseRowVersion = dirty.baseRowVersion,
                payload = if (dirty.op == "DELETE") null else localStore.processPayloadForUpload(dirty.tableName, dirty.localPayload ?: ""),
            )
        }
    }

    private suspend fun fetchCommittedPushBundle(
        state: RuntimeState,
        committed: CommittedPushBundle,
    ) {
        clientStateStore.clearPushStage()

        var afterRowOrdinal: Long? = null
        var stagedRowCount = 0L
        while (true) {
            val chunk = remoteApi.fetchCommittedBundleChunk(
                bundleSeq = committed.bundleSeq,
                afterRowOrdinal = afterRowOrdinal,
                maxRows = committedBundleChunkRows(),
            )
            validateCommittedBundleRowsResponse(chunk, committed, afterRowOrdinal)
            stageStore.stageCommittedBundleChunk(state, chunk, afterRowOrdinal)
            stagedRowCount += chunk.rows.size
            if (!chunk.hasMore) {
                break
            }
            afterRowOrdinal = chunk.nextRowOrdinal
        }

        require(stagedRowCount == committed.rowCount) {
            "staged committed bundle row count $stagedRowCount does not match expected row_count ${committed.rowCount}"
        }
        val bundleHash = stageStore.computeStagedPushBundleHash(state, committed.bundleSeq)
        require(bundleHash == committed.bundleHash) {
            "staged committed bundle hash $bundleHash does not match expected ${committed.bundleHash}"
        }
    }

    private suspend fun resolvePushConflict(
        state: RuntimeState,
        snapshot: PushOutboundSnapshot,
        exception: PushConflictException,
    ): Set<String> {
        val conflict = exception.conflict
        val conflictingRow = snapshot.rows.firstOrNull {
            it.schemaName == conflict.schema &&
                it.tableName == conflict.table &&
                it.wireKey == conflict.key
        } ?: throw exception

        val localPayload = parseDirtyRowPayloadAsObject(conflictingRow)
        val context = ConflictContext(
            schema = conflict.schema,
            table = conflict.table,
            key = conflict.key,
            localOp = conflictingRow.op,
            localPayload = localPayload,
            baseRowVersion = conflict.baseRowVersion,
            serverRowVersion = conflict.serverRowVersion,
            serverRowDeleted = conflict.serverRowDeleted,
            serverRow = conflict.serverRow,
        )
        val resolution = resolver.resolve(context)
        val expectedColumns = if (resolution is MergeResult.KeepMerged) {
            tableInfoCache.get(db, context.table).columnNamesLower.toSet()
        } else {
            emptySet()
        }

        val decision = buildConflictPlanDecision(
            context = context,
            result = resolution,
            localPayload = localPayload,
            expectedColumns = expectedColumns,
        )
        return when (decision) {
            is ConflictPlanDecision.Apply -> applyExecutor.inApplyModeTransaction(state) { statementCache ->
                conflictExecutor.applyConflictResolutionPlan(
                    state = state,
                    snapshot = snapshot,
                    conflictingRow = conflictingRow,
                    tableName = context.table,
                    plan = decision.plan,
                    statementCache = statementCache,
                )
            }

            is ConflictPlanDecision.Invalid -> {
                restoreOutboundSnapshotToDirtyRows(state, snapshot)
                throw InvalidConflictResolutionException(conflict = context, result = resolution, message = decision.message)
            }
        }
    }

    private suspend fun restoreOutboundSnapshotToDirtyRows(
        state: RuntimeState,
        snapshot: PushOutboundSnapshot,
    ) {
        applyExecutor.inApplyModeTransaction(state) { statementCache ->
            conflictExecutor.restoreOutboundSnapshotToDirtyRows(snapshot, statementCache)
        }
    }

    private fun parseDirtyRowPayloadAsObject(row: DirtyRowCapture): JsonObject? {
        val payloadText = row.localPayload ?: return null
        return json.parseToJsonElement(payloadText) as? JsonObject
            ?: error("local ${row.op} conflict payload must be a JSON object")
    }

    private fun pushChunkRows(): Int = config.uploadLimit.takeIf { it > 0 } ?: 1000

    private fun committedBundleChunkRows(): Int = config.downloadLimit.takeIf { it > 0 } ?: 1000
}

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
    private val db: SafeSQLiteConnection,
    private val tableInfoCache: TableInfoCache,
    private val stageStore: OversqliteStageStore,
    private val syncStateStore: OversqliteSyncStateStore,
    private val bundleApplier: OversqliteBundleApplier,
    private val localStore: OversqliteLocalStore,
    private val clientStateStore: OversqliteClientStateStore,
    private val json: Json,
    private val log: ((() -> String)) -> Unit,
) {
    private val planner = OversqliteReplayPlanner(json)

    suspend fun applyCommittedBundle(
        state: RuntimeState,
        uploadedRows: List<DirtyRowCapture>,
        committed: CommittedPushBundle,
        statementCache: StatementCache,
    ): Set<String> {
        val stagedRows = stageStore.loadStagedPushBundleRows(state, committed.bundleSeq)
        require(stagedRows.size.toLong() == committed.rowCount) {
            "staged push replay row count ${stagedRows.size} does not match expected row_count ${committed.rowCount}"
        }

        val plans = buildReplayPlans(uploadedRows, statementCache)
        val updatedTables = linkedSetOf<String>()
        for (row in stagedRows) {
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

        clientStateStore.advanceAfterCommittedPush(
            userId = state.userId,
            bundleSeq = committed.bundleSeq,
            sourceBundleId = committed.sourceBundleId,
            statementCache = statementCache,
        )
        stageStore.deletePushOutboundSnapshot(committed.sourceBundleId, statementCache)
        stageStore.deletePushStage(committed.bundleSeq, statementCache)
        assertReplayResidualDirtyState(uploadedRows, plans, statementCache)
        return updatedTables
    }

    private suspend fun buildReplayPlans(
        uploadedRows: List<DirtyRowCapture>,
        statementCache: StatementCache,
    ): Map<DirtyRowRef, UploadedReplayPlan> {
        return buildMap {
            for (uploaded in uploadedRows) {
                val tableInfo = tableInfoCache.get(db, uploaded.tableName)
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
                ?: error("missing replay plan for ${uploaded.tableName} key=${uploaded.wireKey}")
            val dirty = syncStateStore.loadDirtyUploadState(
                schemaName = uploaded.schemaName,
                tableName = uploaded.tableName,
                keyJson = uploaded.keyJson,
                statementCache = statementCache,
            )
            when (plan.action) {
                ReplayRowAction.AcceptAuthoritative -> require(!dirty.exists) {
                    "successful push replay left unexpected dirty row for ${uploaded.tableName} key=${uploaded.wireKey} " +
                        "uploadedOp=${uploaded.op} uploadedPayload=${uploaded.localPayload ?: "null"} " +
                        "currentDirtyOp=${dirty.op} currentDirtyPayload=${dirty.payload ?: "null"}"
                }

                is ReplayRowAction.PreserveLocal -> require(dirty.exists) {
                    "successful push replay lost preserved dirty row for ${uploaded.tableName} key=${uploaded.wireKey} " +
                        "requeueOp=${plan.action.requeueOp} expectedPayload=${plan.action.requeuePayload ?: "null"}"
                }
            }
        }
    }

    private fun replayDecisionLog(
        uploaded: DirtyRowCapture,
        plan: UploadedReplayPlan,
    ): String {
        val diagnostics = plan.diagnostics
        return buildString {
            append("oversqlite replay decision ")
            append("table=").append(uploaded.tableName)
            append(" key=").append(uploaded.wireKey)
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
                    append(" requeueOp=").append(action.requeueOp)
                }
            }
            append(" uploadedPayload=").append(uploaded.localPayload ?: "null")
            append(" livePayload=").append(diagnostics.livePayload ?: "null")
            if (diagnostics.pendingExists) {
                append(" currentDirtyPayload=").append(diagnostics.currentDirtyPayload ?: "null")
            }
        }
    }
}

private class OversqliteConflictExecutor(
    private val localStore: OversqliteLocalStore,
    private val syncStateStore: OversqliteSyncStateStore,
    private val stageStore: OversqliteStageStore,
) {
    suspend fun applyConflictResolutionPlan(
        state: RuntimeState,
        snapshot: PushOutboundSnapshot,
        conflictingRow: DirtyRowCapture,
        tableName: String,
        plan: ConflictResolutionPlan,
        statementCache: StatementCache,
    ): Set<String> {
        val updatedTables = linkedSetOf(tableName.lowercase())

        when (val action = plan.localRowAction) {
            is LocalRowAction.Upsert -> localStore.upsertRow(tableName, action.payload, action.payloadSource, statementCache)
            LocalRowAction.Delete -> localStore.deleteLocalRow(state, tableName, conflictingRow.localPk, statementCache)
        }

        when (val action = plan.rowStateAction) {
            RowStateAction.Delete -> syncStateStore.deleteStructuredRowState(
                state.validated.schema,
                tableName,
                conflictingRow.keyJson,
                statementCache,
            )
            is RowStateAction.Upsert -> syncStateStore.updateStructuredRowState(
                schemaName = state.validated.schema,
                tableName = tableName,
                keyJson = conflictingRow.keyJson,
                rowVersion = action.rowVersion,
                deleted = action.deleted,
                statementCache = statementCache,
            )
        }

        val normalizedLocalPayload = when (plan.localRowAction) {
            is LocalRowAction.Upsert -> localStore.serializeExistingRow(tableName, conflictingRow.localPk, statementCache)
            LocalRowAction.Delete -> null
        }

        syncStateStore.requeueSnapshotRows(snapshot, skipRow = conflictingRow, statementCache = statementCache)
        plan.requeueIntent?.let { intent ->
            val payload = when (intent.payloadSource) {
                RetryPayloadSource.LocalRow ->
                    normalizedLocalPayload ?: error("resolved payload for $tableName must exist before requeue")
                RetryPayloadSource.None -> null
            }
            syncStateStore.requeueDirtyIntent(
                schemaName = conflictingRow.schemaName,
                tableName = conflictingRow.tableName,
                keyJson = conflictingRow.keyJson,
                op = intent.op,
                baseRowVersion = intent.baseRowVersion,
                payload = payload,
                statementCache = statementCache,
            )
        }
        stageStore.deletePushOutboundSnapshot(snapshot.sourceBundleId, statementCache)
        return updatedTables
    }

    suspend fun restoreOutboundSnapshotToDirtyRows(
        snapshot: PushOutboundSnapshot,
        statementCache: StatementCache,
    ) {
        syncStateStore.requeueSnapshotRows(snapshot, statementCache = statementCache)
        stageStore.deletePushOutboundSnapshot(snapshot.sourceBundleId, statementCache)
    }
}

private sealed class ConflictPlanDecision {
    data class Apply(val plan: ConflictResolutionPlan) : ConflictPlanDecision()

    data class Invalid(val message: String) : ConflictPlanDecision()
}

private data class ConflictResolutionPlan(
    val localRowAction: LocalRowAction,
    val rowStateAction: RowStateAction,
    val requeueIntent: ResolvedDirtyIntent? = null,
)

private sealed class LocalRowAction {
    data class Upsert(
        val payload: JsonObject,
        val payloadSource: PayloadSource,
    ) : LocalRowAction()

    data object Delete : LocalRowAction()
}

private sealed class RowStateAction {
    data class Upsert(
        val rowVersion: Long,
        val deleted: Boolean,
    ) : RowStateAction()

    data object Delete : RowStateAction()
}

private data class ResolvedDirtyIntent(
    val op: String,
    val baseRowVersion: Long,
    val payloadSource: RetryPayloadSource,
)

private enum class RetryPayloadSource {
    LocalRow,
    None,
}

private sealed class AuthoritativeConflictState {
    data object Missing : AuthoritativeConflictState()

    data class Deleted(val rowVersion: Long) : AuthoritativeConflictState()

    data class Present(
        val rowVersion: Long,
        val payload: JsonObject,
    ) : AuthoritativeConflictState()
}

private fun buildConflictPlanDecision(
    context: ConflictContext,
    result: MergeResult,
    localPayload: JsonObject?,
    expectedColumns: Set<String>,
): ConflictPlanDecision {
    val authoritative = context.authoritativeConflictState()
    return when (result) {
        MergeResult.AcceptServer -> ConflictPlanDecision.Apply(
            when (authoritative) {
                AuthoritativeConflictState.Missing -> ConflictResolutionPlan(
                    localRowAction = LocalRowAction.Delete,
                    rowStateAction = RowStateAction.Delete,
                )

                is AuthoritativeConflictState.Deleted -> ConflictResolutionPlan(
                    localRowAction = LocalRowAction.Delete,
                    rowStateAction = RowStateAction.Upsert(
                        rowVersion = authoritative.rowVersion,
                        deleted = true,
                    ),
                )

                is AuthoritativeConflictState.Present -> ConflictResolutionPlan(
                    localRowAction = LocalRowAction.Upsert(
                        payload = authoritative.payload,
                        payloadSource = PayloadSource.AUTHORITATIVE_WIRE,
                    ),
                    rowStateAction = RowStateAction.Upsert(
                        rowVersion = authoritative.rowVersion,
                        deleted = false,
                    ),
                )
            }
        )

        MergeResult.KeepLocal -> buildKeepLocalConflictPlan(context, authoritative, localPayload)
        is MergeResult.KeepMerged -> buildKeepMergedConflictPlan(context, authoritative, result.mergedPayload, expectedColumns)
    }
}

private fun buildKeepLocalConflictPlan(
    context: ConflictContext,
    authoritative: AuthoritativeConflictState,
    localPayload: JsonObject?,
): ConflictPlanDecision {
    return when (context.localOp) {
        "INSERT" -> buildInsertConflictPlan(requireLocalPayload(context, localPayload), authoritative)
        "UPDATE" -> buildUpdateConflictPlan(
            context = context,
            payload = requireLocalPayload(context, localPayload),
            authoritative = authoritative,
            invalidResultName = "KeepLocal",
        )
        "DELETE" -> buildDeleteConflictPlan(authoritative)
        else -> error("unsupported local conflict op ${context.localOp}")
    }
}

private fun buildKeepMergedConflictPlan(
    context: ConflictContext,
    authoritative: AuthoritativeConflictState,
    mergedPayload: JsonElement,
    expectedColumns: Set<String>,
): ConflictPlanDecision {
    if (context.localOp == "DELETE") {
        return ConflictPlanDecision.Invalid(
            "KeepMerged is invalid for DELETE conflict on ${context.schema}.${context.table}"
        )
    }

    val mergedObject = mergedPayload as? JsonObject ?: return ConflictPlanDecision.Invalid(
        "KeepMerged for ${context.schema}.${context.table} must provide a JSON object payload"
    )
    val payloadColumns = mergedObject.keys.map { it.lowercase() }.toSet()
    if (payloadColumns != expectedColumns) {
        return ConflictPlanDecision.Invalid(
            "KeepMerged for ${context.schema}.${context.table} must include exactly every table column"
        )
    }

    return when (context.localOp) {
        "INSERT" -> buildInsertConflictPlan(mergedObject, authoritative)
        "UPDATE" -> buildUpdateConflictPlan(
            context = context,
            payload = mergedObject,
            authoritative = authoritative,
            invalidResultName = "KeepMerged",
        )
        else -> error("unsupported local conflict op ${context.localOp}")
    }
}

private fun buildInsertConflictPlan(
    payload: JsonObject,
    authoritative: AuthoritativeConflictState,
): ConflictPlanDecision {
    return when (authoritative) {
        is AuthoritativeConflictState.Present -> ConflictPlanDecision.Apply(
            upsertRetryPlan(
                payload = payload,
                rowVersion = authoritative.rowVersion,
                deleted = false,
                retryOp = "UPDATE",
            )
        )

        is AuthoritativeConflictState.Deleted -> ConflictPlanDecision.Apply(
            upsertRetryPlan(
                payload = payload,
                rowVersion = authoritative.rowVersion,
                deleted = true,
                retryOp = "INSERT",
            )
        )

        AuthoritativeConflictState.Missing ->
            error("local INSERT conflict must include live row or tombstone")
    }
}

private fun buildUpdateConflictPlan(
    context: ConflictContext,
    payload: JsonObject,
    authoritative: AuthoritativeConflictState,
    invalidResultName: String,
): ConflictPlanDecision {
    val live = authoritative as? AuthoritativeConflictState.Present ?: return ConflictPlanDecision.Invalid(
        "$invalidResultName is invalid for stale UPDATE on " +
            "${context.schema}.${context.table}; authoritative row is deleted or missing"
    )
    return ConflictPlanDecision.Apply(
        upsertRetryPlan(
            payload = payload,
            rowVersion = live.rowVersion,
            deleted = false,
            retryOp = "UPDATE",
        )
    )
}

private fun buildDeleteConflictPlan(authoritative: AuthoritativeConflictState): ConflictPlanDecision {
    return when (authoritative) {
        AuthoritativeConflictState.Missing -> ConflictPlanDecision.Apply(
            ConflictResolutionPlan(
                localRowAction = LocalRowAction.Delete,
                rowStateAction = RowStateAction.Delete,
            )
        )

        is AuthoritativeConflictState.Deleted -> ConflictPlanDecision.Apply(
            ConflictResolutionPlan(
                localRowAction = LocalRowAction.Delete,
                rowStateAction = RowStateAction.Upsert(
                    rowVersion = authoritative.rowVersion,
                    deleted = true,
                ),
            )
        )

        is AuthoritativeConflictState.Present -> ConflictPlanDecision.Apply(
            ConflictResolutionPlan(
                localRowAction = LocalRowAction.Delete,
                rowStateAction = RowStateAction.Upsert(
                    rowVersion = authoritative.rowVersion,
                    deleted = false,
                ),
                requeueIntent = ResolvedDirtyIntent(
                    op = "DELETE",
                    baseRowVersion = authoritative.rowVersion,
                    payloadSource = RetryPayloadSource.None,
                ),
            )
        )
    }
}

private fun upsertRetryPlan(
    payload: JsonObject,
    rowVersion: Long,
    deleted: Boolean,
    retryOp: String,
): ConflictResolutionPlan {
    return ConflictResolutionPlan(
        localRowAction = LocalRowAction.Upsert(
            payload = payload,
            payloadSource = PayloadSource.LOCAL_STATE,
        ),
        rowStateAction = RowStateAction.Upsert(
            rowVersion = rowVersion,
            deleted = deleted,
        ),
        requeueIntent = ResolvedDirtyIntent(
            op = retryOp,
            baseRowVersion = rowVersion,
            payloadSource = RetryPayloadSource.LocalRow,
        ),
    )
}

private fun requireLocalPayload(
    context: ConflictContext,
    payload: JsonObject?,
): JsonObject {
    return payload ?: error("local ${context.localOp} conflict payload must be a JSON object")
}

private fun ConflictContext.authoritativeConflictState(): AuthoritativeConflictState {
    if (serverRow == null) {
        return if (serverRowDeleted) {
            AuthoritativeConflictState.Deleted(serverRowVersion)
        } else {
            AuthoritativeConflictState.Missing
        }
    }

    val payload = serverRow as? JsonObject
        ?: error("authoritative row for ${schema}.${table} must be a JSON object")
    return AuthoritativeConflictState.Present(
        rowVersion = serverRowVersion,
        payload = payload,
    )
}

private fun DirtyRowCapture.toRowRef(): DirtyRowRef =
    DirtyRowRef(schemaName = schemaName, tableName = tableName, keyJson = keyJson)

private fun StagedPushBundleRow.toRowRef(): DirtyRowRef =
    DirtyRowRef(schemaName = schemaName, tableName = tableName, keyJson = keyJson)
