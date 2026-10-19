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
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject

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
                    } else if (pending.exists) {
                        ReplayRowAction.PreserveLocal(requeueOp = "DELETE", requeuePayload = null)
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

                        pending.exists -> ReplayRowAction.PreserveLocal(
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

internal class ReplayDecisionContinuation {
    var plans: Map<DirtyRowRef, UploadedReplayPlan>? = null
    var guard: String? = null
    var state: RuntimeState? = null
    var token: String? = null
    var held = WorkspaceCharge()
    fun clear() { plans = null; guard = null; state = null; token = null; held = WorkspaceCharge() }
}

private data class ReplayDecisionSet(val plans: Map<DirtyRowRef, UploadedReplayPlan>, val held: WorkspaceCharge)

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
        reservationToken: String? = null,
        reservationBase: WorkspaceCharge? = null,
        decisionContinuation: ReplayDecisionContinuation? = null,
    ): Set<String> {
        require(committedRows.size.toLong() == committed.rowCount) {
            "committed push replay row count ${committedRows.size} does not match expected row_count ${committed.rowCount}"
        }
        requireCommittedEffectCoverage(uploadedRows, committedRows)

        val decisions = buildReplayPlans(state, uploadedRows, committedRows, statementCache,reservationToken,reservationBase,decisionContinuation)
        val plans = decisions.plans
        val acknowledgementOnly = canAcknowledgeWithoutBusinessWrites(state, committedRows, plans, statementCache)
        // Strategy is part of the derived unit: a resumed writer must not reuse
        // a root-only plan if uploaded-row edits or application triggers require closure.
        val unit = applyUnit(if (acknowledgementOnly) "push-ack" else "push", committed.bundleSeq, committed.bundleHash)
        bundleApplier.survivors.prepare(state, unit, committedRows.size, operationKind = "settlement", workspaceStatementCache = statementCache, replayStatementCache = statementCache, discoverDependents = !acknowledgementOnly) { index ->
            val row = committedRows[index]
            if (plans[row.toRowRef()]?.action is ReplayRowAction.PreserveLocal) {
                bundleApplier.survivors.capture(state, row.tableName, row.localPk, workspaceStatementCache = statementCache, replayStatementCache = statementCache)
            } else ApplySurvivor(row.tableName, row.localPk, row.payload, wire = true)
        }
        // Preparation is ready. No decision cache may survive authoritative mutation.
        if (decisionContinuation?.plans != null && reservationToken != null) {
            decisionContinuation.clear()
            val base = reservationBase ?: bundleApplier.survivors.reservationCharge(reservationToken)
            bundleApplier.survivors.finishDecisionBinding(reservationToken, base, decisions.held, statementCache)
        }
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
                null -> bundleApplier.applyAuthoritativeRow(state, bundleRow, row.localPk, row.keyJson, statementCache,"settlement",committed.bundleSeq)
                ReplayRowAction.AcceptAuthoritative -> {
                    if (acknowledgementOnly) bundleApplier.recordAuthoritativeRow(state, bundleRow, row.keyJson, statementCache, "settlement", committed.bundleSeq)
                    else bundleApplier.applyAuthoritativeRow(state, bundleRow, row.localPk, row.keyJson, statementCache,"settlement",committed.bundleSeq)
                    syncStateStore.deleteDirtyRow(row.schemaName, row.tableName, row.keyJson, statementCache)
                }
                is ReplayRowAction.PreserveLocal -> {
                    syncStateStore.installAuthoritativeBaseline(
                        schemaName = state.validated.schemaForTable(row.tableName),
                        tableName = row.tableName,
                        keyJson = row.keyJson,
                        rowVersion = row.rowVersion,
                        kind = if(row.op=="DELETE") "deleted" else "live",
                        payload = row.payload?.let { localStore.captureAuthoritativePayload(row.tableName,json.parseToJsonElement(it) as JsonObject,statementCache) },
                        origin = "settlement", bundleSeq = committed.bundleSeq,
                        statementCache = statementCache,
                    )
                    syncStateStore.rebaseDirtyIntent(
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
        updatedTables += restoreDirtyBusinessRows(state, statementCache)
        bundleApplier.survivors.restore(state, statementCache, verifyOnly = true, workspaceStatementCache = statementCache, replayStatementCache = statementCache)
        bundleApplier.survivors.restorePrimaryIntent(state, statementCache, verifyOnly = true, workspaceStatementCache = statementCache, replayStatementCache = statementCache)
        bundleApplier.survivors.finish(replayStatementCache = statementCache)

        sourceStateStore.advanceAfterCommittedPush(state.sourceId, committed.sourceBundleId)
        attachmentStateStore.advanceAfterCommittedPush(committed.bundleSeq, statementCache)
        outboxStateStore.clearBundleAndRows(statementCache, pagedSettlement = true)
        assertReplayResidualDirtyState(uploadedRows, plans, statementCache)
        return updatedTables
    }

    private suspend fun canAcknowledgeWithoutBusinessWrites(
        state: RuntimeState,
        rows: List<CommittedReplayRow>,
        plans: Map<DirtyRowRef, UploadedReplayPlan>,
        cache: StatementCache,
    ): Boolean {
        for (row in rows) {
            if (row.op != "INSERT" && row.op != "UPDATE") return false
            val plan = plans[row.toRowRef()] ?: return false
            bundleApplier.survivors.reserveAcknowledgementCheck()
            if (plan.action != ReplayRowAction.AcceptAuthoritative ||
                !OversqliteValueCodec.equivalentPayloadTexts(
                    state.validated.tableInfoByName.getValue(row.tableName),
                    plan.diagnostics.livePayload, row.payload,
                    PayloadSource.LOCAL_STATE, PayloadSource.AUTHORITATIVE_WIRE, json,
                )) return false
        }
        val db = syncStateStore.db
        // Existing decision-copy reservations cover comparisons. Retain all
        // ordinary root preflight/atomic admission and final FK validation.
        // Other pending owners retain the existing preservation passes;
        // unchanged business rows do not require a global dependency index.
        for (table in rows.map { it.tableName }.distinct()) {
            bundleApplier.survivors.reserveAcknowledgementCheck()
            val owned = oversqliteManagedTriggerNames(setOf(table)).toList()
            val sql = "SELECT 1 FROM sqlite_schema WHERE type='trigger' AND lower(tbl_name)=? AND name NOT IN (${owned.joinToString(",") { "?" }}) " +
                "UNION ALL SELECT 1 FROM sqlite_temp_schema WHERE type='trigger' AND lower(tbl_name)=? LIMIT 1"
            val applicationTrigger = db.withPreparedStatement(sql, cache) { statement ->
                statement.bindText(1, table)
                owned.forEachIndexed { index, name -> statement.bindText(index + 2, name) }
                statement.bindText(owned.size + 2, table)
                statement.step()
            }
            if (applicationTrigger) return false
        }
        return true
    }

    private fun requireCommittedEffectCoverage(
        uploadedRows: List<DirtyRowCapture>,
        committedRows: List<CommittedReplayRow>,
    ) {
        val committedKeys = mutableSetOf<String>()
        for (row in committedRows) {
            require(committedKeys.add(replayIdentity(row.schemaName, row.tableName, row.wireKey))) {
                "committed effect stream contains a duplicate wire key"
            }
        }
        for (uploaded in uploadedRows) {
            require(replayIdentity(uploaded.schemaName, uploaded.tableName, uploaded.wireKey) in committedKeys) {
                "committed effect stream does not cover every frozen outbox wire key"
            }
        }
    }

    private fun replayIdentity(schema: String, table: String, key: SyncKey): String {
        val canonicalKey = canonicalizeJsonElement(
            buildJsonObject {
                for ((name, value) in key.entries.sortedBy { it.key }) {
                    put(name, JsonPrimitive(value))
                }
            },
        )
        return "$schema\u0000$table\u0000$canonicalKey"
    }

    private suspend fun buildReplayPlans(
        state: RuntimeState,
        uploadedRows: List<DirtyRowCapture>,
        committedRows: List<CommittedReplayRow>,
        statementCache: StatementCache,
        reservationToken: String?,
        reservationBase: WorkspaceCharge?,
        continuation: ReplayDecisionContinuation?,
    ): ReplayDecisionSet {
        val base=reservationBase ?: if(reservationToken!=null) bundleApplier.survivors.reservationCharge(reservationToken) else WorkspaceCharge()
        if (continuation?.plans != null && reservationToken != null) {
            require(continuation.state === state && continuation.token == reservationToken) { "replay decision continuation belongs to another operation" }
            try {
                val current = bundleApplier.survivors.currentDecisionBinding(state,reservationToken,base,continuation.held,statementCache)
                if (current.context == continuation.guard) {
                    return ReplayDecisionSet(checkNotNull(continuation.plans), continuation.held).also {
                        bundleApplier.survivors.recordDecisionContinuationHit()
                    }
                }
            } catch (capacity: SyncWorkspaceLimitException) {
                if (capacity.suppressedExceptions.isNotEmpty()) throw capacity
            }
            val previous = continuation.held
            continuation.clear()
            bundleApplier.survivors.finishDecisionBinding(reservationToken,base,previous,statementCache)
            bundleApplier.survivors.recordDecisionContinuationInvalidation()
        }
        var held=WorkspaceCharge()
        var binding: WorkspaceReservationBinding? = null
        var memoizeBinding = true
        var bindingCharged = false
        var primaryFailure: Throwable? = null
        var transferred = false
        suspend fun holdDecision(schema: String, table: String, key: String, pk: String) {
            val reserved = bundleApplier.survivors.reserveDecisionCopies(state,reservationToken,base,held,
                schema,table,key,pk,binding,statementCache,memoizeBinding)
            held = reserved.held
            binding = reserved.binding
            if (binding != null) bindingCharged = true
            if (reservationToken != null && binding == null) memoizeBinding = false
        }
        return try {
            val plans = buildMap<DirtyRowRef, UploadedReplayPlan> {
            for (uploaded in uploadedRows) {
                val tableInfo = state.validated.tableInfoByName[uploaded.tableName.lowercase()]
                    ?: error(
                        "missing validated table info for " +
                            safeSnapshotDiagnosticIdentifier(uploaded.tableName),
                    )
                holdDecision(uploaded.schemaName,uploaded.tableName,uploaded.keyJson,uploaded.localPk)
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
            for (effect in committedRows) {
                val ref = effect.toRowRef()
                if (ref in this) continue
                holdDecision(effect.schemaName,effect.tableName,effect.keyJson,effect.localPk)
                val dirty = syncStateStore.loadDirtyUploadState(
                    effect.schemaName, effect.tableName, effect.keyJson, statementCache,
                )
                if (!dirty.exists) continue
                val desired = localStore.serializeExistingRow(effect.tableName, effect.localPk, statementCache)
                put(
                    ref,
                    UploadedReplayPlan(
                        rowRef = ref,
                        action = ReplayRowAction.PreserveLocal(
                            requeueOp = when {
                                desired == null -> "DELETE"
                                effect.op == "DELETE" -> "INSERT"
                                else -> "UPDATE"
                            },
                            requeuePayload = desired,
                        ),
                        diagnostics = ReplayPlanDiagnostics(true, false, desired, false, dirty.payload),
                    ),
                )
            }
            }
            if (binding != null && continuation != null) {
                continuation.plans = plans
                continuation.guard = binding?.context
                continuation.state = state
                continuation.token = reservationToken
                continuation.held = held
                transferred = true
            }
            bundleApplier.survivors.recordDecisionContinuationBuild()
            ReplayDecisionSet(plans,held)
        } catch (failure: Throwable) {
            primaryFailure = failure
            throw failure
        } finally {
            binding = null
            if (bindingCharged && !transferred && reservationToken != null) try {
                kotlinx.coroutines.withContext(kotlinx.coroutines.NonCancellable) {
                    bundleApplier.survivors.finishDecisionBinding(reservationToken, base, held, statementCache)
                }
            } catch (cleanup: Throwable) {
                if (primaryFailure != null) primaryFailure.addSuppressed(cleanup) else throw cleanup
            }
        }
    }

    private suspend fun restoreDirtyBusinessRows(
        state: RuntimeState,
        statementCache: StatementCache,
    ): Set<String> {
        val changed = linkedSetOf<String>()
        // A committed effect can run an application trigger that changes an entirely
        // different dirty key. Its stored payload remains the desired local state
        // while apply mode suppresses capture triggers.
        repeat(64) {
            val survivorChanges = bundleApplier.survivors.restore(state, statementCache, workspaceStatementCache = statementCache, replayStatementCache = statementCache)
            changed += survivorChanges
            var restored = survivorChanges.isNotEmpty()
            val primaryChanges = bundleApplier.survivors.restorePrimaryIntent(state, statementCache, workspaceStatementCache = statementCache, replayStatementCache = statementCache)
            changed += primaryChanges
            restored = restored || primaryChanges.isNotEmpty()
            if (!restored) return changed
        }
        error("dirty business rows did not stabilize after committed effects")
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

internal fun CommittedReplayRow.toRowRef(): DirtyRowRef =
    DirtyRowRef(schemaName = schemaName, tableName = tableName, keyJson = keyJson)
