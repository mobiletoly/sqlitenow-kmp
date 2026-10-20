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

import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject

internal class OversqliteConflictExecutor(
    private val localStore: OversqliteLocalStore,
    private val syncStateStore: OversqliteSyncStateStore,
    private val outboxStateStore: OversqliteOutboxStateStore,
    private val survivors: OversqliteApplySurvivors,
    private val lifecycle: OversqliteApplyLifecycle,
    private val workspace: OversqliteWorkspace,
) {
    suspend fun applyConflictResolutionPlan(
        state: RuntimeState,
        snapshot: PushOutboundSnapshot,
        conflictingRow: DirtyRowCapture,
        tableName: String,
        plan: ConflictResolutionPlan,
        statementCache: StatementCache,
        reservationToken: String,
        reservationBase: WorkspaceCharge,
    ): Set<String> {
        val updatedTables = linkedSetOf(tableName.lowercase())

        // Freezing removes these dirty rows. Any row present now represents a later
        // application write and must survive resolution of the rejected snapshot.
        val hasNewerIntent = syncStateStore.loadDirtyUploadState(
            conflictingRow.schemaName, conflictingRow.tableName, conflictingRow.keyJson, statementCache,
        ).exists
        val upsert = plan.localRowAction as? LocalRowAction.Upsert
        val observed = plan.rowStateAction as? RowStateAction.Upsert
        val unitInput = "${snapshot.canonicalRequestHash}/${conflictingRow.keyJson}/${observed?.rowVersion}/${observed?.deleted}/${plan.requeueIntent}"
        val size = upsert?.payload?.let(::encodedJsonSize) ?: 0
        val unit = applyUnit("conflict", snapshot.sourceBundleId, unitInput)
        workspace.resizeInWriter(state, reservationToken, workspace.add(unit, reservationBase, WorkspaceCharge(32, workspace.scale(unit, size, 16))))
        val desiredText = upsert?.payload?.toString()
        val selectedUnit = applyUnit("conflict", snapshot.sourceBundleId, "$unitInput/$desiredText/$hasNewerIntent")
        survivors.prepare(state, selectedUnit, 1, "conflict", observed?.rowVersion,
            workspaceStatementCache = statementCache, replayStatementCache = statementCache) {
            if (hasNewerIntent) survivors.capture(state, tableName, conflictingRow.localPk,
                workspaceStatementCache = statementCache, replayStatementCache = statementCache)
            else ApplySurvivor(tableName, conflictingRow.localPk, desiredText,
                wire = upsert?.payloadSource == PayloadSource.AUTHORITATIVE_WIRE, role = "intended")
        }
        if (!hasNewerIntent) when (val action = plan.localRowAction) {
            is LocalRowAction.Upsert -> localStore.upsertRow(tableName, action.payload, action.payloadSource, statementCache)
            LocalRowAction.Delete -> localStore.deleteLocalRow(state, tableName, conflictingRow.localPk, statementCache)
        }

        when (val action = plan.rowStateAction) {
            RowStateAction.Delete -> syncStateStore.deleteStructuredRowState(
                state.validated.schemaForTable(tableName),
                tableName,
                conflictingRow.keyJson,
                statementCache,
            )
            is RowStateAction.Upsert -> syncStateStore.installAuthoritativeBaseline(
                schemaName = state.validated.schemaForTable(tableName),
                tableName = tableName,
                keyJson = conflictingRow.keyJson,
                rowVersion = action.rowVersion,
                kind = if(action.deleted) "deleted" else "live",
                payload = action.payload?.let { localStore.captureAuthoritativePayload(tableName,it,statementCache) },
                origin = "explicit_conflict", bundleSeq = null,
                statementCache = statementCache,
            )
        }

        val normalizedLocalPayload = when {
            hasNewerIntent -> localStore.serializeExistingRow(tableName, conflictingRow.localPk, statementCache)
            upsert == null -> null
            upsert.payloadSource == PayloadSource.LOCAL_STATE -> desiredText
            else -> localStore.captureAuthoritativePayload(tableName, upsert.payload, statementCache)
        }
        val retryIntent = if (hasNewerIntent) {
            val serverState = plan.rowStateAction as? RowStateAction.Upsert
            val serverHasRow = serverState != null && !serverState.deleted
            when {
                normalizedLocalPayload != null -> ResolvedDirtyIntent(
                    if (serverHasRow) "UPDATE" else "INSERT",
                    serverState?.rowVersion ?: 0L,
                    RetryPayloadSource.LocalRow,
                )
                serverHasRow -> ResolvedDirtyIntent("DELETE", serverState.rowVersion, RetryPayloadSource.None)
                else -> null
            }
        } else plan.requeueIntent

        syncStateStore.requeueSnapshotRows(snapshot, skipRow = conflictingRow, statementCache = statementCache)
        if (hasNewerIntent && retryIntent == null) syncStateStore.deleteDirtyRow(
            conflictingRow.schemaName, conflictingRow.tableName, conflictingRow.keyJson, statementCache,
        )
        retryIntent?.let { intent ->
            val payload = when (intent.payloadSource) {
                RetryPayloadSource.LocalRow ->
                    normalizedLocalPayload ?: error("resolved payload for $tableName must exist before requeue")
                RetryPayloadSource.None -> null
            }
            syncStateStore.rebaseDirtyIntent(
                schemaName = conflictingRow.schemaName,
                tableName = conflictingRow.tableName,
                keyJson = conflictingRow.keyJson,
                op = intent.op,
                baseRowVersion = intent.baseRowVersion,
                payload = payload,
                statementCache = statementCache,
            )
        }
        updatedTables += lifecycle.restoreAndVerify(state, statementCache)
        survivors.finish(replayStatementCache = statementCache)
        outboxStateStore.clearBundleAndRows(statementCache)
        return updatedTables
    }

    suspend fun restoreOutboundSnapshotToDirtyRows(
        snapshot: PushOutboundSnapshot,
        statementCache: StatementCache,
    ) {
        syncStateStore.requeueSnapshotRows(snapshot, statementCache = statementCache)
        outboxStateStore.clearBundleAndRows(statementCache)
    }
}

internal sealed class ConflictPlanDecision {
    data class Apply(val plan: ConflictResolutionPlan) : ConflictPlanDecision()

    data class Invalid(val message: String) : ConflictPlanDecision()
}

internal data class ConflictResolutionPlan(
    val localRowAction: LocalRowAction,
    val rowStateAction: RowStateAction,
    val requeueIntent: ResolvedDirtyIntent? = null,
)

internal sealed class LocalRowAction {
    data class Upsert(
        val payload: JsonObject,
        val payloadSource: PayloadSource,
    ) : LocalRowAction()

    data object Delete : LocalRowAction()
}

internal sealed class RowStateAction {
    data class Upsert(
        val rowVersion: Long,
        val deleted: Boolean,
        val payload: JsonObject? = null,
    ) : RowStateAction()

    data object Delete : RowStateAction()
}

internal data class ResolvedDirtyIntent(
    val op: String,
    val baseRowVersion: Long,
    val payloadSource: RetryPayloadSource,
)

internal enum class RetryPayloadSource {
    LocalRow,
    None,
}

internal sealed class AuthoritativeConflictState {
    data object Missing : AuthoritativeConflictState()

    data class Deleted(val rowVersion: Long) : AuthoritativeConflictState()

    data class Present(
        val rowVersion: Long,
        val payload: JsonObject,
    ) : AuthoritativeConflictState()
}

internal fun buildConflictPlanDecision(
    context: ConflictContext,
    result: MergeResult,
    localPayload: JsonObject?,
    expectedColumns: Set<String>,
): ConflictPlanDecision {
    val authoritative = context.authoritativeConflictState()
    val decision = when (result) {
        MergeResult.DeferConflict -> ConflictPlanDecision.Invalid(
            "deferred conflicts must be resolved through the durable conflict API"
        )
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
    if(decision is ConflictPlanDecision.Apply && decision.plan.rowStateAction is RowStateAction.Upsert) {
        return ConflictPlanDecision.Apply(decision.plan.copy(rowStateAction=decision.plan.rowStateAction.copy(payload=(authoritative as? AuthoritativeConflictState.Present)?.payload)))
    }
    return decision
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

    val decision = when (context.localOp) {
        "INSERT" -> buildInsertConflictPlan(mergedObject, authoritative)
        "UPDATE" -> buildUpdateConflictPlan(
            context = context,
            payload = mergedObject,
            authoritative = authoritative,
            invalidResultName = "KeepMerged",
        )
        else -> error("unsupported local conflict op ${context.localOp}")
    }
    // Resolver payloads use the same wire values exposed by ConflictContext.
    return if (decision is ConflictPlanDecision.Apply) {
        val action = decision.plan.localRowAction as LocalRowAction.Upsert
        ConflictPlanDecision.Apply(decision.plan.copy(localRowAction = action.copy(payloadSource = PayloadSource.AUTHORITATIVE_WIRE)))
    } else decision

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
