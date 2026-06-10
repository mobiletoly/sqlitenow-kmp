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
