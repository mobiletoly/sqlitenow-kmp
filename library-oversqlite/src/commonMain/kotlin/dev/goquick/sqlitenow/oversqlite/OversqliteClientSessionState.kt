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
import kotlin.concurrent.Volatile
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

internal class OversqliteClientSessionState(
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
    private val tableInfoCache: TableInfoCache,
    private val runtimeInitializer: SyncRuntimeInitializer,
    private val managedTableStore: OversqliteManagedTableStore,
    private val applyStateStore: OversqliteApplyStateStore,
    private val sourceStateStore: OversqliteSourceStateStore,
    private val attachmentStateStore: OversqliteAttachmentStateStore,
    private val operationStateStore: OversqliteOperationStateStore,
    private val syncStateStore: OversqliteSyncStateStore,
    private val outboxStateStore: OversqliteOutboxStateStore,
) {
    internal var openingMutationAuthority: OversqliteOpeningMutationAuthority? = null

    @Volatile
    internal var coordinatorAuthorized = false

    @Volatile
    internal var validatedConfig: ValidatedConfig? = null

    @Volatile
    internal var admittedContract: AdmittedOversqliteContract? = null

    @Volatile
    internal var currentSourceId: String? = null

    @Volatile
    internal var currentUserId: String? = null

    @Volatile
    internal var sessionConnected = false

    @Volatile
    internal var pendingInitializationId: String = ""

    internal suspend fun openLocalRuntime(): ValidatedConfig {
        admittedContract = null
        // Census incomplete retained state before changing any SQLite connection setting.
        rejectPartialRetainedRuntimeBeforeConfiguration(db)
        val originalForeignKeys = db.scalarLong("PRAGMA foreign_keys")
        try {
            db.execSQL("PRAGMA foreign_keys = ON")
            require(db.scalarLong("PRAGMA foreign_keys") == 1L) {
                "foreign keys must be enabled before opening sync"
            }
            lateinit var validated: ValidatedConfig
            var attachmentState: OversqliteAttachmentState? = null
            db.transaction(TransactionMode.IMMEDIATE) {
                // Recheck under the writer lock to close the preflight-to-initialization race.
                if (inspectRetainedRuntime(db) != "FRESH") checkRetainedApplyWorkspace(db, config)
                validated = runtimeInitializer.prepareLocalRuntime(db, managedTableStore)
                applyStateStore.resetOnStartup()
                val operation = operationStateStore.loadState()
                val currentAttachment = attachmentStateStore.loadState()
                val boundSourceId = currentAttachment.currentSourceId
                    .takeIf { it.isNotEmpty() }
                    ?.let(::requireValidOversqliteSourceId)
                    ?: generateFreshSourceId()
                sourceStateStore.ensureSource(boundSourceId)
                if (currentAttachment.currentSourceId.isBlank()) {
                    runtimeInitializer.capturePreexistingAnonymousRows(db, validated)
                }
                val normalizedAttachment = currentAttachment.copy(
                    currentSourceId = boundSourceId,
                    bindingState = if (
                        operation.kind == operationKindRemoteReplace ||
                        currentAttachment.bindingState != attachmentBindingAttached ||
                        currentAttachment.attachedUserId.isBlank()
                    ) {
                        attachmentBindingAnonymous
                    } else {
                        attachmentBindingAttached
                    },
                    attachedUserId = currentAttachment.attachedUserId.takeIf {
                        operation.kind != operationKindRemoteReplace &&
                            currentAttachment.bindingState == attachmentBindingAttached &&
                            it.isNotBlank()
                    }.orEmpty(),
                    schemaName = unconfirmedLocalSchema.takeIf {
                        operation.kind != operationKindRemoteReplace &&
                            currentAttachment.bindingState == attachmentBindingAttached &&
                            currentAttachment.attachedUserId.isNotBlank()
                    }.orEmpty(),
                    lastBundleSeqSeen = currentAttachment.lastBundleSeqSeen.takeIf {
                        operation.kind != operationKindRemoteReplace &&
                            currentAttachment.bindingState == attachmentBindingAttached &&
                            currentAttachment.attachedUserId.isNotBlank()
                    } ?: 0L,
                    rebuildRequired = currentAttachment.rebuildRequired &&
                        operation.kind != operationKindRemoteReplace &&
                        currentAttachment.bindingState == attachmentBindingAttached &&
                        currentAttachment.attachedUserId.isNotBlank(),
                    pendingInitializationId = currentAttachment.pendingInitializationId.takeIf {
                        operation.kind == operationKindNone &&
                            currentAttachment.bindingState == attachmentBindingAttached &&
                            currentAttachment.attachedUserId.isNotBlank()
                    }.orEmpty(),
                )
                attachmentStateStore.persistState(normalizedAttachment)
                attachmentState = normalizedAttachment
            }

            validatedConfig = validated
            currentSourceId = requireValidOversqliteSourceId(attachmentState!!.currentSourceId)
            pendingInitializationId = attachmentState.pendingInitializationId
            currentUserId = attachmentState.attachedUserId.takeIf {
                attachmentState.bindingState == attachmentBindingAttached && it.isNotBlank()
            }
            return validated
        } catch (error: Throwable) {
            if (originalForeignKeys == 0L) {
                runCatching { db.execSQL("PRAGMA foreign_keys = OFF") }
                    .exceptionOrNull()
                    ?.let(error::addSuppressed)
            }
            throw error
        }
    }

    internal suspend fun restoreInMemoryLifecycleState() {
        val state = loadDurableLifecycleView()
        if (state.isAttached) {
            currentUserId = state.attachedUserId
            pendingInitializationId = state.pendingInitializationId
            sessionConnected = false
            return
        }
        currentUserId = null
        pendingInitializationId = ""
        sessionConnected = false
    }

    internal suspend fun loadDurableLifecycleView(): DurableLifecycleView {
        val attachment = attachmentStateStore.loadState()
        val operation = operationStateStore.loadState()
        return DurableLifecycleView(
            sourceId = attachment.currentSourceId,
            bindingState = attachment.bindingState,
            attachedUserId = attachment.attachedUserId,
            pendingInitializationId = attachment.pendingInitializationId,
            operation = operation,
        )
    }

    internal suspend fun requireConnectedRuntimeState(operation: String): RuntimeState {
        val validated = requireValidatedConfig()
        val attachment = attachmentStateStore.loadState()
        val sourceId = attachment.currentSourceId
            .takeIf { it.isNotEmpty() }
            ?.let(::requireValidOversqliteSourceId)
            ?: (currentSourceId ?: throw OpenRequiredException(operation))
        val userId = attachment.attachedUserId.takeIf {
            attachment.bindingState == attachmentBindingAttached && it.isNotBlank()
        } ?: throw ConnectRequiredException(operation)
        if (!sessionConnected || (currentUserId != null && currentUserId != userId)) {
            throw ConnectRequiredException(operation)
        }
        if (sourceStateStore.loadState(sourceId) == null) {
            throw ConnectRequiredException(operation)
        }
        pendingInitializationId = attachment.pendingInitializationId
        return RuntimeState(
            validated = validated,
            admittedContract = requireAdmittedContract(),
            userId = userId,
            sourceId = sourceId,
            pendingInitializationId = attachment.pendingInitializationId,
        )
    }

    internal fun requireValidatedConfig(): ValidatedConfig =
        validatedConfig ?: throw OpenRequiredException("oversqlite runtime access")

    internal fun requireAdmittedContract(): AdmittedOversqliteContract =
        admittedContract ?: throw OpenRequiredException("oversqlite admitted contract access")

    internal fun requireCurrentSourceId(): String = requireValidOversqliteSourceId(
        currentSourceId ?: throw OpenRequiredException("oversqlite runtime access"),
    )

    internal fun resolveSourceIdOrCurrent(sourceId: String): String =
        if (sourceId.isEmpty()) requireCurrentSourceId() else requireValidOversqliteSourceId(sourceId)

    internal fun ensureOpened(operation: String) {
        if (currentSourceId == null || validatedConfig == null) {
            throw OpenRequiredException(operation)
        }
    }

    internal fun applyDownloadResult(result: DownloadWorkflowResult) {
        result.rotatedSourceId?.let { currentSourceId = requireValidOversqliteSourceId(it) }
    }

    internal suspend fun clearStaleInitializationState() {
        val sourceId = currentSourceId ?: return
        attachmentStateStore.persistAnonymousState(sourceId)
        operationStateStore.persistState(OversqliteOperationState())
        currentUserId = null
        pendingInitializationId = ""
        sessionConnected = false
    }

    internal suspend fun pendingLocalChangeCount(): Long = OversqliteCountEvaluation(
        dirtyRowCount = syncStateStore.countDirtyRows(),
        outboundRowCount = outboxStateStore.countRows(),
    ).totalPendingRowCount

    internal suspend fun pendingSyncStatus(): PendingSyncStatus {
        val total = pendingLocalChangeCount()
        val attachment = attachmentStateStore.loadState()
        return PendingSyncStatus(
            hasPendingSyncData = total > 0,
            pendingRowCount = total,
            blocksDetach = attachment.bindingState == attachmentBindingAttached && total > 0,
        )
    }

    internal suspend fun syncStatus(state: RuntimeState): SyncStatus {
        val authority = when {
            state.pendingInitializationId.isNotEmpty() -> AuthorityStatus.PENDING_LOCAL_SEED
            syncStateStore.countLiveStructuredRows() == 0L -> AuthorityStatus.AUTHORITATIVE_EMPTY
            else -> AuthorityStatus.AUTHORITATIVE_MATERIALIZED
        }
        val attachment = attachmentStateStore.loadState()
        return SyncStatus(
            authority = authority,
            pending = pendingSyncStatus(),
            lastBundleSeqSeen = attachment.lastBundleSeqSeen,
        )
    }

    internal suspend fun ensureNoDestructiveTransition() {
        val operation = operationStateStore.loadState()
        if (
            operation.kind != operationKindNone &&
            operation.kind != operationKindRemoteReplace &&
            operation.kind != operationKindSourceRecovery
        ) {
            throw DestructiveTransitionInProgressException(operation.kind)
        }
    }

    internal suspend fun persistSourceRecoveryRequiredState(
        state: RuntimeState,
        reason: SourceRecoveryReason,
        replacementSourceId: String? = null,
    ) {
        db.transaction(TransactionMode.IMMEDIATE) {
            attachmentStateStore.setRebuildRequired(true)
            val reservedReplacementSourceId = reserveReplacementSourceId(
                currentSourceId = state.sourceId,
                preferredReplacementSourceId = replacementSourceId,
            )
            operationStateStore.persistState(
                OversqliteOperationState(
                    kind = operationKindSourceRecovery,
                    reason = reason.toPersistedOperationReason(),
                    replacementSourceId = reservedReplacementSourceId,
                ),
            )
        }
    }

    internal suspend fun reserveReplacementSourceId(
        currentSourceId: String,
        preferredReplacementSourceId: String? = null,
    ): String {
        requireValidOversqliteSourceId(currentSourceId)
        val durableOperation = operationStateStore.loadState()
        val existingReplacementSourceId = durableOperation.replacementSourceId
            .takeIf { it.isNotEmpty() }
            ?.let(::requireValidOversqliteSourceId)
            .orEmpty()
        val preferred = preferredReplacementSourceId
            ?.let(::requireValidOversqliteSourceId)
            .orEmpty()
        val reserved = when {
            existingReplacementSourceId.isNotBlank() && preferred.isNotBlank() &&
                existingReplacementSourceId != preferred -> throw SourceReplacementDivergedException()
            existingReplacementSourceId.isNotBlank() -> existingReplacementSourceId
            preferred.isNotBlank() -> preferred
            else -> generateFreshReplacementSourceId(currentSourceId)
        }
        ensureFreshReplacementSourceState(currentSourceId, reserved)
        return reserved
    }

    private suspend fun ensureFreshReplacementSourceState(
        currentSourceId: String,
        replacementSourceId: String,
    ) {
        requireValidOversqliteSourceId(currentSourceId)
        requireValidOversqliteSourceId(replacementSourceId)
        check(replacementSourceId != currentSourceId) {
            "replacement source id must differ from current source id"
        }
        sourceStateStore.ensureSource(replacementSourceId)
        val state = sourceStateStore.loadState(replacementSourceId)
            ?: error("_sync_source_state is missing for the reserved replacement source")
        check(state.nextSourceBundleId == 1L && state.replacedBySourceId.isBlank()) {
            "reserved replacement source is not fresh for source recovery"
        }
    }

    internal fun generateFreshReplacementSourceId(currentSourceId: String): String {
        var candidate = generateFreshSourceId()
        while (candidate == currentSourceId) {
            candidate = generateFreshSourceId()
        }
        return candidate
    }

    @OptIn(ExperimentalUuidApi::class)
    private fun generateFreshSourceId(): String = Uuid.random().toString()
}

internal data class DurableLifecycleView(
    val sourceId: String,
    val bindingState: String,
    val attachedUserId: String,
    val pendingInitializationId: String,
    val operation: OversqliteOperationState,
) {
    val isAttached: Boolean
        get() = bindingState == attachmentBindingAttached && attachedUserId.isNotBlank()
}
