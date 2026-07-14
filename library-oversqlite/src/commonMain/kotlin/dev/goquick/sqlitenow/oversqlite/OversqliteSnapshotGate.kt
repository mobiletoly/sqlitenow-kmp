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

internal data class SnapshotOutboxFingerprint(
    val state: String,
    val sourceId: String,
    val sourceBundleId: Long,
    val canonicalRequestHash: String,
    val rowCount: Long,
    val remoteBundleSeq: Long,
    val remoteBundleHash: String,
    val actualRowCount: Long,
)

internal data class SnapshotLifecycleFingerprint(
    val attachment: OversqliteAttachmentState,
    val operation: OversqliteOperationState,
)

internal data class SnapshotApplyGuard(
    val mode: SnapshotRebuildOutboxMode,
    val dirtyRowCount: Long,
    val outbox: SnapshotOutboxFingerprint,
    val lifecycle: SnapshotLifecycleFingerprint,
    val remoteReplace: Boolean,
    val remoteTargetUserId: String,
)

internal class OversqliteSnapshotGate(
    private val db: SafeSQLiteConnection,
    private val syncStateStore: OversqliteSyncStateStore,
    private val outboxStateStore: OversqliteOutboxStateStore,
    private val attachmentStateStore: OversqliteAttachmentStateStore,
    private val operationStateStore: OversqliteOperationStateStore,
) {
    suspend fun pin(
        mode: SnapshotRebuildOutboxMode,
        remoteReplace: Boolean = false,
        remoteTargetUserId: String = "",
    ): SnapshotApplyGuard = db.transaction(TransactionMode.DEFERRED) {
        load(mode, remoteReplace, remoteTargetUserId).also(::validateMode)
    }

    suspend fun validateFinal(
        pinned: SnapshotApplyGuard,
        session: SnapshotSession,
    ) {
        val current = load(
            mode = pinned.mode,
            remoteReplace = pinned.remoteReplace,
            remoteTargetUserId = pinned.remoteTargetUserId,
        )
        if (current.dirtyRowCount != pinned.dirtyRowCount) {
            throw SnapshotFinalApplyGateException(
                mode = pinned.mode.name,
                reason = "dirty row count changed",
                cause = DirtyStateRejectedException(current.dirtyRowCount),
            )
        }
        if (current.outbox != pinned.outbox) {
            throw SnapshotFinalApplyGateException(
                mode = pinned.mode.name,
                reason = "outbox fingerprint or actual row count changed",
                cause = PendingPushReplayException(
                    saturatingLegacyCount(current.outbox.actualRowCount),
                ),
            )
        }
        if (current.lifecycle != pinned.lifecycle) {
            val cause = if (pinned.mode == SnapshotRebuildOutboxMode.PRESERVE_SOURCE_RECOVERY) {
                SourceRecoveryRequiredException(SourceRecoveryReason.SOURCE_SEQUENCE_CHANGED)
            } else {
                RebuildRequiredException()
            }
            throw SnapshotFinalApplyGateException(
                mode = pinned.mode.name,
                reason = "lifecycle guard changed",
                cause = cause,
            )
        }
        try {
            validateMode(current)
        } catch (cause: Throwable) {
            throw SnapshotFinalApplyGateException(
                mode = pinned.mode.name,
                reason = "pending-work mode is no longer valid",
                cause = cause,
            )
        }
        if (
            pinned.mode == SnapshotRebuildOutboxMode.PRESERVE_COMMITTED_REMOTE &&
            session.snapshotBundleSeq < current.outbox.remoteBundleSeq
        ) {
            throw SnapshotFinalApplyGateException(
                mode = pinned.mode.name,
                reason = "snapshot is older than the committed remote bundle",
                cause = PendingPushReplayException(
                    saturatingLegacyCount(current.outbox.actualRowCount),
                ),
            )
        }
    }

    private suspend fun load(
        mode: SnapshotRebuildOutboxMode,
        remoteReplace: Boolean,
        remoteTargetUserId: String,
    ): SnapshotApplyGuard {
        val outbox = outboxStateStore.loadBundleState()
        return SnapshotApplyGuard(
            mode = mode,
            dirtyRowCount = syncStateStore.countDirtyRows(),
            outbox = SnapshotOutboxFingerprint(
                state = outbox.state,
                sourceId = outbox.sourceId,
                sourceBundleId = outbox.sourceBundleId,
                canonicalRequestHash = outbox.canonicalRequestHash,
                rowCount = outbox.rowCount,
                remoteBundleSeq = outbox.remoteBundleSeq,
                remoteBundleHash = outbox.remoteBundleHash,
                actualRowCount = outboxStateStore.countRows(),
            ),
            lifecycle = SnapshotLifecycleFingerprint(
                attachment = attachmentStateStore.loadState(),
                operation = operationStateStore.loadState(),
            ),
            remoteReplace = remoteReplace,
            remoteTargetUserId = remoteTargetUserId,
        )
    }

    private fun validateMode(guard: SnapshotApplyGuard) {
        val counts = OversqliteCountEvaluation(
            dirtyRowCount = guard.dirtyRowCount,
            outboundRowCount = guard.outbox.actualRowCount,
        )
        if (counts.hasDirtyRows) {
            throw DirtyStateRejectedException(guard.dirtyRowCount)
        }
        val attachment = guard.lifecycle.attachment
        val operation = guard.lifecycle.operation
        when (guard.mode) {
            SnapshotRebuildOutboxMode.CLEAR_ALL -> {
                requireNoOutbox(guard)
                if (guard.remoteReplace) {
                    if (
                        attachment.bindingState != attachmentBindingAnonymous ||
                        operation.kind != operationKindRemoteReplace ||
                        operation.targetUserId != guard.remoteTargetUserId
                    ) {
                        throw RebuildRequiredException()
                    }
                } else if (
                    attachment.bindingState != attachmentBindingAttached ||
                    !attachment.rebuildRequired ||
                    operation.kind != operationKindNone
                ) {
                    throw RebuildRequiredException()
                }
            }

            SnapshotRebuildOutboxMode.PRESERVE_COMMITTED_REMOTE -> {
                val outbox = guard.outbox
                if (
                    outbox.state != outboxStateCommittedRemote ||
                    outbox.sourceId.isBlank() ||
                    outbox.sourceBundleId <= 0L ||
                    outbox.canonicalRequestHash.isBlank() ||
                    outbox.rowCount <= 0L ||
                    outbox.remoteBundleSeq <= 0L ||
                    outbox.remoteBundleHash.isBlank() ||
                    outbox.actualRowCount != outbox.rowCount
                ) {
                    throw PendingPushReplayException(
                        saturatingLegacyCount(outbox.actualRowCount),
                    )
                }
                if (
                    attachment.bindingState != attachmentBindingAttached ||
                    !attachment.rebuildRequired ||
                    operation.kind != operationKindNone
                ) {
                    throw RebuildRequiredException()
                }
            }

            SnapshotRebuildOutboxMode.PRESERVE_SOURCE_RECOVERY -> {
                if (
                    attachment.bindingState != attachmentBindingAttached ||
                    !attachment.rebuildRequired ||
                    operation.kind != operationKindSourceRecovery
                ) {
                    throw SourceRecoveryRequiredException(SourceRecoveryReason.SOURCE_SEQUENCE_CHANGED)
                }
                when (guard.outbox.state) {
                    outboxStateNone -> requireNoOutbox(guard)
                    outboxStatePrepared -> {
                        val outbox = guard.outbox
                        if (
                            outbox.sourceId.isBlank() ||
                            outbox.sourceBundleId <= 0L ||
                            outbox.canonicalRequestHash.isBlank() ||
                            outbox.rowCount <= 0L ||
                            outbox.actualRowCount != outbox.rowCount ||
                            outbox.remoteBundleSeq != 0L ||
                            outbox.remoteBundleHash.isNotBlank()
                        ) {
                            throw PendingPushReplayException(
                                saturatingLegacyCount(outbox.actualRowCount),
                            )
                        }
                    }
                    else -> throw SourceRecoveryRequiredException(SourceRecoveryReason.SOURCE_SEQUENCE_CHANGED)
                }
            }
        }
    }

    private fun requireNoOutbox(guard: SnapshotApplyGuard) {
        if (guard.outbox.state != outboxStateNone || guard.outbox.actualRowCount != 0L) {
            throw PendingPushReplayException(
                saturatingLegacyCount(guard.outbox.actualRowCount),
            )
        }
    }
}
