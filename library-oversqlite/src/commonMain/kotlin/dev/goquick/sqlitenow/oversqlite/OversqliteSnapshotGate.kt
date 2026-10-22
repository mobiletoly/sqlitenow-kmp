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
import androidx.sqlite.async.step
import kotlinx.coroutines.currentCoroutineContext

internal data class SnapshotOutboxFingerprint(
    val state: String,
    val sourceId: String,
    val sourceBundleId: Long,
    val canonicalRequestHash: String,
    val rowCount: Long,
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
    val automaticPin: AutomaticRecoveryPin? = null,
)

internal class OversqliteSnapshotGate(
    private val automaticRecovery: OversqliteAutomaticRecovery,
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
    ) {
        pinned.automaticPin?.let { automaticRecovery.validate(it) }
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
                    current.outbox.actualRowCount,
                ),
            )
        }
        if (current.lifecycle != pinned.lifecycle) {
            val cause = if (pinned.mode == SnapshotRebuildOutboxMode.SOURCE_RECOVERY) {
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
    }

    private suspend fun load(
        mode: SnapshotRebuildOutboxMode,
        remoteReplace: Boolean,
        remoteTargetUserId: String,
    ): SnapshotApplyGuard {
        requireCleanSnapshotWork(db)
        val outbox = outboxStateStore.loadBundleState()
        return SnapshotApplyGuard(
            mode = mode,
            automaticPin = currentCoroutineContext()[AutomaticRecoveryContext]?.pin,
            dirtyRowCount = syncStateStore.countDirtyRows(),
            outbox = SnapshotOutboxFingerprint(
                state = outbox.state,
                sourceId = outbox.sourceId,
                sourceBundleId = outbox.sourceBundleId,
                canonicalRequestHash = outbox.canonicalRequestHash,
                rowCount = outbox.rowCount,
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
            SnapshotRebuildOutboxMode.CLEAN -> {
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

            SnapshotRebuildOutboxMode.SOURCE_RECOVERY -> {
                requireNoOutbox(guard)
                if (
                    attachment.bindingState != attachmentBindingAttached ||
                    !attachment.rebuildRequired ||
                    operation.kind != operationKindSourceRecovery
                ) {
                    throw SourceRecoveryRequiredException(SourceRecoveryReason.SOURCE_SEQUENCE_CHANGED)
                }
            }
        }
    }

    private fun requireNoOutbox(guard: SnapshotApplyGuard) {
        if (guard.outbox.state != outboxStateNone || guard.outbox.actualRowCount != 0L) {
            throw PendingPushReplayException(
                guard.outbox.actualRowCount,
            )
        }
    }
}

/** Snapshot admission covers the entire scope, including an ack with no outbox. */
internal suspend fun requireCleanSnapshotWork(db: SafeSQLiteConnection) {
    val dirty = db.scalarLong("SELECT count(*) FROM _sync_dirty_rows")
    if (dirty != 0L) throw DirtyStateRejectedException(dirty)
    val pending = db.withPreparedStatement("""SELECT
        (SELECT count(*) FROM _sync_outbox_rows),
        (SELECT state FROM _sync_outbox_bundle WHERE singleton_key=1),
        EXISTS(SELECT 1 FROM _sync_upload_journal)""") { st ->
        check(st.step()); st.getLong(0) to (st.getText(1) != outboxStateNone || st.getLong(2) != 0L)
    }
    if (pending.first != 0L || pending.second) throw PendingPushReplayException(pending.first)
}
