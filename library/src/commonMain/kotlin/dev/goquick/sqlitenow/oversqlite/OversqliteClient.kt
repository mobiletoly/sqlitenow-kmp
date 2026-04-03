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

import dev.goquick.sqlitenow.core.sqliteNetworkDispatcher
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.flow.StateFlow

/**
 * Public oversqlite lifecycle and sync API.
 *
 * Construction is side-effect free. A client is not usable for account-backed sync until
 * [open] has initialized the local runtime and [attach] has attached or resumed an authenticated
 * account scope.
 *
 * Create at most one client instance per local database at a time. Sync serialization is enforced
 * per client instance, not across multiple client objects that point at the same database.
 */
interface OversqliteClient {
    /** Coarse UI-oriented progress for the active lifecycle-aware operation, if any. */
    val progress: StateFlow<OversqliteProgress>

    /** Temporarily suppresses automatic/background uploads without changing local dirty tracking. */
    suspend fun pauseUploads()

    /** Re-enables uploads after a prior [pauseUploads] call. */
    suspend fun resumeUploads()

    /** Temporarily suppresses automatic/background downloads without changing local attachment state. */
    suspend fun pauseDownloads()

    /** Re-enables downloads after a prior [pauseDownloads] call. */
    suspend fun resumeDownloads()

    /**
     * Performs the local-only startup phase for this database.
     *
     * This validates sync-managed table configuration, creates or repairs lifecycle metadata,
     * installs write-guard and change-capture triggers, durably restores or creates the current
     * source identity, and captures pre-existing managed rows exactly once as anonymous pending
     * sync data.
     *
     * [open] never contacts the server and never attaches an authenticated user. Call it on every
     * app launch before any other lifecycle-aware sync operation.
     *
     * Oversqlite manages the current sync writer identity internally and persists it in local sync
     * metadata. Callers do not generate, persist, or rotate that identity directly.
     */
    suspend fun open(): Result<Unit>

    /**
     * Resolves or resumes account attachment for the authenticated [userId].
     *
     * This is the only operation that may move the local database into an attached account state.
     * Depending on local state and server authority, it may:
     * - resume the same attached account locally
     * - attach to an already-authoritative remote scope
     * - authorize the first local seed upload
     * - establish an authoritative empty scope
     * - ask the caller to retry later without treating the condition as auth failure
     *
     * Call [attach] whenever an authenticated user session exists, not only on the initial
     * sign-in gesture.
     *
     * [attach] is account-scoped. It must not be used to change source identity.
     */
    suspend fun attach(userId: String): Result<AttachResult>

    /**
     * Returns read-only source diagnostics.
     *
     * This surface exists for debug/support tooling only. [SourceInfo.currentSourceId] is opaque:
     * callers must not persist it externally, infer lifecycle meaning from its format, or treat it
     * as a control surface.
     *
     * Requires successful [open]. A fresh attached/authenticated session is not required.
     */
    suspend fun sourceInfo(): Result<SourceInfo>

    /**
     * Returns the canonical sync status for the currently connected scope.
     *
     * Requires successful [open] and [attach].
     */
    suspend fun syncStatus(): Result<SyncStatus>

    /**
     * Detaches safely from the currently attached account.
     *
     * If unsynced attached sync data exists, returns [DetachOutcome.BLOCKED_UNSYNCED_DATA] and
     * makes no destructive local changes. On success, detach clears sync-managed local state and
     * returns the database to anonymous lifecycle state bound to a fresh internal source identity.
     *
     * Non-destructive exits preserve the existing source identity. This includes
     * [DetachOutcome.BLOCKED_UNSYNCED_DATA] and metadata-only cancellation of a pending
     * `remote_replace`.
     */
    suspend fun detach(): Result<DetachOutcome>

    /**
     * Freezes local dirty rows into one logical bundle and uploads it through chunked push
     * sessions.
     *
     * Requires successful [open] and [attach].
     */
    suspend fun pushPending(): Result<PushReport>

    /**
     * Pulls authoritative remote bundles until the current stable bundle sequence is fully applied.
     *
     * Requires successful [open] and [attach].
     */
    suspend fun pullToStable(): Result<RemoteSyncReport>

    /**
     * Runs the standard interactive sync flow: push first, then pull.
     *
     * Requires successful [open] and [attach].
     */
    suspend fun sync(): Result<SyncReport>

    /**
     * Runs bounded best-effort sync rounds and then attempts [detach].
     *
     * This is convenience sugar over [sync] plus [detach]. It may run multiple sync rounds when
     * fresh local writes arrive during the previous round. It never loops indefinitely; if detach
     * remains blocked, the final blocked outcome is returned explicitly. When the final detach
     * succeeds through the destructive cleanup path, the local anonymous state is rebound to a
     * fresh internal source identity.
     *
     * Requires successful [open] and [attach].
     */
    suspend fun syncThenDetach(): Result<SyncThenDetachResult>

    /**
     * Rebuilds local managed tables from a full staged server snapshot.
     *
     * Requires successful [open] and [attach]. Rebuild remains an attached/authenticated
     * operation because snapshot recovery still depends on remote authority.
     *
     * Oversqlite chooses the internal recovery mode. Callers trigger rebuild explicitly but do not
     * provide or rotate source ids directly.
     */
    suspend fun rebuild(): Result<RemoteSyncReport>

    /** Releases client-owned resources. */
    fun close()
}

/** Result of [OversqliteClient.attach]. */
sealed interface AttachResult {
    /** The database is attached and ready for normal sync operations. */
    data class Connected(
        val outcome: AttachOutcome,
        val status: SyncStatus,
        val restore: RestoreSummary? = null,
    ) : AttachResult

    /**
     * Account attachment is pending and should be retried after [retryAfterSeconds].
     *
     * This is a normal lifecycle outcome, not an authentication failure.
     */
    data class RetryLater(
        val retryAfterSeconds: Long,
    ) : AttachResult
}

/** Successful attachment/resume outcomes returned from [AttachResult.Connected]. */
enum class AttachOutcome {
    /** The same user/source attachment was already present locally and was resumed. */
    RESUMED_ATTACHED_STATE,

    /** The server scope was already authoritative and local managed tables were rebuilt from it. */
    USED_REMOTE_STATE,

    /** The server granted this source the right to seed first data from local pending rows. */
    SEEDED_FROM_LOCAL,

    /** The server established an authoritative empty scope without requiring a first seed push. */
    STARTED_EMPTY,
}

/** Public result of [OversqliteClient.detach]. */
sealed interface DetachOutcome {
    /** Detach succeeded and the local database returned to anonymous lifecycle state. */
    data object DETACHED : DetachOutcome

    /** Detach was refused because attached sync rows are still pending upload. */
    data object BLOCKED_UNSYNCED_DATA : DetachOutcome
}

/** Canonical sync status for the currently connected scope. */
data class SyncStatus(
    val authority: AuthorityStatus,
    val pending: PendingSyncStatus,
    val lastBundleSeqSeen: Long,
)

/** Library-level authority/materialization state for the currently connected scope. */
enum class AuthorityStatus {
    PENDING_LOCAL_SEED,
    AUTHORITATIVE_EMPTY,
    AUTHORITATIVE_MATERIALIZED,
}

/** Summary of a snapshot-based restore applied locally. */
data class RestoreSummary(
    val bundleSeq: Long,
    val rowCount: Long,
)

/** Public result of [OversqliteClient.pushPending]. */
data class PushReport(
    val outcome: PushOutcome,
    val status: SyncStatus,
)

enum class PushOutcome {
    NO_CHANGE,
    COMMITTED,
}

/** Public result of [OversqliteClient.pullToStable] and [OversqliteClient.rebuild]. */
data class RemoteSyncReport(
    val outcome: RemoteSyncOutcome,
    val status: SyncStatus,
    val restore: RestoreSummary? = null,
)

enum class RemoteSyncOutcome {
    ALREADY_AT_TARGET,
    APPLIED_INCREMENTAL,
    APPLIED_SNAPSHOT,
}

/** Public result of [OversqliteClient.sync]. */
data class SyncReport(
    val pushOutcome: PushOutcome,
    val remoteOutcome: RemoteSyncOutcome,
    val status: SyncStatus,
    val restore: RestoreSummary? = null,
)

/** Public result of [OversqliteClient.syncThenDetach]. */
data class SyncThenDetachResult(
    val lastSync: SyncReport,
    val detach: DetachOutcome,
    val syncRounds: Int,
    val remainingPendingRowCount: Long,
) {
    fun isSuccess(): Boolean = detach == DetachOutcome.DETACHED
}

/** High-level pending sync status returned by [OversqliteClient.pendingSyncStatus]. */
data class PendingSyncStatus(
    /** `true` when any local sync-managed pending rows still exist. */
    val hasPendingSyncData: Boolean,

    /** Total number of local pending sync rows currently tracked by the runtime. */
    val pendingRowCount: Long,

    /** `true` when plain [OversqliteClient.detach] would currently refuse to proceed. */
    val blocksDetach: Boolean,
)

/**
 * Read-only source diagnostics.
 *
 * [currentSourceId] is provided for diagnostics only. Callers must treat it as opaque and must not
 * derive lifecycle meaning from its format.
 */
data class SourceInfo(
    val currentSourceId: String,
    val rebuildRequired: Boolean,
    val sourceRecoveryRequired: Boolean,
    val sourceRecoveryReason: SourceRecoveryReason? = null,
)

enum class SourceRecoveryReason {
    HISTORY_PRUNED,
    SOURCE_SEQUENCE_OUT_OF_ORDER,
    SOURCE_SEQUENCE_CHANGED,
    SOURCE_RETIRED,
}

/** Coarse UI-oriented progress state for lifecycle-aware operations. */
sealed interface OversqliteProgress {
    data object Idle : OversqliteProgress

    data class Active(
        val operation: OversqliteOperation,
        val phase: OversqlitePhase,
    ) : OversqliteProgress
}

enum class OversqliteOperation {
    ATTACH,
    PUSH_PENDING,
    PULL_TO_STABLE,
    SYNC,
    REBUILD_KEEP_SOURCE,
    REBUILD_ROTATE_SOURCE,
}

enum class OversqlitePhase {
    ATTACHING,
    SEEDING,
    PUSHING,
    PULLING,
    STAGING_REMOTE_STATE,
    APPLYING_REMOTE_STATE,
}

/** Small holder for platform-specific dispatchers used by oversqlite internals. */
open class PlatformDispatchers {
    open val io: CoroutineDispatcher = sqliteNetworkDispatcher()
}
