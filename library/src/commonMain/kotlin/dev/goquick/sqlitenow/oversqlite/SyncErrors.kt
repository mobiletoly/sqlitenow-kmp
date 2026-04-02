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

/** Thrown when the server does not advertise the required connect-lifecycle capability. */
class ConnectLifecycleUnsupportedException(
    val reason: String? = null,
) : RuntimeException(
    if (reason.isNullOrBlank()) {
        "server does not support the oversqlite connect lifecycle"
    } else {
        "server does not support the oversqlite connect lifecycle: $reason"
    }
)

/** Thrown when a database already attached to one user is asked to attach as another user. */
class ConnectBindingConflictException(
    val attachedUserId: String,
    val requestedUserId: String,
) : RuntimeException(
    "local database is already attached to user \"$attachedUserId\"; detach before " +
        "attaching user \"$requestedUserId\""
)

/** Thrown when local lifecycle state is incompatible with the requested attach flow. */
class ConnectLocalStateConflictException(
    val reason: String,
) : RuntimeException(
    "local sync state is incompatible with the requested attach lifecycle: $reason"
)

/** Thrown when a pending remote-authoritative replacement must be completed by [OversqliteClient.attach]. */
class RemoteReplacePendingException(
    val targetUserId: String,
) : RuntimeException(
    "remote-authoritative replacement for user \"$targetUserId\" is pending and must be finalized by attach(userId)"
)

/** Thrown while a destructive local lifecycle transition is still in progress. */
class DestructiveTransitionInProgressException(
    val transitionKind: String,
) : RuntimeException(
    "destructive local lifecycle transition \"$transitionKind\" is in progress"
)

/** Thrown when a lifecycle-aware sync operation is called before [OversqliteClient.open]. */
class OpenRequiredException(
    val operation: String,
) : RuntimeException("open() must be called before $operation")

/** Thrown when a lifecycle-aware sync operation is called before [OversqliteClient.attach]. */
class ConnectRequiredException(
    val operation: String,
) : RuntimeException("attach(userId) must complete successfully before $operation")

/** Thrown when the client must rebuild from snapshot before it can continue syncing. */
class RebuildRequiredException : RuntimeException(
    "client rebuild is required; run rebuild() before syncing"
)

/** Thrown when the current source stream is blocked and recovery requires internal source rotation. */
class SourceRecoveryRequiredException(
    val reason: SourceRecoveryReason,
) : RuntimeException(
    "source recovery is required ($reason); run rebuild() before syncing",
)

/** Thrown when the same client instance is already running another sync operation. */
class SyncOperationInProgressException : RuntimeException(
    "another sync operation is already in progress for this client"
)

/** Thrown when rebuild/recovery is attempted while staged outbound push replay is still pending. */
class PendingPushReplayException(
    val outboundCount: Int,
) : RuntimeException(
    "cannot rebuild while $outboundCount staged push rows are pending authoritative replay"
)

/** Thrown when a configured conflict resolver returns a structurally invalid decision. */
class InvalidConflictResolutionException(
    val conflict: ConflictContext,
    val result: MergeResult,
    message: String,
) : RuntimeException(message)

/** Thrown after automatic push-conflict retries are exhausted. */
class PushConflictRetryExhaustedException(
    val retryCount: Int,
    val remainingDirtyCount: Int,
) : RuntimeException(
    "push conflict auto-retry exhausted after $retryCount retries; " +
        "$remainingDirtyCount dirty rows remain replayable"
)

/** Thrown when [OversqliteClient.detach] is blocked by unsynced attached rows. */
class SignOutBlockedException(
    val pendingRowCount: Long,
) : RuntimeException(
    "cannot detach while $pendingRowCount attached sync rows are pending upload"
)

/** Thrown when a previously granted initialization lease is stale or expired. */
class InitializationLeaseInvalidException(
    val reasonCode: String,
) : RuntimeException(
    "initialization lease is no longer valid: $reasonCode; call attach(userId) again"
)

/** Thrown when a reused source bundle sequence cannot be proven to match server history. */
class SourceSequenceMismatchException(
    message: String,
) : RuntimeException(message)

/** Returns `true` when [error] represents normal local sync-operation contention. */
fun isExpectedSyncContention(error: Throwable?): Boolean {
    if (error == null) return false
    var current: Throwable? = error
    while (current != null) {
        if (current is SyncOperationInProgressException) {
            return true
        }
        current = current.cause
    }
    return false
}

/** Returns `true` when [error] represents a lifecycle-precondition failure instead of transport failure. */
fun isLifecyclePreconditionError(error: Throwable?): Boolean {
    if (error == null) return false
    var current: Throwable? = error
    while (current != null) {
        if (
            current is OpenRequiredException ||
            current is ConnectRequiredException ||
            current is DestructiveTransitionInProgressException ||
            current is SourceRecoveryRequiredException
        ) {
            return true
        }
        current = current.cause
    }
    return false
}
