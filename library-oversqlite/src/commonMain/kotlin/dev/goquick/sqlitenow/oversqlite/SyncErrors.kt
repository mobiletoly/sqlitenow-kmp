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

const val OVERSQLITE_PROTOCOL_VERSION: String = "v1"

/** Thrown before local or remote sync work when the server protocol is incompatible. */
class ProtocolVersionMismatchException(
    val expected: String = OVERSQLITE_PROTOCOL_VERSION,
    val actual: String,
) : RuntimeException(
    "oversqlite protocol version mismatch",
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.PROTOCOL
}

internal fun CapabilitiesResponse.requireSupportedProtocol(): CapabilitiesResponse {
    if (protocolVersion != OVERSQLITE_PROTOCOL_VERSION) {
        throw ProtocolVersionMismatchException(actual = "")
    }
    return this
}

/** Thrown when advertised snapshot limits cannot support the configured bounded transfer. */
class SnapshotCapabilitiesException(
    message: String,
) : RuntimeException(message), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.PROTOCOL
}

/** Thrown when the decoded body of a bounded snapshot response exceeds its operation limit. */
class SnapshotResponseBodyTooLargeException(
    val operation: String,
    val limit: Long,
) : RuntimeException("$operation decoded response body exceeds $limit bytes"), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.PROTOCOL
}

/** Thrown when a bounded snapshot response uses an unsupported content encoding. */
class SnapshotUnsupportedContentEncodingException(
    val operation: String,
    val encoding: String,
) : RuntimeException("$operation response uses unsupported content encoding"), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.PROTOCOL
}

internal enum class SnapshotSemanticFailure {
    DUPLICATE_OBJECT_MEMBER,
    EXCESSIVE_NESTING,
    INVALID_SESSION,
    INVALID_CHUNK,
    INVALID_ROW,
    INVALID_PAYLOAD_SHAPE,
    INVALID_PAYLOAD_VALUE,
    KEY_PAYLOAD_MISMATCH,
}

/** Redacted, typed failure for snapshot data that decoded as JSON but violated the wire contract. */
internal class SnapshotSemanticException(
    val failure: SnapshotSemanticFailure,
) : IllegalArgumentException(
    "snapshot response failed semantic validation: ${failure.name.lowercase()}",
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.PROTOCOL
}

/** Redacted, typed failure for an invalid local or remote source identifier. */
internal class InvalidOversqliteSourceIdException : IllegalArgumentException(
    "oversqlite source id must contain one or more visible ASCII characters",
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.PROTOCOL
}

internal class RemoteResponseDecodeException(
    val operation: String,
) : RuntimeException("$operation response could not be decoded"), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.PROTOCOL
}

internal class RemoteResponseSemanticException(
    val operation: String,
) : IllegalArgumentException("$operation response failed semantic validation"), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.PROTOCOL
}

/** Thrown when the server reports that the next snapshot row cannot fit the configured byte budget. */
class SnapshotChunkTooSmallException(
    val configuredBytes: Long,
    val requiredBytes: Long,
) : RuntimeException(
    "snapshot chunk byte budget $configuredBytes is too small for the next row requiring $requiredBytes bytes; " +
        "increase snapshotChunkBytes",
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.PROTOCOL
}

/** Identifies a no-data-loss condition that changed after download and before destructive apply. */
class SnapshotFinalApplyGateException(
    val mode: String,
    val reason: String,
    cause: Throwable,
) : RuntimeException("snapshot final apply gate rejected $mode mode: $reason", cause), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

/** Redacted wrapper for an individual staged-row apply failure. */
class SnapshotRowApplyException(
    val rowOrdinal: Long,
    val schemaName: String,
    val tableName: String,
) : RuntimeException(
    "failed to apply snapshot row ordinal=$rowOrdinal " +
        "schema=${safeSnapshotDiagnosticIdentifier(schemaName)} " +
        "table=${safeSnapshotDiagnosticIdentifier(tableName)}",
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

internal fun safeSnapshotDiagnosticIdentifier(value: String): String =
    if (
        value.length in 1..63 &&
        (value.first() == '_' || value.first().isLetter()) &&
        value.all { it == '_' || it.isLetterOrDigit() }
    ) {
        value
    } else {
        "<redacted>"
    }

/** Thrown before destructive work when one staged row exceeds the configured live-page byte budget. */
class SnapshotApplyRowTooLargeException(
    val rowOrdinal: Long,
    val retainedTextBytes: Long,
    val limit: Long,
) : RuntimeException(
    "snapshot staged row ordinal=$rowOrdinal requires $retainedTextBytes retained text bytes, exceeding limit $limit",
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

/** Thrown when the server does not advertise the required connect-lifecycle capability. */
class ConnectLifecycleUnsupportedException(
    val reason: String? = null,
) : RuntimeException(
    if (reason.isNullOrBlank()) {
        "server does not support the oversqlite connect lifecycle"
    } else {
        "server does not support the oversqlite connect lifecycle: $reason"
    }
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

/** Thrown when a database already attached to one user is asked to attach as another user. */
class ConnectBindingConflictException(
    val attachedUserId: String,
    val requestedUserId: String,
) : RuntimeException(
    "local database is already attached to user \"$attachedUserId\"; detach before " +
        "attaching user \"$requestedUserId\""
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

/** Thrown when local lifecycle state is incompatible with the requested attach flow. */
class ConnectLocalStateConflictException(
    val reason: String,
) : RuntimeException(
    "local sync state is incompatible with the requested attach lifecycle: $reason"
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

/** Thrown when a pending remote-authoritative replacement must be completed by [OversqliteClient.attach]. */
class RemoteReplacePendingException(
    val targetUserId: String,
) : RuntimeException(
    "remote-authoritative replacement for user \"$targetUserId\" is pending and must be finalized by attach(userId)"
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

/** Thrown while a destructive local lifecycle transition is still in progress. */
class DestructiveTransitionInProgressException(
    val transitionKind: String,
) : RuntimeException(
    "destructive local lifecycle transition \"$transitionKind\" is in progress"
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

/** Thrown when a lifecycle-aware sync operation is called before [OversqliteClient.open]. */
class OpenRequiredException(
    val operation: String,
) : RuntimeException("open() must be called before $operation"), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

/** Thrown when a lifecycle-aware sync operation is called before [OversqliteClient.attach]. */
class ConnectRequiredException(
    val operation: String,
) : RuntimeException("attach(userId) must complete successfully before $operation"), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

/** Thrown when the client must rebuild from snapshot before it can continue syncing. */
class RebuildRequiredException : RuntimeException(
	"client checkpoint recovery is in progress"
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

enum class CheckpointRecoveryBlockedReason {
    UPLOAD_PAUSED,
    PENDING_WORK,
    PUSH_FAILED,
}

class CheckpointRecoveryBlockedException(
    val reason: CheckpointRecoveryBlockedReason,
    val dirtyCount: Int,
    val outboundCount: Int,
    val replayState: String,
    cause: Throwable? = null,
) : RuntimeException(
    "checkpoint recovery is blocked ($reason): dirty_rows=$dirtyCount " +
        "outbox_rows=$outboundCount replay_state=\"$replayState\"",
    cause,
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

/** Thrown when the current source stream is blocked and recovery requires internal source rotation. */
class SourceRecoveryRequiredException(
    val reason: SourceRecoveryReason,
) : RuntimeException(
    "source recovery is required ($reason); run rebuild() before syncing",
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

/** Thrown when local and server replacement-source state disagree. */
class SourceReplacementDivergedException : RuntimeException(
    "replacement source diverged between local and server recovery state",
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

/** Thrown when the server rejects a rotated replacement request as invalid. */
class SourceReplacementInvalidException : RuntimeException(
    "server rejected the source replacement request",
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

/** Thrown when the same client instance is already running another sync operation. */
class SyncOperationInProgressException : RuntimeException(
    "another sync operation is already in progress for this client"
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

/** Thrown when rebuild/recovery is attempted while staged outbound push replay is still pending. */
class PendingPushReplayException(
    val outboundCount: Int,
) : RuntimeException(
    "cannot rebuild while $outboundCount staged push rows are pending authoritative replay"
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

/** Thrown when a configured conflict resolver returns a structurally invalid decision. */
class InvalidConflictResolutionException(
    val conflict: ConflictContext,
    val result: MergeResult,
    message: String,
) : RuntimeException(message), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.CONFLICT
}

/** Thrown after automatic push-conflict retries are exhausted. */
class PushConflictRetryExhaustedException(
    val retryCount: Int,
    val remainingDirtyCount: Int,
) : RuntimeException(
    "push conflict auto-retry exhausted after $retryCount retries; " +
        "$remainingDirtyCount dirty rows remain replayable"
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.CONFLICT
}

/** Thrown when [OversqliteClient.detach] is blocked by unsynced attached rows. */
class SignOutBlockedException(
    val pendingRowCount: Long,
) : RuntimeException(
    "cannot detach while $pendingRowCount attached sync rows are pending upload"
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

internal fun saturatingLegacyCount(value: Long): Int {
    require(value >= 0L)
    return if (value >= Int.MAX_VALUE.toLong()) Int.MAX_VALUE else value.toInt()
}

/** Thrown when a previously granted initialization lease is stale or expired. */
class InitializationLeaseInvalidException(
    val reasonCode: String,
) : RuntimeException(
    "initialization lease is no longer valid: $reasonCode; call attach(userId) again"
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

/** Thrown when a reused source bundle sequence cannot be proven to match server history. */
class SourceSequenceMismatchException(
    message: String,
) : RuntimeException(message), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

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
