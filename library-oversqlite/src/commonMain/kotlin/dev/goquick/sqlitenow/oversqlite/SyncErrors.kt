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

/** Push stopped after durably preserving a conflict and its frozen local intent. */
class ConflictNeedsResolutionException(
    val pending: PendingConflict,
) : RuntimeException("oversqlite conflict needs explicit resolution"), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.CONFLICT
}

/** The selected conflict no longer describes the current durable local intent. */
class StaleConflictResolutionException : RuntimeException(
    "selected oversqlite conflict is stale; inspect pendingConflict() again",
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.CONFLICT
}

/** Thrown before local or remote sync work when the server protocol is incompatible. */
class ProtocolVersionMismatchException(
    val expected: String = OVERSQLITE_PROTOCOL_VERSION,
    val actual: String,
) : RuntimeException(
    "oversqlite protocol version mismatch",
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.PROTOCOL
}

/** The server changed the immutable descriptor selected by this database. */
class GenerationContractMismatchException : RuntimeException(
    "server generation differs from the admitted contract; sync is stopped",
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.PROTOCOL
}

/**
 * A validated generation-contract error returned by the server.
 *
 * These fields are the stable, machine-readable contract. [message] is intended for diagnostics;
 * callers should branch on [code], [httpStatus], [operation], [remediation], [retryable], and [generationId].
 */
class OversqliteRemoteContractException internal constructor(
    val code: String,
    val httpStatus: Int,
    val operation: String,
    val remediation: String?,
    val retryable: Boolean,
    val generationId: Long?,
    message: String,
) : RuntimeException(message), OversqliteCategorizedException {
    internal var retryAfterMillis: Long? = null
    override val category: OversqliteErrorCategory =
        if (retryable) OversqliteErrorCategory.NETWORK else OversqliteErrorCategory.PROTOCOL
}

/** Thrown before a request when effective caller/default headers conflict with SDK-owned identity. */
internal class OversqliteRequestIdentityHeaderException(
    val headerName: String,
) : IllegalArgumentException(
    "effective $headerName header conflicts with the Oversqlite request identity",
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.PROTOCOL
}

/** One ordered sync-key disagreement for a table present in both local and server contracts. */
data class SyncKeyContractMismatch(
    val qualifiedTable: String,
    val clientSyncKeyColumns: List<String>,
    val serverSyncKeyColumns: List<String>,
)

/** Thrown before sync work when the server advertises a different table/key contract. */
class SyncTableContractMismatchException(
    serverOnlyTables: List<String>,
    clientOnlyTables: List<String>,
    syncKeyMismatches: List<SyncKeyContractMismatch>,
) : RuntimeException(
    syncTableContractMismatchMessage(serverOnlyTables, clientOnlyTables, syncKeyMismatches),
), OversqliteCategorizedException {
    val serverOnlyTables: List<String> = serverOnlyTables.distinct().sorted()
    val clientOnlyTables: List<String> = clientOnlyTables.distinct().sorted()
    val syncKeyMismatches: List<SyncKeyContractMismatch> = syncKeyMismatches
        .map { mismatch ->
            mismatch.copy(
                clientSyncKeyColumns = mismatch.clientSyncKeyColumns.toList(),
                serverSyncKeyColumns = mismatch.serverSyncKeyColumns.toList(),
            )
        }
        .sortedBy(SyncKeyContractMismatch::qualifiedTable)

    override val category: OversqliteErrorCategory = OversqliteErrorCategory.PROTOCOL
}

internal fun AdmittedOversqliteContract.requireSupportedProtocol(): AdmittedOversqliteContract {
    if (protocolVersion != OVERSQLITE_PROTOCOL_VERSION) {
        throw ProtocolVersionMismatchException(actual = "")
    }
    return this
}

/** Thrown when admitted snapshot limits cannot support the configured bounded transfer. */
class SnapshotTransferLimitException(
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
    "snapshot chunk decoded-body target $configuredBytes is too small for the next row requiring $requiredBytes bytes",
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

internal fun safeSyncTableDiagnosticIdentifier(schema: String, table: String): String =
    "${safeSnapshotDiagnosticIdentifier(schema)}.${safeSnapshotDiagnosticIdentifier(table)}"

private fun syncTableContractMismatchMessage(
    serverOnlyTables: List<String>,
    clientOnlyTables: List<String>,
    syncKeyMismatches: List<SyncKeyContractMismatch>,
): String {
    val safeServerOnly = serverOnlyTables.distinct().sorted().map(::safeQualifiedTableDiagnosticIdentifier)
    val safeClientOnly = clientOnlyTables.distinct().sorted().map(::safeQualifiedTableDiagnosticIdentifier)
    val safeKeyMismatches = syncKeyMismatches
        .sortedBy(SyncKeyContractMismatch::qualifiedTable)
        .map { mismatch ->
            val clientKeys = mismatch.clientSyncKeyColumns.map(::safeSnapshotDiagnosticIdentifier)
            val serverKeys = mismatch.serverSyncKeyColumns.map(::safeSnapshotDiagnosticIdentifier)
            "${safeQualifiedTableDiagnosticIdentifier(mismatch.qualifiedTable)}" +
                "(client=$clientKeys,server=$serverKeys)"
        }
    return "oversqlite sync table contract mismatch: " +
        "server_only=$safeServerOnly, client_only=$safeClientOnly, " +
        "sync_key_mismatches=$safeKeyMismatches"
}

private fun safeQualifiedTableDiagnosticIdentifier(value: String): String {
    val parts = value.split('.')
    return if (parts.size == 2) {
        safeSyncTableDiagnosticIdentifier(parts[0], parts[1])
    } else {
        "<redacted>.<redacted>"
    }
}

internal fun Throwable.isTerminalSyncCompatibilityFailure(): Boolean =
    this is ProtocolVersionMismatchException ||
        this is SyncTableContractMismatchException ||
        this is GenerationContractMismatchException ||
        this is RemoteResponseDecodeException ||
        this is RemoteResponseSemanticException ||
        (this is OversqliteRemoteContractException && !retryable)

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

/** Thrown when the admitted discovery contract does not support the required connect lifecycle. */
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
    val dirtyCount: Long,
    val outboundCount: Long,
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
    val outboundCount: Long,
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
    val remainingDirtyCount: Long,
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

/** A pending logical row cannot be represented under the admitted per-row hard limit. */
class PushRowExceedsAdmittedLimitException(
    val actualBytes: Long,
    val admittedLimit: Long,
) : RuntimeException(
    "push_row_exceeds_admitted_limit: row requires $actualBytes decoded bytes; admitted limit is $admittedLimit",
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.PROTOCOL
}

@kotlinx.serialization.Serializable
data class PushDependencyRecordRef(val schema: String,val table: String,val key: SyncKey)

data class SyncProgress(val dirtyRows: Long=0,val outboundRows: Long=0,val committedBundles: Long=0)

data class PushDependencyDiagnosticPage(val diagnosticId: String,val identities: List<PushDependencyRecordRef>,val nextCursor: String?=null)

class PushDependencyGroupExceedsAdmittedLimitException(
    val dimension: String,val limit: Long,val actual: Long,
    val componentRows: Long,val componentDecodedBytes: Long,val diagnosticId: String,
    val sample: List<PushDependencyRecordRef>,val hasMore: Boolean,
) : RuntimeException("push_dependency_group_exceeds_admitted_limit: dimension=$dimension limit=$limit actual=$actual"), OversqliteCategorizedException {
    val reason = "push_dependency_group_exceeds_admitted_limit"
    var progress = SyncProgress(); internal set
    override val category = OversqliteErrorCategory.PROTOCOL
}

class FKReconciliationRequiredException(val detail: String,val unit: String,val sample: List<PushDependencyRecordRef>,val hasMore: Boolean) :
    RuntimeException("fk_reconciliation_required: $detail"), OversqliteCategorizedException {
    val reason = "fk_reconciliation_required"
    var progress = SyncProgress(); internal set
    override val category = OversqliteErrorCategory.STATE
}

class StalePushDependencyDiagnosticException(val diagnosticId: String) :
    RuntimeException("stale_push_dependency_diagnostic: $diagnosticId"), OversqliteCategorizedException {
    val reason = "stale_push_dependency_diagnostic"
    override val category = OversqliteErrorCategory.STATE
}

/** Why one interactive invocation left recoverable local work pending. */
enum class SyncIncompleteReason { PUSH_ATTEMPT_LIMIT, NO_PROGRESS }

/** A bounded interactive sync stopped before another push or unsafe pull. */
class SyncIncompleteException(
    val reason: SyncIncompleteReason,
    val dirtyCount: Long,
    val outboundCount: Long,
    val confirmedBundleCount: Int,
    val pushAttempts: Int,
) : RuntimeException(
    "sync incomplete ($reason): dirty=$dirtyCount outbound=$outboundCount " +
        "confirmed=$confirmedBundleCount attempts=$pushAttempts",
), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

/** The server discovery contract cannot represent a required bounded v1 message. */
class InvalidAdmittedContractException(
    message: String,
) : RuntimeException("invalid_admitted_contract: $message"), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.PROTOCOL
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

/** The retained full historical base required by this transition is unavailable. */
class ResolutionBaseUnavailableException(val unit: String="",val sample: List<PushDependencyRecordRef> = emptyList(),val hasMore: Boolean=false) : RuntimeException("resolution_base_unavailable"), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
    val reason = "resolution_base_unavailable"
    var progress = SyncProgress(); internal set
}
