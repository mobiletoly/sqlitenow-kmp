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

internal data class RuntimeState(
    val validated: ValidatedConfig,
    val userId: String,
    val sourceId: String,
    val pendingInitializationId: String = "",
)

internal data class DirtySnapshotRow(
    val schemaName: String,
    val tableName: String,
    val keyJson: String,
    val baseRowVersion: Long,
    val payload: String?,
    val dirtyOrdinal: Long,
    val stateExists: Boolean,
    val stateDeleted: Boolean,
)

internal data class DirtyRowCapture(
    val schemaName: String,
    val tableName: String,
    val keyJson: String,
    val localPk: String,
    val wireKey: SyncKey,
    val op: String,
    val baseRowVersion: Long,
    val localPayload: String?,
    val dirtyOrdinal: Long,
)

internal data class DirtyRowRef(
    val schemaName: String,
    val tableName: String,
    val keyJson: String,
)

internal data class PreparedPush(
    val sourceBundleId: Long,
    val rows: List<DirtyRowCapture>,
    val discardedRows: List<DirtyRowRef> = emptyList(),
)

internal data class PushOutboundSnapshot(
    val sourceBundleId: Long,
    val rows: List<DirtyRowCapture>,
    val canonicalRows: List<OversqliteOutboxRow>,
    val canonicalRequestHash: String,
    val remoteBundleSeq: Long = 0,
    val remoteBundleHash: String = "",
)

internal val PushOutboundSnapshot.isRemoteCommitted: Boolean
    get() = remoteBundleSeq > 0L && remoteBundleHash.isNotBlank()

internal data class CommittedPushBundle(
    val bundleSeq: Long,
    val sourceId: String,
    val sourceBundleId: Long,
    val rowCount: Long,
	val bundleHash: String,
	val canonicalRequestHash: String,
)

internal data class CommittedReplayRow(
    val schemaName: String,
    val tableName: String,
    val keyJson: String,
    val localPk: String,
    val wireKey: SyncKey,
    val op: String,
    val rowVersion: Long,
    val payload: String?,
)

internal data class StagedSnapshotRow(
    val rowOrdinal: Long,
    val schemaName: String,
    val tableName: String,
    val keyJson: String,
    val rowVersion: Long,
    val payload: String,
)

internal data class SnapshotNegotiation(
    val maxRows: Int,
    val maxBytes: Long,
    val maxRowBytes: Long,
)

internal data class SnapshotTransferTotals(
    val rows: Long = 0,
    val bytes: Long = 0,
)

internal data class StagedSnapshotPage(
    val rows: List<StagedSnapshotRow>,
    val retainedTextBytes: Long,
) {
    val lastRowOrdinal: Long?
        get() = rows.lastOrNull()?.rowOrdinal
}

internal data class StructuredRowState(
    val exists: Boolean = false,
    val rowVersion: Long = 0,
    val deleted: Boolean = false,
)

internal data class DirtyUploadState(
    val exists: Boolean = false,
    val op: String = "",
    val payload: String? = null,
    val baseRowVersion: Long = 0,
    val currentOrdinal: Long = 0,
)

internal class DirtyStateRejectedException(
    private val dirtyCount: Long,
) : RuntimeException("cannot pull while $dirtyCount local dirty rows exist"), OversqliteCategorizedException {
    override val category: OversqliteErrorCategory = OversqliteErrorCategory.STATE
}

internal data class OversqliteCountEvaluation(
    val dirtyRowCount: Long,
    val outboundRowCount: Long,
    val sourceBundleRowCount: Long = outboundRowCount,
    val expectedOutboxRowCount: Long = outboundRowCount,
) {
    init {
        require(dirtyRowCount >= 0L)
        require(outboundRowCount >= 0L)
        require(sourceBundleRowCount >= 0L)
        require(expectedOutboxRowCount >= 0L)
    }

    val hasDirtyRows: Boolean = dirtyRowCount > 0L
    val hasOutboundRows: Boolean = outboundRowCount > 0L
    val hasPendingRows: Boolean = hasDirtyRows || hasOutboundRows
    val preparedOutboxMatches: Boolean =
        sourceBundleRowCount == outboundRowCount && sourceBundleRowCount == expectedOutboxRowCount

    val totalPendingRowCount: Long = checkCountSum(dirtyRowCount, outboundRowCount)
}

private fun checkCountSum(left: Long, right: Long): Long {
    require(left <= Long.MAX_VALUE - right) { "pending row count overflow" }
    return left + right
}
