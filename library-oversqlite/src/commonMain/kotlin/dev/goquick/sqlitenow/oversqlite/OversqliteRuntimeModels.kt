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

internal data class CanonicalOutboxComparableRow(
    val rowOrdinal: Long,
    val schemaName: String,
    val tableName: String,
    val wireKey: SyncKey,
    val op: String,
    val wirePayload: String?,
)

internal data class CommittedPushBundle(
    val bundleSeq: Long,
    val sourceId: String,
    val sourceBundleId: Long,
    val rowCount: Long,
    val bundleHash: String,
    val requiresStrictOutboxMatch: Boolean = false,
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
    val schemaName: String,
    val tableName: String,
    val keyJson: String,
    val localPk: String,
    val wireKey: SyncKey,
    val rowVersion: Long,
    val payload: String,
)

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
    private val dirtyCount: Int,
) : RuntimeException("cannot pull while $dirtyCount local dirty rows exist")

internal fun DirtyRowCapture.toVerboseSummary(): String {
    return "table=$tableName op=$op key=$wireKey baseRowVersion=$baseRowVersion " +
        "dirtyOrdinal=$dirtyOrdinal payloadLength=${localPayload?.length ?: 0}"
}

internal fun PushRequestRow.toVerboseSummary(): String {
    return "table=$table op=$op key=$key baseRowVersion=$baseRowVersion payload=${
        payload?.toString() ?: "null"
    }"
}
