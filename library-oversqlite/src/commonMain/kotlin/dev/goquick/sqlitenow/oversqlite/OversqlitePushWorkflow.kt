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
import kotlinx.coroutines.delay
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject

internal class OversqlitePushWorkflow(
    private val config: OversqliteConfig,
    private val resolver: Resolver,
    private val db: SafeSQLiteConnection,
    private val tableInfoCache: TableInfoCache,
    private val remoteApi: OversqliteRemoteApi,
    private val sourceStateStore: OversqliteSourceStateStore,
    private val attachmentStateStore: OversqliteAttachmentStateStore,
    private val outboxStateStore: OversqliteOutboxStateStore,
    private val syncStateStore: OversqliteSyncStateStore,
    private val localStore: OversqliteLocalStore,
    private val bundleApplier: OversqliteBundleApplier,
    private val stageStore: OversqliteStageStore,
    private val applyExecutor: OversqliteApplyExecutor,
    private val json: Json,
    private val log: ((() -> String)) -> Unit,
) {
    private companion object {
        const val MAX_PUSH_CONFLICT_AUTO_RETRIES = 2
        const val MAX_COMMITTED_BUNDLE_FETCH_ATTEMPTS = 6
        const val INITIAL_COMMITTED_BUNDLE_FETCH_BACKOFF_MILLIS = 75L
        const val MAX_COMMITTED_BUNDLE_FETCH_BACKOFF_MILLIS = 600L

    }

    private val replayExecutor = OversqlitePushReplayExecutor(
        syncStateStore = syncStateStore,
        bundleApplier = bundleApplier,
        localStore = localStore,
        sourceStateStore = sourceStateStore,
        attachmentStateStore = attachmentStateStore,
        outboxStateStore = outboxStateStore,
        json = json,
        log = log,
    )
    private val conflictExecutor = OversqliteConflictExecutor(localStore, syncStateStore, outboxStateStore)

    suspend fun pushPending(
        state: RuntimeState,
        allowCheckpointRecovery: Boolean = false,
    ): PushWorkflowResult {
		return pushPending(state, conflictRetryCount = 0, allowCheckpointRecovery = allowCheckpointRecovery)
    }

    private suspend fun pushPending(
        state: RuntimeState,
        conflictRetryCount: Int,
        allowCheckpointRecovery: Boolean,
    ): PushWorkflowResult {
        if (attachmentStateStore.loadState().rebuildRequired && !allowCheckpointRecovery) {
            throw RebuildRequiredException()
        }

        val initialDirtyCount = syncStateStore.countDirtyRows()
        val initialOutboundCount = outboxStateStore.countRows()
        log {
            "oversqlite pushPending start user=${state.userId} source=${state.sourceId} " +
                "pendingDirty=$initialDirtyCount pendingOutbound=$initialOutboundCount"
        }
        val snapshot = ensurePushOutboundSnapshot(state)
        if (snapshot.rows.isEmpty()) {
            log { "oversqlite pushPending no-op: no rows ready for upload" }
            return PushWorkflowResult(
                outcome = PushOutcome.NO_CHANGE,
                updatedTables = emptySet(),
            )
        }

        log {
            "oversqlite pushPending snapshot sourceBundleId=${snapshot.sourceBundleId} " +
                "rows=${snapshot.rows.size} ${snapshot.rows.joinToString(" | ") { it.toVerboseSummary() }}"
        }
        val committed = try {
            if (snapshot.isRemoteCommitted) {
                CommittedPushBundle(
                    bundleSeq = snapshot.remoteBundleSeq,
                    sourceId = state.sourceId,
                    sourceBundleId = snapshot.sourceBundleId,
					rowCount = snapshot.canonicalRows.size.toLong(),
					bundleHash = snapshot.remoteBundleHash,
					canonicalRequestHash = snapshot.canonicalRequestHash,
                )
            } else {
                commitPushOutboundSnapshot(state, snapshot)
            }
        } catch (e: PushConflictException) {
            val updatedTables = resolvePushConflict(state, snapshot, e)
            val remainingDirtyCount = syncStateStore.countDirtyRows()
            if (remainingDirtyCount == 0) {
                return PushWorkflowResult(
                    outcome = PushOutcome.NO_CHANGE,
                    updatedTables = updatedTables,
                )
            }
            if (conflictRetryCount >= MAX_PUSH_CONFLICT_AUTO_RETRIES) {
                throw PushConflictRetryExhaustedException(
                    retryCount = MAX_PUSH_CONFLICT_AUTO_RETRIES,
                    remainingDirtyCount = remainingDirtyCount,
                )
            }
            val retryResult = pushPending(
                state,
                conflictRetryCount = conflictRetryCount + 1,
                allowCheckpointRecovery = allowCheckpointRecovery,
            )
            return PushWorkflowResult(
                outcome = retryResult.outcome,
                updatedTables = updatedTables + retryResult.updatedTables,
            )
        }

        log {
            "oversqlite pushPending committed bundleSeq=${committed.bundleSeq} rowCount=${committed.rowCount} " +
                "sourceBundleId=${committed.sourceBundleId}"
        }
        val committedRows = fetchCommittedPushBundle(state, snapshot, committed)
        persistRemoteCommitAcknowledgement(state, snapshot, committed)
        val updatedTables = applyExecutor.inApplyModeTransaction(state) { statementCache ->
            replayExecutor.applyCommittedBundle(
                state = state,
                uploadedRows = snapshot.rows,
                committedRows = committedRows,
                committed = committed,
                statementCache = statementCache,
            )
        }
        log { "oversqlite pushPending finished updatedTables=$updatedTables" }
        return PushWorkflowResult(
            outcome = PushOutcome.COMMITTED,
            updatedTables = updatedTables,
        )
    }

    private suspend fun ensurePushOutboundSnapshot(state: RuntimeState): PushOutboundSnapshot {
        val outboxState = outboxStateStore.loadBundleState()
        if (outboxState.state != outboxStateNone) {
            val existing = loadPersistedOutboxSnapshot(state, outboxState)
            log {
                "oversqlite reusing frozen outbound snapshot sourceBundleId=${existing.sourceBundleId} rows=${existing.rows.size}"
            }
            return existing
        }

        val prepared = stageStore.preparePush(state, sourceStateStore.loadNextSourceBundleId(state.sourceId))
        log {
            "oversqlite collectDirtyRows prepared=${prepared.rows.size} noOps=${prepared.discardedRows.size} " +
                prepared.rows.joinToString(" | ") { it.toVerboseSummary() }
        }
        val frozen = persistPreparedOutboxSnapshot(state, prepared)
        if (frozen.rows.isNotEmpty()) {
            log {
                "oversqlite froze outbound snapshot sourceBundleId=${frozen.sourceBundleId} rows=${frozen.rows.size}"
            }
        }
        return frozen
    }

    private suspend fun commitPushOutboundSnapshot(
        state: RuntimeState,
        snapshot: PushOutboundSnapshot,
    ): CommittedPushBundle {
        require(snapshot.canonicalRows.isNotEmpty()) { "outbound push snapshot is empty" }

        val createResponse = remoteApi.createPushSession(
            sourceBundleId = snapshot.sourceBundleId,
			plannedRowCount = snapshot.canonicalRows.size.toLong(),
			canonicalRequestHash = snapshot.canonicalRequestHash,
            sourceId = state.sourceId,
            initializationId = state.pendingInitializationId.takeIf { it.isNotBlank() },
        )
        return when (createResponse.status) {
            "already_committed" -> committedPushBundleFromCreateResponse(
                createResponse,
                state.sourceId,
                snapshot.sourceBundleId,
            )
            "staging" -> {
                val pushId = createResponse.pushId.trim()
                require(pushId.isNotEmpty()) { "push session response missing push_id" }
                try {
                    val chunkSize = pushChunkRows()
                    var start = 0
                    while (start < snapshot.canonicalRows.size) {
                        val end = minOf(start + chunkSize, snapshot.canonicalRows.size)
                        val chunkResponse = remoteApi.uploadPushChunk(
                            pushId = pushId,
                            sourceId = state.sourceId,
                            request = PushSessionChunkRequest(
                                startRowOrdinal = start.toLong(),
                                rows = buildPushRequestRows(snapshot.canonicalRows.subList(start, end)),
                            ),
                        )
                        require(chunkResponse.pushId == pushId) {
                            "push chunk response push_id ${chunkResponse.pushId} does not match requested $pushId"
                        }
                        require(chunkResponse.nextExpectedRowOrdinal == end.toLong()) {
                            "push chunk response next_expected_row_ordinal ${chunkResponse.nextExpectedRowOrdinal} does not match expected $end"
                        }
                        start = end
                    }
                    val committed = committedPushBundleFromCommitResponse(
                        remoteApi.commitPushSession(pushId, state.sourceId),
                        state.sourceId,
                        snapshot.sourceBundleId,
                    )
                    persistRemoteCommitAcknowledgement(state, snapshot, committed)
                    committed
                } catch (t: Throwable) {
                    remoteApi.deletePushSessionBestEffort(pushId, state.sourceId)
                    throw t
                }
            }
            else -> error("unexpected push session status ${createResponse.status}")
        }
    }

    private suspend fun buildPushRequestRows(rows: List<OversqliteOutboxRow>): List<PushRequestRow> {
        return rows.map { dirty ->
            PushRequestRow(
                schema = dirty.schemaName,
                table = dirty.tableName,
                key = parseWireKey(dirty.wireKeyJson),
                op = dirty.op,
                baseRowVersion = dirty.baseRowVersion,
                payload = dirty.wirePayload?.let { json.parseToJsonElement(it) },
            )
        }
    }

    private suspend fun fetchCommittedPushBundle(
        state: RuntimeState,
        snapshot: PushOutboundSnapshot,
        committed: CommittedPushBundle,
    ): List<CommittedReplayRow> {
        var afterRowOrdinal: Long? = null
        var fetchedRowCount = 0L
        val committedRows = mutableListOf<CommittedReplayRow>()
        while (true) {
            val chunk = fetchCommittedBundleChunkWithRetry(committed, afterRowOrdinal)
            validateCommittedBundleRowsResponse(chunk, committed, afterRowOrdinal)
            committedRows += decodeCommittedBundleChunkRows(state, chunk)
            fetchedRowCount += chunk.rows.size
            if (!chunk.hasMore) {
                break
            }
            afterRowOrdinal = chunk.nextRowOrdinal
        }

        require(fetchedRowCount == committed.rowCount) {
            "fetched committed bundle row count $fetchedRowCount does not match expected row_count ${committed.rowCount}"
        }
        val bundleHash = computeCommittedBundleHash(committedRows)
        require(bundleHash == committed.bundleHash) {
            "fetched committed bundle hash $bundleHash does not match expected ${committed.bundleHash}"
        }
		if (committed.canonicalRequestHash != snapshot.canonicalRequestHash) {
            throw SourceSequenceMismatchException(
                "committed canonical_request_hash ${committed.canonicalRequestHash} does not match prepared outbox hash ${snapshot.canonicalRequestHash}",
            )
		}
        return committedRows
    }

    private suspend fun fetchCommittedBundleChunkWithRetry(
        committed: CommittedPushBundle,
        afterRowOrdinal: Long?,
    ): CommittedBundleRowsResponse {
        var attempt = 1
        var backoffMillis = INITIAL_COMMITTED_BUNDLE_FETCH_BACKOFF_MILLIS
        while (true) {
            try {
                return remoteApi.fetchCommittedBundleChunk(
                    bundleSeq = committed.bundleSeq,
                    sourceId = committed.sourceId,
                    afterRowOrdinal = afterRowOrdinal,
                    maxRows = committedBundleChunkRows(),
                )
            } catch (error: CommittedBundleNotFoundException) {
                if (attempt >= MAX_COMMITTED_BUNDLE_FETCH_ATTEMPTS) {
                    throw error
                }
                log {
                    "oversqlite committed bundle not visible yet bundleSeq=${committed.bundleSeq} " +
                        "afterRowOrdinal=${afterRowOrdinal ?: -1L} attempt=$attempt/$MAX_COMMITTED_BUNDLE_FETCH_ATTEMPTS " +
                        "retryInMs=$backoffMillis"
                }
                delay(backoffMillis)
                backoffMillis = minOf(backoffMillis * 2, MAX_COMMITTED_BUNDLE_FETCH_BACKOFF_MILLIS)
                attempt++
            }
        }
    }

    private suspend fun decodeCommittedBundleChunkRows(
        state: RuntimeState,
        chunk: CommittedBundleRowsResponse,
    ): List<CommittedReplayRow> {
        return chunk.rows.map { row ->
            require(row.schema == state.validated.schema) {
                "committed bundle row schema ${row.schema} does not match client schema ${state.validated.schema}"
            }
            validateBundleRow(row)
            val (keyJson, localPk) = localStore.bundleRowKeyToLocalKey(state, row.table, row.key)
            CommittedReplayRow(
                schemaName = row.schema,
                tableName = row.table,
                keyJson = keyJson,
                localPk = localPk,
                wireKey = row.key,
                op = row.op,
                rowVersion = row.rowVersion,
                payload = if (row.op == "DELETE" || row.payload == null) null else row.payload.toString(),
            )
        }
    }

    private fun computeCommittedBundleHash(rows: List<CommittedReplayRow>): String {
        val logicalRows = rows.mapIndexed { index, row ->
            buildJsonObject {
				put("row_ordinal", JsonPrimitive(index.toString()))
                put("schema", JsonPrimitive(row.schemaName))
                put("table", JsonPrimitive(row.tableName))
                put("key", syncKeyToJsonElement(row.wireKey))
                put("op", JsonPrimitive(row.op))
				put("row_version", JsonPrimitive(row.rowVersion.toString()))
                put("payload", row.payload?.let { json.parseToJsonElement(it) } ?: JsonNull)
            }
        }
        return sha256Hex(canonicalizeJsonElement(JsonArray(logicalRows)).encodeToByteArray())
    }

    private suspend fun loadPersistedOutboxSnapshot(
        state: RuntimeState,
        outboxState: OversqliteOutboxBundleState,
    ): PushOutboundSnapshot {
        require(outboxState.sourceId == state.sourceId) {
            "persisted outbox source ${outboxState.sourceId} does not match current source ${state.sourceId}"
        }
        if (outboxState.state == outboxStateCommittedRemote) {
            require(outboxState.remoteBundleSeq > 0L) {
                "committed_remote outbox is missing remote_bundle_seq"
            }
            require(outboxState.remoteBundleHash.isNotBlank()) {
                "committed_remote outbox is missing remote_bundle_hash"
            }
        }
        val outboxRows = outboxStateStore.loadRows()
        require(outboxRows.isNotEmpty()) {
            "_sync_outbox_bundle state ${outboxState.state} exists without rows"
        }
        require(outboxRows.all { it.sourceBundleId == outboxState.sourceBundleId }) {
            "_sync_outbox_rows source_bundle_id does not match _sync_outbox_bundle"
        }
        require(outboxRows.size.toLong() == outboxState.rowCount) {
            "_sync_outbox_rows row count ${outboxRows.size} does not match _sync_outbox_bundle ${outboxState.rowCount}"
        }
        val canonicalRequestHash = computeCanonicalRequestHash(outboxRows)
        require(canonicalRequestHash == outboxState.canonicalRequestHash) {
            "persisted canonical outbox hash $canonicalRequestHash does not match expected ${outboxState.canonicalRequestHash}"
        }
        val rows = outboxRows.map { row ->
            val (localPk, _) = localStore.decodeDirtyKeyForPush(state, row.tableName, row.keyJson)
            DirtyRowCapture(
                schemaName = row.schemaName,
                tableName = row.tableName,
                keyJson = row.keyJson,
                localPk = localPk,
                wireKey = parseWireKey(row.wireKeyJson),
                op = row.op,
                baseRowVersion = row.baseRowVersion,
                localPayload = row.localPayload,
                dirtyOrdinal = row.rowOrdinal,
            )
        }
        return PushOutboundSnapshot(
            sourceBundleId = outboxState.sourceBundleId,
            rows = rows,
            canonicalRows = outboxRows,
            canonicalRequestHash = canonicalRequestHash,
            remoteBundleSeq = outboxState.remoteBundleSeq,
            remoteBundleHash = outboxState.remoteBundleHash,
        )
    }

    private suspend fun persistPreparedOutboxSnapshot(
        state: RuntimeState,
        prepared: PreparedPush,
    ): PushOutboundSnapshot {
        val canonicalRows = buildCanonicalOutboxRows(prepared.sourceBundleId, prepared.rows)
        val canonicalRequestHash = canonicalRows.takeIf { it.isNotEmpty() }?.let(::computeCanonicalRequestHash) ?: ""
        val allMovedRows = (prepared.rows.map { it.toRowRef() } + prepared.discardedRows).distinct()
        db.transaction(TransactionMode.IMMEDIATE) {
            val statementCache = StatementCache(db)
            try {
                outboxStateStore.clearBundleAndRows(statementCache)
                if (canonicalRows.isNotEmpty()) {
                    outboxStateStore.persistBundleState(
                        state = OversqliteOutboxBundleState(
                            state = outboxStatePrepared,
                            sourceId = state.sourceId,
                            sourceBundleId = prepared.sourceBundleId,
                            initializationId = state.pendingInitializationId,
                            canonicalRequestHash = canonicalRequestHash,
                            rowCount = canonicalRows.size.toLong(),
                        ),
                        statementCache = statementCache,
                    )
                    outboxStateStore.appendRows(canonicalRows, statementCache)
                }
                for (row in allMovedRows) {
                    syncStateStore.deleteDirtyRow(row.schemaName, row.tableName, row.keyJson, statementCache)
                }
            } finally {
                statementCache.close()
            }
        }
        return PushOutboundSnapshot(
            sourceBundleId = prepared.sourceBundleId,
            rows = prepared.rows,
            canonicalRows = canonicalRows,
            canonicalRequestHash = canonicalRequestHash,
        )
    }

    private suspend fun buildCanonicalOutboxRows(
        sourceBundleId: Long,
        rows: List<DirtyRowCapture>,
    ): List<OversqliteOutboxRow> {
        return rows.mapIndexed { index, row ->
            val wirePayload = if (row.op == "DELETE") {
                null
            } else {
                canonicalizeJsonElement(
                    localStore.processPayloadForUpload(row.tableName, row.localPayload ?: ""),
                )
            }
            OversqliteOutboxRow(
                sourceBundleId = sourceBundleId,
                rowOrdinal = index.toLong(),
                schemaName = row.schemaName,
                tableName = row.tableName,
                keyJson = row.keyJson,
                wireKeyJson = canonicalizeJsonElement(syncKeyToJsonElement(row.wireKey)),
                op = row.op,
                baseRowVersion = row.baseRowVersion,
                localPayload = row.localPayload,
                wirePayload = wirePayload,
            )
        }
    }

	private suspend fun persistRemoteCommitAcknowledgement(
        state: RuntimeState,
        snapshot: PushOutboundSnapshot,
        committed: CommittedPushBundle,
	) {
		if (committed.canonicalRequestHash != snapshot.canonicalRequestHash) {
            throw SourceSequenceMismatchException(
                "committed canonical_request_hash ${committed.canonicalRequestHash} does not match prepared outbox hash ${snapshot.canonicalRequestHash}",
            )
		}
        outboxStateStore.persistBundleState(
            OversqliteOutboxBundleState(
                state = outboxStateCommittedRemote,
                sourceId = state.sourceId,
                sourceBundleId = snapshot.sourceBundleId,
                initializationId = state.pendingInitializationId,
                canonicalRequestHash = snapshot.canonicalRequestHash,
                rowCount = snapshot.canonicalRows.size.toLong(),
                remoteBundleHash = committed.bundleHash,
                remoteBundleSeq = committed.bundleSeq,
            ),
        )
    }

    private fun computeCanonicalRequestHash(rows: List<OversqliteOutboxRow>): String {
        val logicalRows = rows.map { row ->
            buildJsonObject {
				put("row_ordinal", JsonPrimitive(row.rowOrdinal.toString()))
                put("schema", JsonPrimitive(row.schemaName))
                put("table", JsonPrimitive(row.tableName))
                put("key", syncKeyToJsonElement(parseWireKey(row.wireKeyJson)))
                put("op", JsonPrimitive(row.op))
				put("base_row_version", JsonPrimitive(row.baseRowVersion.toString()))
                put("payload", row.wirePayload?.let { json.parseToJsonElement(it) } ?: JsonNull)
            }
        }
        return sha256Hex(canonicalizeJsonElement(JsonArray(logicalRows)).encodeToByteArray())
    }

    private fun parseWireKey(wireKeyJson: String): SyncKey {
        val parsed = json.parseToJsonElement(wireKeyJson) as? JsonObject
            ?: error("wire key must be a JSON object")
        return parsed.entries.associate { (key, value) ->
            val primitive = value as? JsonPrimitive
                ?: error("wire key $key must be a JSON string")
            key to primitive.content
        }
    }

    private fun syncKeyToJsonElement(key: SyncKey): JsonElement {
        return buildJsonObject {
            for ((entryKey, entryValue) in key.entries.sortedBy { it.key }) {
                put(entryKey, JsonPrimitive(entryValue))
            }
        }
    }

    private suspend fun resolvePushConflict(
        state: RuntimeState,
        snapshot: PushOutboundSnapshot,
        exception: PushConflictException,
    ): Set<String> {
        val conflict = exception.conflict
        val conflictingRow = snapshot.rows.firstOrNull {
            it.schemaName == conflict.schema &&
                it.tableName == conflict.table &&
                it.wireKey == conflict.key
        } ?: throw exception

        val localPayload = parseDirtyRowPayloadAsObject(conflictingRow)
        val context = ConflictContext(
            schema = conflict.schema,
            table = conflict.table,
            key = conflict.key,
            localOp = conflictingRow.op,
            localPayload = localPayload,
            baseRowVersion = conflict.baseRowVersion,
            serverRowVersion = conflict.serverRowVersion,
            serverRowDeleted = conflict.serverRowDeleted,
            serverRow = conflict.serverRow,
        )
        val resolution = resolver.resolve(context)
        val expectedColumns = if (resolution is MergeResult.KeepMerged) {
            tableInfoCache.get(db, context.table).columnNamesLower.toSet()
        } else {
            emptySet()
        }

        val decision = buildConflictPlanDecision(
            context = context,
            result = resolution,
            localPayload = localPayload,
            expectedColumns = expectedColumns,
        )
        return when (decision) {
            is ConflictPlanDecision.Apply -> applyExecutor.inApplyModeTransaction(state) { statementCache ->
                conflictExecutor.applyConflictResolutionPlan(
                    state = state,
                    snapshot = snapshot,
                    conflictingRow = conflictingRow,
                    tableName = context.table,
                    plan = decision.plan,
                    statementCache = statementCache,
                )
            }

            is ConflictPlanDecision.Invalid -> {
                restoreOutboundSnapshotToDirtyRows(state, snapshot)
                throw InvalidConflictResolutionException(conflict = context, result = resolution, message = decision.message)
            }
        }
    }

    private suspend fun restoreOutboundSnapshotToDirtyRows(
        state: RuntimeState,
        snapshot: PushOutboundSnapshot,
    ) {
        applyExecutor.inApplyModeTransaction(state) { statementCache ->
            conflictExecutor.restoreOutboundSnapshotToDirtyRows(snapshot, statementCache)
        }
    }

    private fun parseDirtyRowPayloadAsObject(row: DirtyRowCapture): JsonObject? {
        val payloadText = row.localPayload ?: return null
        return json.parseToJsonElement(payloadText) as? JsonObject
            ?: error("local ${row.op} conflict payload must be a JSON object")
    }

    private fun pushChunkRows(): Int = config.uploadLimit.takeIf { it > 0 } ?: 1000

    private fun committedBundleChunkRows(): Int = config.downloadLimit.takeIf { it > 0 } ?: 1000
}

internal data class PushWorkflowResult(
    val outcome: PushOutcome,
    val updatedTables: Set<String>,
)
