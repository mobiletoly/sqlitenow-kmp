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
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

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

        val RFC3339_TIMESTAMP_REGEX = Regex(
            "^(\\d{4}-\\d{2}-\\d{2})T(\\d{2}:\\d{2}:\\d{2})(?:\\.\\d+)?(?:Z|[+-]\\d{2}:\\d{2})$",
        )
    }

    private val replayExecutor = OversqlitePushReplayExecutor(
        db = db,
        tableInfoCache = tableInfoCache,
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

    suspend fun pushPending(state: RuntimeState): PushWorkflowResult {
        return pushPending(state, conflictRetryCount = 0)
    }

    private suspend fun pushPending(
        state: RuntimeState,
        conflictRetryCount: Int,
    ): PushWorkflowResult {
        if (attachmentStateStore.loadState().rebuildRequired) {
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
            val retryResult = pushPending(state, conflictRetryCount = conflictRetryCount + 1)
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
            sourceId = state.sourceId,
            initializationId = state.pendingInitializationId.takeIf { it.isNotBlank() },
        )
        return when (createResponse.status) {
            "already_committed" -> committedPushBundleFromCreateResponse(createResponse, state.sourceId, snapshot.sourceBundleId)
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
        assertCommittedBundleMatchesOutbox(snapshot, committed, committedRows)
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
                put("row_ordinal", JsonPrimitive(index.toLong()))
                put("schema", JsonPrimitive(row.schemaName))
                put("table", JsonPrimitive(row.tableName))
                put("key", syncKeyToJsonElement(row.wireKey))
                put("op", JsonPrimitive(row.op))
                put("row_version", JsonPrimitive(row.rowVersion))
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

    private suspend fun assertCommittedBundleMatchesOutbox(
        snapshot: PushOutboundSnapshot,
        committed: CommittedPushBundle,
        committedRows: List<CommittedReplayRow>,
    ) {
        val actualRows = committedRows.mapIndexed { index, row ->
            CanonicalOutboxComparableRow(
                rowOrdinal = index.toLong(),
                schemaName = row.schemaName,
                tableName = row.tableName,
                wireKey = row.wireKey,
                op = row.op,
                wirePayload = row.payload?.let { canonicalizeJsonElement(json.parseToJsonElement(it)) },
            )
        }
        val expectedRows = snapshot.canonicalRows.map { row ->
            CanonicalOutboxComparableRow(
                rowOrdinal = row.rowOrdinal,
                schemaName = row.schemaName,
                tableName = row.tableName,
                wireKey = parseWireKey(row.wireKeyJson),
                op = row.op,
                wirePayload = row.wirePayload,
            )
        }
        val requirePayloadMatch = snapshot.isRemoteCommitted || committed.requiresStrictOutboxMatch
        val firstMismatchIndex = when {
            actualRows.size != expectedRows.size -> minOf(actualRows.size, expectedRows.size)
            else -> actualRows.indices.firstOrNull { index ->
                !equivalentComparableRows(
                    expected = expectedRows[index],
                    actual = actualRows[index],
                    requirePayloadMatch = requirePayloadMatch,
                )
            }
        }
        if (firstMismatchIndex != null) {
            val expectedRow = expectedRows.getOrNull(firstMismatchIndex)
            val actualRow = actualRows.getOrNull(firstMismatchIndex)
            throw SourceSequenceMismatchException(
                "committed bundle for source ${committed.sourceId} source_bundle_id ${committed.sourceBundleId} " +
                    "does not match the canonical prepared outbox rows at index $firstMismatchIndex; " +
                    "expected=$expectedRow actual=$actualRow"
            )
        }
    }

    private suspend fun equivalentComparableRows(
        expected: CanonicalOutboxComparableRow,
        actual: CanonicalOutboxComparableRow,
        requirePayloadMatch: Boolean,
    ): Boolean {
        return expected.rowOrdinal == actual.rowOrdinal &&
            expected.schemaName == actual.schemaName &&
            expected.tableName == actual.tableName &&
            expected.wireKey == actual.wireKey &&
            expected.op == actual.op &&
            (
                !requirePayloadMatch ||
                    equivalentCommittedPayload(
                        tableName = expected.tableName,
                        expectedPayload = expected.wirePayload,
                        actualPayload = actual.wirePayload,
                    )
                )
    }

    private suspend fun equivalentCommittedPayload(
        tableName: String,
        expectedPayload: String?,
        actualPayload: String?,
    ): Boolean {
        if (expectedPayload == null && actualPayload == null) return true
        if (expectedPayload == null || actualPayload == null) return false
        val tableInfo = tableInfoCache.get(db, tableName)
        val expectedObject = json.parseToJsonElement(expectedPayload) as? JsonObject ?: return false
        val actualObject = json.parseToJsonElement(actualPayload) as? JsonObject ?: return false
        if (expectedObject.size != tableInfo.columns.size || actualObject.size != tableInfo.columns.size) {
            return false
        }
        return tableInfo.columns.all { column ->
            val expectedValue = expectedObject[column.name.lowercase()] ?: expectedObject[column.name] ?: return@all false
            val actualValue = actualObject[column.name.lowercase()] ?: actualObject[column.name] ?: return@all false
            OversqliteValueCodec.equivalent(
                column = column,
                left = expectedValue,
                right = actualValue,
                leftSource = PayloadSource.AUTHORITATIVE_WIRE,
                rightSource = PayloadSource.AUTHORITATIVE_WIRE,
            ) || rfc3339InstantEquivalent(expectedValue, actualValue)
        }
    }

    /**
     * Replay safety gives semantic equality only to explicit RFC3339 instants.
     *
     * Naive/local timestamp text remains opaque payload text and must not be normalized here.
     */
    @OptIn(ExperimentalTime::class)
    private fun rfc3339InstantEquivalent(
        expectedValue: JsonElement,
        actualValue: JsonElement,
    ): Boolean {
        val expectedPrimitive = expectedValue as? JsonPrimitive ?: return false
        val actualPrimitive = actualValue as? JsonPrimitive ?: return false
        if (!expectedPrimitive.isString || !actualPrimitive.isString) return false
        val expectedInstant = parseRfc3339InstantOrNull(expectedPrimitive.content)
        val actualInstant = parseRfc3339InstantOrNull(actualPrimitive.content)
        return expectedInstant != null && actualInstant != null && expectedInstant == actualInstant
    }

    @OptIn(ExperimentalTime::class)
    private fun parseRfc3339InstantOrNull(value: String): Instant? {
        val trimmed = value.trim()
        if (!RFC3339_TIMESTAMP_REGEX.matches(trimmed)) return null
        return runCatching { Instant.parse(trimmed) }.getOrNull()
    }

    private fun computeCanonicalRequestHash(rows: List<OversqliteOutboxRow>): String {
        val logicalRows = rows.map { row ->
            buildJsonObject {
                put("row_ordinal", JsonPrimitive(row.rowOrdinal))
                put("schema", JsonPrimitive(row.schemaName))
                put("table", JsonPrimitive(row.tableName))
                put("key", syncKeyToJsonElement(parseWireKey(row.wireKeyJson)))
                put("op", JsonPrimitive(row.op))
                put("base_row_version", JsonPrimitive(row.baseRowVersion))
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

internal sealed class ReplayRowAction {
    data object AcceptAuthoritative : ReplayRowAction()

    data class PreserveLocal(
        val requeueOp: String,
        val requeuePayload: String?,
    ) : ReplayRowAction()
}

internal data class ReplayPlanDiagnostics(
    val pendingExists: Boolean,
    val pendingMatches: Boolean,
    val livePayload: String?,
    val liveMatches: Boolean,
    val currentDirtyPayload: String?,
)

internal data class UploadedReplayPlan(
    val rowRef: DirtyRowRef,
    val action: ReplayRowAction,
    val diagnostics: ReplayPlanDiagnostics,
)

internal class OversqliteReplayPlanner(
    private val json: Json,
) {
    fun plan(
        tableInfo: TableInfo,
        uploaded: DirtyRowCapture,
        pending: DirtyUploadState,
        livePayload: String?,
    ): UploadedReplayPlan {
        val pendingMatches = pending.exists &&
            pending.op == uploaded.op &&
            equivalentLocalPayload(tableInfo, pending.payload, uploaded.localPayload)
        val liveMatches = livePayloadMatchesUploadedIntent(tableInfo, uploaded, livePayload)
        val diagnostics = ReplayPlanDiagnostics(
            pendingExists = pending.exists,
            pendingMatches = pendingMatches,
            livePayload = livePayload,
            liveMatches = liveMatches,
            currentDirtyPayload = pending.payload,
        )
        return UploadedReplayPlan(
            rowRef = uploaded.toRowRef(),
            action = when (uploaded.op) {
                "DELETE" -> {
                    if (livePayload != null) {
                        ReplayRowAction.PreserveLocal(
                            requeueOp = "INSERT",
                            requeuePayload = livePayload,
                        )
                    } else {
                        ReplayRowAction.AcceptAuthoritative
                    }
                }

                else -> {
                    when {
                        livePayload == null -> ReplayRowAction.PreserveLocal(
                            requeueOp = "DELETE",
                            requeuePayload = null,
                        )

                        pending.exists && !pendingMatches -> ReplayRowAction.PreserveLocal(
                            requeueOp = "UPDATE",
                            requeuePayload = livePayload,
                        )

                        !liveMatches -> ReplayRowAction.PreserveLocal(
                            requeueOp = "UPDATE",
                            requeuePayload = livePayload,
                        )

                        else -> ReplayRowAction.AcceptAuthoritative
                    }
                }
            },
            diagnostics = diagnostics,
        )
    }

    private fun livePayloadMatchesUploadedIntent(
        tableInfo: TableInfo,
        uploaded: DirtyRowCapture,
        livePayload: String?,
    ): Boolean {
        return if (uploaded.op == "DELETE") {
            livePayload == null
        } else {
            livePayload != null && equivalentLocalPayload(tableInfo, livePayload, uploaded.localPayload)
        }
    }

    private fun equivalentLocalPayload(
        tableInfo: TableInfo,
        left: String?,
        right: String?,
    ): Boolean {
        return OversqliteValueCodec.equivalentPayloadTexts(
            tableInfo = tableInfo,
            left = left,
            right = right,
            leftSource = PayloadSource.LOCAL_STATE,
            rightSource = PayloadSource.LOCAL_STATE,
            json = json,
        )
    }
}

internal class OversqlitePushReplayExecutor(
    private val db: SafeSQLiteConnection,
    private val tableInfoCache: TableInfoCache,
    private val syncStateStore: OversqliteSyncStateStore,
    private val bundleApplier: OversqliteBundleApplier,
    private val localStore: OversqliteLocalStore,
    private val sourceStateStore: OversqliteSourceStateStore,
    private val attachmentStateStore: OversqliteAttachmentStateStore,
    private val outboxStateStore: OversqliteOutboxStateStore,
    private val json: Json,
    private val log: ((() -> String)) -> Unit,
) {
    private val planner = OversqliteReplayPlanner(json)

    suspend fun applyCommittedBundle(
        state: RuntimeState,
        uploadedRows: List<DirtyRowCapture>,
        committedRows: List<CommittedReplayRow>,
        committed: CommittedPushBundle,
        statementCache: StatementCache,
    ): Set<String> {
        require(committedRows.size.toLong() == committed.rowCount) {
            "committed push replay row count ${committedRows.size} does not match expected row_count ${committed.rowCount}"
        }

        val plans = buildReplayPlans(uploadedRows, statementCache)
        val updatedTables = linkedSetOf<String>()
        for (row in committedRows) {
            val bundleRow = BundleRow(
                schema = row.schemaName,
                table = row.tableName,
                key = row.wireKey,
                op = row.op,
                rowVersion = row.rowVersion,
                payload = row.payload?.let { json.parseToJsonElement(it) },
            )
            when (val action = plans[row.toRowRef()]?.action) {
                null -> bundleApplier.applyAuthoritativeRow(state, bundleRow, row.localPk, row.keyJson, statementCache)
                ReplayRowAction.AcceptAuthoritative -> {
                    bundleApplier.applyAuthoritativeRow(state, bundleRow, row.localPk, row.keyJson, statementCache)
                    syncStateStore.deleteDirtyRow(row.schemaName, row.tableName, row.keyJson, statementCache)
                }
                is ReplayRowAction.PreserveLocal -> {
                    syncStateStore.updateStructuredRowState(
                        schemaName = state.validated.schema,
                        tableName = row.tableName,
                        keyJson = row.keyJson,
                        rowVersion = row.rowVersion,
                        deleted = row.op == "DELETE",
                        statementCache = statementCache,
                    )
                    syncStateStore.requeueDirtyIntent(
                        schemaName = row.schemaName,
                        tableName = row.tableName,
                        keyJson = row.keyJson,
                        op = action.requeueOp,
                        baseRowVersion = row.rowVersion,
                        payload = action.requeuePayload,
                        statementCache = statementCache,
                    )
                }
            }
            updatedTables += row.tableName.lowercase()
        }

        sourceStateStore.advanceAfterCommittedPush(state.sourceId, committed.sourceBundleId)
        attachmentStateStore.advanceAfterCommittedPush(committed.bundleSeq, statementCache)
        outboxStateStore.clearBundleAndRows(statementCache)
        assertReplayResidualDirtyState(uploadedRows, plans, statementCache)
        return updatedTables
    }

    private suspend fun buildReplayPlans(
        uploadedRows: List<DirtyRowCapture>,
        statementCache: StatementCache,
    ): Map<DirtyRowRef, UploadedReplayPlan> {
        return buildMap {
            for (uploaded in uploadedRows) {
                val tableInfo = tableInfoCache.get(db, uploaded.tableName)
                val currentDirty = syncStateStore.loadDirtyUploadState(
                    schemaName = uploaded.schemaName,
                    tableName = uploaded.tableName,
                    keyJson = uploaded.keyJson,
                    statementCache = statementCache,
                )
                val livePayload = localStore.serializeExistingRow(
                    tableName = uploaded.tableName,
                    localPk = uploaded.localPk,
                    statementCache = statementCache,
                )
                val plan = planner.plan(tableInfo, uploaded, currentDirty, livePayload)
                if (plan.action is ReplayRowAction.PreserveLocal ||
                    plan.diagnostics.pendingExists ||
                    !plan.diagnostics.liveMatches
                ) {
                    log { replayDecisionLog(uploaded, plan) }
                }
                put(plan.rowRef, plan)
            }
        }
    }

    private suspend fun assertReplayResidualDirtyState(
        uploadedRows: List<DirtyRowCapture>,
        plans: Map<DirtyRowRef, UploadedReplayPlan>,
        statementCache: StatementCache,
    ) {
        for (uploaded in uploadedRows) {
            val plan = plans[uploaded.toRowRef()]
                ?: error("missing replay plan for ${uploaded.tableName} key=${uploaded.wireKey}")
            val dirty = syncStateStore.loadDirtyUploadState(
                schemaName = uploaded.schemaName,
                tableName = uploaded.tableName,
                keyJson = uploaded.keyJson,
                statementCache = statementCache,
            )
            when (plan.action) {
                ReplayRowAction.AcceptAuthoritative -> require(!dirty.exists) {
                    "successful push replay left unexpected dirty row for ${uploaded.tableName} key=${uploaded.wireKey} " +
                        "uploadedOp=${uploaded.op} uploadedPayload=${uploaded.localPayload ?: "null"} " +
                        "currentDirtyOp=${dirty.op} currentDirtyPayload=${dirty.payload ?: "null"}"
                }

                is ReplayRowAction.PreserveLocal -> require(dirty.exists) {
                    "successful push replay lost preserved dirty row for ${uploaded.tableName} key=${uploaded.wireKey} " +
                        "requeueOp=${plan.action.requeueOp} expectedPayload=${plan.action.requeuePayload ?: "null"}"
                }
            }
        }
    }

    private fun replayDecisionLog(
        uploaded: DirtyRowCapture,
        plan: UploadedReplayPlan,
    ): String {
        val diagnostics = plan.diagnostics
        return buildString {
            append("oversqlite replay decision ")
            append("table=").append(uploaded.tableName)
            append(" key=").append(uploaded.wireKey)
            append(" pendingExists=").append(diagnostics.pendingExists)
            append(" pendingMatches=").append(diagnostics.pendingMatches)
            append(" liveExists=").append(diagnostics.livePayload != null)
            append(" liveMatches=").append(diagnostics.liveMatches)
            when (val action = plan.action) {
                ReplayRowAction.AcceptAuthoritative -> {
                    append(" needsRequeue=false")
                    append(" requeueOp=<none>")
                }
                is ReplayRowAction.PreserveLocal -> {
                    append(" needsRequeue=true")
                    append(" requeueOp=").append(action.requeueOp)
                }
            }
            append(" uploadedPayload=").append(uploaded.localPayload ?: "null")
            append(" livePayload=").append(diagnostics.livePayload ?: "null")
            if (diagnostics.pendingExists) {
                append(" currentDirtyPayload=").append(diagnostics.currentDirtyPayload ?: "null")
            }
        }
    }
}

private class OversqliteConflictExecutor(
    private val localStore: OversqliteLocalStore,
    private val syncStateStore: OversqliteSyncStateStore,
    private val outboxStateStore: OversqliteOutboxStateStore,
) {
    suspend fun applyConflictResolutionPlan(
        state: RuntimeState,
        snapshot: PushOutboundSnapshot,
        conflictingRow: DirtyRowCapture,
        tableName: String,
        plan: ConflictResolutionPlan,
        statementCache: StatementCache,
    ): Set<String> {
        val updatedTables = linkedSetOf(tableName.lowercase())

        when (val action = plan.localRowAction) {
            is LocalRowAction.Upsert -> localStore.upsertRow(tableName, action.payload, action.payloadSource, statementCache)
            LocalRowAction.Delete -> localStore.deleteLocalRow(state, tableName, conflictingRow.localPk, statementCache)
        }

        when (val action = plan.rowStateAction) {
            RowStateAction.Delete -> syncStateStore.deleteStructuredRowState(
                state.validated.schema,
                tableName,
                conflictingRow.keyJson,
                statementCache,
            )
            is RowStateAction.Upsert -> syncStateStore.updateStructuredRowState(
                schemaName = state.validated.schema,
                tableName = tableName,
                keyJson = conflictingRow.keyJson,
                rowVersion = action.rowVersion,
                deleted = action.deleted,
                statementCache = statementCache,
            )
        }

        val normalizedLocalPayload = when (plan.localRowAction) {
            is LocalRowAction.Upsert -> localStore.serializeExistingRow(tableName, conflictingRow.localPk, statementCache)
            LocalRowAction.Delete -> null
        }

        syncStateStore.requeueSnapshotRows(snapshot, skipRow = conflictingRow, statementCache = statementCache)
        plan.requeueIntent?.let { intent ->
            val payload = when (intent.payloadSource) {
                RetryPayloadSource.LocalRow ->
                    normalizedLocalPayload ?: error("resolved payload for $tableName must exist before requeue")
                RetryPayloadSource.None -> null
            }
            syncStateStore.requeueDirtyIntent(
                schemaName = conflictingRow.schemaName,
                tableName = conflictingRow.tableName,
                keyJson = conflictingRow.keyJson,
                op = intent.op,
                baseRowVersion = intent.baseRowVersion,
                payload = payload,
                statementCache = statementCache,
            )
        }
        outboxStateStore.clearBundleAndRows(statementCache)
        return updatedTables
    }

    suspend fun restoreOutboundSnapshotToDirtyRows(
        snapshot: PushOutboundSnapshot,
        statementCache: StatementCache,
    ) {
        syncStateStore.requeueSnapshotRows(snapshot, statementCache = statementCache)
        outboxStateStore.clearBundleAndRows(statementCache)
    }
}

private sealed class ConflictPlanDecision {
    data class Apply(val plan: ConflictResolutionPlan) : ConflictPlanDecision()

    data class Invalid(val message: String) : ConflictPlanDecision()
}

private data class ConflictResolutionPlan(
    val localRowAction: LocalRowAction,
    val rowStateAction: RowStateAction,
    val requeueIntent: ResolvedDirtyIntent? = null,
)

private sealed class LocalRowAction {
    data class Upsert(
        val payload: JsonObject,
        val payloadSource: PayloadSource,
    ) : LocalRowAction()

    data object Delete : LocalRowAction()
}

private sealed class RowStateAction {
    data class Upsert(
        val rowVersion: Long,
        val deleted: Boolean,
    ) : RowStateAction()

    data object Delete : RowStateAction()
}

private data class ResolvedDirtyIntent(
    val op: String,
    val baseRowVersion: Long,
    val payloadSource: RetryPayloadSource,
)

private enum class RetryPayloadSource {
    LocalRow,
    None,
}

private sealed class AuthoritativeConflictState {
    data object Missing : AuthoritativeConflictState()

    data class Deleted(val rowVersion: Long) : AuthoritativeConflictState()

    data class Present(
        val rowVersion: Long,
        val payload: JsonObject,
    ) : AuthoritativeConflictState()
}

private fun buildConflictPlanDecision(
    context: ConflictContext,
    result: MergeResult,
    localPayload: JsonObject?,
    expectedColumns: Set<String>,
): ConflictPlanDecision {
    val authoritative = context.authoritativeConflictState()
    return when (result) {
        MergeResult.AcceptServer -> ConflictPlanDecision.Apply(
            when (authoritative) {
                AuthoritativeConflictState.Missing -> ConflictResolutionPlan(
                    localRowAction = LocalRowAction.Delete,
                    rowStateAction = RowStateAction.Delete,
                )

                is AuthoritativeConflictState.Deleted -> ConflictResolutionPlan(
                    localRowAction = LocalRowAction.Delete,
                    rowStateAction = RowStateAction.Upsert(
                        rowVersion = authoritative.rowVersion,
                        deleted = true,
                    ),
                )

                is AuthoritativeConflictState.Present -> ConflictResolutionPlan(
                    localRowAction = LocalRowAction.Upsert(
                        payload = authoritative.payload,
                        payloadSource = PayloadSource.AUTHORITATIVE_WIRE,
                    ),
                    rowStateAction = RowStateAction.Upsert(
                        rowVersion = authoritative.rowVersion,
                        deleted = false,
                    ),
                )
            }
        )

        MergeResult.KeepLocal -> buildKeepLocalConflictPlan(context, authoritative, localPayload)
        is MergeResult.KeepMerged -> buildKeepMergedConflictPlan(context, authoritative, result.mergedPayload, expectedColumns)
    }
}

private fun buildKeepLocalConflictPlan(
    context: ConflictContext,
    authoritative: AuthoritativeConflictState,
    localPayload: JsonObject?,
): ConflictPlanDecision {
    return when (context.localOp) {
        "INSERT" -> buildInsertConflictPlan(requireLocalPayload(context, localPayload), authoritative)
        "UPDATE" -> buildUpdateConflictPlan(
            context = context,
            payload = requireLocalPayload(context, localPayload),
            authoritative = authoritative,
            invalidResultName = "KeepLocal",
        )
        "DELETE" -> buildDeleteConflictPlan(authoritative)
        else -> error("unsupported local conflict op ${context.localOp}")
    }
}

private fun buildKeepMergedConflictPlan(
    context: ConflictContext,
    authoritative: AuthoritativeConflictState,
    mergedPayload: JsonElement,
    expectedColumns: Set<String>,
): ConflictPlanDecision {
    if (context.localOp == "DELETE") {
        return ConflictPlanDecision.Invalid(
            "KeepMerged is invalid for DELETE conflict on ${context.schema}.${context.table}"
        )
    }

    val mergedObject = mergedPayload as? JsonObject ?: return ConflictPlanDecision.Invalid(
        "KeepMerged for ${context.schema}.${context.table} must provide a JSON object payload"
    )
    val payloadColumns = mergedObject.keys.map { it.lowercase() }.toSet()
    if (payloadColumns != expectedColumns) {
        return ConflictPlanDecision.Invalid(
            "KeepMerged for ${context.schema}.${context.table} must include exactly every table column"
        )
    }

    return when (context.localOp) {
        "INSERT" -> buildInsertConflictPlan(mergedObject, authoritative)
        "UPDATE" -> buildUpdateConflictPlan(
            context = context,
            payload = mergedObject,
            authoritative = authoritative,
            invalidResultName = "KeepMerged",
        )
        else -> error("unsupported local conflict op ${context.localOp}")
    }
}

private fun buildInsertConflictPlan(
    payload: JsonObject,
    authoritative: AuthoritativeConflictState,
): ConflictPlanDecision {
    return when (authoritative) {
        is AuthoritativeConflictState.Present -> ConflictPlanDecision.Apply(
            upsertRetryPlan(
                payload = payload,
                rowVersion = authoritative.rowVersion,
                deleted = false,
                retryOp = "UPDATE",
            )
        )

        is AuthoritativeConflictState.Deleted -> ConflictPlanDecision.Apply(
            upsertRetryPlan(
                payload = payload,
                rowVersion = authoritative.rowVersion,
                deleted = true,
                retryOp = "INSERT",
            )
        )

        AuthoritativeConflictState.Missing ->
            error("local INSERT conflict must include live row or tombstone")
    }
}

private fun buildUpdateConflictPlan(
    context: ConflictContext,
    payload: JsonObject,
    authoritative: AuthoritativeConflictState,
    invalidResultName: String,
): ConflictPlanDecision {
    val live = authoritative as? AuthoritativeConflictState.Present ?: return ConflictPlanDecision.Invalid(
        "$invalidResultName is invalid for stale UPDATE on " +
            "${context.schema}.${context.table}; authoritative row is deleted or missing"
    )
    return ConflictPlanDecision.Apply(
        upsertRetryPlan(
            payload = payload,
            rowVersion = live.rowVersion,
            deleted = false,
            retryOp = "UPDATE",
        )
    )
}

private fun buildDeleteConflictPlan(authoritative: AuthoritativeConflictState): ConflictPlanDecision {
    return when (authoritative) {
        AuthoritativeConflictState.Missing -> ConflictPlanDecision.Apply(
            ConflictResolutionPlan(
                localRowAction = LocalRowAction.Delete,
                rowStateAction = RowStateAction.Delete,
            )
        )

        is AuthoritativeConflictState.Deleted -> ConflictPlanDecision.Apply(
            ConflictResolutionPlan(
                localRowAction = LocalRowAction.Delete,
                rowStateAction = RowStateAction.Upsert(
                    rowVersion = authoritative.rowVersion,
                    deleted = true,
                ),
            )
        )

        is AuthoritativeConflictState.Present -> ConflictPlanDecision.Apply(
            ConflictResolutionPlan(
                localRowAction = LocalRowAction.Delete,
                rowStateAction = RowStateAction.Upsert(
                    rowVersion = authoritative.rowVersion,
                    deleted = false,
                ),
                requeueIntent = ResolvedDirtyIntent(
                    op = "DELETE",
                    baseRowVersion = authoritative.rowVersion,
                    payloadSource = RetryPayloadSource.None,
                ),
            )
        )
    }
}

private fun upsertRetryPlan(
    payload: JsonObject,
    rowVersion: Long,
    deleted: Boolean,
    retryOp: String,
): ConflictResolutionPlan {
    return ConflictResolutionPlan(
        localRowAction = LocalRowAction.Upsert(
            payload = payload,
            payloadSource = PayloadSource.LOCAL_STATE,
        ),
        rowStateAction = RowStateAction.Upsert(
            rowVersion = rowVersion,
            deleted = deleted,
        ),
        requeueIntent = ResolvedDirtyIntent(
            op = retryOp,
            baseRowVersion = rowVersion,
            payloadSource = RetryPayloadSource.LocalRow,
        ),
    )
}

private fun requireLocalPayload(
    context: ConflictContext,
    payload: JsonObject?,
): JsonObject {
    return payload ?: error("local ${context.localOp} conflict payload must be a JSON object")
}

private fun ConflictContext.authoritativeConflictState(): AuthoritativeConflictState {
    if (serverRow == null) {
        return if (serverRowDeleted) {
            AuthoritativeConflictState.Deleted(serverRowVersion)
        } else {
            AuthoritativeConflictState.Missing
        }
    }

    val payload = serverRow as? JsonObject
        ?: error("authoritative row for ${schema}.${table} must be a JSON object")
    return AuthoritativeConflictState.Present(
        rowVersion = serverRowVersion,
        payload = payload,
    )
}

private fun DirtyRowCapture.toRowRef(): DirtyRowRef =
    DirtyRowRef(schemaName = schemaName, tableName = tableName, keyJson = keyJson)

private fun CommittedReplayRow.toRowRef(): DirtyRowRef =
    DirtyRowRef(schemaName = schemaName, tableName = tableName, keyJson = keyJson)
