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
    private val conflictStateStore: OversqliteConflictStateStore,
    private val syncStateStore: OversqliteSyncStateStore,
    private val localStore: OversqliteLocalStore,
    private val bundleApplier: OversqliteBundleApplier,
    private val stageStore: OversqliteStageStore,
    private val applyExecutor: OversqliteApplyExecutor,
    private val openingMutationAuthority: () -> OversqliteOpeningMutationAuthority?,
    private val json: Json,
    private val log: ((() -> String)) -> Unit,
) {
    private companion object {
        const val MAX_PUSH_CONFLICT_AUTO_RETRIES = 2
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
    private val conflictExecutor = OversqliteConflictExecutor(localStore, syncStateStore, outboxStateStore,
        bundleApplier.survivors, OversqliteApplyLifecycle(db, config, localStore, bundleApplier.survivors, json),
        OversqliteWorkspace(db, config))

    suspend fun pushPending(
        state: RuntimeState,
        allowCheckpointRecovery: Boolean = false,
        beforeAttempt: suspend () -> Unit = {},
    ): PushWorkflowResult {
        return pushPending(state, conflictRetryCount = 0, allowCheckpointRecovery = allowCheckpointRecovery, beforeAttempt = beforeAttempt)
    }

    private suspend fun pushPending(
        state: RuntimeState,
        conflictRetryCount: Int,
        allowCheckpointRecovery: Boolean,
        beforeAttempt: suspend () -> Unit,
    ): PushWorkflowResult {
        conflictStateStore.load()?.let { throw ConflictNeedsResolutionException(it.pending) }
        if (attachmentStateStore.loadState().rebuildRequired && !allowCheckpointRecovery) {
            throw RebuildRequiredException()
        }
        beforeAttempt()

        val initialDirtyCount = syncStateStore.countDirtyRows()
        val initialOutboundCount = outboxStateStore.countRows()
        log {
            pushPendingStartDiagnostic(state, initialDirtyCount, initialOutboundCount)
        }
        val workspace=OversqliteWorkspace(db,config)
        val unit=applyUnit("push-buffers",0,state.sourceId)
        val token=if(initialDirtyCount>0 || initialOutboundCount>0) workspace.reserve(state,unit,
            WorkspaceCharge(64,256*1024)) else null
        var primaryFailure: Throwable? = null
        try {
        val snapshot = ensurePushOutboundSnapshot(state,token)
        if (snapshot.rows.isEmpty()) {
            log { "oversqlite pushPending no-op: no rows ready for upload" }
            return PushWorkflowResult(
                outcome = PushOutcome.NO_CHANGE,
                updatedTables = emptySet(),
            )
        }

        log {
            "oversqlite pushPending snapshot sourceBundleId=${snapshot.sourceBundleId} " +
                "rows=${snapshot.rows.size}"
        }
        val result = try {
            commitPushOutboundSnapshot(state, snapshot,token)
        } catch (e: PushConflictException) {
            val updatedTables = resolvePushConflict(state, snapshot, e, checkNotNull(token))
            db.reportExternalTableChanges(updatedTables)
            val remainingDirtyCount = syncStateStore.countDirtyRows()
            if (remainingDirtyCount == 0L) {
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
                beforeAttempt = beforeAttempt,
            )
            return PushWorkflowResult(
                outcome = retryResult.outcome,
                updatedTables = updatedTables + retryResult.updatedTables,
                confirmedBundles = retryResult.confirmedBundles,
            )
        }

        log {
            "oversqlite pushPending committed bundleSeq=${result.committed.bundleSeq} " +
                "rowCount=${result.committed.rowCount} sourceBundleId=${result.committed.sourceBundleId}"
        }
        stageStore.recordConfirmedBundle(state.sourceId,result.committed.sourceBundleId)
        // This buffer owner survives continuation. Decision copies reuse its
        // captured base instead of adding a preceding slice's temporary charge.
        val decisionReservationBase = token?.let { workspace.reserved(it) }
        val decisionContinuation = ReplayDecisionContinuation()
        val updatedTables = try { applyExecutor.inApplyModeTransaction(state) { statementCache ->
            openingMutationAuthority()?.beforeMutation(
                OversqliteOpeningMutationKind.APPLY_PUSH_RESPONSE,
            )
            replayExecutor.applyCommittedBundle(
                state = state,
                uploadedRows = snapshot.rows,
                committedRows = result.rows,
                committed = result.committed,
                statementCache = statementCache,
                reservationToken=token,
                reservationBase=decisionReservationBase,
                decisionContinuation=decisionContinuation,
            )
        } } finally { decisionContinuation.clear() }
        db.reportExternalTableChanges(updatedTables)
        log { "oversqlite pushPending finished updatedTables=$updatedTables" }
        return PushWorkflowResult(
            outcome = PushOutcome.COMMITTED,
            updatedTables = updatedTables,
            confirmedBundles = setOf(state.sourceId to result.committed.sourceBundleId),
        )
        } catch (failure: Throwable) {
            primaryFailure=failure
            throw failure
        } finally {
            if(token!=null) try { workspace.release(token) } catch(cleanup: Throwable) {
                if(primaryFailure!=null) primaryFailure.addSuppressed(cleanup) else throw cleanup
            }
        }
    }

    suspend fun pendingConflict(): PendingConflict? = conflictStateStore.load()?.pending

    suspend fun completePushPlanCleanup() = stageStore.clearPushPlanForLifecycle(onlyInvalid=true)

    suspend fun retryConflict(selected: PendingConflict) {
        db.transaction(TransactionMode.IMMEDIATE) {
            val openingRecord = openingMutationAuthority()?.beforeMutation(
                OversqliteOpeningMutationKind.RETRY_CONFLICT,
            )
            conflictStateStore.requireCurrent(selected)
            requireMatchingPendingOutbox(selected)
            conflictStateStore.clear()
            if (openingRecord != null) {
                openingMutationAuthority()?.completeConflictDecision(openingRecord)
            }
        }
    }

    suspend fun resolveConflict(state: RuntimeState, selected: PendingConflict, result: MergeResult): Set<String> {
        require(result != MergeResult.DeferConflict) { "resolveConflict requires an explicit resolution" }
        conflictStateStore.requireCurrentIdentity(selected)
        val workspace = OversqliteWorkspace(db, config)
        val token = workspace.reserve(state, applyUnit("conflict-buffers", selected.sourceBundleId, selected.canonicalRequestHash), WorkspaceCharge(64, 256*1024))
        var primaryFailure: Throwable? = null
        try {
        val stored = conflictStateStore.requireCurrent(selected)
        val snapshot = loadPersistedOutboxSnapshot(state, outboxStateStore.loadBundleState(), token)
        val row = requireMatchingConflictRow(snapshot, stored.details)
        validateAuthoritativeConflictRow(state, stored.details)
        val localPayload = parseDirtyRowPayloadAsObject(row)
        val expectedColumns = if (result is MergeResult.KeepMerged) {
            tableInfoCache.get(db, stored.details.table).columnNamesLower.toSet()
        } else emptySet()
        val decision = buildConflictPlanDecision(selected.conflict, result, localPayload, expectedColumns)
        if (decision is ConflictPlanDecision.Invalid) {
            throw InvalidConflictResolutionException(selected.conflict, result, decision.message)
        }
        val reservationBase = workspace.reserved(token)
        return applyExecutor.inApplyModeTransaction(state) { statementCache ->
            val openingRecord = openingMutationAuthority()?.beforeMutation(
                OversqliteOpeningMutationKind.RESOLVE_CONFLICT,
            )
            conflictStateStore.requireCurrent(selected)
            requireMatchingPendingOutbox(selected)
            val updated = conflictExecutor.applyConflictResolutionPlan(
                state, snapshot, row, stored.details.table,
                (decision as ConflictPlanDecision.Apply).plan, statementCache, token, reservationBase,
            )
            conflictStateStore.clear()
            if (openingRecord != null) {
                openingMutationAuthority()?.completeConflictDecision(openingRecord)
            }
            updated
        }
        } catch (failure: Throwable) {
            primaryFailure = failure
            throw failure
        } finally {
            try { workspace.release(token) } catch (cleanup: Throwable) {
                if (primaryFailure == null) throw cleanup
                attachSuppressedOnce(primaryFailure, cleanup)
            }
        }
    }

    private suspend fun requireMatchingPendingOutbox(selected: PendingConflict) {
        val outbox = outboxStateStore.loadBundleState()
        if (outbox.state == outboxStateNone || outbox.sourceId != selected.sourceId ||
            outbox.sourceBundleId != selected.sourceBundleId ||
            outbox.canonicalRequestHash != selected.canonicalRequestHash
        ) throw StaleConflictResolutionException()
    }

    private suspend fun ensurePushOutboundSnapshot(state: RuntimeState, reservationToken: String?): PushOutboundSnapshot =
        stageStore.completeWorkPhase("outbox freeze") { ensurePushOutboundSnapshotSlice(state, reservationToken) }

    private suspend fun ensurePushOutboundSnapshotSlice(state: RuntimeState, reservationToken: String?): PushOutboundSnapshot {
        val outboxState = outboxStateStore.loadBundleState()
        if (outboxState.state != outboxStateNone) {
            val existing = loadPersistedOutboxSnapshot(state, outboxState,reservationToken)
            log {
                "oversqlite reusing frozen outbound snapshot sourceBundleId=${existing.sourceBundleId} rows=${existing.rows.size}"
            }
            return existing
        }

        val plan=stageStore.preparePushPlan(state)
        val frozen = db.transaction(TransactionMode.IMMEDIATE) {
            val currentOutbox = outboxStateStore.loadBundleState()
            if (currentOutbox.state != outboxStateNone)
                return@transaction loadPersistedOutboxSnapshot(state,currentOutbox,reservationToken)
            if (plan == null) return@transaction PushOutboundSnapshot(0,emptyList(),emptyList(),"")
            openingMutationAuthority()?.beforeMutation(
                OversqliteOpeningMutationKind.FREEZE_OUTBOUND,
            )
            val prepared = stageStore.preparePush(
                state,
                plan,
                reservationToken,
            )
            persistPreparedOutboxSnapshot(state, prepared)
        }
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
        reservationToken: String?,
    ): PushCommitResult {
        require(snapshot.canonicalRows.isNotEmpty()) { "outbound push snapshot is empty" }
        val workspace=OversqliteWorkspace(db,config)
        val base=reservationToken?.let { workspace.reserved(it) } ?: WorkspaceCharge()
        val response = remoteApi.push(
            sourceId = state.sourceId,
            sourceBundleId = snapshot.sourceBundleId,
            canonicalRequestHash = snapshot.canonicalRequestHash,
            initializationId = state.pendingInitializationId.ifEmpty { null },
            rows = buildPushRequestRows(snapshot.canonicalRows),
            maxRequestDecodedBytes = state.admittedContract.limits.maxPushDecodedBytes,
            maxResponseDecodedBytes = state.admittedContract.limits.maxPushDecodedBytes,
            onBufferGrowth = { bytes,entries ->
                stageStore.completeWorkPhase("HTTP buffer admission") {
                if(reservationToken!=null) workspace.resize(state,reservationToken,workspace.add(applyUnit("push-buffers",0,state.sourceId),base,
                    WorkspaceCharge(entries,workspace.scale(applyUnit("push-buffers",0,state.sourceId),bytes,64))))
                stageStore.chargeBufferWork(applyUnit("push-buffers",0,state.sourceId),16)
                }
            },
        )
        require(canonicalCommittedBundleHash(response.rows) == response.bundleHash) {
            "push response bundle_hash does not match authoritative rows"
        }
        val decoded = decodeCommittedRows(state, response.rows)
        val committed = CommittedPushBundle(
            bundleSeq = response.bundleSeq,
            sourceBundleId = snapshot.sourceBundleId,
            rowCount = decoded.size.toLong(),
            bundleHash = response.bundleHash,
        )
        return PushCommitResult(committed, decoded)
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

    private suspend fun decodeCommittedRows(
        state: RuntimeState,
        rows: List<BundleRow>,
    ): List<CommittedReplayRow> {
        return rows.map { row ->
            require(row.schema == state.validated.schemaForTable(row.table)) {
                "committed bundle row schema ${row.schema} does not match client schema ${state.validated.schemaForTable(row.table)}"
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

    private suspend fun loadPersistedOutboxSnapshot(
        state: RuntimeState,
        outboxState: OversqliteOutboxBundleState,
        reservationToken: String? = null,
    ): PushOutboundSnapshot = stageStore.completeWorkPhase("outbox read") {
        loadPersistedOutboxSnapshotSlice(state, outboxState, reservationToken)
    }

    private suspend fun loadPersistedOutboxSnapshotSlice(
        state: RuntimeState,
        outboxState: OversqliteOutboxBundleState,
        reservationToken: String? = null,
    ): PushOutboundSnapshot {
        requireMatchingOutboxSourceId(outboxState.sourceId, state.sourceId)
        require(outboxState.state == outboxStatePrepared) { "outbox state must be prepared" }
        if(reservationToken!=null) measurePersistedOutboxCopies(state,reservationToken)
        val outboxRows = outboxStateStore.loadRows()
        for(row in outboxRows) localStore.validateHistoricalBase(row.tableName,row.baseRowVersion,row.base)
        require(outboxRows.isNotEmpty()) {
            "_sync_outbox_bundle state ${outboxState.state} exists without rows"
        }
        require(outboxRows.all { it.sourceBundleId == outboxState.sourceBundleId }) {
            "_sync_outbox_rows source_bundle_id does not match _sync_outbox_bundle"
        }
        require(outboxRows.size.toLong() == outboxState.rowCount) {
            "_sync_outbox_rows row count ${outboxRows.size} does not match _sync_outbox_bundle ${outboxState.rowCount}"
        }
        val canonicalRequestHash = computeCanonicalRequestHash(outboxRows, checkNotNull(state.validated.generation))
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
                base = row.base,
                dirtyOrdinal = row.rowOrdinal,
            )
        }
        return PushOutboundSnapshot(
            sourceBundleId = outboxState.sourceBundleId,
            rows = rows,
            canonicalRows = outboxRows,
            canonicalRequestHash = canonicalRequestHash,
        )
    }

    private suspend fun measurePersistedOutboxCopies(state: RuntimeState, token: String) = db.transaction(TransactionMode.IMMEDIATE) {
        val workspace=OversqliteWorkspace(db,config)
        val unit=applyUnit("push-buffers",0,state.sourceId)
        var cursor = -1L
        var charge=WorkspaceCharge(64,256*1024)
        while(true) {
            stageStore.chargeBufferWork(unit,8)
            val row=db.withPreparedStatement("""SELECT row_ordinal,2048+64*(length(CAST(COALESCE(local_payload,'') AS BLOB))+
                length(CAST(COALESCE(wire_payload,'') AS BLOB))+length(CAST(COALESCE(base_payload,'') AS BLOB))+
                length(CAST(base_provenance AS BLOB))+length(CAST(key_json AS BLOB))+length(CAST(wire_key_json AS BLOB)))
                FROM _sync_outbox_rows WHERE row_ordinal>? ORDER BY row_ordinal LIMIT 1""") {
                it.bindLong(1,cursor)
                if(it.step()) it.getLong(0) to it.getLong(1) else null
            } ?: break
            cursor=row.first
            charge=workspace.add(unit,charge,WorkspaceCharge(32,row.second))
        }
        workspace.resizeInWriter(state,token,charge)
    }

    private suspend fun persistPreparedOutboxSnapshot(
        state: RuntimeState,
        prepared: PreparedPush,
    ): PushOutboundSnapshot {
        val canonicalRows = buildCanonicalOutboxRows(
            state,
            prepared.sourceBundleId,
            prepared,
            checkNotNull(state.validated.generation),
        )
        val selectedRows = prepared.rows.take(canonicalRows.size)
        val canonicalRequestHash = canonicalRows.takeIf { it.isNotEmpty() }?.let { computeCanonicalRequestHash(it, checkNotNull(state.validated.generation)) } ?: ""
        val allMovedRows = selectedRows.map { it.toRowRef() }
        db.withStatementCache { statementCache ->
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
        }
        stageStore.invalidateFrozenPushPlan()
        return PushOutboundSnapshot(
            sourceBundleId = prepared.sourceBundleId,
            rows = selectedRows,
            canonicalRows = canonicalRows,
            canonicalRequestHash = canonicalRequestHash,
        )
    }

    private suspend fun buildCanonicalOutboxRows(
        state: RuntimeState,
        sourceBundleId: Long,
        prepared: PreparedPush,
        generation: SyncGeneration,
    ): List<OversqliteOutboxRow> {
        val rows=prepared.rows.mapIndexed { ordinal,row ->
            val wirePayload=if(row.op=="DELETE") null else canonicalizeJsonElement(localStore.processPayloadForUpload(row.tableName,row.localPayload ?: ""))
            OversqliteOutboxRow(sourceBundleId,ordinal.toLong(),row.schemaName,row.tableName,row.keyJson,
                canonicalizeJsonElement(syncKeyToJsonElement(row.wireKey)),row.op,row.baseRowVersion,row.localPayload,wirePayload,row.base)
        }
        for(row in rows) {
            val bytes=OversqliteCompactCodec.expandedRowDecodedBytes(row.toPushRequestRow())
            if(bytes>state.admittedContract.limits.maxRowDecodedBytes) throw PushRowExceedsAdmittedLimitException(bytes,state.admittedContract.limits.maxRowDecodedBytes)
        }
        val hash=computeCanonicalRequestHash(rows,generation)
        val bytes=compactPushRequestText(sourceBundleId,hash,state.pendingInitializationId.ifEmpty { null },rows,generation).encodeToByteArray().size.toLong()
        check(bytes==prepared.decodedBytes && rows.size==prepared.rows.size) { "guarded push measurement disagrees with final encoding" }
        check(rows.size<=state.admittedContract.limits.maxPushRows && bytes<=state.admittedContract.limits.maxPushDecodedBytes) { "guarded push exceeds hard request limits" }
        return rows
    }

    private fun compactPushRequestText(
        sourceBundleId: Long,
        canonicalRequestHash: String,
        initializationId: String?,
        rows: List<OversqliteOutboxRow>,
        generation: SyncGeneration,
    ): String = canonicalizeJsonElement(buildJsonObject {
        put("source_bundle_id", JsonPrimitive(sourceBundleId))
        put("canonical_request_hash", JsonPrimitive(canonicalRequestHash))
        initializationId?.let { put("initialization_id", JsonPrimitive(it)) }
        put("rows", OversqliteCompactCodec.encodePushRows(generation, rows.map { it.toPushRequestRow() }))
    })

    private fun OversqliteOutboxRow.toPushRequestRow(): PushRequestRow = PushRequestRow(
        schema = schemaName,
        table = tableName,
        key = parseWireKey(wireKeyJson),
        op = op,
        baseRowVersion = baseRowVersion,
        payload = wirePayload?.let(json::parseToJsonElement),
    )

    private fun computeCanonicalRequestHash(rows: List<OversqliteOutboxRow>, generation: SyncGeneration): String =
        sha256Hex(canonicalRequestText(rows, generation).encodeToByteArray())

    private fun canonicalRequestRow(row: OversqliteOutboxRow): JsonObject = buildJsonObject {
        put("row_ordinal", JsonPrimitive(row.rowOrdinal.toString()))
        put("schema", JsonPrimitive(row.schemaName))
        put("table", JsonPrimitive(row.tableName))
        put("key", syncKeyToJsonElement(parseWireKey(row.wireKeyJson)))
        put("op", JsonPrimitive(row.op))
        put("base_row_version", JsonPrimitive(row.baseRowVersion.toString()))
        put("payload", row.wirePayload?.let { json.parseToJsonElement(it) } ?: JsonNull)
    }

    private fun canonicalRequestText(rows: List<OversqliteOutboxRow>, generation: SyncGeneration): String =
        canonicalizeJsonElement(buildJsonObject {
            put("generation_fingerprint", JsonPrimitive(generation.fingerprint))
            put("generation_id", JsonPrimitive(generation.id))
            put("rows", JsonArray(rows.map(::canonicalRequestRow)))
            put("type", JsonPrimitive("oversync.generation-pinned-push-request.v1"))
        })

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
        reservationToken: String,
    ): Set<String> {
        val conflict = exception.conflict
        val conflictingRow = requireMatchingConflictRow(snapshot, conflict)

        val localPayload = parseDirtyRowPayloadAsObject(conflictingRow)
        validateAuthoritativeConflictRow(state, conflict)
        val context = ConflictContext(
            schema = conflict.schema,
            table = conflict.table,
            key = conflict.key,
            localOp = conflictingRow.op,
            localPayload = conflictingRow.takeIf { it.op != "DELETE" }?.localPayload?.let {
                localStore.processPayloadForUpload(conflictingRow.tableName, it)
            },
            baseRowVersion = conflict.baseRowVersion,
            serverRowVersion = conflict.serverRowVersion,
            serverRowDeleted = conflict.serverRowDeleted,
            serverRow = conflict.serverRow,
        )
        val pending = db.transaction(TransactionMode.IMMEDIATE) {
            val openingRecord = openingMutationAuthority()?.beforeMutation(
                OversqliteOpeningMutationKind.RECORD_CONFLICT,
            )
            val stored = conflictStateStore.preserve(state, snapshot, conflict, context.localPayload)
            if (openingRecord != null) {
                openingMutationAuthority()?.recordConflict(openingRecord, stored.pending)
            }
            stored
        }
        val resolution = resolver.resolve(context)
        if (resolution == MergeResult.DeferConflict) {
            throw ConflictNeedsResolutionException(pending.pending)
        }
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
            is ConflictPlanDecision.Apply -> {
                // Capture before the driver: each slice reconstructs the same buffers.
                val reservationBase = OversqliteWorkspace(db, config).reserved(reservationToken)
                applyExecutor.inApplyModeTransaction(state) { statementCache ->
                    val openingRecord = openingMutationAuthority()?.beforeMutation(
                        OversqliteOpeningMutationKind.RESOLVE_CONFLICT,
                    )
                    conflictStateStore.requireCurrent(pending.pending)
                    conflictExecutor.applyConflictResolutionPlan(
                        state = state,
                        snapshot = snapshot,
                        conflictingRow = conflictingRow,
                        tableName = context.table,
                        plan = decision.plan,
                        statementCache = statementCache,
                        reservationToken = reservationToken,
                        reservationBase = reservationBase,
                    ).also {
                        conflictStateStore.clear()
                        if (openingRecord != null) {
                            openingMutationAuthority()?.completeConflictDecision(openingRecord)
                        }
                    }
                }
            }

            is ConflictPlanDecision.Invalid -> {
                applyExecutor.inApplyModeTransaction(state) { statementCache ->
                    val openingRecord = openingMutationAuthority()?.beforeMutation(
                        OversqliteOpeningMutationKind.RESTORE_INVALID_RESOLUTION,
                    )
                    conflictStateStore.requireCurrent(pending.pending)
                    conflictExecutor.restoreOutboundSnapshotToDirtyRows(snapshot, statementCache)
                    conflictStateStore.clear()
                    if (openingRecord != null) {
                        openingMutationAuthority()?.completeConflictDecision(openingRecord)
                    }
                }
                throw InvalidConflictResolutionException(conflict = context, result = resolution, message = decision.message)
            }
        }
    }

    private fun requireMatchingConflictRow(
        snapshot: PushOutboundSnapshot,
        conflict: PushConflictDetails,
    ): DirtyRowCapture = snapshot.rows.firstOrNull {
        it.schemaName == conflict.schema && it.tableName == conflict.table &&
            it.wireKey == conflict.key && it.op == conflict.op &&
            it.baseRowVersion == conflict.baseRowVersion
    } ?: throw RemoteResponseSemanticException("push conflict response")

    private suspend fun validateAuthoritativeConflictRow(
        state: RuntimeState,
        conflict: PushConflictDetails,
    ) {
        val expectedSchema = state.validated.schemaForTable(conflict.table)
        require(conflict.schema == expectedSchema) {
            "push conflict schema ${conflict.schema} does not match client schema $expectedSchema"
        }
        val serverRow = conflict.serverRow ?: return
        val payload = serverRow as? JsonObject
            ?: throw RemoteResponseSemanticException("push conflict response")
        localStore.validateAndNormalizeSnapshotPayload(state, conflict.table, conflict.key, payload)
    }

    private fun parseDirtyRowPayloadAsObject(row: DirtyRowCapture): JsonObject? {
        if (row.op == "DELETE") return null
        val payloadText = row.localPayload ?: return null
        return json.parseToJsonElement(payloadText) as? JsonObject
            ?: error("local ${row.op} conflict payload must be a JSON object")
    }

}

private data class PushCommitResult(
    val committed: CommittedPushBundle,
    val rows: List<CommittedReplayRow>,
)

internal fun pushPendingStartDiagnostic(
    state: RuntimeState,
    pendingDirtyCount: Long,
    pendingOutboundCount: Long,
): String {
    requireValidOversqliteSourceId(state.sourceId)
    return "oversqlite pushPending start pendingDirty=$pendingDirtyCount pendingOutbound=$pendingOutboundCount"
}

internal fun requireMatchingOutboxSourceId(
    persistedSourceId: String,
    currentSourceId: String,
) {
    val validatedPersistedSourceId = requireValidOversqliteSourceId(persistedSourceId)
    val validatedCurrentSourceId = requireValidOversqliteSourceId(currentSourceId)
    require(validatedPersistedSourceId == validatedCurrentSourceId) {
        "persisted outbox source does not match current source"
    }
}

internal data class PushWorkflowResult(
    val outcome: PushOutcome,
    val updatedTables: Set<String>,
    val confirmedBundles: Set<Pair<String, Long>> = emptySet(),
)
