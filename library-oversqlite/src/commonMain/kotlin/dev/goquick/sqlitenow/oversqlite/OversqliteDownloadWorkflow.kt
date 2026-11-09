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

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject

internal data class DownloadWorkflowResult(
    val outcome: RemoteSyncOutcome,
    val updatedTables: Set<String>,
    val restore: RestoreSummary? = null,
    val rotatedSourceId: String? = null,
)

private data class StagedSnapshotPageProgress(
    val rowCount: Int,
    val lastRowOrdinal: Long?,
)

internal enum class SnapshotRebuildOutboxMode {
    CLEAN,
    SOURCE_RECOVERY,
}

internal class OversqliteDownloadWorkflow(
    private val automaticRecovery: OversqliteAutomaticRecovery,
    private val operations: OversqliteClientOperationController,
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
    private val remoteApi: OversqliteRemoteApi,
    private val sourceStateStore: OversqliteSourceStateStore,
    private val outboxStateStore: OversqliteOutboxStateStore,
    private val attachmentStateStore: OversqliteAttachmentStateStore,
    private val operationStateStore: OversqliteOperationStateStore,
    private val syncStateStore: OversqliteSyncStateStore,
    private val localStore: OversqliteLocalStore,
    private val bundleApplier: OversqliteBundleApplier,
    private val stageStore: OversqliteStageStore,
    private val applyExecutor: OversqliteApplyExecutor,
    private val snapshotGate: OversqliteSnapshotGate,
    private val snapshotDiagnostics: () -> SnapshotDiagnosticsRecorder?,
    private val json: Json,
    private val notifyUpdatedTables: (Set<String>) -> Unit = {},
    private val log: ((() -> String)) -> Unit,
) {
    // One bounded identity avoids repeating best-effort retirement on each work yield.
    // A reopened owner may retry retirement; no prepared handle or payload is retained.
    private var retiredSnapshotId: String? = null

    private suspend fun retireOperationSnapshot(id: String, source: String) {
        if (retiredSnapshotId == id) return
        remoteApi.deleteSnapshotSessionBestEffort(id, source)
        retiredSnapshotId = id
    }

    internal suspend fun requireCleanSnapshotEntry() = requireCleanSnapshotWork(db)

    private val installation by lazy { OversqliteSnapshotInstallation(db, config, localStore, bundleApplier.survivors, json) }

    private val sourceRecoveryIntent by lazy {
        OversqliteSourceRecoveryIntent(db, config, localStore, syncStateStore, outboxStateStore,
            OversqliteConflictStateStore(db, json), operationStateStore, stageStore, json)
    }

    /** Explicit, consented exit for pending intent a source-recovery rebuild cannot carry. */
    internal suspend fun discardPendingIntentForSourceRecovery(): DiscardedPendingIntent = sourceRecoveryIntent.discard()

    internal fun retainedSnapshotSession(operation: OversqliteOperationState, source: String): SnapshotSession =
        checkNotNull(installation.retained(operation, source)) { "retained snapshot authority changed" }

    internal suspend fun retainGenerationSnapshot(state: RuntimeState, session: SnapshotSession, source: String) {
        requireCleanSnapshotEntry()
        check(session.snapshotBundleSeq >= installation.floor()) { "snapshot boundary precedes installed observation floor" }
        val lifecycle = OversqliteApplyLifecycle(db, config, localStore, bundleApplier.survivors, json)
        val current = operationStateStore.loadState()
        require(current.snapshotEngine == generalSnapshotEngine) { "generation staging cannot reinterpret typed bootstrap" }
        operationStateStore.persistState(current.copy(stagedSnapshotId = session.snapshotId,
            snapshotBundleSeq = session.snapshotBundleSeq, snapshotRowCount = session.rowCount,
            snapshotByteCount = session.logicalByteCount, snapshotStageComplete = false,
            snapshotStagedRowCount = 0, snapshotStagedByteCount = 0,
            snapshotStageGuard = lifecycle.snapshotStageGuard(state, session), snapshotTransfer = installation.encodeSeal(session, source)))
    }

    internal suspend fun verifyGenerationSnapshot(state: RuntimeState, session: SnapshotSession, source: String, phase: String) {
        syncPhase(phase, session.rowCount, session.logicalByteCount) {
            db.withStatementCache(cleanupTimeoutMillis = 5_000) { cache -> installation.verify(state, session, source, cache) }
        }
    }

    internal suspend fun generationRecoveryRequest(): SnapshotSessionCreateRequest =
        SnapshotSessionCreateRequest(recovery = SnapshotRecovery(installation.floor()))

    suspend fun pullToStable(
        state: RuntimeState,
        onPhaseChanged: suspend (OversqlitePhase) -> Unit = {},
    ): DownloadWorkflowResult {
        requireOrdinarySyncAllowed()
        val initialDirtyCount = syncStateStore.countDirtyRows()
        val initialOutboundCount = outboxStateStore.countRows()
        log {
            "oversqlite pullToStable start pendingDirty=$initialDirtyCount pendingOutbound=$initialOutboundCount"
        }
        return try {
            onPhaseChanged(OversqlitePhase.PULLING)
            val pullResult = pullIncremental(state)
            val result = DownloadWorkflowResult(
                outcome = pullResult.outcome,
                updatedTables = pullResult.updatedTables,
            )
            log { "oversqlite pullToStable finished successfully" }
            result
        } catch (error: Throwable) {
            val reason=historicalRecoveryReason(error) ?: throw error
            if(!automaticRecovery.enabled())throw error
            bundleApplier.survivors.inspectSnapshotCapabilities(state)
            if (error !is HistoricalBaselineRequired) automaticRecovery.capture(state,reason,0,reason) { installation.context(state,"automatic-recovery",automatic=false) }
            resumeAutomaticRecovery(state,onPhaseChanged)
        }
    }

    internal suspend fun automaticActive(): Boolean = automaticRecovery.active()
    internal suspend fun resumeAutomaticRecovery(state: RuntimeState,onPhaseChanged: suspend (OversqlitePhase)->Unit = {}): DownloadWorkflowResult =
        automaticRecovery.run(state,{ installation.context(state,"automatic-recovery",automatic=false) }) {
            rebuildFromSnapshot(state,null,null,SnapshotRebuildOutboxMode.CLEAN,onPhaseChanged)
        }

    suspend fun rebuildFromSnapshot(
        state: RuntimeState,
        rotatedSourceId: String?,
        sourceReplacementReason: String?,
        outboxMode: SnapshotRebuildOutboxMode = SnapshotRebuildOutboxMode.CLEAN,
        onPhaseChanged: suspend (OversqlitePhase) -> Unit = {},
        finalizer: suspend () -> Unit = {},
        carryPendingIntent: Boolean = false,
    ): DownloadWorkflowResult {
        if (carryPendingIntent && currentCoroutineContext()[SourceRecoveryIntentAdmission] == null) {
            require(outboxMode == SnapshotRebuildOutboxMode.SOURCE_RECOVERY && rotatedSourceId != null) {
                "pending intent is carried only across a rotated source-recovery rebuild"
            }
            // Pending intent is admissible only inside this rotated rebuild, which carries it forward.
            return withContext(SourceRecoveryIntentAdmission) {
                rebuildFromSnapshot(state, rotatedSourceId, sourceReplacementReason, outboxMode, onPhaseChanged, finalizer, true)
            }
        }
        requireCleanSnapshotEntry()
        if (rotatedSourceId == null && outboxMode == SnapshotRebuildOutboxMode.CLEAN) markCheckpointRecoveryRequired("explicit_rebuild")
        // The typed bootstrap installs into empty managed state and has no place for carried intent.
        val carriesIntent = carryPendingIntent && sourceRecoveryIntent.hasPending()
        if (!carriesIntent) tryBootstrapSnapshot(state, outboxMode, rotatedSourceId, sourceReplacementReason,
            onPhaseChanged = onPhaseChanged, finalizer = finalizer)?.let { return it }
        bundleApplier.survivors.inspectSnapshotCapabilities(state)
        val diagnostics = requireNotNull(snapshotDiagnostics()) {
            "snapshot diagnostics scope is missing for rebuild"
        }
        diagnostics.markRestoreAttempt()
        requireSnapshotRebuildState(outboxMode)
        onPhaseChanged(OversqlitePhase.STAGING_REMOTE_STATE)
        val (session, guard) = ensureOperationSnapshotStaged(state, outboxMode,
            rotatedSourceId, sourceReplacementReason)
        diagnostics.recordStagedRows(session.rowCount)
        automaticRecovery.phase("prepare_transition")
        onPhaseChanged(OversqlitePhase.APPLYING_REMOTE_STATE)
        return applyStagedSnapshot(state, session, rotatedSourceId, outboxMode, guard, finalizer)
    }

    /** Automatic candidate entry: capability/state refusal is possible only before typed effects. */
    internal suspend fun tryBootstrapSnapshot(
        state: RuntimeState, mode: SnapshotRebuildOutboxMode, rotatedSourceId: String? = null,
        replacementReason: String? = null, remoteReplace: Boolean = false,
        onPhaseChanged: suspend (OversqlitePhase) -> Unit = {}, finalizer: suspend () -> Unit = {},
    ): DownloadWorkflowResult? {
        requireCleanSnapshotEntry()
        val limits = negotiateSnapshotLimits(state.admittedContract, config)
        val workspace = OversqliteWorkspace(db, config)
        val prepared = syncPhase("snapshot.bootstrap.admission") { operations.completeWorkPhase("bootstrap route admission") {
            db.transaction(TransactionMode.IMMEDIATE) {
                db.withStatementCache(cleanupTimeoutMillis = 5_000) { cache ->
                    val old = readBootstrapOperation(db, workspace, cache).first
                    if (old.snapshotEngine == generalSnapshotEngine && old.stagedSnapshotId.isNotEmpty()) {
                        syncDiagnosticEvent("snapshot.engine", "general.pinned")
                        return@withStatementCache null
                    }
                    if (old.snapshotEngine == generalSnapshotEngine && !bootstrapManagedStateEmpty(db, state.validated, cache)) {
                        syncDiagnosticEvent("snapshot.engine", "general.managed_nonempty")
                        return@withStatementCache null
                    }
                    val version = db.scalarLong("PRAGMA main.schema_version")
                    val metadata = SyncRuntimeInitializer(config, TableInfoCache()).validateConfig(db)
                        .withGeneration(state.admittedContract.generation)
                    require(managedPhysicalShape(metadata.tableInfoByName, metadata.keyByTable) ==
                        managedPhysicalShape(state.validated.tableInfoByName, state.validated.keyByTable)) { "bootstrap physical admission changed" }
                    val admitted = admitBootstrapSchema(db, metadata, version, workspace, "bootstrap-route", cache)
                    if (admitted is BootstrapSchemaAdmission.Unavailable) {
                        require(old.snapshotEngine == generalSnapshotEngine) { "pinned bootstrap capability changed" }
                        syncDiagnosticEvent("snapshot.engine", "general." + admitted.reason.name.lowercase())
                        sqliteNowLogger.w {
                            "Oversqlite snapshot: typed bootstrap unavailable (${admitted.reason.label}); using the general snapshot installer." +
                                (admitted.missingChildIndex?.let { index ->
                                    " Required leading child index: table ${quoteIdent(index.table)}, column ${quoteIdent(index.column)}, collation ${index.collation}." +
                                        " Add a matching index to the application schema."
                                } ?: "")
                        }
                        return@withStatementCache null
                    }
                    admitted as BootstrapSchemaAdmission.Eligible
                    if (old.snapshotEngine == generalSnapshotEngine) bindBootstrapDescriptor(db, admitted, metadata, workspace, cache)
                    else { verifyBootstrapDescriptorHeader(db, admitted.descriptor, cache); verifyBootstrapFamilyObjects(db, admitted.descriptor, cache) }
                    // Owned layout creation ends the old schema pin before the operation pin.
                    val afterBinding = db.scalarLong("PRAGMA main.schema_version")
                    val current = if (afterBinding == version) admitted else
                        admitBootstrapSchema(db, metadata, afterBinding, workspace, "bootstrap-route", cache)
                    require(current is BootstrapSchemaAdmission.Eligible)
                    metadata to current
                }
            }
        } } ?: return null
        val (metadata, admission) = prepared
        // The immutable descriptor remains live across session creation and HTTP.
        // Its holder ends only after the transfer/publication owner has ended.
        workspace.check("bootstrap-route", admission.descriptor.charge)
        val descriptorScope = bootstrapReceiptScope(db, workspace, "bootstrap-route", admission.descriptor.charge)
        return try { withContext(descriptorScope) {
        val runtime = state.copy(validated = metadata)
        val source = rotatedSourceId ?: state.sourceId
        val pin = operations.completeWorkPhase("bootstrap lifecycle capture") { db.transaction(TransactionMode.DEFERRED) {
            db.withStatementCache(cleanupTimeoutMillis = 5_000) { cache ->
                val (operation, controlCharge) = readBootstrapOperation(db, workspace, cache)
                val context = bootstrapLifecycleContext(runtime, admission, mode, remoteReplace, cache, controlCharge, reserveLifecycle = true)
                BootstrapLifecyclePin(context, operation, controlCharge,
                    currentCoroutineContext()[AutomaticRecoveryContext]?.pin)
            }
        } }
        descriptorScope.retain(workspace, "bootstrap-route", pin.controlCharge)
        val operation = pin.operation
        val retained = operation.snapshotEngine == typedSnapshotEngine
        val session = if (retained) {
            val transfer = json.decodeFromString(RetainedSnapshotSeal.serializer(), operation.snapshotTransfer)
            validateSnapshotSession(transfer.session)
            require(transfer.authority == config.serviceId && transfer.session.snapshotId == operation.stagedSnapshotId) { "pinned bootstrap authority changed" }
            if (transfer.source != source) {
                BootstrapSnapshotRetirement.use(db, config, admission.descriptor, state.admittedContract.generation, operations) {
                    while (!it.step()) operations.nextWorkSliceIfNeeded()
                }
                retireOperationSnapshot(transfer.session.snapshotId, transfer.source)
                throw RebuildRequiredException()
            }
            transfer.session
        } else {
            val floor = if (remoteReplace) 0L else operations.completeWorkPhase("bootstrap observation floor") { installation.floor() }
            val request = if (remoteReplace) null else if (rotatedSourceId != null) SnapshotSessionCreateRequest(
                SnapshotSourceReplacement(state.sourceId, rotatedSourceId, checkNotNull(replacementReason)))
                else SnapshotSessionCreateRequest(recovery = SnapshotRecovery(floor))
            remoteApi.createSnapshotSession(state.sourceId, request) {
                validateBootstrapLifecycle(runtime, admission, mode, remoteReplace, pin)
            }.also {
                require(it.snapshotBundleSeq >= floor) { "snapshot boundary precedes installed observation floor" }
            }
        }
        val currentStageGuard = bootstrapSnapshotStageGuard(pin.context, session)
        val stageGuard = if (retained) operation.snapshotStageGuard else currentStageGuard
        val transfer = if (retained) operation.snapshotTransfer else installation.encodeSeal(session, source)
        val phase = db.withPreparedStatement("SELECT phase FROM _sync_bootstrap_state WHERE singleton_key=1") {
            require(it.step()); it.getText(0)
        }
        if (phase == "retiring") {
            BootstrapSnapshotRetirement.use(db, config, admission.descriptor, state.admittedContract.generation, operations) {
                while (!it.step()) operations.nextWorkSliceIfNeeded()
            }
            throw RebuildRequiredException()
        }
        suspend fun validate(stage: TypedSnapshotStage) {
            pin.operation = stage.retainedOperation
            operations.completeWorkPhase("bootstrap lifecycle guard") {
                validateBootstrapLifecycle(runtime, admission, mode, remoteReplace, pin)
            }
        }
        suspend fun install(stage: TypedSnapshotStage): DownloadWorkflowResult {
            syncDiagnosticEvent("snapshot.engine", typedSnapshotEngine)
            snapshotDiagnostics()?.markRestoreAttempt()
            onPhaseChanged(OversqlitePhase.STAGING_REMOTE_STATE)
            validate(stage)
            if (!stage.retainedOperation.snapshotStageComplete) {
                var after = stage.retainedOperation.snapshotStagedRowCount
                var more = true
                // Rows per chunk request. Staged rows stay charged until install, so once a chunk's
                // transport cost per row is measured, every later request is sized from fresh usage to
                // leave room for receipt and staging, not only after a transport refusal.
                var chunkRows = limits.maxRows
                var transportEntriesPerRow = 0L
                val maxColumns = stage.descriptor.tables.maxOfOrNull { it.columns.size } ?: 0
                suspend fun quotaRows() = bootstrapChunkRowsForQuota(
                    config.maxSyncWorkspaceEntries - workspace.usage("bootstrap-transport").entries,
                    transportEntriesPerRow, maxColumns, limits.maxRows)
                while (more) {
                    operations.nextWorkSliceIfNeeded()
                    if (transportEntriesPerRow > 0) chunkRows = quotaRows()
                    val progress = run {
                        while (true) {
                            var halveForBytes = false
                            val received = BootstrapTransportScope.use(workspace) { transport ->
                                val hardLimit = state.admittedContract.limits.maxSnapshotDecodedBytesPerChunk
                                suspend fun fetch(rows: Int, bytes: Long) = remoteApi.fetchAndStageBootstrapChunk(stage, after, rows, bytes,
                                    hardLimit, state.admittedContract.limits.maxRowDecodedBytes, transport) { validate(stage) }
                                try {
                                    val chunk = try { fetch(chunkRows, limits.maxBytes) }
                                    catch (tooSmall: SnapshotChunkTooSmallException) {
                                        if (tooSmall.requiredBytes > hardLimit) throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK)
                                        try { fetch(1, tooSmall.requiredBytes) }
                                        catch (_: SnapshotChunkTooSmallException) { throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK) }
                                    }
                                    if (chunk.rows > 0) transportEntriesPerRow = maxOf(transportEntriesPerRow,
                                        bootstrapCeilDiv(transport.charge.entries, chunk.rows))
                                    chunk
                                } catch (capacity: SyncWorkspaceLimitException) {
                                    // A transport refusal happens before any JSON tree, receipt or staging,
                                    // so the same ordinal is requested again with fewer rows. Any other
                                    // refusal, or one row that did not fit, stays final.
                                    if (capacity.unit != "bootstrap-transport" || capacity.resumable || chunkRows <= 1) throw capacity
                                    if (capacity.dimension == "entries") {
                                        val receivable = minOf(chunkRows.toLong(), stage.session.rowCount - after).coerceAtLeast(1)
                                        transportEntriesPerRow = maxOf(transportEntriesPerRow,
                                            bootstrapCeilDiv(transport.charge.entries + capacity.reserved, receivable))
                                    } else halveForBytes = true
                                    null
                                }
                            }
                            if (received != null) return@run received
                            // Body bytes are buffered incrementally, so a bytes refusal is not proportional
                            // to one admission and the request halves. Every retry requests fewer rows.
                            chunkRows = minOf(chunkRows - 1, if (halveForBytes) chunkRows / 2 else quotaRows()).coerceAtLeast(1)
                        }
                        @Suppress("UNREACHABLE_CODE") error("unreachable")
                    }
                    after = progress.nextOrdinal
                    validate(stage)
                    more = progress.hasMore
                    require(more == (after < session.rowCount))
                }
                stage.seal()
            }
            snapshotDiagnostics()?.recordStagedRows(session.rowCount)
            automaticRecovery.phase("prepare_transition")
            onPhaseChanged(OversqlitePhase.APPLYING_REMOTE_STATE)
            // Accept only progress produced by this stage owner. The final
            // writer below reads and checks SQL again after the callback.
            pin.operation = stage.retainedOperation
            val result = applyExecutor.inApplyModeTransaction(runtime) { cache ->
                validateBootstrapLifecycle(runtime, admission, mode, remoteReplace, pin, cache)
                if (!remoteReplace) check(session.snapshotBundleSeq >= installation.floor())
                automaticRecovery.ready()
                val target = OversqliteProvenanceContextStore(db).target(runtime, source)
                val tables = SnapshotBootstrapInstaller(db, config, operations).install(stage, admission, metadata, target, cache)
                syncPhase("snapshot.bootstrap.publication", session.rowCount, session.logicalByteCount) {
                    if (rotatedSourceId != null && state.sourceId != rotatedSourceId) sourceStateStore.markRotated(state.sourceId, rotatedSourceId)
                    attachmentStateStore.persistAttachedState(source, state.userId, unconfirmedLocalSchema,
                        session.snapshotBundleSeq, false, "", cache)
                    automaticRecovery.completed(session.snapshotBundleSeq)
                    finalizer()
                    syncPhase("snapshot.bootstrap.cleanup", session.rowCount) { stage.clearAfterInstallation(cache) }
                }
                DownloadWorkflowResult(RemoteSyncOutcome.APPLIED_SNAPSHOT, tables,
                    RestoreSummary(session.snapshotBundleSeq, session.rowCount), rotatedSourceId)
            }
            snapshotDiagnostics()?.recordAppliedRows(session.rowCount)
            retireOperationSnapshot(session.snapshotId, source)
            notifyUpdatedTables(result.updatedTables)
            return result
        }
        if (retained && currentStageGuard != stageGuard) {
            BootstrapSnapshotRetirement.use(db, config, admission.descriptor, state.admittedContract.generation, operations) {
                while (!it.step()) operations.nextWorkSliceIfNeeded()
            }
            throw RebuildRequiredException()
        }
        try {
            if (retained) TypedSnapshotStage.resume(db, config, admission.descriptor, session,
                state.admittedContract.generation, source, operations, ::install)
            else TypedSnapshotStage.begin(db, config, admission, metadata, session, state.admittedContract.generation,
                source, stageGuard, transfer, operations, beforeStaging = {
                    validateBootstrapLifecycle(runtime, admission, mode, remoteReplace, pin)
                }, block = ::install)
        } catch (failure: Throwable) {
            if (failure is SnapshotHttpException && failure.errorCode in setOf("snapshot_session_expired", "snapshot_session_not_found")) {
                // Retirement completes this transfer. A later legal operation may create another one.
                try {
                    BootstrapSnapshotRetirement.use(db, config, admission.descriptor, state.admittedContract.generation, operations) {
                        while (!it.step()) operations.nextWorkSliceIfNeeded()
                    }
                } catch (cleanup: Throwable) { if (cleanup !== failure) failure.addSuppressed(cleanup) }
            }
            if (!retained) {
                try {
                    val owned = db.withPreparedStatement("SELECT snapshot_engine='typed_bootstrap' AND staged_snapshot_id=? FROM _sync_operation_state WHERE singleton_key=1") {
                        it.bindText(1, session.snapshotId); require(it.step()); it.getLong(0) == 1L
                    }
                    if (!owned) retireOperationSnapshot(session.snapshotId, source)
                } catch (cleanup: Throwable) { attachSuppressedOnce(failure, cleanup) }
            }
            throw failure
        }
        } } finally { descriptorScope.close() }
    }

    private data class BootstrapLifecyclePin(
        val context: String, var operation: OversqliteOperationState,
        val controlCharge: WorkspaceCharge, val automatic: AutomaticRecoveryPin?,
    )

    private fun bootstrapSnapshotStageGuard(context: String, session: SnapshotSession): String =
        sha256Hex(kotlinx.serialization.json.buildJsonArray {
            add(kotlinx.serialization.json.JsonPrimitive("oversqlite.bootstrap.lifecycle.v1"))
            for (value in listOf(context, session.snapshotId, session.snapshotBundleSeq.toString(), session.rowCount.toString(),
                session.logicalByteCount.toString(), session.snapshotHash)) add(kotlinx.serialization.json.JsonPrimitive(value))
        }.toString().encodeToByteArray())

    /** No actual mutable operation strings are retrieved by a pinned check. */
    private suspend fun validateBootstrapLifecycle(
        state: RuntimeState, admission: BootstrapSchemaAdmission.Eligible, mode: SnapshotRebuildOutboxMode,
        remoteReplace: Boolean, pin: BootstrapLifecyclePin, writer: StatementCache? = null,
    ) {
        suspend fun check(cache: StatementCache) {
            pin.automatic?.let { automaticRecovery.validate(it) }
            requireBootstrapOperationMatches(db, pin.operation, cache)
            require(bootstrapLifecycleContext(state, admission, mode, remoteReplace, cache) == pin.context) {
                "bootstrap lifecycle context changed"
            }
        }
        if (writer != null) check(writer)
        else db.transaction(TransactionMode.DEFERRED) { db.withStatementCache(cleanupTimeoutMillis = 5_000) { check(it) } }
    }

    /** Measure scalar/TEMP lengths before SQL JSON, driver TEXT and UTF-8 copies. */
    private suspend fun bootstrapLifecycleContext(
        state: RuntimeState, admission: BootstrapSchemaAdmission.Eligible, mode: SnapshotRebuildOutboxMode,
        remoteReplace: Boolean, cache: StatementCache, independent: WorkspaceCharge = WorkspaceCharge(), reserveLifecycle: Boolean = false,
    ): String {
        cache.requireConnectionOwner(db)
        admission.pin.requireCurrent(db, state.validated)
        val workspace = OversqliteWorkspace(db, config)
        val sourceTables = """FROM _sync_contract_identity i CROSS JOIN _sync_attachment_state a
            CROSS JOIN _sync_conflict_state f CROSS JOIN _sync_outbox_bundle o CROSS JOIN _sync_operation_state p
            JOIN _sync_source_state s ON s.source_id=a.current_source_id"""
        val measure = db.withPreparedStatement("""SELECT
            length(CAST(i.database_id AS BLOB))+length(CAST(i.service_id AS BLOB))+length(CAST(i.physical_shape AS BLOB))+
            length(CAST(i.fingerprint AS BLOB))+length(CAST(a.current_source_id AS BLOB))+length(CAST(a.attached_user_id AS BLOB))+
            length(CAST(a.binding_state AS BLOB))+length(CAST(a.pending_initialization_id AS BLOB))+
            length(CAST(o.state AS BLOB))+length(CAST(o.source_id AS BLOB))+length(CAST(o.canonical_request_hash AS BLOB))+
            length(CAST(p.kind AS BLOB))+length(CAST(p.target_user_id AS BLOB))+length(CAST(p.reason AS BLOB))+
            length(CAST(p.replacement_source_id AS BLOB))+
            (SELECT COALESCE(sum(length(CAST(name AS BLOB))+length(CAST(type AS BLOB))+length(CAST(tbl_name AS BLOB))+
                length(CAST(COALESCE(sql,'') AS BLOB))),0) FROM sqlite_temp_schema),
            (SELECT count(*) FROM sqlite_temp_schema) $sourceTables""", cache) {
            require(it.step()); it.getLong(0) to it.getLong(1)
        }
        val unit = "bootstrap-lifecycle"
        if (reserveLifecycle) {
            // The existing64-exchange hard bound permits at most two checks per
            // exchange, plus eight local boundaries. Unchanged context lengths
            // are pinned; a changed context fails before adopting any values.
            val checkWork = snapshotWorkAdd(512, snapshotWorkAdd(snapshotWorkScale(measure.first, 4), snapshotWorkScale(measure.second, 128)))
            operations.reserveSyncWork(unit, snapshotWorkScale(checkWork, 136), readyAtomic = true)
        }
        val host = WorkspaceCharge(workspace.add(unit, WorkspaceCharge(64), WorkspaceCharge(workspace.scale(unit, measure.second, 8))).entries,
            bootstrapCheckedAdd(32768, bootstrapCheckedAdd(workspace.scale(unit, measure.first, 24), workspace.scale(unit, measure.second, 512))))
        workspace.check(unit, workspace.add(unit, independent, host), statementCache = cache)
        val condition = if (remoteReplace) "a.binding_state='anonymous' AND p.kind='remote_replace' AND p.target_user_id=?"
            else "a.binding_state='attached' AND a.rebuild_required=1 AND a.attached_user_id=? AND p.kind='" +
                (if (mode == SnapshotRebuildOutboxMode.CLEAN) operationKindNone else operationKindSourceRecovery) + "'"
        val value = db.withPreparedStatement("""SELECT json_array(i.database_id,i.service_id,i.generation_id,i.local_version,
            i.physical_shape,i.fingerprint,a.attachment_epoch,a.current_source_id,a.binding_state,a.attached_user_id,
            a.pending_initialization_id,a.last_bundle_seq_seen,a.rebuild_required,f.dirty_revision,f.baseline_revision,
            s.next_source_bundle_id,o.state,o.source_id,o.source_bundle_id,o.canonical_request_hash,
            p.kind,p.target_user_id,p.reason,p.replacement_source_id,(SELECT schema_version FROM pragma_schema_version),
            json(COALESCE((SELECT json_group_array(json_array(name,type,tbl_name,COALESCE(sql,''))) FROM
                (SELECT name,type,tbl_name,sql FROM sqlite_temp_schema ORDER BY name,type)),'[]')))
            $sourceTables WHERE i.singleton_key=1 AND a.singleton_key=1 AND f.singleton_key=1 AND o.singleton_key=1 AND p.singleton_key=1
            AND i.database_id=? AND i.service_id=? AND i.generation_id=? AND i.fingerprint=? AND i.physical_shape=?
            AND a.current_source_id=? AND $condition AND o.state='none'
            AND NOT EXISTS(SELECT 1 FROM _sync_dirty_rows) AND NOT EXISTS(SELECT 1 FROM _sync_outbox_rows)
            AND NOT EXISTS(SELECT 1 FROM _sync_upload_journal)""", cache) {
            it.bindText(1, config.databaseId); it.bindText(2, config.serviceId); it.bindLong(3, config.generationId)
            it.bindText(4, state.admittedContract.generation.fingerprint)
            it.bindText(5, managedPhysicalShape(state.validated.tableInfoByName, state.validated.keyByTable))
            it.bindText(6, state.sourceId); it.bindText(7, state.userId)
            if (it.step()) it.getText(0) else null
        }
        requireNotNull(value) { "bootstrap lifecycle admission changed" }
        val policy = kotlinx.serialization.json.buildJsonArray {
            for (field in listOf("oversqlite.bootstrap.context.v1", config.maxSyncWorkspaceEntries.toString(),
                config.maxSyncWorkspaceBytes.toString(), config.maxSyncWorkUnitsPerInvocation.toString(), state.admittedContract.limits.toString()))
                add(kotlinx.serialization.json.JsonPrimitive(field))
        }.toString().encodeToByteArray()
        val bytes = value.encodeToByteArray()
        val digest = newSnapshotDigest()
        digest.append(bootstrapBigEndian(policy.size.toLong(), 8)); digest.append(policy)
        digest.append(bootstrapBigEndian(bytes.size.toLong(), 8)); digest.append(bytes)
        return digest.finishHex()
    }

    internal suspend fun ensureOperationSnapshotStaged(
        state: RuntimeState,
        mode: SnapshotRebuildOutboxMode,
        rotatedSourceId: String? = null,
        replacementReason: String? = null,
        remoteReplace: Boolean = false,
        allowSessionRefresh: Boolean = true,
    ): Pair<SnapshotSession, SnapshotApplyGuard> {
        requireCleanSnapshotEntry()
        bundleApplier.survivors.inspectSnapshotCapabilities(state)
        // Intrinsic row/apply capacity is known from admitted authority and
        // must fail before any recovery snapshot request or replacement.
        val limits=negotiateSnapshotLimits(state.admittedContract,config)
        var guard = snapshotGate.pin(mode, remoteReplace, if (remoteReplace) state.userId else "")
        var operation = guard.lifecycle.operation
        val source = rotatedSourceId ?: state.sourceId
        val lifecycle = OversqliteApplyLifecycle(db, config, localStore, bundleApplier.survivors, json)
        val replacementEstablished = rotatedSourceId != null && operation.stagedSnapshotId.isNotEmpty()
        var session = installation.retained(operation, source)
        if (session != null) {
            val retained = session
            val matches = operations.completeWorkPhase("retained snapshot admission") { db.transaction(TransactionMode.DEFERRED) {
                syncDiagnosticWork("snapshot.final_guard") { snapshotGate.validateFinal(guard) }
                if (lifecycle.snapshotStageGuard(state, retained) != operation.snapshotStageGuard ||
                    (!remoteReplace && retained.snapshotBundleSeq < installation.floor())) false
                else { validateOperationSnapshotCursor(operation); true }
            } }
            if (!matches) {
                if(currentCoroutineContext()[AutomaticRecoveryContext]!=null)operations.chargeRecoveryReplan()
                session = null
            }
        }
        if (session == null) {
            val floor = if (remoteReplace) 0L else operations.completeWorkPhase("snapshot floor") { db.transaction(TransactionMode.DEFERRED) { installation.floor() } }
            val request = if (remoteReplace) null else if (rotatedSourceId != null && !replacementEstablished) {
                val reason = checkNotNull(replacementReason)
                check(reason.isNotEmpty())
                SnapshotSessionCreateRequest(SnapshotSourceReplacement(state.sourceId, rotatedSourceId, reason))
            } else SnapshotSessionCreateRequest(recovery = SnapshotRecovery(floor))
            session = createGuardedSnapshotSession(state, if (replacementEstablished) source else state.sourceId, request, guard)
            check(session.snapshotBundleSeq >= floor) { "snapshot boundary precedes installed observation floor" }
            guard = publishSnapshotSession(state, mode, session, guard, source)
            operation = guard.lifecycle.operation
        }
        val current = checkNotNull(session)
        if (!operation.snapshotStageComplete) {
            try {
                downloadSnapshotSession(state.copy(sourceId = source), current,
                    limits,
                    initialTotals = SnapshotTransferTotals(operation.snapshotStagedRowCount, operation.snapshotStagedByteCount)) { totals ->
                    syncDiagnosticWork("snapshot.final_guard") { snapshotGate.validateFinal(guard) }
                    check(lifecycle.snapshotStageGuard(state, current) == operation.snapshotStageGuard) { "snapshot staging context changed" }
                    val complete = totals.rows == current.rowCount && totals.bytes == current.logicalByteCount
                    if (complete) verifyGenerationSnapshot(state, current, source, "verification_last_chunk")
                    val next = operation.copy(snapshotStagedRowCount = totals.rows, snapshotStagedByteCount = totals.bytes,
                        snapshotStageComplete = complete)
                    operationStateStore.persistState(next)
                    operation = next
                    guard = guard.copy(lifecycle = guard.lifecycle.copy(operation = next))
                }
            } catch (error: SnapshotHttpException) {
                if (!allowSessionRefresh || (error.errorCode != "snapshot_session_expired" && error.errorCode != "snapshot_session_not_found")) throw error
                operations.chargeExpiredSnapshotReplacement()
                validateSnapshotDispatch(state.copy(sourceId = source),current)
                val floor = if (remoteReplace) 0L else operations.completeWorkPhase("snapshot floor") { db.transaction(TransactionMode.DEFERRED) { installation.floor() } }
                val request = if (remoteReplace) null else SnapshotSessionCreateRequest(recovery = SnapshotRecovery(floor))
                val fresh = remoteApi.createSnapshotSession(source, request) {
                    validateSnapshotDispatch(state.copy(sourceId = source), current)
                }
                check(fresh.snapshotBundleSeq >= floor)
                publishSnapshotSession(state, mode, fresh, guard, source)
                return ensureOperationSnapshotStaged(state, mode, rotatedSourceId, replacementReason, remoteReplace, false)
            }
        }
        // Complete verified local staging owns its lifetime independently of server TTL.
        operations.completeWorkPhase("snapshot seal verification") { db.transaction(TransactionMode.DEFERRED) {
            syncDiagnosticWork("snapshot.final_guard") { snapshotGate.validateFinal(guard) }
            check(lifecycle.snapshotStageGuard(state, current) == operation.snapshotStageGuard) { "snapshot staging context changed" }
            verifyGenerationSnapshot(state, current, source, "verification_seal")
        } }
        return current to guard
    }

    private suspend fun publishSnapshotSession(state: RuntimeState, mode: SnapshotRebuildOutboxMode,
        session: SnapshotSession, guard: SnapshotApplyGuard, source: String): SnapshotApplyGuard {
        val lifecycle = OversqliteApplyLifecycle(db, config, localStore, bundleApplier.survivors, json)
        try {
            val operation = operations.completeWorkPhase("snapshot publication") { db.transaction(TransactionMode.IMMEDIATE) {
                syncDiagnosticWork("snapshot.final_guard") { snapshotGate.validateFinal(guard) }
                stageStore.clearAllSnapshotStages()
                val next = guard.lifecycle.operation.copy(stagedSnapshotId = session.snapshotId,
                    snapshotBundleSeq = session.snapshotBundleSeq, snapshotRowCount = session.rowCount,
                    snapshotByteCount = session.logicalByteCount, snapshotStageComplete = false,
                    snapshotStagedRowCount = 0, snapshotStagedByteCount = 0,
                    snapshotStageGuard = lifecycle.snapshotStageGuard(state, session),
                    snapshotTransfer = installation.encodeSeal(session, source))
                operationStateStore.persistState(next)
                OversqliteWorkspace(db, config).check(applyUnit("snapshot-stage", session.snapshotBundleSeq, session.snapshotId))
                next
            } }
            return guard.copy(lifecycle = guard.lifecycle.copy(operation = operation))
        } catch (error: Throwable) {
            try {
                retireOperationSnapshot(session.snapshotId, source)
            } catch (cleanup: Throwable) {
                if (cleanup !== error) error.addSuppressed(cleanup)
            }
            throw error
        }
    }

    private suspend fun validateOperationSnapshotCursor(operation: OversqliteOperationState) {
        operation.validateSnapshotStageMetadata()
        db.withPreparedStatement("""SELECT snapshot_stage_entries,
            (SELECT COALESCE(min(row_ordinal),0) FROM _sync_snapshot_stage WHERE snapshot_id=?),
            (SELECT COALESCE(max(row_ordinal),0) FROM _sync_snapshot_stage WHERE snapshot_id=?),
            EXISTS(SELECT 1 FROM _sync_snapshot_stage WHERE snapshot_id<? LIMIT 1),
            EXISTS(SELECT 1 FROM _sync_snapshot_stage WHERE snapshot_id>? LIMIT 1)
            FROM _sync_operation_state WHERE singleton_key=1""") { statement ->
            for (index in 1..4) statement.bindText(index, operation.stagedSnapshotId)
            check(statement.step())
            require(statement.getLong(0) == operation.snapshotStagedRowCount &&
                statement.getLong(1) == (if (operation.snapshotStagedRowCount == 0L) 0L else 1L) &&
                statement.getLong(2) == operation.snapshotStagedRowCount && statement.getLong(3) == 0L &&
                statement.getLong(4) == 0L) { "retained snapshot cursor differs from staged rows" }
        }
    }

    internal suspend fun downloadSnapshotSession(
        state: RuntimeState,
        session: SnapshotSession,
        limits: SnapshotNegotiation,
        initialTotals: SnapshotTransferTotals = SnapshotTransferTotals(),
        fetchChunk: (suspend (afterRowOrdinal: Long) -> SnapshotChunkResponse)? = null,
        onChunkStaged: suspend (SnapshotTransferTotals) -> Unit = {},
    ): SnapshotTransferTotals = syncPhase("download_staging", session.rowCount, session.logicalByteCount) {
        var totals = initialTotals
        var afterRowOrdinal = initialTotals.rows
        while (true) {
            stageStore.ensureSnapshotWorkAvailable(applyUnit("snapshot-stage", session.snapshotBundleSeq, session.snapshotId))
            validateSnapshotDispatch(state,session)
            val chunk = fetchChunk?.invoke(afterRowOrdinal) ?: fetchSnapshotChunkWithOneRowProgress(
                state, session, afterRowOrdinal, limits,
            )
            val nextTotals = SnapshotTransferTotals(
                rows = checkedAddSnapshotLong(totals.rows, chunk.rows.size.toLong()) {
                    "snapshot accumulated row count overflow"
                },
                bytes = checkedAddSnapshotLong(totals.bytes, chunk.logicalByteCount) {
                    "snapshot accumulated byte count overflow"
                },
            )
            if (nextTotals.rows > session.rowCount || nextTotals.bytes > session.logicalByteCount) {
                throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK)
            }
            if (chunk.hasMore && (nextTotals.rows == session.rowCount || nextTotals.bytes == session.logicalByteCount)) {
                throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK)
            }
            if (!chunk.hasMore && (nextTotals.rows != session.rowCount || nextTotals.bytes != session.logicalByteCount)) {
                throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK)
            }
            stageStore.stageSnapshotChunk(state, session, chunk, afterRowOrdinal) { onChunkStaged(nextTotals) }
            totals = nextTotals
            if (chunk.hasMore) {
                afterRowOrdinal = chunk.nextRowOrdinal
                continue
            }
            return@syncPhase totals
        }
        @Suppress("UNREACHABLE_CODE")
        error("snapshot transfer did not terminate")
    }

    internal suspend fun createGuardedSnapshotSession(
        state: RuntimeState,
        source: String,
        request: SnapshotSessionCreateRequest?,
        pinned: SnapshotApplyGuard? = null,
    ): SnapshotSession {
        val guard = pinned ?: snapshotGate.pin(SnapshotRebuildOutboxMode.CLEAN)
        val unit = "snapshot-create"
        val authority = operations.completeWorkPhase("snapshot create admission") { db.transaction(TransactionMode.DEFERRED) {
            requireCleanSnapshotWork(db)
            syncDiagnosticWork("snapshot.final_guard") { snapshotGate.validateFinal(guard) }
            installation.context(state, unit)
        } }
        return remoteApi.createSnapshotSession(source, request) {
            bundleApplier.survivors.beginSnapshotVerification()
            db.transaction(TransactionMode.DEFERRED) {
                requireCleanSnapshotWork(db)
                syncDiagnosticWork("snapshot.final_guard") { snapshotGate.validateFinal(guard) }
                check(installation.context(state, unit) == authority) { "snapshot dispatch context changed" }
            }
        }
    }

    internal suspend fun validateSnapshotDispatch(state: RuntimeState, session: SnapshotSession) {
        operations.completeWorkPhase("snapshot dispatch admission") { db.transaction(TransactionMode.DEFERRED) {
            kotlinx.coroutines.currentCoroutineContext()[AutomaticRecoveryContext]?.pin?.let { automaticRecovery.validate(it) }
            requireCleanSnapshotWork(db)
            val operation = operationStateStore.loadState()
            require(installation.retained(operation,state.sourceId) == session) { "snapshot dispatch context changed" }
            val lifecycle = OversqliteApplyLifecycle(db,config,localStore,bundleApplier.survivors,json)
            check(lifecycle.snapshotStageGuard(state,session) == operation.snapshotStageGuard) { "snapshot dispatch context changed" }
        } }
    }

    private suspend fun pullIncremental(state: RuntimeState): IncrementalPullResult {
        requireIncrementalPullState(state)

        val initialBundleSeq = attachmentStateStore.loadState().lastBundleSeqSeen
        var afterBundleSeq = initialBundleSeq
        val maxBundles = minOf(config.transferTargets.maxPullBundles, state.admittedContract.limits.maxPullBundles)
        var targetBundleSeq = 0L
        val updatedTables = linkedSetOf<String>()

        while (true) {
            val response = remoteApi.sendPullRequest(
                afterBundleSeq, maxBundles, targetBundleSeq, state.sourceId,
                state.admittedContract.limits.maxPullDecodedBytes,
            )
            validatePullResponse(response, afterBundleSeq)
            response.bundles.forEach { bundle ->
                bundle.rows.forEach { row ->
                    require(row.schema == state.validated.schemaForTable(row.table)) {
                        "bundle row schema ${row.schema} does not match client schema ${state.validated.schemaForTable(row.table)}"
                    }
                }
            }

            if (targetBundleSeq == 0L) {
                targetBundleSeq = response.stableBundleSeq
            } else if (response.stableBundleSeq != targetBundleSeq) {
                error("pull response stable bundle seq changed from $targetBundleSeq to ${response.stableBundleSeq}")
            }

            for (bundle in response.bundles) {
                try { updatedTables += applyPulledBundle(state,bundle) }
                catch (failure: Throwable) {
                    val reason=historicalRecoveryReason(failure) ?: throw failure
                    if(!automaticRecovery.enabled())throw failure
                    bundleApplier.survivors.inspectSnapshotCapabilities(state)
                    automaticRecovery.capture(state,reason,bundle.bundleSeq,bundle.bundleHash) { installation.context(state,"automatic-recovery",automatic=false) }
                    throw HistoricalBaselineRequired(bundle.bundleSeq,bundle.bundleHash,reason)
                }
                afterBundleSeq = bundle.bundleSeq
            }

            if (afterBundleSeq >= response.stableBundleSeq) {
                return IncrementalPullResult(
                    updatedTables = updatedTables,
                    outcome = if (afterBundleSeq == initialBundleSeq && response.stableBundleSeq == initialBundleSeq) {
                        RemoteSyncOutcome.ALREADY_AT_TARGET
                    } else {
                        RemoteSyncOutcome.APPLIED_INCREMENTAL
                    },
                )
            }
            if (!response.hasMore && response.bundles.isEmpty()) {
                error("pull ended before reaching stable bundle seq ${response.stableBundleSeq}")
            }
            if (!response.hasMore && afterBundleSeq < response.stableBundleSeq) {
                error("pull ended early at bundle seq $afterBundleSeq before stable bundle seq ${response.stableBundleSeq}")
            }
        }
    }

    private suspend fun applyPulledBundle(
        state: RuntimeState,
        bundle: Bundle,
    ): Set<String> {
        val updatedTables = applyExecutor.inApplyModeTransaction(
            state,
            beforeApplyMode = {
                val outboundCount = outboxStateStore.countRows()
                if (outboundCount > 0 || outboxStateStore.loadBundleState().state != outboxStateNone) {
                    throw PendingPushReplayException(outboundCount)
                }
                val dirtyCount = syncStateStore.countDirtyRows()
                if (dirtyCount > 0) throw DirtyStateRejectedException(dirtyCount)
            },
        ) { statementCache ->
            val updatedTables = linkedSetOf<String>()
            bundleApplier.survivors.prepare(
                state, applyUnit("pull", bundle.bundleSeq, bundle.bundleHash), bundle.rows.size,
                inputSequence = bundle.bundleSeq,
                workspaceStatementCache = statementCache, replayStatementCache = statementCache,
            ) { index ->
                val row = bundle.rows[index]
                val (key, pk) = localStore.bundleRowKeyToLocalKey(state, row.table, row.key)
                val current = syncStateStore.loadStructuredRowState(state.validated.schemaForTable(row.table), row.table, key, statementCache)
                if (current.exists && current.rowVersion >= row.rowVersion) {
                    bundleApplier.survivors.capture(state, row.table, pk,
                        workspaceStatementCache = statementCache, replayStatementCache = statementCache)
                } else bundleApplier.survivors.wire(row.table, pk, row.payload)
            }
            bundleApplier.survivors.installUniqueFinalState(state,statementCache)
            for (row in bundle.rows) {
                val schema = state.validated.schemaForTable(row.table)
                val (keyJson, localPk) = localStore.bundleRowKeyToLocalKey(state, row.table, row.key)
                val current = syncStateStore.loadStructuredRowState(schema, row.table, keyJson, statementCache)
                if (current.exists && current.rowVersion >= row.rowVersion) {
                    if(current.rowVersion==row.rowVersion) syncStateStore.installAuthoritativeBaseline(schema,row.table,keyJson,if(row.op=="DELETE") "deleted" else "live",row.rowVersion,(row.payload as? JsonObject)?.let { localStore.captureAuthoritativePayload(row.table,it,statementCache) },"pull",bundle.bundleSeq,statementCache)
                    continue
                }
                bundleApplier.applyAuthoritativeRow(state, row, localPk, keyJson, statementCache,"pull",bundle.bundleSeq)
                syncStateStore.deleteDirtyRow(schema, row.table, keyJson, statementCache)
                updatedTables += row.table.lowercase()
            }
            for (pass in 0 until 64) {
                val restored = bundleApplier.survivors.restore(state, statementCache,
                    workspaceStatementCache = statementCache, replayStatementCache = statementCache)
                updatedTables += restored
                if (restored.isEmpty()) break
            }
            bundleApplier.survivors.restore(state, statementCache, verifyOnly = true,
                workspaceStatementCache = statementCache, replayStatementCache = statementCache)
            bundleApplier.survivors.finish(replayStatementCache = statementCache)
            attachmentStateStore.markBundleSeen(bundle.bundleSeq, statementCache)
            updatedTables
        }
        notifyUpdatedTables(updatedTables)
        return updatedTables
    }

    internal suspend fun applyStagedSnapshot(
        state: RuntimeState,
        session: SnapshotSession,
        rotatedSourceId: String?,
        outboxMode: SnapshotRebuildOutboxMode,
        pinnedGuard: SnapshotApplyGuard? = null,
        finalizer: suspend () -> Unit = {},
    ): DownloadWorkflowResult {
        if (rotatedSourceId != null) {
            requireFreshRotatedSourceState(rotatedSourceId)
        }
        val guard = pinnedGuard ?: snapshotGate.pin(outboxMode)
        val result = applyExecutor.inApplyModeTransaction(state) { statementCache ->
            syncDiagnosticWork("snapshot.final_guard") { snapshotGate.validateFinal(guard) }
            val lifecycle = OversqliteApplyLifecycle(db, config, localStore, bundleApplier.survivors, json, statementCache)
            if (guard.lifecycle.operation.snapshotStageGuard.isNotEmpty())
                check(lifecycle.snapshotStageGuard(state, session) == guard.lifecycle.operation.snapshotStageGuard) { "snapshot final context changed" }
            if (!guard.remoteReplace) check(session.snapshotBundleSeq >= installation.floor()) { "snapshot boundary precedes installed observations" }
            syncPhase("preparation_reconciliation", session.rowCount, session.logicalByteCount) { lifecycle.prepareSnapshot(state, session) }
            operations.admitSnapshotFinalTransition {
                syncPhase("verification_final_apply", session.rowCount, session.logicalByteCount) {
                    installation.verify(state, session, rotatedSourceId ?: state.sourceId, statementCache)
                }
                syncDiagnosticWork("snapshot.preservation") { bundleApplier.survivors.validateSnapshotPreservation(state, statementCache) }
            }
            syncDiagnosticWork("snapshot.recovery_ready") { automaticRecovery.ready() }
            val targetContext = OversqliteProvenanceContextStore(db).target(state, rotatedSourceId ?: state.sourceId)
            syncDiagnosticWork("snapshot.unique_state_installation") { bundleApplier.survivors.installUniqueFinalState(state, statementCache) }
            var stagedRowCount = 0L
            var afterRowOrdinal = 0L
            syncPhase("row_installation", session.rowCount, session.logicalByteCount) {
            while (true) {
                val progress = consumeNextStagedSnapshotPage(
                    state = state,
                    session = session,
                    afterRowOrdinal = afterRowOrdinal,
                ) { rows ->
                    for (row in rows) {
                        try {
                            val payload = json.parseToJsonElement(row.payload) as? JsonObject
                                ?: error(
                                    "bundle row payload must be a JSON object for " +
                                        "${row.schemaName}.${row.tableName}",
                                )
                            localStore.upsertRow(row.tableName,payload,PayloadSource.AUTHORITATIVE_WIRE,statementCache)
                            syncStateStore.installAuthoritativeBaseline(
                                schemaName = row.schemaName,
                                tableName = row.tableName,
                                keyJson = row.keyJson,
                                rowVersion = row.rowVersion,
                                kind = "live", payload = localStore.captureAuthoritativePayload(row.tableName,payload,statementCache),
                                origin = "snapshot", bundleSeq = session.snapshotBundleSeq, statementCache = statementCache,
                                projectionStatementCache = statementCache, targetContext = targetContext,
                            )
                            applyExecutor.afterAppliedRowForTest()
                        } catch (error: Throwable) {
                            if (error is kotlinx.coroutines.CancellationException || error is SyncWorkspaceLimitException ||
                                error is SyncWorkLimitException || error is FKReconciliationRequiredException || error is ResolutionBaseUnavailableException) throw error
                            throw SnapshotRowApplyException(
                                rowOrdinal = row.rowOrdinal,
                                schemaName = row.schemaName,
                                tableName = row.tableName,
                            )
                        }
                    }
                }
                stagedRowCount = checkedAddSnapshotLong(stagedRowCount, progress.rowCount.toLong()) {
                    "staged snapshot applied row count overflow"
                }
                val lastRowOrdinal = progress.lastRowOrdinal ?: break
                afterRowOrdinal = lastRowOrdinal
            }

            }
            if (stagedRowCount != session.rowCount) {
                throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK)
            }

            if (rotatedSourceId != null && state.sourceId != rotatedSourceId) sourceStateStore.markRotated(state.sourceId, rotatedSourceId)
            val currentSourceId = rotatedSourceId ?: state.sourceId
            installation.installAbsence(state, session, statementCache, targetContext)
            syncPhase("final_validation", session.rowCount, session.logicalByteCount) {
                lifecycle.restoreAndVerify(state, statementCache)
                lifecycle.verifySnapshot(state, session, statementCache)
            }
            bundleApplier.survivors.finish(replayStatementCache = statementCache)
            // Source recovery keeps pending local intent in place through the installation. The server
            // accepted the replacement source to produce this snapshot, so the intent now moves onto
            // the verified authoritative state before the replacement source is published.
            if (currentCoroutineContext()[SourceRecoveryIntentAdmission] != null) syncDiagnosticWork("snapshot.recovery_intent") {
                sourceRecoveryIntent.carryOntoInstalledSnapshot(state, statementCache)
            }
            syncPhase("snapshot.publication", session.rowCount, session.logicalByteCount) {
            attachmentStateStore.persistAttachedState(
                sourceId = currentSourceId,
                userId = state.userId,
                schemaName = unconfirmedLocalSchema,
                lastBundleSeqSeen = session.snapshotBundleSeq,
                rebuildRequired = false,
                pendingInitializationId = "",
                statementCache = statementCache,
            )
            automaticRecovery.completed(session.snapshotBundleSeq)
            operationStateStore.persistState(OversqliteOperationState(), statementCache)

            finalizer()
            }

            syncPhase("snapshot.cleanup", session.rowCount) { stageStore.deleteSnapshotStage(session.snapshotId, statementCache) }
            DownloadWorkflowResult(
                outcome = RemoteSyncOutcome.APPLIED_SNAPSHOT,
                updatedTables = state.validated.tables.map { it.tableName.lowercase() }.toSet(),
                restore = RestoreSummary(
                    bundleSeq = session.snapshotBundleSeq,
                    rowCount = session.rowCount,
                ),
                rotatedSourceId = rotatedSourceId,
            )
        }
        snapshotDiagnostics()?.recordAppliedRows(session.rowCount)
        retireOperationSnapshot(session.snapshotId, rotatedSourceId ?: state.sourceId)
        notifyUpdatedTables(result.updatedTables)
        return result
    }

    private suspend fun consumeNextStagedSnapshotPage(
        state: RuntimeState,
        session: SnapshotSession,
        afterRowOrdinal: Long,
        consume: suspend (List<StagedSnapshotRow>) -> Unit,
    ): StagedSnapshotPageProgress {
        val page = stageStore.loadStagedSnapshotPage(
            state = state,
            snapshotId = session.snapshotId,
            afterRowOrdinal = afterRowOrdinal,
            maxRows = config.snapshotApplyBatchRows,
            maxBytes = config.snapshotApplyBatchBytes,
        )
        if (page.rows.isNotEmpty()) {
            snapshotDiagnostics()?.recordApplyPage(page.rows.size, page.retainedTextBytes)
            applyExecutor.afterApplyPageLoadedForTest()
        }
        consume(page.rows)
        if (page.rows.isNotEmpty()) snapshotDiagnostics()?.recordInstalledPage(page.rows.size, page.retainedTextBytes)
        return StagedSnapshotPageProgress(
            rowCount = page.rows.size,
            lastRowOrdinal = page.lastRowOrdinal,
        )
    }

    private suspend fun requireIncrementalPullState(state: RuntimeState) {
        requireSnapshotRebuildState(SnapshotRebuildOutboxMode.CLEAN)
        requireOrdinarySyncAllowed()
    }

    private suspend fun requireSnapshotRebuildState(outboxMode: SnapshotRebuildOutboxMode) = requireCleanSnapshotEntry()

    private suspend fun requireOrdinarySyncAllowed() {
        val operation = operationStateStore.loadState()
        if (operation.kind == operationKindSourceRecovery) {
            throw SourceRecoveryRequiredException(operation.requireSourceRecoveryReason())
        }
        if (attachmentStateStore.loadState().rebuildRequired) {
            throw RebuildRequiredException()
        }
    }

    private suspend fun markCheckpointRecoveryRequired(reason: String) {
        db.transaction(TransactionMode.IMMEDIATE) {
            attachmentStateStore.setRebuildRequired(true)
            val operation = operationStateStore.loadState()
            if (operation.kind == operationKindSourceRecovery) {
                return@transaction
            }
            check(operation.kind == operationKindNone) {
                "cannot begin checkpoint recovery while ${operation.kind} is in progress"
            }
            operationStateStore.persistState(
                operation.copy(
                    reason = operation.reason.takeIf(::isCheckpointRecoveryReason) ?: reason,
                ),
            )
        }
    }

    private suspend fun fetchSnapshotChunkWithOneRowProgress(
        state: RuntimeState,
        session: SnapshotSession,
        afterRowOrdinal: Long,
        limits: SnapshotNegotiation,
    ): SnapshotChunkResponse = try {
        remoteApi.fetchSnapshotChunk(
            snapshotId = session.snapshotId,
            sourceId = state.sourceId,
            snapshotBundleSeq = session.snapshotBundleSeq,
            afterRowOrdinal = afterRowOrdinal,
            maxRows = limits.maxRows,
            maxBytes = limits.maxBytes,
            admittedMaxBytes = state.admittedContract.limits.maxSnapshotDecodedBytesPerChunk,
            beforeDispatch = { validateSnapshotDispatch(state, session) },
        )
    } catch (tooSmall: SnapshotChunkTooSmallException) {
        val hardLimit = state.admittedContract.limits.maxSnapshotDecodedBytesPerChunk
        if (tooSmall.requiredBytes > hardLimit) {
            throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK)
        }
        try {
            remoteApi.fetchSnapshotChunk(
                snapshotId = session.snapshotId,
                sourceId = state.sourceId,
                snapshotBundleSeq = session.snapshotBundleSeq,
                afterRowOrdinal = afterRowOrdinal,
                maxRows = 1,
                maxBytes = tooSmall.requiredBytes,
                admittedMaxBytes = hardLimit,
                beforeDispatch = { validateSnapshotDispatch(state, session) },
            )
        } catch (_: SnapshotChunkTooSmallException) {
            throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK)
        }
    }

    private suspend fun requireFreshRotatedSourceState(rotatedSourceId: String) {
        val sourceState = sourceStateStore.loadState(rotatedSourceId)
            ?: error("_sync_source_state is missing for the rotated source")
        require(sourceState.nextSourceBundleId == 1L && sourceState.replacedBySourceId.isBlank()) {
            "rotated rebuild requires a fresh replacement source"
        }
    }
}

internal fun isCheckpointRecoveryReason(reason: String): Boolean =
    reason in setOf("history_pruned", "checkpoint_ahead", "historical_repair_required", "conflicting_desired_state", "explicit_rebuild", "resume")

internal data class IncrementalPullResult(
    val updatedTables: Set<String>,
    val outcome: RemoteSyncOutcome,
)

/**
 * Typed bootstrap chunk rows that fit [freeEntries] of fresh workspace quota. Each row costs its
 * measured [transportEntriesPerRow] while its chunk is received, plus its durable typed row, wire
 * frame and page record; one receipt page also needs transient room for its rows. Never below one row
 * (the exact admission checks then decide) and never above the negotiated [maxRows].
 */
internal fun bootstrapChunkRowsForQuota(freeEntries: Long, transportEntriesPerRow: Long, maxColumns: Int, maxRows: Int): Int {
    require(transportEntriesPerRow > 0 && maxColumns >= 0 && maxRows > 0)
    val available = freeEntries - 2 * (32 + 4L * maxColumns) - 256
    val rows = if (available <= 0) 1L else available / (transportEntriesPerRow + 3)
    return rows.coerceIn(1L, maxRows.toLong()).toInt()
}

internal fun bootstrapCeilDiv(value: Long, divisor: Long): Long {
    require(value >= 0 && divisor > 0)
    return value / divisor + if (value % divisor == 0L) 0 else 1
}
