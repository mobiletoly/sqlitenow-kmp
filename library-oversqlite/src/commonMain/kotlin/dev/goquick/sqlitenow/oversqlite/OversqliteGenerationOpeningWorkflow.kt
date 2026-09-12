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
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowCoordinatedMigrationSqlPolicy
import dev.goquick.sqlitenow.core.SqliteNowInternalApi
import dev.goquick.sqlitenow.core.SqliteNowUnpublishedSession
import dev.goquick.sqlitenow.core.TransactionMode
import dev.goquick.sqlitenow.core.sqlite.use
import io.ktor.client.HttpClient
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

private const val generationPlanDigestDomain = "sqlitenow-generation-plan-v1"
private const val sourceRecoveryReceiptDigestDomain = "sqlitenow-source-recovery-receipt-v1"

@OptIn(SqliteNowInternalApi::class)
internal class OversqliteGenerationOpeningWorkflow(
    private val session: SqliteNowUnpublishedSession,
    private val config: OversqliteConfig,
    private val httpClient: HttpClient,
    private val resolver: Resolver,
    private val plan: OversqliteUpgradePlan?,
    private val clientLeaseGroup: Any,
    private val currentInspection: () -> OpeningInspection,
    private val updateInspection: (OpeningInspection) -> Unit,
    private val currentStatus: () -> OversqliteOpeningStatus,
    private val updateStatus: (OversqliteOpeningStatus) -> Unit,
    private val publishReadyClient: suspend (DefaultOversqliteClient) -> Unit,
    private val mutationAuthorityFactory: (SafeSQLiteConnection) -> OversqliteOpeningMutationAuthority,
    private val failureStatusFor: (Throwable) -> OversqliteOpeningStatus,
) {
    private var inspection: OpeningInspection
        get() = currentInspection()
        set(value) = updateInspection(value)

    private var cachedStatus: OversqliteOpeningStatus
        get() = currentStatus()
        set(value) = updateStatus(value)

    internal var failureAdmissionEdge: OversqliteOpeningRecord? = null
    internal var oldClient: DefaultOversqliteClient? = null
    internal var targetClient: DefaultOversqliteClient? = null

    suspend fun advance(): OversqliteOpeningStatus {
        val installed = checkNotNull(inspection.installed)
        val connection = session.withConnection { it }
        val store = OversqliteOpeningStore(connection)
        var record = store.readOrNull()
        if (record?.isRetiredPendingMarker() == true) {
            cachedStatus = statusFromRecord(record)
            return cachedStatus
        }
        if (record?.outcome == "ready" && generationRecordIdentifiesPlan(record)) {
            return publishCompletedGeneration(record)
        }
        if (record == null || record.outcome == "ready") {
            val client = prepareOldGenerationClient(connection, installed, open = false)
            var retainedByOpening = false
            try {
                val attachment = OversqliteAttachmentStateStore(connection).loadState()
                require(attachment.bindingState == attachmentBindingAttached && attachment.attachedUserId.isNotBlank()) {
                    "generation change requires a completed attached old-generation lifecycle"
                }
                require(attachment.currentSourceId.isNotBlank() && attachment.pendingInitializationId.isBlank()) {
                    "generation change requires a stable old source without pending initialization"
                }
                val operation = OversqliteOperationStateStore(connection).loadState()
                val pending = inspectPreU0OldIntent(connection)
                if (pending.hasAny) {
                    try {
                        val oldAuthority = client.previewGenerationForUpgrade(
                            attachment.currentSourceId,
                            installed.generationId,
                        )
                        require(oldAuthority.fingerprint == installed.fingerprint) {
                            "old generation authority changed before coordinated drain"
                        }
                    } catch (failure: OversqliteRemoteContractException) {
                        if (failure.code != "retired_schema_generation" ||
                            failure.generationId != installed.generationId
                        ) {
                            throw failure
                        }
                        record = persistRetiredPendingMarker(
                            expected = checkNotNull(record),
                            pending = pending,
                            failure = failure,
                        )
                        cachedStatus = statusFromRecord(record)
                        return cachedStatus
                    }
                }
                require(operation.kind == operationKindNone) {
                    "generation change cannot start during another lifecycle operation"
                }
                val target = client.previewGenerationForUpgrade(attachment.currentSourceId, config.generationId)
                requireSchemaEvolutionCompatibility(
                    OversqliteContractStore(connection).readGeneration(installed.generationId),
                    target,
                )
                requireExactTargetSelection(target)
                requireNoAddedSynchronizedTableNamespaceCollisions(
                    connection,
                    OversqliteContractStore(connection).readGeneration(installed.generationId),
                    target,
                )
                val digest = generationPlanDigest(
                    target = target,
                    attachedUserId = attachment.attachedUserId,
                    initialOldSourceId = attachment.currentSourceId,
                )
                openingAdmissionReservationForTest?.invoke("generation_u0", connection)
                connection.transaction(TransactionMode.IMMEDIATE) {
                    evaluateOpeningAdmission(connection, config, plan, session.migrationTargetVersion!!)
                    recheckGenerationAdmissionAtU0(
                        connection = connection,
                        installed = installed,
                        target = target,
                        expectedAttachment = attachment,
                        expectedOperation = operation,
                    )
                    installPreservedLocalTableGuards(connection, plan!!.preservedLocalTables)
                    val preservation = preservedStateDigest(connection, plan.preservedLocalTables)
                    OversqliteContractStore(connection).installAdditional(target)
                    val current = store.readOrNull()
                    val initial = generationRecord(
                        phase = "upgrade_started",
                        revision = (current?.revision ?: 0L) + 1L,
                        planDigest = digest,
                        target = target,
                        attachment = attachment,
                        preservationDigest = preservation,
                    )
                    openingDurableCommitForTest?.invoke("before_u0", connection)
                    if (current == null) store.create(initial) else store.replaceReady(initial, current)
                    record = initial
                }
                openingDurableCommitForTest?.invoke("after_u0", connection)
                client.open().getOrThrow()
                oldClient = client
                retainedByOpening = true
            } finally {
                if (!retainedByOpening) {
                    client.closeFromCoordinatedOwner()
                }
            }
            cachedStatus = statusFromRecord(checkNotNull(record))
        } else {
            requireGenerationRecordMatchesPlan(record, installed)
        }

        if (record.nextAction == "resolve_rejection") {
            cachedStatus = statusFromRecord(record)
            return cachedStatus
        }

        if (record.phase == "upgrade_started") {
            record = transitionGenerationRecord(record, "resolving_old_intent")
        }
        if (record.phase == "resolving_old_intent" && !record.drainComplete) {
            val client = prepareOldGenerationClient(connection, installed)
            val attachment = OversqliteAttachmentStateStore(connection).loadState()
            require(attachment.currentSourceId == record.oldSourceId &&
                attachment.attachedUserId == record.attachedUserId
            ) { "old attachment changed during coordinated drain" }
            client.attach(record.attachedUserId).getOrThrow()
            if (!oldGenerationDrained(connection, record)) {
                try {
                    client.sync().getOrThrow()
                } catch (failure: Throwable) {
                    if (failure is SourceRecoveryRequiredException) {
                        record = recoverOldSource(record, client, failure)
                        try {
                            client.sync().getOrThrow()
                        } catch (nested: SourceRecoveryRequiredException) {
                            throw SourceReplacementDivergedException()
                        }
                    } else {
                    val conflict = client.pendingConflict().getOrThrow()
                    if (conflict != null) {
                        record = recordNeedsResolution(record, conflict)
                        cachedStatus = statusFromRecord(record)
                        return cachedStatus
                    }
                    if (failure is OversqliteRemoteContractException &&
                        failure.httpStatus == 422 && !failure.retryable
                    ) {
                        record = recordNeedsRejection(record, failure)
                        cachedStatus = statusFromRecord(record)
                        return cachedStatus
                    }
                    throw failure
                    }
                }
            }
            record = commitDrainWitness(record)
        }
        if (record.phase == "resolving_old_intent" && record.drainComplete) {
            val client = prepareOldGenerationClient(connection, installed)
            val retainedTarget = OversqliteContractStore(connection).readGeneration(config.generationId)
            val confirmed = client.previewGenerationForUpgrade(record.oldSourceId, config.generationId)
            require(confirmed.fingerprint == retainedTarget.fingerprint) {
                "target generation changed after U0 admission"
            }
            record = enterLocalMigration(record)
        }
        if (record.phase == "migrating_local") {
            record = runGenerationMigration(record)
        }
        if (record.phase == "local_migrated") {
            record = prepareGenerationReplacement(record)
        }
        if (record.phase == "reattach_pending") {
            record = confirmGenerationReplacement(record)
        }
        if (record.phase == "rebuild_running") {
            return rebuildAndPublishGeneration(record)
        }
        cachedStatus = statusFromRecord(record)
        return cachedStatus
    }

    private data class PreU0OldIntent(
        val dirtyCount: Long,
        val frozenCount: Long,
        val conflictCount: Long,
        val operationPending: Boolean,
    ) {
        val hasAny: Boolean
            get() = dirtyCount != 0L || frozenCount != 0L || conflictCount != 0L || operationPending
    }

    private suspend fun inspectPreU0OldIntent(connection: SafeSQLiteConnection): PreU0OldIntent {
        val outboxRows = connection.scalarLong("SELECT count(*) FROM _sync_outbox_rows")
        val outboxPending = connection.scalarLong(
            "SELECT count(*) FROM _sync_outbox_bundle WHERE singleton_key=1 AND state!='none'",
        ) != 0L
        return PreU0OldIntent(
            dirtyCount = connection.scalarLong("SELECT count(*) FROM _sync_dirty_rows"),
            frozenCount = if (outboxRows != 0L) outboxRows else if (outboxPending) 1L else 0L,
            conflictCount = connection.scalarLong(
                "SELECT count(*) FROM _sync_conflict_state WHERE singleton_key=1 AND decision_state='pending'",
            ),
            operationPending = connection.scalarLong(
                "SELECT count(*) FROM _sync_operation_state WHERE singleton_key=1 AND kind!='none'",
            ) != 0L,
        )
    }

    private suspend fun persistRetiredPendingMarker(
        expected: OversqliteOpeningRecord,
        pending: PreU0OldIntent,
        failure: OversqliteRemoteContractException,
    ): OversqliteOpeningRecord = session.withConnection { connection ->
        connection.transaction(TransactionMode.IMMEDIATE) {
            val current = checkNotNull(OversqliteOpeningStore(connection).readOrNull())
            require(current == expected && current.outcome == "ready") {
                "completed old-generation journal changed before retirement was recorded"
            }
            try {
                preservedStateDigest(connection, plan!!.preservedLocalTables)
            } catch (failure: Throwable) {
                throw OversqlitePreservationFailureException(failure)
            }
            val next = current.copy(
                outcome = "old_generation_unavailable",
                revision = current.revision + 1L,
                pendingDirtyCount = pending.dirtyCount,
                pendingFrozenCount = pending.frozenCount +
                    if (pending.operationPending && pending.frozenCount == 0L) 1L else 0L,
                pendingConflictCount = pending.conflictCount,
                targetReadsAvailable = false,
                writesAvailable = false,
                nextAction = "restore_support",
                failureCode = failure.code,
                failureMessage = "${failure.operation}:${failure.code}:generation=${failure.generationId}:" +
                    "fingerprint=${current.targetFingerprint}",
            )
            OversqliteOpeningStore(connection).update(next, current)
            next
        }
    }

    internal fun generationRecordIdentifiesPlan(record: OversqliteOpeningRecord): Boolean {
        val selected = plan ?: return false
        return record.mode == "generation_change" && record.databaseId == config.databaseId &&
            record.serviceId == config.serviceId && record.migrationId == selected.migrationId &&
            record.fromGenerationId == selected.fromGenerationId &&
            record.toGenerationId == selected.toGenerationId &&
            record.fromLocalVersion == selected.fromLocalVersion.toLong() &&
            record.toLocalVersion == selected.toLocalVersion.toLong()
    }

    internal suspend fun prepareOldGenerationClient(
        connection: SafeSQLiteConnection,
        installed: InstalledOpeningIdentity,
        open: Boolean = true,
    ): DefaultOversqliteClient {
        oldClient?.let { return it }
        val generation = try {
            OversqliteContractStore(connection).readGeneration(installed.generationId)
        } catch (failure: IllegalArgumentException) {
            throw OversqliteUpgradePreconditionException(
                "installed generation authority is missing or corrupt; restore the retained generation record",
            )
        }
        require(generation.fingerprint == installed.fingerprint) { "installed generation admission changed" }
        val oldConfig = config.copy(
            generationId = installed.generationId,
            syncTables = generation.tables.map { table ->
                SyncTable(tableName = table.table, syncKeyColumns = table.syncKeyColumns)
            },
        )
        val client = DefaultOversqliteClient.forOpening(
            connection,
            oldConfig,
            httpClient,
            DeferConflictResolver,
            clientLeaseGroup,
            mutationAuthorityFactory(connection),
        )
        if (open) {
            client.open().getOrThrow()
            oldClient = client
        }
        return client
    }

    private suspend fun recoverOldSource(
        expected: OversqliteOpeningRecord,
        client: DefaultOversqliteClient,
        failure: SourceRecoveryRequiredException,
    ): OversqliteOpeningRecord {
        require(expected.oldSourceRecoveryReceipt.isBlank()) {
            "nested old-source replacement is not supported during coordinated opening"
        }
        var recovered: OversqliteOpeningRecord? = null
        client.rebuildForOpening { replacementSourceId ->
            val connection = session.withConnection { it }
            val attachment = OversqliteAttachmentStateStore(connection).loadState()
            require(attachment.bindingState == attachmentBindingAttached &&
                attachment.attachedUserId == expected.attachedUserId &&
                replacementSourceId.isNotBlank() && replacementSourceId == attachment.currentSourceId &&
                replacementSourceId != expected.oldSourceId
            ) { "old-source recovery did not produce one stable replacement source" }
            val store = OversqliteOpeningStore(connection)
            val current = checkNotNull(store.readOrNull())
            require(current == expected && current.oldSourceRecoveryReceipt.isBlank()) {
                "coordinated opening journal changed during old-source recovery"
            }
            requirePreservation(connection, current)
            require(OversqliteOperationStateStore(connection).loadState().kind == operationKindNone) {
                "old-source recovery left an unresolved lifecycle operation"
            }
            val receipt = sha256Hex(
                listOf(
                    sourceRecoveryReceiptDigestDomain,
                    current.initialOldSourceId,
                    replacementSourceId,
                    failure.reason.name.lowercase(),
                ).joinToString("\u001f").encodeToByteArray(),
            )
            current.copy(
                oldSourceId = replacementSourceId,
                oldSourceRecoveryReceipt = receipt,
                revision = current.revision + 1L,
                outcome = "",
                failureCode = "",
                failureMessage = "",
            ).also {
                store.update(it, current)
                recovered = it
            }
        }.getOrThrow()
        return checkNotNull(recovered) { "old-source recovery omitted its atomic opening finalizer" }
    }

    private fun generationPlanDigest(
        target: SyncGeneration,
        attachedUserId: String,
        initialOldSourceId: String,
    ): String {
        val installed = checkNotNull(inspection.installed)
        return sha256Hex(listOf(
            generationPlanDigestDomain, config.databaseId, config.serviceId,
            attachedUserId, initialOldSourceId,
            installed.generationId, installed.fingerprint,
            target.id, target.fingerprint,
            installed.localVersion, session.migrationTargetVersion,
            plan!!.migrationId, inspection.preservedTables,
        ).joinToString("\u001f").encodeToByteArray())
    }

    private suspend fun requireInstalledIdentityUnchanged(
        connection: SafeSQLiteConnection,
        installed: InstalledOpeningIdentity,
    ) {
        val current = readInstalledOpeningIdentity(connection)
        require(current == installed) { "installed identity changed before U0" }
    }

    private suspend fun recheckGenerationAdmissionAtU0(
        connection: SafeSQLiteConnection,
        installed: InstalledOpeningIdentity,
        target: SyncGeneration,
        expectedAttachment: OversqliteAttachmentState,
        expectedOperation: OversqliteOperationState,
    ) {
        requireInstalledIdentityUnchanged(connection, installed)
        inspectRetainedRuntime(connection)

        val contractStore = OversqliteContractStore(connection)
        val oldGeneration = contractStore.readGeneration(installed.generationId)
        require(oldGeneration.fingerprint == installed.fingerprint) {
            "installed generation admission changed before U0"
        }
        val oldConfig = config.copy(
            generationId = installed.generationId,
            syncTables = oldGeneration.tables.map { table ->
                SyncTable(tableName = table.table, syncKeyColumns = table.syncKeyColumns)
            },
        )
        val oldPhysical = SyncRuntimeInitializer(oldConfig, TableInfoCache()).validateConfig(connection)
        oldGeneration.requireMatchingLocalSchema(oldPhysical.tableInfoByName, oldPhysical.keyByTable)
        val oldIdentity = oldPhysical.contractIdentity(oldConfig, connection)
        require(
            oldIdentity.databaseId == installed.databaseId &&
                oldIdentity.serviceId == installed.serviceId &&
                oldIdentity.generationId == installed.generationId &&
                oldIdentity.localVersion == installed.localVersion.toLong() &&
                oldIdentity.physicalShape == installed.physicalShape
        ) { "installed physical contract changed before U0" }

        require(OversqliteAttachmentStateStore(connection).loadState() == expectedAttachment) {
            "attachment state changed before U0"
        }
        require(OversqliteOperationStateStore(connection).loadState() == expectedOperation) {
            "operation state changed before U0"
        }
        require(expectedOperation.kind == operationKindNone) {
            "generation change cannot start during another lifecycle operation"
        }

        requireSchemaEvolutionCompatibility(oldGeneration, target)
        requireExactTargetSelection(target)
        requireNoAddedSynchronizedTableNamespaceCollisions(connection, oldGeneration, target)
        requireExactPreservedTableDeclaration(
            connection,
            plan!!.preservedLocalTables,
            oldGeneration.tables.map { it.table }.toSet(),
            target.tables.map { it.table }.toSet(),
        )
    }

    private fun requireExactTargetSelection(target: SyncGeneration) {
        require(config.syncTables.size == target.tables.size) {
            "target sync table selection must match the complete discovered generation"
        }
        val configuredTarget = linkedMapOf<String, List<String>>()
        config.syncTables.forEach { table ->
            require(table.tableName.matches(Regex("[a-z0-9_]{1,63}"))) {
                "target sync table selection must use exact canonical table identifiers"
            }
            val keys = table.syncKeyColumns.ifEmpty { listOfNotNull(table.syncKeyColumnName) }
            require(keys.isNotEmpty() && keys.distinct().size == keys.size &&
                keys.all { it.matches(Regex("[a-z0-9_]{1,63}")) }) {
                "target sync table selection must use unique canonical key identifiers"
            }
            require(configuredTarget.put(table.tableName.lowercase(), keys) == null) {
                "target sync table selection must not contain duplicate tables"
            }
        }
        val discoveredTarget = target.tables.associate { it.table.lowercase() to it.syncKeyColumns }
        require(configuredTarget == discoveredTarget) {
            "target sync table selection must match the complete discovered generation"
        }
    }

    private fun generationRecord(
        phase: String,
        revision: Long,
        planDigest: String,
        target: SyncGeneration,
        attachment: OversqliteAttachmentState,
        preservationDigest: String,
    ): OversqliteOpeningRecord {
        val installed = checkNotNull(inspection.installed)
        return OversqliteOpeningRecord(
        mode = "generation_change", phase = phase, outcome = "", revision = revision,
        planDigest = planDigest, migrationId = plan!!.migrationId,
        databaseId = config.databaseId, serviceId = config.serviceId,
        attachedUserId = attachment.attachedUserId, oldSourceId = attachment.currentSourceId,
        newSourceId = "", replacementReason = "schema_generation_upgrade",
        fromGenerationId = installed.generationId, toGenerationId = config.generationId,
        fromLocalVersion = installed.localVersion.toLong(),
        toLocalVersion = session.migrationTargetVersion!!.toLong(),
        preservedTables = inspection.preservedTables,
        oldFingerprint = installed.fingerprint, targetFingerprint = target.fingerprint,
        preservationDigest = preservationDigest, migrationComplete = false, contractConfirmed = true,
        pendingDirtyCount = checkNotNull(inspection.status.pendingDirtyCount),
        pendingFrozenCount = checkNotNull(inspection.status.pendingFrozenCount),
        pendingConflictCount = checkNotNull(inspection.status.pendingConflictCount),
        drainComplete = false, replacementState = "none", snapshotId = "",
        snapshotBundleSeq = 0, snapshotRowCount = 0, snapshotByteCount = 0,
        stagedRowCount = null, stagedByteCount = null,
        targetReadsAvailable = false, writesAvailable = false,
        nextAction = "advance",
        )
    }

    private suspend fun requireGenerationRecordMatchesPlan(
        record: OversqliteOpeningRecord,
        installed: InstalledOpeningIdentity,
    ) {
        val selected = checkNotNull(plan)
        require(record.mode == "generation_change" && record.databaseId == config.databaseId &&
            record.serviceId == config.serviceId && record.migrationId == selected.migrationId &&
            record.fromGenerationId == installed.generationId && record.toGenerationId == config.generationId &&
            record.fromLocalVersion == installed.localVersion.toLong() &&
            record.toLocalVersion == session.migrationTargetVersion!!.toLong() &&
            record.oldFingerprint == installed.fingerprint && record.preservedTables == inspection.preservedTables
        ) { "durable generation opening journal differs from the requested edge" }
        session.withConnection { connection ->
            val target = OversqliteContractStore(connection).readGeneration(config.generationId)
            require(target.fingerprint == record.targetFingerprint) {
                "durable target admission differs from the coordinated opening journal"
            }
            val attachment = OversqliteAttachmentStateStore(connection).loadState()
            require(attachment.currentSourceId == record.oldSourceId &&
                attachment.attachedUserId == record.attachedUserId
            ) { "durable attachment differs from the coordinated opening journal" }
            require(generationPlanDigest(
                target = target,
                attachedUserId = record.attachedUserId,
                initialOldSourceId = record.initialOldSourceId,
            ) == record.planDigest) {
                "durable generation plan digest mismatch"
            }
            requirePreservation(connection, record)
        }
    }

    private suspend fun transitionGenerationRecord(
        expected: OversqliteOpeningRecord,
        phase: String,
    ): OversqliteOpeningRecord = session.withConnection { connection ->
        val committed = connection.transaction(TransactionMode.IMMEDIATE) {
            val current = OversqliteOpeningStore(connection).readOrNull()
            require(current == expected) { "coordinated opening journal changed during phase transition" }
            requirePreservation(connection, current)
            val next = current.copy(
                phase = phase, outcome = "", revision = current.revision + 1L,
                failureCode = "", failureMessage = "",
            )
            openingDurableCommitForTest?.invoke("before_u1", connection)
            OversqliteOpeningStore(connection).update(next, current)
            next
        }
        openingDurableCommitForTest?.invoke("after_u1", connection)
        committed
    }

    private suspend fun recordNeedsResolution(
        expected: OversqliteOpeningRecord,
        conflict: PendingConflict,
    ): OversqliteOpeningRecord = session.withConnection { connection ->
        connection.transaction(TransactionMode.IMMEDIATE) {
            val current = checkNotNull(OversqliteOpeningStore(connection).readOrNull())
            if (current.phase == "resolving_old_intent" &&
                current.outcome == "needs_resolution" &&
                current.nextAction == "resolve_conflict" &&
                current.pendingConflictCount == 1L &&
                current.pendingRequestHash == conflict.canonicalRequestHash
            ) {
                return@transaction current
            }
            require(current == expected) { "coordinated opening journal changed while recording conflict" }
            openingPreservationCheckForTest?.invoke("record_conflict", connection)
            requirePreservation(connection, current)
            val next = current.copy(
                outcome = "needs_resolution", revision = current.revision + 1L,
                pendingConflictCount = 1, pendingRequestHash = conflict.canonicalRequestHash,
                nextAction = "resolve_conflict",
            )
            OversqliteOpeningStore(connection).update(next, current)
            next
        }
    }

    private suspend fun recordNeedsRejection(
        expected: OversqliteOpeningRecord,
        failure: OversqliteRemoteContractException,
    ): OversqliteOpeningRecord = session.withConnection { connection ->
        connection.transaction(TransactionMode.IMMEDIATE) {
            val current = checkNotNull(OversqliteOpeningStore(connection).readOrNull())
            require(current == expected) { "coordinated opening journal changed while recording rejection" }
            openingPreservationCheckForTest?.invoke("record_rejection", connection)
            requirePreservation(connection, current)
            val outbox = OversqliteOutboxStateStore(connection).loadBundleState()
            require(outbox.state == outboxStatePrepared && outbox.sourceId == current.oldSourceId &&
                outbox.sourceBundleId > 0L && outbox.canonicalRequestHash.isNotBlank()
            ) { "definitive rejection does not identify a frozen old-generation request" }
            val intentRevision = connection.scalarLong(
                "SELECT dirty_revision FROM _sync_conflict_state WHERE singleton_key=1",
            )
            val next = current.copy(
                outcome = "needs_resolution", revision = current.revision + 1L,
                pendingFrozenCount = outbox.rowCount, pendingRequestHash = outbox.canonicalRequestHash,
                pendingIntentRevision = intentRevision, failureCode = failure.code,
                failureMessage = failure.message.orEmpty(), nextAction = "resolve_rejection",
            )
            OversqliteOpeningStore(connection).update(next, current)
            next
        }
    }

    private suspend fun commitDrainWitness(expected: OversqliteOpeningRecord): OversqliteOpeningRecord =
        session.withConnection { connection ->
            val committed = connection.transaction(TransactionMode.IMMEDIATE) {
                val current = checkNotNull(OversqliteOpeningStore(connection).readOrNull())
                require(current == expected) { "coordinated opening journal changed before U2" }
                requirePreservation(connection, current)
                requireOldGenerationDrained(connection, current)
                val next = current.copy(
                    outcome = "", revision = current.revision + 1L, drainComplete = true,
                    pendingDirtyCount = 0, pendingFrozenCount = 0, pendingConflictCount = 0,
                    pendingRequestHash = "", nextAction = "advance", failureCode = "", failureMessage = "",
                )
                openingDurableCommitForTest?.invoke("before_u2", connection)
                OversqliteOpeningStore(connection).update(next, current)
                next
            }
            openingDurableCommitForTest?.invoke("after_u2", connection)
            committed
        }

    private suspend fun requireOldGenerationDrained(
        connection: SafeSQLiteConnection,
        record: OversqliteOpeningRecord,
    ) {
        require(oldGenerationDrained(connection, record)) { "old-generation intent drain is incomplete" }
    }

    private suspend fun oldGenerationDrained(
        connection: SafeSQLiteConnection,
        record: OversqliteOpeningRecord,
    ): Boolean {
        if (connection.scalarLong("SELECT count(*) FROM _sync_dirty_rows") != 0L) return false
        if (connection.scalarLong("SELECT count(*) FROM _sync_outbox_rows") != 0L) return false
        if (connection.scalarLong("SELECT count(*) FROM _sync_outbox_bundle WHERE singleton_key=1 AND state='none'") != 1L) return false
        if (connection.scalarLong("SELECT count(*) FROM _sync_conflict_state WHERE singleton_key=1 AND decision_state='none'") != 1L) return false
        if (connection.scalarLong("SELECT count(*) FROM _sync_operation_state WHERE singleton_key=1 AND kind='none'") != 1L) return false
        val attachment = OversqliteAttachmentStateStore(connection).loadState()
        return attachment.currentSourceId == record.oldSourceId &&
            attachment.attachedUserId == record.attachedUserId && attachment.pendingInitializationId.isBlank()
    }

    private suspend fun enterLocalMigration(expected: OversqliteOpeningRecord): OversqliteOpeningRecord =
        session.withConnection { connection ->
            val committed = connection.transaction(TransactionMode.IMMEDIATE) {
                val current = checkNotNull(OversqliteOpeningStore(connection).readOrNull())
                require(current == expected && current.drainComplete) { "durable U2 witness is missing" }
                requirePreservation(connection, current)
                requireOldGenerationDrained(connection, current)
                val target = OversqliteContractStore(connection).readGeneration(config.generationId)
                require(target.fingerprint == current.targetFingerprint) { "target admission changed before U3" }
                val next = current.copy(
                    phase = "migrating_local", outcome = "", revision = current.revision + 1L,
                    nextAction = "advance", failureCode = "", failureMessage = "",
                )
                openingDurableCommitForTest?.invoke("before_u3", connection)
                OversqliteOpeningStore(connection).update(next, current)
                next
            }
            openingDurableCommitForTest?.invoke("after_u3", connection)
            committed
        }

    private suspend fun runGenerationMigration(
        expected: OversqliteOpeningRecord,
    ): OversqliteOpeningRecord {
        targetClient?.takeIf { it.cleanupPending }?.let {
            it.closeFromCoordinatedOwner()
            targetClient = null
        }
        var committed: OversqliteOpeningRecord? = null
        var retainedCapture = emptyList<Pair<String, String>>()
        try {
            failureAdmissionEdge = expected
            openingAdmissionReservationForTest?.invoke(
                "generation_u4",
                session.withConnection { it },
            )
            session.migrateAndValidate(
                migrationSqlPolicy = generationMigrationSqlPolicy(session.withConnection { it }),
                preInit = { connection ->
                    connection.execSQL("PRAGMA foreign_keys = ON")
                    require(connection.scalarLong("PRAGMA foreign_keys") == 1L) {
                        "foreign keys must be enabled before coordinated generation migration"
                    }
                },
                transactionSetup = { connection ->
                    evaluateOpeningAdmission(
                        connection,
                        config,
                        plan,
                        session.migrationTargetVersion!!,
                        expected,
                    )
                    val managedNames = connection.queryList("SELECT table_name FROM _sync_managed_tables") { it.getText(0) }
                    val captureNames = managedNames.flatMap { name ->
                        listOf("trg_${name}_ai", "trg_${name}_au", "trg_${name}_ad")
                    }.toSet()
                    retainedCapture = connection.queryList("SELECT name,sql FROM sqlite_schema WHERE type='trigger' ORDER BY name") {
                        it.getText(0) to it.getText(1)
                    }.filter { it.first in captureNames }
                    require(retainedCapture.size == captureNames.size) { "admitted generation capture is incomplete" }
                    connection.execSQL(
                        "UPDATE _sync_apply_state SET apply_mode=1 WHERE singleton_key=1 AND apply_mode=0",
                    )
                    require(connection.scalarLong("SELECT changes()") == 1L) {
                        "U4 capture bypass is unavailable or already active"
                    }
                },
                validateBeforeCommit = { connection ->
                    val openingStore = OversqliteOpeningStore(connection)
                    val current = checkNotNull(openingStore.readOrNull())
                    require(current == expected && current.phase == "migrating_local" && current.drainComplete) {
                        "coordinated opening journal changed before U4"
                    }
                    openingPreservationCheckForTest?.invoke("generation_u4", connection)
                    requirePreservation(connection, current)
                    requireOldGenerationDrained(connection, current)
                    val contractStore = OversqliteContractStore(connection)
                    val oldGeneration = contractStore.readGeneration(current.fromGenerationId)
                    require(oldGeneration.fingerprint == current.oldFingerprint) {
                        "old admission changed during U4"
                    }
                    val target = contractStore.readGeneration(config.generationId)
                    require(target.fingerprint == current.targetFingerprint) { "target admission changed during U4" }
                    val physical = SyncRuntimeInitializer(config, TableInfoCache()).validateConfig(connection)
                    target.requireMatchingLocalSchema(physical.tableInfoByName, physical.keyByTable)
                    val targetIdentity = physical.contractIdentity(config, connection)
                    require(targetIdentity.localVersion == current.toLocalVersion) {
                        "generation migration did not reach its target local version"
                    }
                    val retainedNames = retainedCapture.map { it.first }.toSet()
                    val currentCapture = connection.queryList("SELECT name,sql FROM sqlite_schema WHERE type='trigger' ORDER BY name") {
                        it.getText(0) to it.getText(1)
                    }.filter { it.first in retainedNames }
                    require(currentCapture == retainedCapture) { "admitted capture changed during U4 application migration" }
                    openingDurableCommitForTest?.invoke("before_u4_identity", connection)
                    connection.withPreparedStatement(
                        """
                        UPDATE _sync_contract_identity
                        SET generation_id=?,local_version=?,physical_shape=?,fingerprint=?
                        WHERE singleton_key=1 AND database_id=? AND service_id=? AND generation_id=?
                            AND local_version=? AND fingerprint=?
                        """.trimIndent(),
                    ) { statement ->
                        statement.bindLong(1, targetIdentity.generationId)
                        statement.bindLong(2, targetIdentity.localVersion)
                        statement.bindText(3, targetIdentity.physicalShape)
                        statement.bindText(4, target.fingerprint)
                        statement.bindText(5, current.databaseId)
                        statement.bindText(6, current.serviceId)
                        statement.bindLong(7, current.fromGenerationId)
                        statement.bindLong(8, current.fromLocalVersion)
                        statement.bindText(9, current.oldFingerprint)
                        statement.step()
                    }
                    require(connection.scalarLong("SELECT changes()") == 1L) {
                        "installed identity changed during U4"
                    }
                    openingDurableCommitForTest?.invoke("after_u4_identity", connection)
                    openingDurableCommitForTest?.invoke("before_u4_capture", connection)
                    // Only this admitted generation edge may replace retained capture. Ordinary open stays strict.
                    for ((name, _) in retainedCapture) connection.execSQL("DROP TRIGGER ${quoteIdent(name)}")
                    SyncRuntimeInitializer(config, TableInfoCache()).prepareLocalRuntime(
                        connection, OversqliteManagedTableStore(connection),
                    )
                    openingDurableCommitForTest?.invoke("after_u4_capture", connection)
                    inspectRetainedRuntime(connection)
                    val client = DefaultOversqliteClient.forOpening(
                        connection, config, httpClient, resolver, clientLeaseGroup,
                    )
                    targetClient = client
                    client.open().getOrThrow()
                    connection.execSQL("UPDATE _sync_apply_state SET apply_mode=0 WHERE singleton_key=1")
                    require(
                        connection.scalarLong(
                            "SELECT apply_mode FROM _sync_apply_state WHERE singleton_key=1",
                        ) == 0L,
                    ) { "U4 capture bypass remained active before commit" }
                    requirePreservation(connection, current)
                    val next = current.copy(
                        phase = "local_migrated", outcome = "", revision = current.revision + 1L,
                        migrationComplete = true, contractConfirmed = true,
                        pendingDirtyCount = 0, pendingFrozenCount = 0, pendingConflictCount = 0,
                        targetReadsAvailable = false, writesAvailable = false,
                        nextAction = "advance", failureCode = "", failureMessage = "",
                    )
                    openingDurableCommitForTest?.invoke("before_u4", connection)
                    openingStore.update(next, current)
                    inspectRetainedRuntime(connection)
                    committed = next
                },
            )
            openingDurableCommitForTest?.invoke("after_u4", session.withConnection { it })
        } catch (failure: Throwable) {
            try {
                targetClient?.closeFromCoordinatedOwner()
                targetClient = null
            } catch (cleanupFailure: Throwable) {
                failure.addSuppressed(cleanupFailure)
            }
            cachedStatus = failureStatusFor(failure)
            throw failure
        }
        oldClient?.closeFromCoordinatedOwner()
        oldClient = null
        targetClient?.closeFromCoordinatedOwner()
        targetClient = null
        failureAdmissionEdge = null
        return checkNotNull(committed) { "U4 completed without a durable journal record" }
    }

    private suspend fun prepareTargetGenerationClient(
        connection: SafeSQLiteConnection,
        attachedUserId: String,
    ): DefaultOversqliteClient {
        targetClient?.takeIf { it.cleanupPending }?.let {
            it.closeFromCoordinatedOwner()
            targetClient = null
        }
        targetClient?.let { return it }
        return DefaultOversqliteClient.forOpening(
            connection, config, httpClient, resolver, clientLeaseGroup,
        ).also {
            it.open().getOrThrow()
            it.admitAttachedGenerationOpening(attachedUserId)
            targetClient = it
        }
    }

    @OptIn(ExperimentalUuidApi::class)
    private suspend fun prepareGenerationReplacement(
        expected: OversqliteOpeningRecord,
    ): OversqliteOpeningRecord {
        val connection = session.withConnection { it }
        val client = prepareTargetGenerationClient(connection, expected.attachedUserId)
        var newSourceId = Uuid.random().toString()
        while (newSourceId == expected.oldSourceId) newSourceId = Uuid.random().toString()
        var committed: OversqliteOpeningRecord? = null
        client.prepareGenerationReplacement(
            oldSourceId = expected.oldSourceId,
            newSourceId = newSourceId,
            userId = expected.attachedUserId,
            precondition = {
                val current = checkNotNull(OversqliteOpeningStore(connection).readOrNull())
                require(current == expected) { "coordinated opening journal changed before U5" }
                requirePreservation(connection, current)
            },
        ) {
            val openingStore = OversqliteOpeningStore(connection)
            val current = checkNotNull(openingStore.readOrNull())
            require(current == expected && current.phase == "local_migrated" && current.migrationComplete) {
                "coordinated opening journal changed before U5"
            }
            requirePreservation(connection, current)
            val next = current.copy(
                phase = "reattach_pending", outcome = "", revision = current.revision + 1L,
                newSourceId = newSourceId, replacementState = "reserved",
                targetReadsAvailable = false, writesAvailable = false, nextAction = "advance",
                failureCode = "", failureMessage = "",
            )
            openingDurableCommitForTest?.invoke("before_u5", connection)
            openingStore.update(next, current)
            committed = next
        }
        val durable = checkNotNull(committed) { "U5 completed without a durable journal record" }
        openingDurableCommitForTest?.invoke("after_u5", connection)
        return durable
    }

    private suspend fun confirmGenerationReplacement(
        expected: OversqliteOpeningRecord,
    ): OversqliteOpeningRecord {
        require(expected.newSourceId.isNotBlank() && expected.replacementState == "reserved") {
            "U6 requires one durable replacement tuple"
        }
        val connection = session.withConnection { it }
        val client = prepareTargetGenerationClient(connection, expected.attachedUserId)
        val snapshot = client.createGenerationReplacementSnapshot(
            oldSourceId = expected.oldSourceId,
            newSourceId = expected.newSourceId,
            reason = expected.replacementReason,
        )
        val committed = connection.transaction(TransactionMode.IMMEDIATE) {
            val openingStore = OversqliteOpeningStore(connection)
            val current = checkNotNull(openingStore.readOrNull())
            require(current == expected) { "replacement tuple changed before U6 receipt" }
            connection.execSQL("DELETE FROM _sync_snapshot_stage")
            val next = current.copy(
                phase = "rebuild_running", outcome = "", revision = current.revision + 1L,
                replacementState = "confirmed", snapshotId = snapshot.snapshotId,
                snapshotBundleSeq = snapshot.snapshotBundleSeq, snapshotRowCount = snapshot.rowCount,
                snapshotByteCount = snapshot.logicalByteCount, stagedRowCount = 0, stagedByteCount = 0,
                nextAction = "advance", failureCode = "", failureMessage = "",
            )
            openingDurableCommitForTest?.invoke("before_u6", connection)
            openingStore.update(next, current)
            next
        }
        openingDurableCommitForTest?.invoke("after_u6", connection)
        return committed
    }

    private suspend fun rebuildAndPublishGeneration(
        initial: OversqliteOpeningRecord,
    ): OversqliteOpeningStatus {
        val connection = session.withConnection { it }
        val client = prepareTargetGenerationClient(connection, initial.attachedUserId)
        var record = initial
        val snapshot = SnapshotSession(
            snapshotId = record.snapshotId,
            snapshotBundleSeq = record.snapshotBundleSeq,
            rowCount = record.snapshotRowCount,
            logicalByteCount = record.snapshotByteCount,
            expiresAt = "",
        )
        val stagedRows = record.stagedRowCount ?: 0L
        val stagedBytes = record.stagedByteCount ?: 0L
        val stagedState = connection.withPreparedStatement(
            "SELECT count(*),COALESCE(max(row_ordinal),0) FROM _sync_snapshot_stage WHERE snapshot_id=?",
        ) { statement ->
            statement.bindText(1, record.snapshotId)
            require(statement.step()) { "snapshot stage census is missing" }
            statement.getLong(0) to statement.getLong(1)
        }
        require(stagedState.first == stagedRows && stagedState.second == stagedRows) {
            "staged snapshot differs from the durable U7 totals"
        }
        try {
            client.stageGenerationSnapshot(
                session = snapshot,
                newSourceId = record.newSourceId,
                userId = record.attachedUserId,
                initialTotals = SnapshotTransferTotals(stagedRows, stagedBytes),
            ) { totals ->
                val openingStore = OversqliteOpeningStore(connection)
                val current = checkNotNull(openingStore.readOrNull())
                require(current == record && current.phase == "rebuild_running" && current.replacementState == "confirmed") {
                    "coordinated opening journal changed during U7"
                }
                val next = current.copy(
                    revision = current.revision + 1L,
                    stagedRowCount = totals.rows,
                    stagedByteCount = totals.bytes,
                    outcome = "", failureCode = "", failureMessage = "",
                )
                openingDurableCommitForTest?.invoke("before_u7", connection)
                openingStore.update(next, current)
                record = next
            }
            openingDurableCommitForTest?.invoke("after_u7", connection)
        } catch (failure: SnapshotHttpException) {
            if (failure.errorCode != "snapshot_session_expired" && failure.errorCode != "snapshot_session_not_found") {
                throw failure
            }
            record = replaceExpiredTargetSnapshot(record, client)
            cachedStatus = statusFromRecord(record)
            return cachedStatus
        }
        require(record.stagedRowCount == record.snapshotRowCount && record.stagedByteCount == record.snapshotByteCount) {
            "staged snapshot totals differ from the U6 receipt"
        }
        var readyRecord: OversqliteOpeningRecord? = null
        client.applyGenerationSnapshot(
            session = snapshot,
            oldSourceId = record.oldSourceId,
            newSourceId = record.newSourceId,
            userId = record.attachedUserId,
        ) {
            val openingStore = OversqliteOpeningStore(connection)
            val current = checkNotNull(openingStore.readOrNull())
            require(current == record && current.phase == "rebuild_running") {
                "coordinated opening journal changed before U8"
            }
            requirePreservation(connection, current)
            require(connection.scalarLong("SELECT count(*) FROM pragma_foreign_key_check") == 0L) {
                "target snapshot violates foreign keys"
            }
            val retained = readInstalledOpeningIdentity(connection)
            require(retained.generationId == current.toGenerationId &&
                retained.localVersion.toLong() == current.toLocalVersion &&
                retained.fingerprint == current.targetFingerprint
            ) { "target identity changed before ready publication" }
            dropPreservedLocalTableGuards(connection, plan!!.preservedLocalTables)
            val next = current.copy(
                phase = "ready", outcome = "ready", revision = current.revision + 1L,
                stagedRowCount = current.snapshotRowCount, stagedByteCount = current.snapshotByteCount,
                targetReadsAvailable = true, writesAvailable = true, nextAction = "use_ready",
            )
            openingDurableCommitForTest?.invoke("before_u8", connection)
            openingStore.update(next, current)
            inspectRetainedRuntime(connection)
            readyRecord = next
        }
        openingDurableCommitForTest?.invoke("after_u8", connection)
        publishReadyClient(client)
        targetClient = null
        client.deleteGenerationSnapshotBestEffort(snapshot.snapshotId, record.newSourceId)
        cachedStatus = statusFromRecord(checkNotNull(readyRecord))
        return cachedStatus
    }

    private suspend fun replaceExpiredTargetSnapshot(
        expected: OversqliteOpeningRecord,
        client: DefaultOversqliteClient,
    ): OversqliteOpeningRecord {
        val session = client.createGenerationRecoverySnapshot(expected.newSourceId)
        val connection = this.session.withConnection { it }
        return connection.transaction(TransactionMode.IMMEDIATE) {
            val store = OversqliteOpeningStore(connection)
            val current = checkNotNull(store.readOrNull())
            require(current == expected && current.phase == "rebuild_running" &&
                current.replacementState == "confirmed" && current.newSourceId == expected.newSourceId
            ) { "coordinated opening journal changed while replacing an expired target snapshot" }
            connection.execSQL("DELETE FROM _sync_snapshot_stage")
            current.copy(
                revision = current.revision + 1L,
                snapshotId = session.snapshotId,
                snapshotBundleSeq = session.snapshotBundleSeq,
                snapshotRowCount = session.rowCount,
                snapshotByteCount = session.logicalByteCount,
                stagedRowCount = 0L,
                stagedByteCount = 0L,
            ).also { store.update(it, current) }
        }
    }

    private suspend fun publishCompletedGeneration(
        record: OversqliteOpeningRecord,
    ): OversqliteOpeningStatus {
        val connection = session.withConnection { it }
        openingAdmissionReservationForTest?.invoke("completed_generation_replay", connection)
        session.migrateAndValidate(
            migrationSqlPolicy = generationMigrationSqlPolicy(connection),
            transactionSetup = { current ->
                evaluateOpeningAdmission(
                    current,
                    config,
                    plan,
                    session.migrationTargetVersion!!,
                    record,
                )
            },
            validateBeforeCommit = { current ->
                require(current.scalarLong("PRAGMA user_version") == record.toLocalVersion) {
                    "completed generation edge has a different local version"
                }
                require(readInstalledOpeningIdentity(current).let {
                    it.generationId == record.toGenerationId && it.fingerprint == record.targetFingerprint
                }) { "completed generation edge has a different installed identity" }
            inspectRetainedRuntime(current)
            requirePreservation(current, record)
            },
        )
        val client = prepareTargetGenerationClient(connection, record.attachedUserId)
        publishReadyClient(client)
        targetClient = null
        cachedStatus = statusFromRecord(record)
        return cachedStatus
    }

    private suspend fun requirePreservation(
        connection: SafeSQLiteConnection,
        record: OversqliteOpeningRecord,
        includePreservedLocalTables: Boolean = false,
    ) = requireOpeningPreservation(connection, record, config, plan, includePreservedLocalTables)

    private suspend fun generationMigrationSqlPolicy(
        connection: SafeSQLiteConnection,
    ): SqliteNowCoordinatedMigrationSqlPolicy {
        val installed = checkNotNull(inspection.installed)
        val contractStore = OversqliteContractStore(connection)
        val managedTables = buildSet {
            addAll(contractStore.readGeneration(installed.generationId).tables.map { it.table })
            addAll(contractStore.readGeneration(config.generationId).tables.map { it.table })
        }
        return coordinatedMigrationSqlPolicy(managedTables, plan!!.preservedLocalTables)
    }


    private fun statusFromRecord(record: OversqliteOpeningRecord): OversqliteOpeningStatus =
        openingStatusFromRecord(inspection.status, record)
}
