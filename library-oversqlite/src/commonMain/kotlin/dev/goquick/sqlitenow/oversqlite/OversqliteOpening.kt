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
import dev.goquick.sqlitenow.core.SqliteNowDatabase
import dev.goquick.sqlitenow.core.SqliteNowCoordinatedMigrationCloseContext
import dev.goquick.sqlitenow.core.requireSqliteNowCoordinatedCloseAllowed
import dev.goquick.sqlitenow.core.SqliteNowInternalApi
import dev.goquick.sqlitenow.core.SqliteNowUnpublishedSession
import dev.goquick.sqlitenow.core.TransactionMode
import io.ktor.client.HttpClient
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.concurrent.Volatile
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi
data class OversqliteUpgradePlan(
    val migrationId: String,
    val fromGenerationId: Long,
    val toGenerationId: Long,
    val fromLocalVersion: Int,
    val toLocalVersion: Int,
    val preservedLocalTables: Set<String>,
) {
    init {
        require(migrationId.isNotBlank()) { "migrationId must be provided" }
        require(fromGenerationId in 1L..Int.MAX_VALUE.toLong()) { "fromGenerationId is out of range" }
        require(toGenerationId in 1L..Int.MAX_VALUE.toLong()) { "toGenerationId is out of range" }
        require(fromLocalVersion >= 0 && toLocalVersion >= 0) { "local versions must be nonnegative" }
        require(preservedLocalTables.all { it.matches(Regex("[a-z0-9_]{1,63}")) }) {
            "preserved local tables must use exact canonical identifiers"
        }
    }
}

enum class OversqliteOpeningMode { FRESH, UNCHANGED, LOCAL_ONLY, GENERATION_CHANGE }

enum class OversqliteOpeningOutcome {
    READY,
    WAITING_FOR_NETWORK,
    NEEDS_RESOLUTION,
    CANCELLED,
    UPGRADE_BUSY,
    UPGRADE_PRECONDITION_FAILED,
    UPGRADE_PLAN_MISMATCH,
    OLD_GENERATION_UNAVAILABLE,
    TARGET_GENERATION_UNAVAILABLE,
    LOCAL_MIGRATION_FAILED,
    LOCAL_CONTRACT_MISMATCH,
    LOCAL_PRESERVATION_FAILED,
    UPGRADE_INTEGRITY_FAILED,
    REPLACEMENT_DIVERGED,
}

enum class OversqliteOpeningReplacementState { NONE, RESERVED, CONFIRMED }

enum class OversqliteOpeningNextAction {
    ADVANCE,
    RESOLVE_CONFLICT,
    RESOLVE_REJECTION,
    FINISH_OLD_ATTACHMENT,
    REPAIR_CONFIGURATION,
    RESTORE_SUPPORT,
    INSPECT_INTEGRITY,
    USE_READY,
    CLOSE,
}

data class OversqliteOpeningStatus(
    val mode: OversqliteOpeningMode?,
    val outcome: OversqliteOpeningOutcome?,
    val phase: String?,
    val revision: Long,
    val installedGenerationId: Long?,
    val targetGenerationId: Long,
    val installedLocalVersion: Int?,
    val targetLocalVersion: Int,
    val contractConfirmed: Boolean?,
    val pendingDirtyCount: Long?,
    val pendingFrozenCount: Long?,
    val pendingConflictCount: Long?,
    val drainComplete: Boolean?,
    val replacementState: OversqliteOpeningReplacementState?,
    val stagedRowCount: Long?,
    val stagedByteCount: Long?,
    val targetReadsAvailable: Boolean?,
    val writesAvailable: Boolean?,
    val nextAction: OversqliteOpeningNextAction,
    val diagnosticCode: String? = null,
    val diagnosticMessage: String? = null,
)

data class OversqliteReady<D : SqliteNowDatabase>(
    val database: D,
    val client: OversqliteClient,
)

data class OversqlitePendingRejection(
    val revision: Long,
    val sourceId: String,
    val sourceBundleId: Long,
    val canonicalRequestHash: String,
    val generationId: Long,
    val rejectionCode: String,
)

interface OversqliteOpening<D : SqliteNowDatabase> {
    suspend fun status(): OversqliteOpeningStatus
    suspend fun advance(): OversqliteOpeningStatus
    suspend fun pendingConflict(): Result<PendingConflict?>
    suspend fun resolveConflict(selected: PendingConflict, result: MergeResult): Result<Unit>
    suspend fun retryConflict(selected: PendingConflict): Result<Unit>
    suspend fun pendingRejection(): OversqlitePendingRejection?
    suspend fun discardRejected(selected: OversqlitePendingRejection): OversqliteOpeningStatus
    suspend fun retryRejected(selected: OversqlitePendingRejection): OversqliteOpeningStatus
    suspend fun cancel()
    suspend fun close()
    fun readyOrNull(): OversqliteReady<D>?
}

class OversqliteOpeningBusyException : IllegalStateException("upgrade_busy")
class OversqliteOpeningClosedException : IllegalStateException("coordinated opening is closed")
class OversqliteProviderPreparationRequiredException : IllegalStateException("provider_preparation_required")
class OversqliteUpgradePlanMismatchException(message: String) : IllegalArgumentException(message)
class OversqliteUpgradePreconditionException(message: String) : IllegalStateException(message)

internal class OversqlitePreservationFailureException(cause: Throwable) : IllegalStateException(
    "coordinated opening preservation validation failed",
    cause,
)


@OptIn(SqliteNowInternalApi::class)
suspend fun <D : SqliteNowDatabase> D.openForSync(
    config: OversqliteConfig,
    httpClient: HttpClient,
    resolver: Resolver = DeferConflictResolver,
    upgradePlan: OversqliteUpgradePlan? = null,
): OversqliteOpening<D> = openSqliteNowDatabaseForSync(
    database = this,
    config = config,
    httpClient = httpClient,
    resolver = resolver,
    upgradePlan = upgradePlan,
)

/**
 * Non-extension entry point used by generated database members named [openForSync].
 *
 * Keeping the coordinator here ensures generated and handwritten consumers traverse the same
 * unpublished Core session, admission, journal, and publication implementation.
 */
@OptIn(SqliteNowInternalApi::class)
suspend fun <D : SqliteNowDatabase> openSqliteNowDatabaseForSync(
    database: D,
    config: OversqliteConfig,
    httpClient: HttpClient,
    resolver: Resolver = DeferConflictResolver,
    upgradePlan: OversqliteUpgradePlan? = null,
): OversqliteOpening<D> {
    val frozenConfig = config.copy(
        syncTables = config.syncTables.map { table ->
            table.copy(syncKeyColumns = table.syncKeyColumns.toList())
        },
        transientRetryPolicy = config.transientRetryPolicy.copy(),
        snapshotCapacityRetryPolicy = config.snapshotCapacityRetryPolicy.copy(),
    )
    val plan = upgradePlan?.copy(preservedLocalTables = upgradePlan.preservedLocalTables.toSet())
    val session = database.reserveUnpublishedSession()
    try {
        val targetVersion = session.migrationTargetVersion
            ?: throw OversqliteUpgradePreconditionException(
                "coordinated opening requires a migration runner with an immutable targetVersion",
            )
        session.acquire()
        val inspection = if (session.inspectionAvailable) {
            inspectOpening(session, frozenConfig, plan, targetVersion, freshLifetimeAcquisition = true)
        } else {
            preparationRequiredInspection(frozenConfig, targetVersion)
        }
        return DefaultOversqliteOpening(
            database, session, frozenConfig, httpClient, resolver, plan, inspection,
        ).also { it.bindShutdownOwner() }
    } catch (failure: Throwable) {
        try {
            session.close()
        } catch (closeFailure: Throwable) {
            failure.addSuppressed(closeFailure)
        }
        throw failure
    }
}


// Deterministic fault cut immediately before the transaction which must re-read admission.
internal var openingAdmissionReservationForTest:
    (suspend (String, SafeSQLiteConnection) -> Unit)? = null
internal var openingPreservationCheckForTest:
    (suspend (String, SafeSQLiteConnection) -> Unit)? = null
// Test-only process-death seam. "before_uN" runs inside the writer transaction immediately
// before its durable update; "after_uN" runs only after that transaction has committed.
internal var openingDurableCommitForTest:
    (suspend (String, SafeSQLiteConnection) -> Unit)? = null
internal var openingOperationRegisteredForTest: (suspend (String) -> Unit)? = null
internal var openingAfterCorePublicationForTest: (suspend () -> Unit)? = null
internal var openingBeforeSessionCloseForTest: (suspend () -> Unit)? = null

@OptIn(SqliteNowInternalApi::class, ExperimentalAtomicApi::class)
private class DefaultOversqliteOpening<D : SqliteNowDatabase>(
    private val database: D,
    private val session: SqliteNowUnpublishedSession,
    private val config: OversqliteConfig,
    private val httpClient: HttpClient,
    private val resolver: Resolver,
    private val plan: OversqliteUpgradePlan?,
    initialInspection: OpeningInspection,
) : OversqliteOpening<D> {
    private enum class PublicationState { OPEN, PUBLISHING, READY, CLOSING, CLOSED }
    private data class ActiveOperation(
        val token: Any,
        val job: Job?,
        val completion: CompletableDeferred<Unit>,
    )
    private data class OpeningAuthority<D : SqliteNowDatabase>(
        val state: PublicationState,
        val epoch: Long,
        val operation: ActiveOperation?,
        val ready: OversqliteReady<D>?,
    )
    private val authority = AtomicReference(
        OpeningAuthority<D>(PublicationState.OPEN, 0L, null, null),
    )
    private val shutdownOwnerMutex = Mutex()
    private var shutdownAttempt: CompletableDeferred<Throwable?>? = null
    private var sessionCloseComplete = false
    private val shutdownOwner = SupervisorJob()
    private val shutdownScope = CoroutineScope(shutdownOwner + Dispatchers.Default)
    private val clientLeaseGroup = Any()

    @Volatile
    private var inspection = initialInspection

    @Volatile
    private var providerPrepared = false

    @Volatile
    private var cachedStatus = initialInspection.status

    @Volatile
    private var publishedClient: DefaultOversqliteClient? = null

    private val generationWorkflow = OversqliteGenerationOpeningWorkflow(
        session = session,
        config = config,
        httpClient = httpClient,
        resolver = resolver,
        plan = plan,
        clientLeaseGroup = clientLeaseGroup,
        currentInspection = { inspection },
        updateInspection = { inspection = it },
        currentStatus = { cachedStatus },
        updateStatus = { cachedStatus = it },
        publishReadyClient = { publishReady(it) },
        mutationAuthorityFactory = { oldGenerationMutationAuthority(it) },
        failureStatusFor = { failureStatus(it) },
    )

    private var failureAdmissionEdge: OversqliteOpeningRecord?
        get() = generationWorkflow.failureAdmissionEdge
        set(value) { generationWorkflow.failureAdmissionEdge = value }

    private var oldClient: DefaultOversqliteClient?
        get() = generationWorkflow.oldClient
        set(value) { generationWorkflow.oldClient = value }

    private var targetClient: DefaultOversqliteClient?
        get() = generationWorkflow.targetClient
        set(value) { generationWorkflow.targetClient = value }

    private val localWorkflow = OversqliteLocalOpeningWorkflow(
        session = session,
        config = config,
        httpClient = httpClient,
        resolver = resolver,
        plan = plan,
        clientLeaseGroup = clientLeaseGroup,
        currentInspection = { inspection },
        updateInspection = { inspection = it },
        currentStatus = { cachedStatus },
        updateStatus = { cachedStatus = it },
    )

    private val closed: Boolean
        get() = authority.load().state in setOf(PublicationState.CLOSING, PublicationState.CLOSED)

    suspend fun bindShutdownOwner() {
        session.bindPublishedCloseHandler { joinShutdown() }
    }

    override suspend fun status(): OversqliteOpeningStatus {
        if (closed || authority.load().ready != null) return cachedStatus
        if (!session.inspectionAvailable) return cachedStatus
        val durable = session.withConnection { OversqliteOpeningStore(it).readOrNull() }
        if (durable != null && !(
                durable.outcome == "ready" && cachedStatus.outcome == OversqliteOpeningOutcome.UPGRADE_PLAN_MISMATCH
            )
        ) {
            cachedStatus = statusForCurrentFacade(durable)
        }
        return cachedStatus
    }

    private fun statusForCurrentFacade(record: OversqliteOpeningRecord): OversqliteOpeningStatus {
        val durable = openingStatusFromRecord(inspection.status, record)
        return if (record.outcome == "ready" && authority.load().ready == null) {
            durable.copy(
                targetReadsAvailable = false,
                writesAvailable = false,
                nextAction = OversqliteOpeningNextAction.ADVANCE,
            )
        } else {
            durable
        }
    }

    override suspend fun advance(): OversqliteOpeningStatus {
        if (currentCoroutineContext()[SqliteNowCoordinatedMigrationCloseContext] == null) {
            return withContext(SqliteNowCoordinatedMigrationCloseContext()) { advance() }
        }
        ensureOpen()
        readyOrNull()?.let { return cachedStatus }
        val operation = beginOperation("advance")
        try {
            try {
                if (!providerPrepared) {
                    try {
                        session.prepare()
                        val targetVersion = checkNotNull(session.migrationTargetVersion)
                        inspection = inspectOpening(session, config, plan, targetVersion, freshLifetimeAcquisition = true, reclaimReservations = true)
                        cachedStatus = inspection.status
                        providerPrepared = true
                    } catch (failure: Throwable) {
                        val classified = classifyOpeningFailure(null, failure)
                        cachedStatus = cachedStatus.copy(
                            outcome = openingOutcome(classified.outcome),
                            diagnosticCode = classified.diagnosticCode,
                            diagnosticMessage = classified.safeCause,
                            nextAction = openingNextAction(classified.nextAction),
                        )
                        throw failure
                    }
                }
                if (inspection.status.mode == OversqliteOpeningMode.GENERATION_CHANGE) {
                    cachedStatus = generationWorkflow.advance()
                    return cachedStatus
                }
                val localReady = localWorkflow.advance()
                publishReady(localReady.client)
                cachedStatus = localReady.status
                return cachedStatus
            } catch (failure: Throwable) {
                if (providerPrepared) {
                    withContext(NonCancellable) {
                        openingAdmissionReservationForTest?.invoke(
                            "failure_status",
                            session.withConnection { it },
                        )
                        persistFailureStatus(failure)?.let { cachedStatus = it }
                    }
                }
                throw failure
            }
        } finally {
            endOperation(operation)
        }
    }

    private suspend fun persistFailureStatus(failure: Throwable): OversqliteOpeningStatus? {
        if (failureAdmissionEdge != null &&
            classifyOpeningFailure(null, failure).diagnosticCode == "remote_contract_invalid"
        ) return null

        val connection = session.withConnection { it }
        return connection.transaction(TransactionMode.IMMEDIATE) {
            val store = OversqliteOpeningStore(connection)
            val current = store.readOrNull()
            if (current == null) {
                return@transaction failureStatus(failure)
            }
            if (current.outcome == "ready") return@transaction failureStatus(failure)

            evaluateOpeningAdmission(
                connection,
                config,
                plan,
                checkNotNull(session.migrationTargetVersion),
                failureAdmissionEdge,
            )
            if (current.mode in setOf("local_only", "generation_change")) {
                requirePreservation(connection, current, includePreservedLocalTables = true)
            }
            val classified = classifyOpeningFailure(current, failure)
            val next = current.copy(
                outcome = classified.outcome,
                revision = current.revision + 1L,
                nextAction = classified.nextAction,
                failureCode = classified.diagnosticCode,
                failureMessage = classified.safeCause,
            )
            store.update(next, current)
            openingStatusFromRecord(inspection.status, next)
        }
    }


    private fun statusFromRecord(record: OversqliteOpeningRecord): OversqliteOpeningStatus =
        openingStatusFromRecord(inspection.status, record)

    private suspend fun requirePreservation(
        connection: SafeSQLiteConnection,
        record: OversqliteOpeningRecord,
        includePreservedLocalTables: Boolean = false,
    ) = requireOpeningPreservation(connection, record, config, plan, includePreservedLocalTables)

    private fun failureStatus(failure: Throwable): OversqliteOpeningStatus {
        val classified = classifyOpeningFailure(null, failure)
        return cachedStatus.copy(
            outcome = openingOutcome(classified.outcome),
            diagnosticCode = classified.diagnosticCode,
            diagnosticMessage = classified.safeCause,
            nextAction = openingNextAction(classified.nextAction),
        )
    }

    private fun classifyOpeningFailure(
        record: OversqliteOpeningRecord?,
        failure: Throwable,
    ): OpeningFailureClassification {
        fun classified(outcome: String, code: String, action: String, safeCause: String = code) =
            OpeningFailureClassification(outcome, code, action, safeCause)

        if (failure is CancellationException) return classified("cancelled", "cancelled", "advance")
        if (failure is OversqliteOpeningBusyException) return classified("upgrade_busy", "upgrade_busy", "advance")
        if (failure is OversqliteUpgradePlanMismatchException) {
            return classified(
                "upgrade_plan_mismatch",
                "upgrade_plan_mismatch",
                "repair_configuration",
                failure.message ?: "upgrade_plan_mismatch",
            )
        }
        if (failure is OversqliteUpgradePreconditionException) {
            return classified("upgrade_precondition_failed", "upgrade_precondition_failed", "restore_support")
        }
        if (failure is OversqlitePreservationFailureException) {
            return classified("local_preservation_failed", "local_preservation_failed", "inspect_integrity")
        }
        if (failure is SourceReplacementDivergedException) {
            return classified("replacement_diverged", "replacement_diverged", "inspect_integrity")
        }
        if (failure is OversqliteRemoteContractException) {
            val outcome = if (failure.retryable) "waiting_for_network" else unavailableOutcomeFor(record)
            return classified(outcome, failure.code, "advance", "${failure.operation}:${failure.code}")
        }
        if (failure is OversqliteCategorizedException) {
            return when (failure.category) {
                OversqliteErrorCategory.AUTH -> classified(
                    "waiting_for_network", "authentication_required", "advance", "authentication_required",
                )
                OversqliteErrorCategory.NETWORK -> classified(
                    "waiting_for_network", "network_unavailable", "advance", "network_unavailable",
                )
                OversqliteErrorCategory.CONFLICT -> classified(
                    "needs_resolution", "conflict_requires_resolution", "resolve_conflict",
                )
                OversqliteErrorCategory.PROTOCOL -> classified(
                    unavailableOutcomeFor(record), "remote_contract_invalid", "repair_configuration",
                )
                OversqliteErrorCategory.STATE -> classified(
                    unavailableOutcomeFor(record), "opening_state_precondition_failed", "advance",
                )
            }
        }
        if (record?.phase == "migrating_local") {
            return classified("local_migration_failed", "local_migration_failed", "advance")
        }
        return when (record?.phase) {
            "reattach_pending", "rebuild_running" ->
                classified("target_generation_unavailable", "target_generation_unavailable", "advance")
            "upgrade_started", "resolving_old_intent" ->
                classified("old_generation_unavailable", "old_generation_unavailable", "advance")
            null -> classified("upgrade_precondition_failed", "provider_preparation_failed", "close")
            else -> classified("upgrade_integrity_failed", "upgrade_integrity_failed", "inspect_integrity")
        }
    }

    private fun unavailableOutcomeFor(record: OversqliteOpeningRecord?): String = when (record?.phase) {
        "reattach_pending", "rebuild_running", "migrating_local" -> "target_generation_unavailable"
        "upgrade_started", "resolving_old_intent" -> "old_generation_unavailable"
        else -> "local_contract_mismatch"
    }

    override suspend fun pendingConflict(): Result<PendingConflict?> = runCatching {
        readyOrNull()?.let { return@runCatching it.client.pendingConflict().getOrThrow() }
        if (inspection.status.mode != OversqliteOpeningMode.GENERATION_CHANGE) return@runCatching null
        generationWorkflow.prepareOldGenerationClient(
            session.withConnection { it },
            checkNotNull(inspection.installed),
        )
            .pendingConflict().getOrThrow()
    }

    override suspend fun resolveConflict(selected: PendingConflict, result: MergeResult): Result<Unit> = runCatching {
        readyOrNull()?.let { return@runCatching it.client.resolveConflict(selected, result).getOrThrow() }
        if (inspection.status.mode == OversqliteOpeningMode.GENERATION_CHANGE) {
            withOldGenerationConflictClient { client ->
                client.resolveConflict(selected, result).getOrThrow()
            }
        } else {
            throw OversqliteUpgradePreconditionException("no ready client can resolve this conflict")
        }
    }

    override suspend fun retryConflict(selected: PendingConflict): Result<Unit> = runCatching {
        readyOrNull()?.let { return@runCatching it.client.retryConflict(selected).getOrThrow() }
        if (inspection.status.mode == OversqliteOpeningMode.GENERATION_CHANGE) {
            withOldGenerationConflictClient { client ->
                client.retryConflict(selected).getOrThrow()
            }
        } else {
            throw OversqliteUpgradePreconditionException("no ready client can retry this conflict")
        }
    }

    private suspend fun withOldGenerationConflictClient(
        action: suspend (DefaultOversqliteClient) -> Unit,
    ) {
        withExclusiveOpeningOperation {
            val client = generationWorkflow.prepareOldGenerationClient(
                session.withConnection { it },
                checkNotNull(inspection.installed),
            )
            val record = cachedGenerationRecord()
            client.attach(record.attachedUserId).getOrThrow()
            action(client)
            clearResolutionOutcome()
        }
    }

    private suspend fun cachedGenerationRecord(): OversqliteOpeningRecord = session.withConnection { connection ->
        checkNotNull(OversqliteOpeningStore(connection).readOrNull()) { "coordinated opening journal is missing" }
    }

    private suspend fun <T> withExclusiveOpeningOperation(block: suspend () -> T): T {
        val operation = beginOperation("conflict_decision")
        return try {
            block()
        } finally {
            endOperation(operation)
        }
    }

    private suspend fun clearResolutionOutcome() {
        session.withConnection { connection ->
            connection.transaction(TransactionMode.IMMEDIATE) {
                val store = OversqliteOpeningStore(connection)
                val current = checkNotNull(store.readOrNull())
                if (current.phase == "resolving_old_intent" && current.nextAction == "advance" &&
                    current.outcome.isEmpty()
                ) {
                    cachedStatus = statusFromRecord(current)
                    return@transaction
                }
                require(current.phase == "resolving_old_intent") { "conflict decision is outside the old drain phase" }
                requirePreservation(connection, current)
                val next = current.copy(
                    outcome = "", revision = current.revision + 1L,
                    pendingConflictCount = 0, pendingRequestHash = "", nextAction = "advance",
                )
                store.update(next, current)
                cachedStatus = statusFromRecord(next)
            }
        }
    }

    private fun oldGenerationMutationAuthority(
        connection: SafeSQLiteConnection,
    ): OversqliteOpeningMutationAuthority = object : OversqliteOpeningMutationAuthority {
        override suspend fun beforeMutation(
            kind: OversqliteOpeningMutationKind,
        ): OversqliteOpeningRecord {
            val current = checkNotNull(OversqliteOpeningStore(connection).readOrNull()) {
                "coordinated opening journal is missing"
            }
            require(
                current.phase == "resolving_old_intent" &&
                    generationWorkflow.generationRecordIdentifiesPlan(current),
            ) {
                "${kind.diagnosticName} is outside the selected old drain state"
            }
            openingPreservationCheckForTest?.invoke(kind.diagnosticName, connection)
            requirePreservation(connection, current)
            return current
        }

        override suspend fun recordConflict(
            expected: OversqliteOpeningRecord,
            pending: PendingConflict,
        ) {
            val store = OversqliteOpeningStore(connection)
            val current = checkNotNull(store.readOrNull())
            require(current == expected) {
                "coordinated opening journal changed while recording conflict"
            }
            val next = current.copy(
                outcome = "needs_resolution",
                revision = current.revision + 1L,
                pendingConflictCount = 1,
                pendingRequestHash = pending.canonicalRequestHash,
                nextAction = "resolve_conflict",
            )
            store.update(next, current)
            cachedStatus = statusFromRecord(next)
        }

        override suspend fun completeConflictDecision(expected: OversqliteOpeningRecord) {
            val store = OversqliteOpeningStore(connection)
            val current = checkNotNull(store.readOrNull())
            require(current == expected) {
                "coordinated opening journal changed during conflict decision"
            }
            val next = current.copy(
                outcome = "",
                revision = current.revision + 1L,
                pendingConflictCount = 0,
                pendingRequestHash = "",
                nextAction = "advance",
            )
            store.update(next, current)
            cachedStatus = statusFromRecord(next)
        }
    }

    override suspend fun pendingRejection(): OversqlitePendingRejection? {
        requireProviderPreparedForConnectionOperation()
        return session.withConnection { connection ->
            val record = OversqliteOpeningStore(connection).readOrNull() ?: return@withConnection null
            if (record.nextAction != "resolve_rejection") return@withConnection null
            pendingRejection(connection, record)
        }
    }

    override suspend fun discardRejected(selected: OversqlitePendingRejection): OversqliteOpeningStatus =
        decideRejected(selected, discard = true)

    override suspend fun retryRejected(selected: OversqlitePendingRejection): OversqliteOpeningStatus =
        decideRejected(selected, discard = false)

    private suspend fun decideRejected(
        selected: OversqlitePendingRejection,
        discard: Boolean,
    ): OversqliteOpeningStatus {
        requireProviderPreparedForConnectionOperation()
        val operation = beginOperation(if (discard) "discard_rejection" else "retry_rejection")
        try {
            return session.withConnection { connection ->
                connection.transaction(TransactionMode.IMMEDIATE) {
                    val store = OversqliteOpeningStore(connection)
                    val current = checkNotNull(store.readOrNull())
                    openingPreservationCheckForTest?.invoke(
                        if (discard) "discard_rejection" else "retry_rejection",
                        connection,
                    )
                    requirePreservation(connection, current)
                    require(pendingRejection(connection, current) == selected) {
                        "selected definitive rejection is stale; inspect pendingRejection() again"
                    }
                    if (discard) {
                        connection.execSQL(
                            """
                            DELETE FROM _sync_dirty_rows
                            WHERE EXISTS (
                                SELECT 1 FROM _sync_outbox_rows o
                                WHERE o.source_bundle_id = ${selected.sourceBundleId}
                                  AND o.schema_name = _sync_dirty_rows.schema_name
                                  AND o.table_name = _sync_dirty_rows.table_name
                                  AND o.key_json = _sync_dirty_rows.key_json
                            )
                            """.trimIndent(),
                        )
                        connection.execSQL("DELETE FROM _sync_outbox_rows WHERE source_bundle_id=${selected.sourceBundleId}")
                        connection.execSQL(
                            """
                            UPDATE _sync_outbox_bundle SET state='none',source_id='',source_bundle_id=0,
                                initialization_id='',canonical_request_hash='',row_count=0
                            WHERE singleton_key=1
                            """.trimIndent(),
                        )
                    }
                    val next = current.copy(
                        outcome = "", revision = current.revision + 1L,
                        pendingDirtyCount = connection.scalarLong("SELECT count(*) FROM _sync_dirty_rows"),
                        pendingFrozenCount = if (discard) 0L else current.pendingFrozenCount,
                        pendingRequestHash = "", pendingIntentRevision = 0,
                        failureCode = "", failureMessage = "", nextAction = "advance",
                    )
                    store.update(next, current)
                    cachedStatus = statusFromRecord(next)
                    cachedStatus
                }
            }
        } finally {
            endOperation(operation)
        }
    }

    private fun requireProviderPreparedForConnectionOperation() {
        ensureOpen()
        if (!providerPrepared) throw OversqliteProviderPreparationRequiredException()
    }

    private suspend fun pendingRejection(
        connection: SafeSQLiteConnection,
        record: OversqliteOpeningRecord,
    ): OversqlitePendingRejection? {
        if (record.nextAction != "resolve_rejection" || record.failureCode.isBlank() ||
            record.pendingRequestHash.isBlank() || record.pendingIntentRevision < 0L
        ) return null
        val outbox = OversqliteOutboxStateStore(connection).loadBundleState()
        val intentRevision = connection.scalarLong(
            "SELECT dirty_revision FROM _sync_conflict_state WHERE singleton_key=1",
        )
        require(outbox.state == outboxStatePrepared && outbox.sourceId == record.oldSourceId &&
            outbox.sourceBundleId > 0L && outbox.canonicalRequestHash == record.pendingRequestHash &&
            intentRevision == record.pendingIntentRevision
        ) { "durable definitive rejection no longer matches the frozen request" }
        return OversqlitePendingRejection(
            revision = record.revision, sourceId = outbox.sourceId,
            sourceBundleId = outbox.sourceBundleId, canonicalRequestHash = outbox.canonicalRequestHash,
            generationId = record.fromGenerationId, rejectionCode = record.failureCode,
        )
    }

    override suspend fun cancel() {
        val operation = authority.load().operation ?: return
        if (operation.job !== currentCoroutineContext()[Job]) {
            operation.job?.cancel()
            operation.completion.await()
        }
    }

    override suspend fun close() {
        requireSqliteNowCoordinatedCloseAllowed()
        joinShutdown()
    }

    override fun readyOrNull(): OversqliteReady<D>? = authority.load().let { current ->
        current.ready?.takeIf { current.state == PublicationState.READY && database.isOpen() }
    }

    private fun ensureOpen() {
        if (authority.load().state in setOf(PublicationState.CLOSING, PublicationState.CLOSED)) {
            throw OversqliteOpeningClosedException()
        }
    }

    private suspend fun publishReady(client: DefaultOversqliteClient) {
        val operation = authority.load().operation ?: throw OversqliteOpeningBusyException()
        val publishing = transitionAuthority { current ->
            if (current.operation !== operation || current.state != PublicationState.OPEN) null else current.copy(
                state = PublicationState.PUBLISHING,
                ready = null,
            )
        } ?: throw OversqliteOpeningClosedException()
        publishedClient = client
        try {
            client.bindCoordinatedClose(::requestShutdown)
            session.publish()
            openingAfterCorePublicationForTest?.invoke()
            val readyValue = OversqliteReady(database, client)
            val published = authority.compareAndSet(
                publishing,
                publishing.copy(state = PublicationState.READY, ready = readyValue),
            )
            if (!published) throw OversqliteOpeningClosedException()
        } catch (failure: Throwable) {
            val restored = authority.compareAndSet(
                publishing,
                publishing.copy(state = PublicationState.OPEN, ready = null),
            )
            if (restored) {
                publishedClient = null
                client.closeFromCoordinatedOwner()
            }
            throw failure
        }
    }

    private fun revokeForShutdown() {
        session.revokePublicAccess()
        transitionAuthority { current ->
            when (current.state) {
                PublicationState.CLOSED, PublicationState.CLOSING -> null
                else -> current.copy(
                    state = PublicationState.CLOSING,
                    epoch = current.epoch + 1L,
                    ready = null,
                )
            }
        }
        cachedStatus = cachedStatus.copy(
            targetReadsAvailable = false,
            writesAvailable = false,
            nextAction = OversqliteOpeningNextAction.CLOSE,
        )
    }

    private fun requestShutdown() {
        revokeForShutdown()
        shutdownScope.launch { runCatching { joinShutdown() } }
    }

    private suspend fun joinShutdown() {
        revokeForShutdown()
        val (attempt, owner) = shutdownOwnerMutex.withLock {
            shutdownAttempt?.let { return@withLock it to false }
            CompletableDeferred<Throwable?>().also { shutdownAttempt = it } to true
        }
        if (owner) shutdownScope.launch(start = CoroutineStart.UNDISPATCHED) {
            val failure = try { closeOwnedResources(); null } catch (failure: Throwable) { failure }
            if (failure == null) {
                transitionAuthority { current -> current.copy(state = PublicationState.CLOSED, ready = null) }
                shutdownOwner.complete()
            }
            attempt.complete(failure)
            if (failure != null) shutdownOwnerMutex.withLock {
                if (shutdownAttempt === attempt) shutdownAttempt = null
            }
        }
        attempt.await()?.let { throw it }
    }

    private suspend fun closeOwnedResources() {
        authority.load().operation?.takeIf { it.job !== currentCoroutineContext()[Job] }?.let { operation ->
            operation.job?.cancel()
            operation.completion.await()
        }
        run {
            var primary: Throwable? = null
            suspend fun closeStage(block: suspend () -> Unit): Boolean {
                try {
                    block()
                    return true
                } catch (failure: Throwable) {
                    val existing = primary
                    if (existing == null) primary = failure else existing.addSuppressed(failure)
                    return false
                }
            }

            val clients = mutableListOf<DefaultOversqliteClient>()
            listOfNotNull(publishedClient, oldClient, targetClient).forEach { candidate ->
                if (clients.none { it === candidate }) clients += candidate
            }
            for (client in clients) {
                closeStage { client.closeFromCoordinatedOwner() }
                if (!client.cleanupPending) {
                    if (publishedClient === client) publishedClient = null
                    if (oldClient === client) oldClient = null
                    if (targetClient === client) targetClient = null
                }
            }
            val sessionClosed = clients.none { it.cleanupPending } && !sessionCloseComplete && closeStage {
                    openingBeforeSessionCloseForTest?.invoke()
                    session.close()
                }
            if (sessionClosed) sessionCloseComplete = true
            primary?.let { throw it }
        }
    }

    private suspend fun beginOperation(label: String): ActiveOperation {
        val operation = ActiveOperation(
            token = Any(),
            job = currentCoroutineContext()[Job],
            completion = CompletableDeferred(),
        )
        while (true) {
            val current = authority.load()
            if (current.state in setOf(PublicationState.CLOSING, PublicationState.CLOSED)) {
                throw OversqliteOpeningClosedException()
            }
            if (current.operation != null) throw OversqliteOpeningBusyException()
            if (authority.compareAndSet(current, current.copy(operation = operation))) {
                try {
                    openingOperationRegisteredForTest?.invoke(label)
                } catch (failure: Throwable) {
                    endOperation(operation)
                    throw failure
                }
                return operation
            }
        }
    }

    private fun endOperation(operation: ActiveOperation) {
        while (true) {
            val current = authority.load()
            if (current.operation !== operation) break
            if (authority.compareAndSet(current, current.copy(operation = null))) break
        }
        operation.completion.complete(Unit)
    }

    private inline fun transitionAuthority(
        update: (OpeningAuthority<D>) -> OpeningAuthority<D>?,
    ): OpeningAuthority<D>? {
        while (true) {
            val current = authority.load()
            val next = update(current) ?: return null
            if (authority.compareAndSet(current, next)) return next
        }
    }
}
