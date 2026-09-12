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
import io.ktor.client.HttpClient

internal data class LocalOpeningReady(
    val client: DefaultOversqliteClient,
    val status: OversqliteOpeningStatus,
)

@OptIn(SqliteNowInternalApi::class)
internal class OversqliteLocalOpeningWorkflow(
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
) {
    private var inspection: OpeningInspection
        get() = currentInspection()
        set(value) = updateInspection(value)

    private var cachedStatus: OversqliteOpeningStatus
        get() = currentStatus()
        set(value) = updateStatus(value)

    suspend fun advance(): LocalOpeningReady {
        ensureLocalMigrationJournal()
        val localCommitMode = inspection.status.mode
            .takeIf { it in setOf(OversqliteOpeningMode.FRESH, OversqliteOpeningMode.LOCAL_ONLY) }
            ?.name
            ?.lowercase()
        val client = DefaultOversqliteClient.forOpening(
            db = session.withConnection { it },
            config = config,
            http = httpClient,
            resolver = resolver,
            leaseGroup = clientLeaseGroup,
        )
        try {
            openingAdmissionReservationForTest?.invoke(
                "local_migration",
                session.withConnection { it },
            )
            session.migrateAndValidate(
                fresh = inspection.status.mode == OversqliteOpeningMode.FRESH,
                migrationSqlPolicy = coordinatedMigrationSqlPolicy(
                    managedTables = config.syncTables.mapTo(linkedSetOf()) { it.tableName },
                ),
                preInit = { connection ->
                    connection.execSQL("PRAGMA foreign_keys = ON")
                    require(connection.scalarLong("PRAGMA foreign_keys") == 1L) {
                        "foreign keys must be enabled before coordinated migration"
                    }
                },
                transactionSetup = { connection ->
                    evaluateOpeningAdmission(connection, config, plan, session.migrationTargetVersion!!)
                },
                validateBeforeCommit = { connection ->
                    validateAndBindLocalMode(connection, client)
                },
            )
            if (localCommitMode != null) {
                openingDurableCommitForTest?.invoke(
                    "after_${localCommitMode}_ready",
                    session.withConnection { it },
                )
            }
            val durableReady = session.withConnection { connection ->
                statusFromRecord(checkNotNull(OversqliteOpeningStore(connection).readOrNull()))
            }
            return LocalOpeningReady(client, durableReady)
        } catch (failure: Throwable) {
            client.closeFromCoordinatedOwner()
            throw failure
        }
    }

    private suspend fun validateAndBindLocalMode(
        connection: SafeSQLiteConnection,
        client: DefaultOversqliteClient,
    ) {
        val targetVersion = session.migrationTargetVersion!!
        if (inspection.status.mode == OversqliteOpeningMode.LOCAL_ONLY) {
            val installed = checkNotNull(inspection.installed)
            val physical = SyncRuntimeInitializer(config, TableInfoCache()).validateConfig(connection)
            val targetIdentity = physical.contractIdentity(config, connection)
            require(targetIdentity.localVersion == targetVersion.toLong()) { "migration did not reach its declared target" }
            connection.withPreparedStatement(
                """
                UPDATE _sync_contract_identity SET local_version=?,physical_shape=?
                WHERE singleton_key=1 AND database_id=? AND service_id=? AND generation_id=?
                    AND local_version=? AND physical_shape=? AND fingerprint=?
                """.trimIndent(),
            ) { statement ->
                statement.bindLong(1, targetIdentity.localVersion)
                statement.bindText(2, targetIdentity.physicalShape)
                statement.bindText(3, installed.databaseId)
                statement.bindText(4, installed.serviceId)
                statement.bindLong(5, installed.generationId)
                statement.bindLong(6, installed.localVersion.toLong())
                statement.bindText(7, installed.physicalShape)
                statement.bindText(8, installed.fingerprint)
                statement.step()
            }
            require(connection.scalarLong("SELECT changes()") == 1L) {
                "installed contract identity changed during local migration"
            }
        }
        client.open().getOrThrow()
        val physical = SyncRuntimeInitializer(config, TableInfoCache()).validateConfig(connection)
        val identity = physical.contractIdentity(config, connection)
        require(identity.localVersion == targetVersion.toLong()) { "ready local version differs from migration target" }
        require(identity.databaseId == config.databaseId && identity.serviceId == config.serviceId) {
            "ready database/service identity mismatch"
        }
        val retained = OversqliteContractStore(connection).read(identity)
        val store = OversqliteOpeningStore(connection)
        val existing = store.readOrNull()
        if (inspection.status.mode == OversqliteOpeningMode.LOCAL_ONLY) {
            requireNotNull(existing) { "local-only preservation witness is missing" }
            requireOpeningPreservation(connection, existing, config, plan)
        }
        val record = readyRecord(
            identity,
            retained?.fingerprint.orEmpty(),
            (existing?.revision ?: 0L) + 1L,
            preservationDigest = existing?.preservationDigest.orEmpty(),
        )
        if (inspection.status.mode in setOf(OversqliteOpeningMode.FRESH, OversqliteOpeningMode.LOCAL_ONLY)) {
            openingDurableCommitForTest?.invoke(
                "before_${inspection.status.mode!!.name.lowercase()}_ready",
                connection,
            )
        }
        if (existing == null) {
            store.create(record)
        } else if (
            inspection.status.mode == OversqliteOpeningMode.UNCHANGED &&
            existing.outcome == "ready" && existing.databaseId == identity.databaseId &&
            existing.serviceId == identity.serviceId && existing.toGenerationId == identity.generationId &&
            existing.toLocalVersion == identity.localVersion
        ) {
            // A prior completed edge is immutable evidence for the now-current installed identity.
        } else if (
            inspection.status.mode == OversqliteOpeningMode.LOCAL_ONLY &&
            existing.databaseId == identity.databaseId && existing.serviceId == identity.serviceId &&
            existing.toGenerationId == inspection.installed?.generationId &&
            existing.toLocalVersion == inspection.installed?.localVersion?.toLong()
        ) {
            store.replaceReady(record, existing)
        } else if (
            inspection.status.mode in setOf(OversqliteOpeningMode.FRESH, OversqliteOpeningMode.LOCAL_ONLY) &&
            existing.planDigest == inspection.planDigest && existing.phase == "migrating_local" &&
            existing.outcome.isEmpty()
        ) {
            store.update(record, existing)
        } else {
            error("completed coordinated opening journal does not match the requested opening")
        }
        inspectRetainedRuntime(connection)
    }

    private suspend fun ensureLocalMigrationJournal() {
        if (inspection.status.mode !in setOf(OversqliteOpeningMode.FRESH, OversqliteOpeningMode.LOCAL_ONLY)) return
        session.withConnection { connection ->
            openingAdmissionReservationForTest?.invoke("local_journal", connection)
            val mode = inspection.status.mode!!.name.lowercase()
            connection.transaction(TransactionMode.IMMEDIATE) {
                inspection = evaluateOpeningAdmission(connection, config, plan, session.migrationTargetVersion!!)
                val store = OversqliteOpeningStore(connection)
                val existing = store.readOrNull()
                if (existing != null && existing.planDigest == inspection.planDigest &&
                    existing.phase == "migrating_local" &&
                    existing.outcome in setOf("", "local_migration_failed", "cancelled")
                ) {
                    if (inspection.status.mode == OversqliteOpeningMode.LOCAL_ONLY) {
                        requireOpeningPreservation(connection, existing, config, plan, includePreservedLocalTables = true)
                    }
                    val resumed = if (existing.outcome.isEmpty()) existing else existing.copy(
                        outcome = "",
                        revision = existing.revision + 1L,
                        failureCode = "",
                        failureMessage = "",
                        nextAction = "advance",
                    ).also { store.update(it, existing) }
                    cachedStatus = statusFromRecord(resumed)
                    return@transaction
                }
                val preservationDigest = if (inspection.status.mode == OversqliteOpeningMode.LOCAL_ONLY) {
                    val installed = checkNotNull(inspection.installed)
                    val managed = if (installed.fingerprint.isEmpty()) {
                        config.syncTables.map { it.tableName }.toSet()
                    } else {
                        OversqliteContractStore(connection).readGeneration(installed.generationId)
                            .tables.map { it.table }.toSet()
                    }
                    requireExactPreservedTableDeclaration(
                        connection,
                        plan!!.preservedLocalTables,
                        managed,
                        managed,
                    )
                    installOversqliteOpeningGuards(connection, managed)
                    localOnlyOpeningDigest(connection, managed, plan.preservedLocalTables)
                } else {
                    ""
                }
                val initial = initialLocalMigrationRecord(
                    revision = (existing?.revision ?: 0L) + 1L,
                    preservationDigest = preservationDigest,
                )
                openingDurableCommitForTest?.invoke("before_${mode}_journal", connection)
                if (existing == null) store.create(initial) else store.replaceReady(initial, existing)
                cachedStatus = statusFromRecord(initial)
            }
            openingDurableCommitForTest?.invoke("after_${mode}_journal", connection)
        }
    }

    private fun initialLocalMigrationRecord(
        revision: Long,
        preservationDigest: String = "",
    ): OversqliteOpeningRecord =
        OversqliteOpeningRecord(
            mode = checkNotNull(inspection.status.mode).name.lowercase(), phase = "migrating_local", outcome = "",
            revision = revision, planDigest = inspection.planDigest, migrationId = plan?.migrationId.orEmpty(),
            databaseId = config.databaseId, serviceId = config.serviceId,
            attachedUserId = "", oldSourceId = "", newSourceId = "", replacementReason = "",
            fromGenerationId = inspection.installed?.generationId ?: 0,
            toGenerationId = config.generationId,
            fromLocalVersion = inspection.installed?.localVersion?.toLong() ?: -1,
            toLocalVersion = session.migrationTargetVersion!!.toLong(),
            preservedTables = inspection.preservedTables,
            oldFingerprint = inspection.installed?.fingerprint.orEmpty(), targetFingerprint = "",
            preservationDigest = preservationDigest, migrationComplete = false,
            contractConfirmed = checkNotNull(inspection.status.contractConfirmed),
            pendingDirtyCount = checkNotNull(inspection.status.pendingDirtyCount),
            pendingFrozenCount = checkNotNull(inspection.status.pendingFrozenCount),
            pendingConflictCount = checkNotNull(inspection.status.pendingConflictCount), drainComplete = false,
            replacementState = "none", snapshotId = "", snapshotBundleSeq = 0,
            snapshotRowCount = 0, snapshotByteCount = 0, stagedRowCount = null, stagedByteCount = null,
            targetReadsAvailable = false, writesAvailable = false, nextAction = "advance",
        )

    private fun statusFromRecord(record: OversqliteOpeningRecord): OversqliteOpeningStatus =
        openingStatusFromRecord(inspection.status, record)

    private fun readyRecord(
        identity: LocalContractIdentity,
        fingerprint: String,
        revision: Long,
        preservationDigest: String = "",
    ): OversqliteOpeningRecord =
        OversqliteOpeningRecord(
            mode = checkNotNull(inspection.status.mode).name.lowercase(), phase = "ready", outcome = "ready", revision = revision,
            planDigest = inspection.planDigest, migrationId = plan?.migrationId.orEmpty(),
            databaseId = identity.databaseId, serviceId = identity.serviceId,
            attachedUserId = "", oldSourceId = "", newSourceId = "", replacementReason = "",
            fromGenerationId = inspection.installed?.generationId ?: 0,
            toGenerationId = identity.generationId,
            fromLocalVersion = inspection.installed?.localVersion?.toLong() ?: -1,
            toLocalVersion = identity.localVersion, preservedTables = inspection.preservedTables,
            oldFingerprint = inspection.installed?.fingerprint.orEmpty(), targetFingerprint = fingerprint,
            preservationDigest = preservationDigest, migrationComplete = true,
            contractConfirmed = fingerprint.isNotEmpty(), pendingDirtyCount = 0,
            pendingFrozenCount = 0, pendingConflictCount = 0, drainComplete = true,
            replacementState = "none", snapshotId = "", snapshotBundleSeq = 0,
            snapshotRowCount = 0, snapshotByteCount = 0, stagedRowCount = null, stagedByteCount = null,
            targetReadsAvailable = true, writesAvailable = true,
            nextAction = "use_ready",
        )

}
