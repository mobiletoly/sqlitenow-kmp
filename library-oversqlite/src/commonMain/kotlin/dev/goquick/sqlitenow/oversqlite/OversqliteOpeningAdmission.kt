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
import dev.goquick.sqlitenow.core.SqliteNowInternalApi
import dev.goquick.sqlitenow.core.SqliteNowUnpublishedSession
import dev.goquick.sqlitenow.core.TransactionMode
import dev.goquick.sqlitenow.core.sqlite.use

private const val openingPlanDigestDomain = "sqlitenow-opening-plan-v1"

internal data class OpeningFailureClassification(
    val outcome: String,
    val diagnosticCode: String,
    val nextAction: String,
    val safeCause: String,
)

internal data class InstalledOpeningIdentity(
    val databaseId: String,
    val serviceId: String,
    val generationId: Long,
    val localVersion: Int,
    val physicalShape: String,
    val fingerprint: String,
)

@OptIn(SqliteNowInternalApi::class)
internal suspend fun inspectOpening(
    session: SqliteNowUnpublishedSession,
    config: OversqliteConfig,
    plan: OversqliteUpgradePlan?,
    targetVersion: Int,
    freshLifetimeAcquisition: Boolean = false,
    reclaimReservations: Boolean = false,
): OpeningInspection = session.withConnection { connection ->
    if (freshLifetimeAcquisition && reclaimReservations) connection.transaction(TransactionMode.IMMEDIATE) {
        if (inspectRetainedRuntime(connection) != "FRESH") {
            OversqliteWorkspace(connection, config).reclaimAfterLifetimeAcquisition()
        }
        evaluateOpeningAdmission(connection, config, plan, targetVersion)
    } else evaluateOpeningAdmission(connection, config, plan, targetVersion, abandonedReservations = freshLifetimeAcquisition)
}

internal suspend fun evaluateOpeningAdmission(
    connection: SafeSQLiteConnection,
    config: OversqliteConfig,
    plan: OversqliteUpgradePlan?,
    targetVersion: Int,
    expectedDurableEdge: OversqliteOpeningRecord? = null,
    abandonedReservations: Boolean = false,
): OpeningInspection {
    val census = inspectRetainedRuntime(connection)
    if (census != "FRESH") checkRetainedApplyWorkspace(connection, config, abandonedReservations)
    val journal = OversqliteOpeningStore(connection).readOrNull()
    if (expectedDurableEdge != null) {
        require(journal == expectedDurableEdge) {
            "durable coordinated opening edge changed before admission"
        }
    }
    val currentIdentity = if (census == "FRESH") null else readInstalledOpeningIdentity(connection)
    validateCompletedOpeningJournal(connection, journal, currentIdentity, config)
    if (journal?.isRetiredPendingMarker() == true) {
        val identity = requireNotNull(currentIdentity) {
            "retired pending marker has no installed contract identity"
        }
        require(journal.databaseId == identity.databaseId && journal.serviceId == identity.serviceId &&
            journal.toGenerationId == identity.generationId &&
            journal.toLocalVersion == identity.localVersion.toLong() &&
            journal.targetFingerprint == identity.fingerprint
        ) { "retired pending marker differs from the installed old-generation identity" }
        val classified = classifyOpening(config, plan, targetVersion, currentIdentity, connection)
        return classified.copy(status = openingStatusFromRecord(classified.status, journal))
    }
    if (journal?.outcome == "ready" && journal.mode == "local_only" && plan != null &&
        journal.migrationId == plan.migrationId && journal.fromGenerationId == plan.fromGenerationId &&
        journal.toGenerationId == plan.toGenerationId && journal.fromLocalVersion == plan.fromLocalVersion.toLong() &&
        journal.toLocalVersion == plan.toLocalVersion.toLong()
    ) {
        val preserved = plan.preservedLocalTables.sorted().joinToString("\n")
        require(journal.preservedTables == preserved) { "completed replay preservation declaration differs" }
        val original = InstalledOpeningIdentity(journal.databaseId, journal.serviceId,
            journal.fromGenerationId, journal.fromLocalVersion.toInt(), "", journal.oldFingerprint)
        require(journal.planDigest == openingPlanDigest(config, plan, targetVersion, original, preserved)) {
            "completed replay immutable edge differs"
        }
        return classifyOpening(config, null, targetVersion, currentIdentity, connection)
    }
    val resumedGeneration = journal?.takeIf {
        it.mode == "generation_change" && plan != null &&
            it.databaseId == config.databaseId && it.serviceId == config.serviceId &&
            it.migrationId == plan.migrationId && it.fromGenerationId == plan.fromGenerationId &&
            it.toGenerationId == plan.toGenerationId && it.fromLocalVersion == plan.fromLocalVersion.toLong() &&
            it.toLocalVersion == plan.toLocalVersion.toLong()
    }
    val installed = resumedGeneration?.let {
        val identityShouldBeTarget = it.phase in setOf("local_migrated", "reattach_pending", "rebuild_running", "ready")
        if (identityShouldBeTarget) {
            require(currentIdentity?.generationId == it.toGenerationId &&
                currentIdentity.localVersion.toLong() == it.toLocalVersion &&
                currentIdentity.fingerprint == it.targetFingerprint
            ) { "installed target identity differs from the durable generation journal" }
        } else {
            require(currentIdentity?.generationId == it.fromGenerationId &&
                currentIdentity.localVersion.toLong() == it.fromLocalVersion &&
                currentIdentity.fingerprint == it.oldFingerprint
            ) { "installed old identity differs from the durable generation journal" }
        }
        InstalledOpeningIdentity(
            databaseId = it.databaseId, serviceId = it.serviceId,
            generationId = it.fromGenerationId, localVersion = it.fromLocalVersion.toInt(),
            physicalShape = "", fingerprint = it.oldFingerprint,
        )
    } ?: currentIdentity
    if (resumedGeneration != null) {
        val retainedIdentity = checkNotNull(currentIdentity)
        val retainedGeneration = OversqliteContractStore(connection).readGeneration(
            retainedIdentity.generationId,
        )
        require(retainedGeneration.fingerprint == retainedIdentity.fingerprint) {
            "installed generation admission changed during coordinated opening"
        }
        val retainedConfig = config.copy(
            generationId = retainedIdentity.generationId,
            syncTables = retainedGeneration.tables.map { table ->
                SyncTable(tableName = table.table, syncKeyColumns = table.syncKeyColumns)
            },
        )
        val physical = SyncRuntimeInitializer(retainedConfig, TableInfoCache()).validateConfig(connection)
        retainedGeneration.requireMatchingLocalSchema(physical.tableInfoByName, physical.keyByTable)
        require(managedPhysicalShape(physical.tableInfoByName, physical.keyByTable) == retainedIdentity.physicalShape) {
            "installed physical contract changed during coordinated opening"
        }
    }
    val classified = classifyOpening(config, plan, targetVersion, installed, connection)
    val journalOriginMatches = if (installed == null) {
        journal?.fromGenerationId == 0L && journal.fromLocalVersion == -1L
    } else {
        journal?.fromGenerationId == installed.generationId &&
            journal.fromLocalVersion == installed.localVersion.toLong()
    }
    if (journal != null &&
        journal.databaseId == config.databaseId && journal.serviceId == config.serviceId &&
        journalOriginMatches && journal.toGenerationId == config.generationId &&
        journal.toLocalVersion == targetVersion.toLong() &&
        journal.migrationId == plan?.migrationId
    ) {
        return classified.copy(status = openingStatusFromRecord(classified.status, journal))
    } else {
        return classified
    }
}

internal fun preparationRequiredInspection(
    config: OversqliteConfig,
    targetVersion: Int,
): OpeningInspection = OpeningInspection(
    installed = null,
    planDigest = "",
    preservedTables = "",
    status = OversqliteOpeningStatus(
        mode = null,
        outcome = null,
        phase = null,
        revision = 0,
        installedGenerationId = null,
        targetGenerationId = config.generationId,
        installedLocalVersion = null,
        targetLocalVersion = targetVersion,
        contractConfirmed = null,
        pendingDirtyCount = null,
        pendingFrozenCount = null,
        pendingConflictCount = null,
        drainComplete = null,
        replacementState = null,
        stagedRowCount = null,
        stagedByteCount = null,
        targetReadsAvailable = null,
        writesAvailable = null,
        nextAction = OversqliteOpeningNextAction.ADVANCE,
        diagnosticCode = "provider_preparation_required",
        diagnosticMessage = "Call advance() to prepare provider storage and complete inspection",
    ),
)

internal data class OpeningInspection(
    val installed: InstalledOpeningIdentity?,
    val status: OversqliteOpeningStatus,
    val planDigest: String,
    val preservedTables: String,
)

internal fun openingOutcome(value: String): OversqliteOpeningOutcome =
    OversqliteOpeningOutcome.valueOf(value.uppercase())

internal fun openingNextAction(value: String): OversqliteOpeningNextAction =
    OversqliteOpeningNextAction.valueOf(value.uppercase())

internal fun openingStatusFromRecord(
    base: OversqliteOpeningStatus,
    record: OversqliteOpeningRecord,
): OversqliteOpeningStatus = base.copy(
    outcome = record.outcome.takeIf(String::isNotEmpty)?.let { OversqliteOpeningOutcome.valueOf(it.uppercase()) },
    phase = record.phase,
    revision = record.revision,
    installedGenerationId = if (record.migrationComplete) record.toGenerationId else record.fromGenerationId,
    installedLocalVersion = if (record.migrationComplete) record.toLocalVersion.toInt() else record.fromLocalVersion.toInt(),
    contractConfirmed = record.contractConfirmed,
    pendingDirtyCount = record.pendingDirtyCount,
    pendingFrozenCount = record.pendingFrozenCount,
    pendingConflictCount = record.pendingConflictCount,
    drainComplete = record.drainComplete,
    replacementState = when (record.replacementState) {
        "reserved" -> OversqliteOpeningReplacementState.RESERVED
        "confirmed" -> OversqliteOpeningReplacementState.CONFIRMED
        else -> OversqliteOpeningReplacementState.NONE
    },
    stagedRowCount = record.stagedRowCount,
    stagedByteCount = record.stagedByteCount,
    targetReadsAvailable = record.targetReadsAvailable,
    writesAvailable = record.writesAvailable,
    nextAction = when (record.nextAction) {
        "resolve_conflict" -> OversqliteOpeningNextAction.RESOLVE_CONFLICT
        "resolve_rejection" -> OversqliteOpeningNextAction.RESOLVE_REJECTION
        "finish_old_attachment" -> OversqliteOpeningNextAction.FINISH_OLD_ATTACHMENT
        "repair_configuration" -> OversqliteOpeningNextAction.REPAIR_CONFIGURATION
        "restore_support" -> OversqliteOpeningNextAction.RESTORE_SUPPORT
        "inspect_integrity" -> OversqliteOpeningNextAction.INSPECT_INTEGRITY
        "use_ready" -> OversqliteOpeningNextAction.USE_READY
        "close" -> OversqliteOpeningNextAction.CLOSE
        else -> OversqliteOpeningNextAction.ADVANCE
    },
    diagnosticCode = record.failureCode.takeIf(String::isNotEmpty),
    diagnosticMessage = record.failureMessage.takeIf(String::isNotEmpty),
)

internal fun OversqliteOpeningRecord.isRetiredPendingMarker(): Boolean =
    phase == "ready" &&
        outcome == "old_generation_unavailable" &&
        failureCode == "retired_schema_generation" &&
        nextAction == "restore_support"

internal suspend fun requireOpeningPreservation(
    connection: SafeSQLiteConnection,
    record: OversqliteOpeningRecord,
    config: OversqliteConfig,
    plan: OversqliteUpgradePlan?,
    includePreservedLocalTables: Boolean = false,
) {
    try {
        if (record.mode == "generation_change" && record.outcome != "ready") {
            requirePreservedLocalTableGuards(connection, plan!!.preservedLocalTables)
        }
        if (record.mode == "local_only") {
            requireLocalOnlyOpeningDigest(
                connection,
                record.preservationDigest,
                config.syncTables.map { it.tableName }.toSet(),
                plan!!.preservedLocalTables,
                includePreservedLocalTables,
            )
        } else {
            require(preservedStateDigest(connection, plan!!.preservedLocalTables) == record.preservationDigest) {
                "declared local-only state changed during coordinated opening"
            }
        }
    } catch (failure: Throwable) {
        if (failure is OversqlitePreservationFailureException) throw failure
        throw OversqlitePreservationFailureException(failure)
    }
}

private suspend fun classifyOpening(
    config: OversqliteConfig,
    plan: OversqliteUpgradePlan?,
    targetVersion: Int,
    installed: InstalledOpeningIdentity?,
    db: SafeSQLiteConnection,
): OpeningInspection {
    require(targetVersion >= 0) { "migration targetVersion must be nonnegative" }
    val mode = when {
        installed == null -> {
            require(plan == null) { "a fresh database must not supply an upgrade plan" }
            require(!db.hasApplicationObjectsWithoutOpeningJournal()) {
                "a database with application objects but no admitted SQLiteNow identity cannot be opened as fresh; recreate it or use the explicit retained-file adoption path"
            }
            OversqliteOpeningMode.FRESH
        }
        plan == null -> {
            require(installed.databaseId == config.databaseId && installed.serviceId == config.serviceId) {
                "installed database/service identity differs from the requested configuration"
            }
            if (installed.generationId != config.generationId || installed.localVersion != targetVersion) {
                throw OversqliteUpgradePlanMismatchException(
                    "an installed generation or local-version mismatch requires an explicit upgrade plan",
                )
            }
            OversqliteOpeningMode.UNCHANGED
        }
        else -> {
            if (config.generationId != plan.toGenerationId) {
                throw OversqliteUpgradePlanMismatchException("config generationId must equal plan target")
            }
            if (targetVersion != plan.toLocalVersion) {
                throw OversqliteUpgradePlanMismatchException(
                    "generated migration target must equal plan target local version",
                )
            }
            require(installed.databaseId == config.databaseId && installed.serviceId == config.serviceId) {
                "installed database/service identity differs from the requested configuration"
            }
            if (installed.generationId != plan.fromGenerationId || installed.localVersion != plan.fromLocalVersion) {
                throw OversqliteUpgradePlanMismatchException(
                    "installed generation/local version does not match the requested edge",
                )
            }
            if (plan.fromGenerationId == plan.toGenerationId) {
                require(plan.toLocalVersion > plan.fromLocalVersion) {
                    "same-generation local-only upgrades must strictly increase local version"
                }
                OversqliteOpeningMode.LOCAL_ONLY
            } else {
                require(plan.toGenerationId > plan.fromGenerationId) { "generation downgrade is unsupported" }
                require(plan.toLocalVersion >= plan.fromLocalVersion) {
                    "generation upgrades cannot decrease local version"
                }
                OversqliteOpeningMode.GENERATION_CHANGE
            }
        }
    }
    val preserved = plan?.preservedLocalTables?.toList()?.sorted()?.joinToString("\n").orEmpty()
    val digest = openingPlanDigest(config, plan, targetVersion, installed, preserved)
    val dirty = if (installed == null) 0L else db.scalarLong("SELECT count(*) FROM _sync_dirty_rows")
    val frozen = if (installed == null) 0L else db.scalarLong("SELECT count(*) FROM _sync_outbox_rows")
    val conflicts = if (installed == null) 0L else db.scalarLong(
        "SELECT count(*) FROM _sync_conflict_state WHERE singleton_key=1 AND decision_state!='none'",
    )
    return OpeningInspection(
        installed = installed,
        planDigest = digest,
        preservedTables = preserved,
        status = OversqliteOpeningStatus(
            mode = mode,
            outcome = null,
            phase = null,
            revision = 0,
            installedGenerationId = installed?.generationId,
            targetGenerationId = config.generationId,
            installedLocalVersion = installed?.localVersion,
            targetLocalVersion = targetVersion,
            contractConfirmed = installed?.fingerprint?.isNotEmpty() == true,
            pendingDirtyCount = dirty,
            pendingFrozenCount = frozen,
            pendingConflictCount = conflicts,
            drainComplete = false,
            replacementState = OversqliteOpeningReplacementState.NONE,
            stagedRowCount = null,
            stagedByteCount = null,
            targetReadsAvailable = false,
            writesAvailable = false,
            nextAction = OversqliteOpeningNextAction.ADVANCE,
            diagnosticCode = "advance_required",
            diagnosticMessage = "Call advance() to perform the selected coordinated opening mode",
        ),
    )
}

private suspend fun SafeSQLiteConnection.hasApplicationObjectsWithoutOpeningJournal(): Boolean {
    val statement = prepare(
        "SELECT 1 FROM sqlite_schema " +
            "WHERE name NOT LIKE 'sqlite_%' " +
            "AND lower(name) != 'android_metadata' " +
            "AND lower(name) != '_sync_opening_state' " +
            "LIMIT 1",
    )
    return statement.use { it.step() }
}

private fun openingPlanDigest(
    config: OversqliteConfig,
    plan: OversqliteUpgradePlan?,
    targetVersion: Int,
    installed: InstalledOpeningIdentity?,
    preservedTables: String,
): String = sha256Hex(
    listOf(
        openingPlanDigestDomain, config.databaseId, config.serviceId,
        installed?.generationId ?: 0L, config.generationId,
        installed?.localVersion ?: -1, targetVersion,
        plan?.migrationId.orEmpty(), preservedTables,
        installed?.fingerprint.orEmpty(),
    ).joinToString("\u001f").encodeToByteArray(),
)

internal suspend fun readInstalledOpeningIdentity(db: SafeSQLiteConnection): InstalledOpeningIdentity {
    val rows = db.queryList(
        """
        SELECT database_id,service_id,generation_id,local_version,physical_shape,fingerprint
        FROM _sync_contract_identity WHERE singleton_key=1
        """.trimIndent(),
    ) { row ->
        InstalledOpeningIdentity(
            databaseId = row.getText(0), serviceId = row.getText(1), generationId = row.getLong(2),
            localVersion = row.getLong(3).toInt(), physicalShape = row.getText(4), fingerprint = row.getText(5),
        )
    }
    require(rows.size == 1) { "retained contract identity is missing or duplicated" }
    return rows.single()
}

private suspend fun validateCompletedOpeningJournal(
    connection: SafeSQLiteConnection,
    record: OversqliteOpeningRecord?,
    installed: InstalledOpeningIdentity?,
    requestedConfig: OversqliteConfig,
) {
    if (record?.outcome != "ready") return
    require(record.phase == "ready" && record.migrationComplete) {
        "completed coordinated opening journal has an incomplete terminal state"
    }
    val identity = requireNotNull(installed) {
        "completed coordinated opening journal has no installed contract identity"
    }
    require(record.databaseId == identity.databaseId && record.serviceId == identity.serviceId) {
        "completed coordinated opening journal database/service identity mismatch"
    }
    require(record.toGenerationId == identity.generationId &&
        record.toLocalVersion == identity.localVersion.toLong() &&
        record.targetFingerprint == identity.fingerprint
    ) { "completed coordinated opening journal target identity mismatch" }
    require(connection.scalarLong("PRAGMA user_version") == identity.localVersion.toLong()) {
        "completed coordinated opening journal local version differs from the database"
    }

    val retained = try {
        OversqliteContractStore(connection).read(
            LocalContractIdentity(
                identity.databaseId,
                identity.serviceId,
                identity.generationId,
                identity.localVersion.toLong(),
                identity.physicalShape,
            ),
        )
    } catch (_: IllegalArgumentException) {
        throw OversqliteUpgradePreconditionException(
            "installed generation authority is missing or corrupt; restore the retained generation record",
        )
    }
    require((retained != null) == record.contractConfirmed) {
        "completed coordinated opening journal confirmation state mismatch"
    }
    if (retained == null) {
        require(identity.fingerprint.isEmpty() &&
            requestedConfig.databaseId == identity.databaseId &&
            requestedConfig.serviceId == identity.serviceId &&
            requestedConfig.generationId == identity.generationId
        ) { "completed unconfirmed opening journal cannot identify the installed selection" }
        val physical = SyncRuntimeInitializer(requestedConfig, TableInfoCache()).validateConfig(connection)
        require(managedPhysicalShape(physical.tableInfoByName, physical.keyByTable) == identity.physicalShape) {
            "completed unconfirmed opening journal installed physical identity mismatch"
        }
        return
    }
    require(retained.fingerprint == record.targetFingerprint) {
        "completed coordinated opening journal retained target descriptor mismatch"
    }
    val retainedConfig = requestedConfig.copy(
        databaseId = identity.databaseId,
        serviceId = identity.serviceId,
        generationId = identity.generationId,
        syncTables = retained.tables.map { table ->
            SyncTable(tableName = table.table, syncKeyColumns = table.syncKeyColumns)
        },
    )
    val physical = SyncRuntimeInitializer(retainedConfig, TableInfoCache()).validateConfig(connection)
    retained.requireMatchingLocalSchema(physical.tableInfoByName, physical.keyByTable)
    require(managedPhysicalShape(physical.tableInfoByName, physical.keyByTable) == identity.physicalShape) {
        "completed coordinated opening journal installed physical identity mismatch"
    }
}
