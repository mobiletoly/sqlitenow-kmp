package dev.goquick.sqlitenow.oversqlite.platform

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.SqlitePersistence
import dev.goquick.sqlitenow.core.createAuthenticLegacySqlJsFixture
import dev.goquick.sqlitenow.core.legacySnapshotPersistenceForTest
import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.core.worker.SqliteWorkerSQLiteConnection
import dev.goquick.sqlitenow.core.worker.SqliteWorkerSQLiteDriver
import dev.goquick.sqlitenow.oversqlite.Phase6OwnedStorage
import dev.goquick.sqlitenow.oversqlite.oversqliteTestConnectionProvider
import dev.goquick.sqlitenow.oversqlite.phase6WorkerStorageEvidence
import dev.goquick.sqlitenow.oversqlite.platformsupport.sha256Hex
import dev.goquick.sqlitenow.oversqlite.webRuntimeKind
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.test.runTest

internal class Phase6MigratedOversqliteStateTest : PlatformCrossTargetTestSupport() {
    private companion object {
        const val FROZEN_PHASE6_REQUEST_HASH =
            "4dbefab29640f8006a98685b8134a032709afe9267f31d76c59cec77666632ae"
        const val FROZEN_PHASE6_INITIALIZATION_ID =
            "00000000-0000-4000-8001-000000000001"
    }

    @Test
    fun opfsIndexedDbAndCustomSourcesPreserveFullOversqliteContinuityMatrix() = runTest {
        if (webRuntimeKind() !in setOf("js-browser", "wasm-browser")) return@runTest

        val scenarios = listOf(
            MigratedSourceScenario("opfs", forceOpfs = true),
            MigratedSourceScenario("indexeddb", forceOpfs = false),
            MigratedSourceScenario("custom", forceOpfs = null),
        )
        try {
            scenarios.forEach { scenario ->
                val ownedStorage = Phase6OwnedStorage()
                val dbName = ownedStorage.newDatabaseName()
                val custom = if (scenario.name == "custom") RecordingSnapshotPersistence() else null
                val persistence = if (custom != null) {
                    custom
                } else {
                    legacySnapshotPersistenceForTest(
                        dbName = dbName,
                        forceOpfs = checkNotNull(scenario.forceOpfs),
                    )
                }
                when (scenario.name) {
                    "opfs" -> ownedStorage.recordLegacyOpfsDatabase(dbName)
                    "indexeddb" -> ownedStorage.recordLegacyIndexedDbDatabase(dbName)
                }
                val server = MockSyncServer()
                var connection: SafeSQLiteConnection? = null
                try {
                    val legacyFixture = createAuthenticLegacySqlJsFixture(dbName)
                    val legacy = legacyFixture.connection
                    connection = legacy
                    legacy.execSQL("PRAGMA foreign_keys = ON")
                    createUsersAndPostsTables(legacy)
                    insertUser(legacy, "local-1", "Ada")
                    legacy.execSQL("PRAGMA user_version = 6")
                    val fixedSourceId = "phase6-source-fixed"
                    val legacyHttp = server.newHttpClient()
                    val legacyClient = newClient(legacy, legacyHttp)
                    val preImportState = try {
                        legacyClient.open().getOrThrow()
                        val generatedSourceId =
                            scalarText(legacy, "SELECT current_source_id FROM _sync_attachment_state")
                        legacy.execSQL(
                            "UPDATE _sync_source_state SET source_id = '$fixedSourceId' " +
                                "WHERE source_id = '$generatedSourceId'",
                        )
                        legacy.execSQL(
                            "UPDATE _sync_attachment_state SET current_source_id = '$fixedSourceId' " +
                                "WHERE singleton_key = 1",
                        )
                        val boundary = assertPendingMigrationBoundary(
                            db = legacy,
                            sourceId = fixedSourceId,
                            expectedUserVersion = 6,
                        )
                        boundary
                    } finally {
                        legacyClient.close()
                        legacyHttp.close()
                    }
                    persistence.persist(dbName, legacyFixture.exportBytes())
                    legacyFixture.close()
                    connection = null

                    val retainedBefore = assertNotNull(persistence.load(dbName)).copyOf()
                    val retainedHash = sha256Hex(retainedBefore)
                    val customLoadCallsAfterSeed = custom?.loadCalls
                    val customPersistCallsAfterSeed = custom?.persistCalls
                    val workerConfig = if (custom == null) {
                        SqliteConnectionConfig()
                    } else {
                        SqliteConnectionConfig(persistence = custom)
                    }
                    val cancellingDriver = SqliteWorkerSQLiteDriver.create()
                    try {
                        val cancellationHold =
                            cancellingDriver.holdMigrationCancellationForTest(dbName, "after-health")
                        coroutineScope {
                            val opening = async {
                                cancellingDriver.open(
                                    fileName = dbName,
                                    legacySourceMode = if (custom == null) "built-in" else "custom",
                                    customPersistence = custom,
                                )
                            }
                            assertEquals(
                                0,
                                cancellingDriver.awaitCancellationHoldForTest(cancellationHold),
                            )
                            opening.cancel()
                            kotlin.test.assertFailsWith<kotlinx.coroutines.CancellationException> {
                                opening.await()
                            }
                        }
                    } finally {
                        cancellingDriver.shutdown()
                    }

                    var worker = oversqliteTestConnectionProvider()
                        .openConnection(dbName, debug = true, config = workerConfig)
                    connection = worker
                    assertEquals(
                        preImportState,
                        assertPendingMigrationBoundary(worker, fixedSourceId, expectedUserVersion = 6),
                        "${scenario.name}: the imported control state must exactly match its legacy source",
                    )
                    val firstMetrics = (worker.ref as SqliteWorkerSQLiteConnection).metricsForTest()
                    assertEquals("browser-worker", firstMetrics.runtimeKind)
                    assertEquals("direct-opfs", firstMetrics.storageMode)
                    assertEquals(scenario.name, firstMetrics.migrationSourceKind)
                    assertEquals(retainedBefore.size.toLong(), firstMetrics.migrationSourceBytes)
                    assertEquals(retainedHash, firstMetrics.migrationSourceSha256)
                    assertEquals("ok", firstMetrics.migrationIntegrityCheck)
                    assertEquals(6, firstMetrics.migrationImportedUserVersion)
                    assertTrue(firstMetrics.migrationSourceRetained)
                    assertEquals(0L, firstMetrics.snapshotExports)
                    assertEquals(0, firstMetrics.liveStatements)

                    val storage = phase6WorkerStorageEvidence(dbName).split('\t', limit = 4)
                    assertEquals(firstMetrics.migrationTargetFileName, storage[0])
                    assertEquals("1", storage[1])
                    assertEquals("0", storage[2])
                    assertTrue(storage[3].contains(""""sourceKind":"${scenario.name}""""))
                    assertTrue(storage[3].contains(""""sourceSha256":"$retainedHash""""))
                    assertTrue(storage[3].contains(""""sourceBytes":${retainedBefore.size}"""))
                    worker.close()
                    connection = null

                    worker = oversqliteTestConnectionProvider()
                        .openConnection(dbName, debug = true, config = workerConfig)
                    connection = worker
                    assertEquals(
                        preImportState,
                        assertPendingMigrationBoundary(worker, fixedSourceId, expectedUserVersion = 6),
                        "${scenario.name}: close/reopen must preserve every imported control value",
                    )
                    assertEquals(
                        "",
                        (worker.ref as SqliteWorkerSQLiteConnection).metricsForTest().migrationSourceKind,
                        "A healthy direct target must win over its retained stale source.",
                    )
                    val sourceId =
                        scalarText(worker, "SELECT current_source_id FROM _sync_attachment_state")
                    assertEquals("phase6-source-fixed", sourceId)
                    val workerHttp = server.newHttpClient()
                    val client = newClient(worker, workerHttp)
                    try {
                        client.open().getOrThrow()
                        var cancelFirstLifecycleCommit = true
                        worker.beforeTransactionCommitForTest = {
                            if (cancelFirstLifecycleCommit) {
                                cancelFirstLifecycleCommit = false
                                throw kotlinx.coroutines.CancellationException(
                                    "cancel first post-import lifecycle transaction",
                                )
                            }
                        }
                        kotlin.test.assertFailsWith<kotlinx.coroutines.CancellationException> {
                            worker.transaction {
                                worker.execSQL(
                                    "UPDATE _sync_attachment_state " +
                                        "SET binding_state = 'attached', " +
                                        "attached_user_id = 'phase6-user', schema_name = 'main' " +
                                        "WHERE singleton_key = 1",
                                )
                            }
                        }
                        worker.beforeTransactionCommitForTest = null
                        assertEquals(
                            "anonymous|||0",
                            scalarText(
                                worker,
                                "SELECT binding_state || '|' || attached_user_id || '|' || " +
                                    "schema_name || '|' || last_bundle_seq_seen " +
                                    "FROM _sync_attachment_state",
                            ),
                        )
                        assertEquals(sourceId, scalarText(worker, "SELECT current_source_id " +
                            "FROM _sync_attachment_state"))
                        client.attach("phase6-user").getOrThrow()
                        assertEquals(sourceId, scalarText(worker, "SELECT current_source_id " +
                            "FROM _sync_attachment_state"))
                        server.cancelNextRequestPathPrefix = "/sync/push-sessions"
                        kotlin.test.assertFailsWith<kotlinx.coroutines.CancellationException> {
                            client.pushPending()
                        }
                        assertPreparedOutbox(worker, sourceId)
                        val requestHash =
                            scalarText(worker, "SELECT canonical_request_hash FROM _sync_outbox_bundle")
                        assertEquals(FROZEN_PHASE6_REQUEST_HASH, requestHash)
                        client.pushPending().getOrThrow()
                        client.pullToStable().getOrThrow()
                        client.sync().getOrThrow()
                        assertEquals(sourceId, scalarText(worker, "SELECT current_source_id " +
                            "FROM _sync_attachment_state"))
                        assertEquals("none", scalarText(worker, "SELECT state FROM _sync_outbox_bundle"))
                        assertEquals(0L, scalarLong(worker, "SELECT COUNT(*) FROM _sync_dirty_rows"))
                        assertEquals(0L, scalarLong(worker, "SELECT COUNT(*) FROM _sync_outbox_rows"))
                        assertCompletedMigrationBoundary(
                            db = worker,
                            sourceId = sourceId,
                            expectedUserVersion = 6,
                            expectedLastBundleSeq = 1,
                            expectedNextSourceBundleId = 2,
                            expectedUserId = "local-1",
                            expectedUserName = "Ada",
                            expectedRowVersion = 1,
                        )
                    } finally {
                        worker.beforeTransactionCommitForTest = null
                        client.close()
                        workerHttp.close()
                    }
                    val completedMetrics = worker.ref.metricsForTest()
                    assertEquals(0L, completedMetrics.snapshotExports)
                    assertEquals(0, completedMetrics.liveStatements)
                    worker.close()
                    connection = null

                    val retainedAfter = assertNotNull(persistence.load(dbName))
                    assertTrue(retainedBefore.contentEquals(retainedAfter))
                    assertEquals(retainedHash, sha256Hex(retainedAfter))
                    if (custom != null) {
                        assertEquals(checkNotNull(customLoadCallsAfterSeed) + 3, custom.loadCalls)
                        assertEquals(customPersistCallsAfterSeed, custom.persistCalls)
                        assertEquals(0, custom.clearCalls)
                    }
                    println(
                        "phase7_default_migrated_matrix runtime=${webRuntimeKind()} source=${scenario.name} " +
                            "bytes=${retainedBefore.size} sha256=$retainedHash " +
                            "requestHash=$FROZEN_PHASE6_REQUEST_HASH " +
                            "target=${storage[0]} snapshotExports=0",
                    )
                } finally {
                    connection?.close()
                    ownedStorage.cleanup()
                }
            }
        } finally {
            // Each scenario owns and cleans only its exact deterministic names.
        }
    }

    @Test
    fun retainedCustomSnapshotPreservesLifecycleAndPreparedOutboxAcrossWorkerReopens() = runTest {
        if (webRuntimeKind() !in setOf("js-browser", "wasm-browser")) return@runTest

        val ownedStorage = Phase6OwnedStorage()
        val dbName = ownedStorage.newDatabaseName()
        val retained = RecordingSnapshotPersistence()
        val server = MockSyncServer()
        var connection: SafeSQLiteConnection? = null
        try {
            val legacyFixture = createAuthenticLegacySqlJsFixture(dbName)
            val legacy = legacyFixture.connection
            connection = legacy
            legacy.execSQL("PRAGMA foreign_keys = ON")
            createUsersAndPostsTables(legacy)
            insertUser(legacy, "local-1", "Ada")
            legacy.execSQL("PRAGMA user_version = 6")
            val legacyClient = newClient(legacy, server.newHttpClient())
            lateinit var sourceId: String
            val preImportState = try {
                legacyClient.open().getOrThrow()
                sourceId = scalarText(
                    legacy,
                    "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1",
                )
                assertPendingMigrationBoundary(legacy, sourceId, expectedUserVersion = 6)
            } finally {
                legacyClient.close()
            }
            retained.persist(dbName, legacyFixture.exportBytes())
            legacyFixture.close()
            connection = null

            val retainedBefore = assertNotNull(retained.bytes).copyOf()
            val retainedHash = sha256Hex(retainedBefore)
            val loadCallsAfterLegacy = retained.loadCalls
            val persistCallsAfterLegacy = retained.persistCalls
            assertEquals(0, loadCallsAfterLegacy)
            assertTrue(persistCallsAfterLegacy > 0)

            val workerConfig = SqliteConnectionConfig(persistence = retained)
            var worker = oversqliteTestConnectionProvider()
                .openConnection(dbName, debug = true, config = workerConfig)
            connection = worker
            assertEquals(
                preImportState,
                assertPendingMigrationBoundary(worker, sourceId, expectedUserVersion = 6),
                "custom: the imported control state must exactly match its legacy source",
            )
            assertEquals(loadCallsAfterLegacy + 1, retained.loadCalls)
            assertEquals(persistCallsAfterLegacy, retained.persistCalls)
            assertEquals(0, retained.clearCalls)
            worker.close()
            connection = null

            worker = oversqliteTestConnectionProvider()
                .openConnection(dbName, debug = true, config = workerConfig)
            connection = worker
            assertEquals(
                preImportState,
                assertPendingMigrationBoundary(worker, sourceId, expectedUserVersion = 6),
                "custom: close/reopen must preserve every imported control value",
            )
            assertEquals(
                loadCallsAfterLegacy + 1,
                retained.loadCalls,
                "A healthy direct target must not reload a stale source.",
            )

            assertEquals(
                sourceId,
                scalarText(worker, "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1"),
            )
            val attachedClient = newClient(worker, server.newHttpClient())
            attachedClient.open().getOrThrow()
            attachedClient.attach("phase6-user").getOrThrow()
            assertEquals(
                "attached|phase6-user|main",
                scalarText(
                    worker,
                    "SELECT binding_state || '|' || attached_user_id || '|' || schema_name " +
                        "FROM _sync_attachment_state WHERE singleton_key = 1",
                ),
            )
            assertEquals(sourceId, scalarText(worker, "SELECT current_source_id FROM _sync_attachment_state"))

            server.rejectPushCreate = true
            assertTrue(attachedClient.pushPending().isFailure)
            val preparedState = assertPreparedOutboxBoundary(worker, sourceId)
            attachedClient.close()
            worker.close()
            connection = null

            worker = oversqliteTestConnectionProvider()
                .openConnection(dbName, debug = true, config = workerConfig)
            connection = worker
            assertEquals(
                preparedState,
                assertPreparedOutboxBoundary(worker, sourceId),
                "prepared outbox rows and all companion control state must survive close/reopen",
            )
            server.rejectPushCreate = false
            val resumedClient = newClient(worker, server.newHttpClient())
            resumedClient.open().getOrThrow()
            resumedClient.attach("phase6-user").getOrThrow()
            resumedClient.pushPending().getOrThrow()
            assertEquals("none", scalarText(worker, "SELECT state FROM _sync_outbox_bundle"))
            assertEquals(0L, scalarLong(worker, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(worker, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(sourceId, scalarText(worker, "SELECT current_source_id FROM _sync_attachment_state"))
            assertCompletedMigrationBoundary(
                db = worker,
                sourceId = sourceId,
                expectedUserVersion = 6,
                expectedLastBundleSeq = 1,
                expectedNextSourceBundleId = 2,
                expectedUserId = "local-1",
                expectedUserName = "Ada",
                expectedRowVersion = 1,
            )
            resumedClient.close()
            worker.close()
            connection = null

            assertTrue(retainedBefore.contentEquals(assertNotNull(retained.bytes)))
            assertEquals(retainedHash, sha256Hex(assertNotNull(retained.bytes)))
            assertEquals(loadCallsAfterLegacy + 1, retained.loadCalls)
            assertEquals(persistCallsAfterLegacy, retained.persistCalls)
            assertEquals(0, retained.clearCalls)
            println(
                "phase6_migrated_oversqlite source=custom bytes=${retainedBefore.size} " +
                    "sha256=$retainedHash loads=${retained.loadCalls} " +
                    "workerSnapshotExports=0 retained=true",
            )
        } finally {
            connection?.close()
            ownedStorage.cleanup()
        }
    }

    @Test
    fun retainedInterruptedApplyCompletesFromCanonicalStageWithoutRebuildOrSourceRotation() = runTest {
        if (webRuntimeKind() !in setOf("js-browser", "wasm-browser")) return@runTest

        val ownedStorage = Phase6OwnedStorage()
        val dbName = ownedStorage.newDatabaseName()
        val retained = RecordingSnapshotPersistence()
        val server = MockSyncServer()
        var connection: SafeSQLiteConnection? = null
        try {
            val legacyFixture = createAuthenticLegacySqlJsFixture(dbName)
            val legacy = legacyFixture.connection
            connection = legacy
            createUsersAndPostsTables(legacy)
            val legacyClient = newClient(legacy, server.newHttpClient())
            legacyClient.open().getOrThrow()
            val sourceId = scalarText(legacy, "SELECT current_source_id FROM _sync_attachment_state")
            legacy.execSQL(
                """
                INSERT INTO _sync_snapshot_stage(
                  snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
                ) VALUES (
                  'phase6-interrupted', 1, 'main', 'users', '{"id":"remote-1"}', 7,
                  '{"id":"remote-1","name":"Remote Ada"}'
                )
                """.trimIndent(),
            )
            legacy.execSQL(
                """
                UPDATE _sync_operation_state
                SET kind = 'remote_replace',
                    target_user_id = 'phase6-user',
                    staged_snapshot_id = 'phase6-interrupted',
                    snapshot_bundle_seq = 7,
                    snapshot_row_count = 1,
                    snapshot_byte_count = 39,
                    snapshot_stage_complete = 1
                WHERE singleton_key = 1
                """.trimIndent(),
            )
            legacy.execSQL("UPDATE _sync_apply_state SET apply_mode = 1 WHERE singleton_key = 1")
            legacy.execSQL("PRAGMA user_version = 6")
            val preImportState = assertInterruptedMigrationBoundary(
                db = legacy,
                sourceId = sourceId,
                expectedApplyMode = 1,
            )
            legacyClient.close()
            retained.persist(dbName, legacyFixture.exportBytes())
            legacyFixture.close()
            connection = null

            val retainedBefore = assertNotNull(retained.bytes).copyOf()
            val retainedHash = sha256Hex(retainedBefore)
            val loadCallsAfterLegacy = retained.loadCalls
            val persistCallsAfterLegacy = retained.persistCalls
            val workerConfig = SqliteConnectionConfig(persistence = retained)
            var worker = oversqliteTestConnectionProvider()
                .openConnection(dbName, debug = true, config = workerConfig)
            connection = worker
            assertEquals(
                preImportState,
                assertInterruptedMigrationBoundary(worker, sourceId, expectedApplyMode = 1),
                "the interrupted apply checkpoint must import without changing any control value",
            )
            worker.close()
            connection = null

            worker = oversqliteTestConnectionProvider()
                .openConnection(dbName, debug = true, config = workerConfig)
            connection = worker
            assertEquals(
                preImportState,
                assertInterruptedMigrationBoundary(worker, sourceId, expectedApplyMode = 1),
                "the interrupted apply checkpoint must survive close/reopen before recovery",
            )

            val resumedClient = newClient(worker, server.newHttpClient())
            resumedClient.open().getOrThrow()
            var cancelRecoveredApply = true
            resumedClient.setSnapshotApplyFaultInjectorForTest(
                dev.goquick.sqlitenow.oversqlite.SnapshotApplyFaultInjector(
                    afterAppliedRow = {
                        if (cancelRecoveredApply) {
                            cancelRecoveredApply = false
                            throw kotlinx.coroutines.CancellationException(
                                "cancel recovered migrated apply",
                            )
                        }
                    },
                ),
            )
            kotlin.test.assertFailsWith<kotlinx.coroutines.CancellationException> {
                resumedClient.attach("phase6-user")
            }
            assertEquals(1L, scalarLong(worker, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            assertEquals(0L, scalarLong(worker, "SELECT COUNT(*) FROM users"))
            assertEquals(0L, scalarLong(worker, "SELECT apply_mode FROM _sync_apply_state"))
            assertEquals(
                preImportState.copy(applyState = listOf("[1,0]")),
                assertInterruptedMigrationBoundary(worker, sourceId, expectedApplyMode = 0),
                "cancellation may reset apply ownership but must preserve the canonical staged checkpoint",
            )
            resumedClient.setSnapshotApplyFaultInjectorForTest(null)
            resumedClient.attach("phase6-user").getOrThrow()
            assertEquals("Remote Ada", scalarText(worker, "SELECT name FROM users WHERE id = 'remote-1'"))
            assertEquals(0L, scalarLong(worker, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            assertEquals("none", scalarText(worker, "SELECT kind FROM _sync_operation_state"))
            assertEquals(0L, scalarLong(worker, "SELECT apply_mode FROM _sync_apply_state"))
            assertEquals(
                "attached|phase6-user|main|7",
                scalarText(
                    worker,
                    "SELECT binding_state || '|' || attached_user_id || '|' || schema_name || '|' || " +
                        "last_bundle_seq_seen FROM _sync_attachment_state",
                ),
            )
            assertEquals(sourceId, scalarText(worker, "SELECT current_source_id FROM _sync_attachment_state"))
            val completedState = assertCompletedMigrationBoundary(
                db = worker,
                sourceId = sourceId,
                expectedUserVersion = 6,
                expectedLastBundleSeq = 7,
                expectedNextSourceBundleId = 1,
                expectedUserId = "remote-1",
                expectedUserName = "Remote Ada",
                expectedRowVersion = 7,
            )
            resumedClient.close()
            worker.close()
            connection = null

            worker = oversqliteTestConnectionProvider()
                .openConnection(dbName, debug = true, config = workerConfig)
            connection = worker
            assertEquals(
                completedState,
                assertCompletedMigrationBoundary(
                    db = worker,
                    sourceId = sourceId,
                    expectedUserVersion = 6,
                    expectedLastBundleSeq = 7,
                    expectedNextSourceBundleId = 1,
                    expectedUserId = "remote-1",
                    expectedUserName = "Remote Ada",
                    expectedRowVersion = 7,
                ),
                "completed interrupted recovery must preserve every control value across reopen",
            )
            worker.close()
            connection = null
            assertTrue(retainedBefore.contentEquals(assertNotNull(retained.bytes)))
            assertEquals(retainedHash, sha256Hex(assertNotNull(retained.bytes)))
            assertEquals(loadCallsAfterLegacy + 1, retained.loadCalls)
            assertEquals(persistCallsAfterLegacy, retained.persistCalls)
            assertEquals(0, retained.clearCalls)
            println(
                "phase6_migrated_interrupted_apply source=custom snapshot=phase6-interrupted " +
                    "bundleSeq=7 rowOrdinal=1 bytes=${retainedBefore.size} sha256=$retainedHash " +
                    "sourceId=$sourceId retained=true",
            )
        } finally {
            connection?.close()
            ownedStorage.cleanup()
        }
    }

    private suspend fun assertPendingMigrationBoundary(
        db: SafeSQLiteConnection,
        sourceId: String,
        expectedUserVersion: Int,
    ): Phase6ControlStateSnapshot {
        val state = captureMigrationBoundary(db, expectedUserVersion)
        assertEquals("Ada", scalarText(db, "SELECT name FROM users WHERE id = 'local-1'"))
        assertEquals(listOf("[1,0]"), state.applyState)
        assertEquals(emptyList(), state.rowState)
        assertEquals(1, state.dirtyRows.size)
        assertEquals(emptyList(), state.snapshotStage)
        assertEquals(1, state.sourceState.size)
        assertEquals(
            listOf("""[1,"$sourceId","anonymous","","",0,0,""]"""),
            state.attachmentState,
        )
        assertEquals(
            listOf("""[1,"none","","",0,0,0,0,"",""]"""),
            state.operationState,
        )
        assertEquals(
            listOf("""[1,"jcs_uniform_numeric_strings_v1","none","",0,"","",0,"",0]"""),
            state.outboxBundle,
        )
        assertEquals(emptyList(), state.outboxRows)
        assertEquals(listOf("""["main","posts"]""", """["main","users"]"""), state.managedTables)

        assertEquals(
            """main|users|{"id":"local-1"}|INSERT|0|{"id":"local-1","name":"Ada"}|1""",
            scalarText(
                db,
                "SELECT schema_name || '|' || table_name || '|' || key_json || '|' || op || '|' || " +
                    "base_row_version || '|' || payload || '|' || dirty_ordinal FROM _sync_dirty_rows",
            ),
        )
        assertEquals(
            "$sourceId|1|",
            scalarText(
                db,
                "SELECT source_id || '|' || next_source_bundle_id || '|' || replaced_by_source_id " +
                    "FROM _sync_source_state",
            ),
        )
        assertPhase6Timestamp(
            scalarText(db, "SELECT updated_at FROM _sync_dirty_rows"),
            "_sync_dirty_rows.updated_at",
        )
        assertPhase6Timestamp(
            scalarText(db, "SELECT created_at FROM _sync_source_state"),
            "_sync_source_state.created_at",
        )
        return state
    }

    private suspend fun assertPreparedOutboxBoundary(
        db: SafeSQLiteConnection,
        sourceId: String,
    ): Phase6ControlStateSnapshot {
        val state = captureMigrationBoundary(db, expectedUserVersion = 6)
        assertEquals("Ada", scalarText(db, "SELECT name FROM users WHERE id = 'local-1'"))
        assertEquals(listOf("[1,0]"), state.applyState)
        assertEquals(emptyList(), state.rowState)
        assertEquals(emptyList(), state.dirtyRows)
        assertEquals(emptyList(), state.snapshotStage)
        assertEquals(1, state.sourceState.size)
        assertEquals(
            listOf(
                """[1,"$sourceId","attached","phase6-user","main",0,0,""" +
                    """"$FROZEN_PHASE6_INITIALIZATION_ID"]""",
            ),
            state.attachmentState,
        )
        assertEquals(
            listOf("""[1,"none","","",0,0,0,0,"",""]"""),
            state.operationState,
        )
        assertEquals(1, state.outboxBundle.size)
        assertEquals(1, state.outboxRows.size)
        assertEquals(listOf("""["main","posts"]""", """["main","users"]"""), state.managedTables)
        assertEquals(
            "$sourceId|1|",
            scalarText(
                db,
                "SELECT source_id || '|' || next_source_bundle_id || '|' || replaced_by_source_id " +
                    "FROM _sync_source_state",
            ),
        )
        assertPhase6Timestamp(
            scalarText(db, "SELECT created_at FROM _sync_source_state"),
            "_sync_source_state.created_at",
        )
        assertPreparedOutbox(db, sourceId)
        val requestHash = scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle")
        assertEquals(
            "jcs_uniform_numeric_strings_v1|prepared|$sourceId|1|" +
                "$FROZEN_PHASE6_INITIALIZATION_ID|$requestHash|1||0",
            scalarText(
                db,
                "SELECT canonical_json_contract || '|' || state || '|' || source_id || '|' || " +
                    "source_bundle_id || '|' || initialization_id || '|' || canonical_request_hash || " +
                    "'|' || row_count || '|' || remote_bundle_hash || '|' || remote_bundle_seq " +
                    "FROM _sync_outbox_bundle WHERE singleton_key = 1",
            ),
        )
        return state
    }

    private suspend fun assertInterruptedMigrationBoundary(
        db: SafeSQLiteConnection,
        sourceId: String,
        expectedApplyMode: Int,
    ): Phase6ControlStateSnapshot {
        val state = captureMigrationBoundary(db, expectedUserVersion = 6)
        assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))
        assertEquals(listOf("[1,$expectedApplyMode]"), state.applyState)
        assertEquals(emptyList(), state.rowState)
        assertEquals(emptyList(), state.dirtyRows)
        assertEquals(
            listOf(
                """["phase6-interrupted",1,"main","users","{\"id\":\"remote-1\"}",7,""" +
                    """"{\"id\":\"remote-1\",\"name\":\"Remote Ada\"}"]""",
            ),
            state.snapshotStage,
        )
        assertEquals(1, state.sourceState.size)
        assertEquals(
            listOf("""[1,"$sourceId","anonymous","","",0,0,""]"""),
            state.attachmentState,
        )
        assertEquals(
            listOf(
                """[1,"remote_replace","phase6-user","phase6-interrupted",7,1,39,1,"",""]""",
            ),
            state.operationState,
        )
        assertEquals(
            listOf("""[1,"jcs_uniform_numeric_strings_v1","none","",0,"","",0,"",0]"""),
            state.outboxBundle,
        )
        assertEquals(emptyList(), state.outboxRows)
        assertEquals(listOf("""["main","posts"]""", """["main","users"]"""), state.managedTables)
        assertEquals(
            "$sourceId|1|",
            scalarText(
                db,
                "SELECT source_id || '|' || next_source_bundle_id || '|' || replaced_by_source_id " +
                    "FROM _sync_source_state",
            ),
        )
        assertPhase6Timestamp(
            scalarText(db, "SELECT created_at FROM _sync_source_state"),
            "_sync_source_state.created_at",
        )
        return state
    }

    private suspend fun assertCompletedMigrationBoundary(
        db: SafeSQLiteConnection,
        sourceId: String,
        expectedUserVersion: Int,
        expectedLastBundleSeq: Int,
        expectedNextSourceBundleId: Int,
        expectedUserId: String,
        expectedUserName: String,
        expectedRowVersion: Int?,
    ): Phase6ControlStateSnapshot {
        val state = captureMigrationBoundary(db, expectedUserVersion)
        assertEquals(
            expectedUserName,
            scalarText(db, "SELECT name FROM users WHERE id = '$expectedUserId'"),
        )
        assertEquals(listOf("[1,0]"), state.applyState)
        assertEquals(if (expectedRowVersion == null) 0 else 1, state.rowState.size)
        assertEquals(emptyList(), state.dirtyRows)
        assertEquals(emptyList(), state.snapshotStage)
        assertEquals(1, state.sourceState.size)
        assertEquals(
            listOf(
                """[1,"$sourceId","attached","phase6-user","main",""" +
                    """$expectedLastBundleSeq,0,""]""",
            ),
            state.attachmentState,
        )
        assertEquals(
            listOf("""[1,"none","","",0,0,0,0,"",""]"""),
            state.operationState,
        )
        assertEquals(
            listOf("""[1,"jcs_uniform_numeric_strings_v1","none","",0,"","",0,"",0]"""),
            state.outboxBundle,
        )
        assertEquals(emptyList(), state.outboxRows)
        assertEquals(listOf("""["main","posts"]""", """["main","users"]"""), state.managedTables)
        assertEquals(
            "$sourceId|$expectedNextSourceBundleId|",
            scalarText(
                db,
                "SELECT source_id || '|' || next_source_bundle_id || '|' || replaced_by_source_id " +
                    "FROM _sync_source_state",
            ),
        )
        if (expectedRowVersion != null) {
            assertEquals(
                """main|users|{"id":"$expectedUserId"}|$expectedRowVersion|0""",
                scalarText(
                    db,
                    "SELECT schema_name || '|' || table_name || '|' || key_json || '|' || " +
                        "row_version || '|' || deleted FROM _sync_row_state",
                ),
            )
        }
        assertPhase6Timestamp(
            scalarText(db, "SELECT created_at FROM _sync_source_state"),
            "_sync_source_state.created_at",
        )
        if (expectedRowVersion != null) {
            assertPhase6Timestamp(
                scalarText(db, "SELECT updated_at FROM _sync_row_state"),
                "_sync_row_state.updated_at",
            )
        }
        return state
    }

    private suspend fun captureMigrationBoundary(
        db: SafeSQLiteConnection,
        expectedUserVersion: Int,
    ): Phase6ControlStateSnapshot {
        assertContinuityBoundary(db, expectedUserVersion)
        return capturePhase6ControlState(db)
    }

    private suspend fun assertContinuityBoundary(
        db: SafeSQLiteConnection,
        expectedUserVersion: Int,
    ) {
        assertEquals("ok", scalarText(db, "PRAGMA integrity_check"))
        db.execSQL("PRAGMA foreign_keys = ON")
        assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM pragma_foreign_key_check"))
        assertEquals(expectedUserVersion.toLong(), scalarLong(db, "PRAGMA user_version"))
        assertEquals(
            "posts|table|users|table",
            scalarText(
                db,
                "SELECT group_concat(name || '|' || type, '|') FROM (" +
                    "SELECT name, type FROM sqlite_schema WHERE name IN ('posts', 'users') ORDER BY name)",
            ),
        )
        assertEquals(10L, scalarLong(
            db,
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name LIKE '_sync_%'",
        ))
        assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_apply_state"))
        assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_attachment_state"))
        assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_operation_state"))
        assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_bundle"))
        assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM _sync_managed_tables"))
        assertPhase6ControlTableSchemasAndIndexes(db)
    }

    private suspend fun assertPreparedOutbox(
        db: SafeSQLiteConnection,
        sourceId: String,
    ) {
        assertEquals(
            "jcs_uniform_numeric_strings_v1|prepared|$sourceId|1|1",
            scalarText(
                db,
                "SELECT canonical_json_contract || '|' || state || '|' || source_id || '|' || " +
                    "source_bundle_id || '|' || row_count FROM _sync_outbox_bundle " +
                    "WHERE singleton_key = 1",
            ),
        )
        val requestHash = scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle")
        assertEquals(64, requestHash.length)
        assertTrue(requestHash.all { it in '0'..'9' || it in 'a'..'f' })
        assertEquals(
            """1|0|main|users|{"id":"local-1"}|{"id":"local-1"}|INSERT|0|""" +
                """{"id":"local-1","name":"Ada"}|{"id":"local-1","name":"Ada"}""",
            scalarText(
                db,
                "SELECT source_bundle_id || '|' || row_ordinal || '|' || schema_name || '|' || " +
                    "table_name || '|' || key_json || '|' || wire_key_json || '|' || op || '|' || " +
                    "base_row_version || '|' || local_payload || '|' || wire_payload " +
                    "FROM _sync_outbox_rows",
            ),
        )
    }
}

private class MigratedSourceScenario(
    val name: String,
    val forceOpfs: Boolean?,
)

private class RecordingSnapshotPersistence : SqlitePersistence {
    var bytes: ByteArray? = null
    var loadCalls: Int = 0
    var persistCalls: Int = 0
    var clearCalls: Int = 0

    override suspend fun load(dbName: String): ByteArray? {
        loadCalls++
        return bytes?.copyOf()
    }

    override suspend fun persist(dbName: String, bytes: ByteArray) {
        persistCalls++
        this.bytes = bytes.copyOf()
    }

    override suspend fun clear(dbName: String) {
        clearCalls++
        bytes = null
    }
}

internal suspend fun assertPhase6ControlTableSchemasAndIndexes(db: SafeSQLiteConnection) {
    val expectedColumns = mapOf(
        "_sync_apply_state" to listOf("singleton_key|INTEGER|1|1", "apply_mode|INTEGER|1|0"),
        "_sync_row_state" to listOf(
            "schema_name|TEXT|1|1", "table_name|TEXT|1|2", "key_json|TEXT|1|3",
            "row_version|INTEGER|1|0", "deleted|INTEGER|1|0", "updated_at|TEXT|1|0",
        ),
        "_sync_dirty_rows" to listOf(
            "schema_name|TEXT|1|1", "table_name|TEXT|1|2", "key_json|TEXT|1|3",
            "op|TEXT|1|0", "base_row_version|INTEGER|1|0", "payload|TEXT|0|0",
            "dirty_ordinal|INTEGER|1|0", "updated_at|TEXT|1|0",
        ),
        "_sync_snapshot_stage" to listOf(
            "snapshot_id|TEXT|1|1", "row_ordinal|INTEGER|1|2", "schema_name|TEXT|1|0",
            "table_name|TEXT|1|0", "key_json|TEXT|1|0", "row_version|INTEGER|1|0",
            "payload|TEXT|1|0",
        ),
        "_sync_source_state" to listOf(
            "source_id|TEXT|1|1", "next_source_bundle_id|INTEGER|1|0",
            "replaced_by_source_id|TEXT|1|0", "created_at|TEXT|1|0",
        ),
        "_sync_attachment_state" to listOf(
            "singleton_key|INTEGER|1|1", "current_source_id|TEXT|1|0",
            "binding_state|TEXT|1|0", "attached_user_id|TEXT|1|0",
            "schema_name|TEXT|1|0", "last_bundle_seq_seen|INTEGER|1|0",
            "rebuild_required|INTEGER|1|0", "pending_initialization_id|TEXT|1|0",
        ),
        "_sync_operation_state" to listOf(
            "singleton_key|INTEGER|1|1", "kind|TEXT|1|0", "target_user_id|TEXT|1|0",
            "staged_snapshot_id|TEXT|1|0", "snapshot_bundle_seq|INTEGER|1|0",
            "snapshot_row_count|INTEGER|1|0", "snapshot_byte_count|INTEGER|1|0",
            "snapshot_stage_complete|INTEGER|1|0", "reason|TEXT|1|0",
            "replacement_source_id|TEXT|1|0",
        ),
        "_sync_outbox_bundle" to listOf(
            "singleton_key|INTEGER|1|1", "canonical_json_contract|TEXT|1|0",
            "state|TEXT|1|0", "source_id|TEXT|1|0", "source_bundle_id|INTEGER|1|0",
            "initialization_id|TEXT|1|0", "canonical_request_hash|TEXT|1|0",
            "row_count|INTEGER|1|0", "remote_bundle_hash|TEXT|1|0",
            "remote_bundle_seq|INTEGER|1|0",
        ),
        "_sync_outbox_rows" to listOf(
            "source_bundle_id|INTEGER|1|1", "row_ordinal|INTEGER|1|2",
            "schema_name|TEXT|1|0", "table_name|TEXT|1|0", "key_json|TEXT|1|0",
            "wire_key_json|TEXT|1|0", "op|TEXT|1|0", "base_row_version|INTEGER|1|0",
            "local_payload|TEXT|0|0", "wire_payload|TEXT|0|0",
        ),
        "_sync_managed_tables" to listOf(
            "schema_name|TEXT|1|1", "table_name|TEXT|1|2",
        ),
    )
    expectedColumns.forEach { (table, expected) ->
        val actual = mutableListOf<String>()
        db.prepare("PRAGMA table_info('$table')").use { statement ->
            while (statement.step()) {
                actual +=
                    "${statement.getText(1)}|${statement.getText(2)}|" +
                        "${statement.getLong(3)}|${statement.getLong(5)}"
            }
        }
        assertEquals(expected, actual, table)
    }

    val expectedIndexes = mapOf(
        "_sync_apply_state" to emptyList(),
        "_sync_row_state" to listOf("sqlite_autoindex__sync_row_state_1|1|pk|0"),
        "_sync_dirty_rows" to listOf(
            "idx_sync_dirty_rows_dirty_ordinal|0|c|0",
            "sqlite_autoindex__sync_dirty_rows_1|1|pk|0",
        ),
        "_sync_snapshot_stage" to listOf(
            "sqlite_autoindex__sync_snapshot_stage_1|1|pk|0",
            "sqlite_autoindex__sync_snapshot_stage_2|1|u|0",
        ),
        "_sync_source_state" to listOf("sqlite_autoindex__sync_source_state_1|1|pk|0"),
        "_sync_attachment_state" to emptyList(),
        "_sync_operation_state" to emptyList(),
        "_sync_outbox_bundle" to emptyList(),
        "_sync_outbox_rows" to listOf("sqlite_autoindex__sync_outbox_rows_1|1|pk|0"),
        "_sync_managed_tables" to listOf("sqlite_autoindex__sync_managed_tables_1|1|pk|0"),
    )
    expectedIndexes.forEach { (table, expected) ->
        val actual = mutableListOf<String>()
        db.prepare("PRAGMA index_list('$table')").use { statement ->
            while (statement.step()) {
                actual +=
                    "${statement.getText(1)}|${statement.getLong(2)}|" +
                        "${statement.getText(3)}|${statement.getLong(4)}"
            }
        }
        assertEquals(expected.sorted(), actual.sorted(), table)
    }
}

private data class Phase6ControlStateSnapshot(
    val applyState: List<String>,
    val rowState: List<String>,
    val dirtyRows: List<String>,
    val snapshotStage: List<String>,
    val sourceState: List<String>,
    val attachmentState: List<String>,
    val operationState: List<String>,
    val outboxBundle: List<String>,
    val outboxRows: List<String>,
    val managedTables: List<String>,
)

private suspend fun capturePhase6ControlState(
    db: SafeSQLiteConnection,
): Phase6ControlStateSnapshot = Phase6ControlStateSnapshot(
    applyState = readPhase6ControlRows(
        db,
        "SELECT json_array(singleton_key, apply_mode) FROM _sync_apply_state ORDER BY singleton_key",
    ),
    rowState = readPhase6ControlRows(
        db,
        "SELECT json_array(schema_name, table_name, key_json, row_version, deleted, updated_at) " +
            "FROM _sync_row_state ORDER BY schema_name, table_name, key_json",
    ),
    dirtyRows = readPhase6ControlRows(
        db,
        "SELECT json_array(schema_name, table_name, key_json, op, base_row_version, payload, " +
            "dirty_ordinal, updated_at) FROM _sync_dirty_rows " +
            "ORDER BY dirty_ordinal, schema_name, table_name, key_json",
    ),
    snapshotStage = readPhase6ControlRows(
        db,
        "SELECT json_array(snapshot_id, row_ordinal, schema_name, table_name, key_json, " +
            "row_version, payload) FROM _sync_snapshot_stage ORDER BY snapshot_id, row_ordinal",
    ),
    sourceState = readPhase6ControlRows(
        db,
        "SELECT json_array(source_id, next_source_bundle_id, replaced_by_source_id, created_at) " +
            "FROM _sync_source_state ORDER BY source_id",
    ),
    attachmentState = readPhase6ControlRows(
        db,
        "SELECT json_array(singleton_key, current_source_id, binding_state, attached_user_id, " +
            "schema_name, last_bundle_seq_seen, rebuild_required, pending_initialization_id) " +
            "FROM _sync_attachment_state ORDER BY singleton_key",
    ),
    operationState = readPhase6ControlRows(
        db,
        "SELECT json_array(singleton_key, kind, target_user_id, staged_snapshot_id, " +
            "snapshot_bundle_seq, snapshot_row_count, snapshot_byte_count, snapshot_stage_complete, " +
            "reason, replacement_source_id) FROM _sync_operation_state ORDER BY singleton_key",
    ),
    outboxBundle = readPhase6ControlRows(
        db,
        "SELECT json_array(singleton_key, canonical_json_contract, state, source_id, " +
            "source_bundle_id, initialization_id, canonical_request_hash, row_count, " +
            "remote_bundle_hash, remote_bundle_seq) FROM _sync_outbox_bundle ORDER BY singleton_key",
    ),
    outboxRows = readPhase6ControlRows(
        db,
        "SELECT json_array(source_bundle_id, row_ordinal, schema_name, table_name, key_json, " +
            "wire_key_json, op, base_row_version, local_payload, wire_payload) " +
            "FROM _sync_outbox_rows ORDER BY source_bundle_id, row_ordinal",
    ),
    managedTables = readPhase6ControlRows(
        db,
        "SELECT json_array(schema_name, table_name) FROM _sync_managed_tables " +
            "ORDER BY schema_name, table_name",
    ),
)

private suspend fun readPhase6ControlRows(
    db: SafeSQLiteConnection,
    sql: String,
): List<String> {
    val rows = mutableListOf<String>()
    db.prepare(sql).use { statement ->
        while (statement.step()) {
            rows += statement.getText(0)
        }
    }
    return rows
}

private fun assertPhase6Timestamp(
    value: String,
    label: String,
) {
    assertTrue(
        Regex("""\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z""").matches(value),
        "$label must be a canonical UTC millisecond timestamp, but was $value",
    )
}
