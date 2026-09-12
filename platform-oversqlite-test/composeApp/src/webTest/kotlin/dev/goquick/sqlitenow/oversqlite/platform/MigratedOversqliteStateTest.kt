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
import dev.goquick.sqlitenow.oversqlite.OwnedWorkerStorage
import dev.goquick.sqlitenow.oversqlite.OversqliteConfig
import dev.goquick.sqlitenow.oversqlite.OversqliteManagedTableStore
import dev.goquick.sqlitenow.oversqlite.OversqliteSourceStateStore
import dev.goquick.sqlitenow.oversqlite.SyncRuntimeInitializer
import dev.goquick.sqlitenow.oversqlite.SyncTable
import dev.goquick.sqlitenow.oversqlite.TableInfoCache
import dev.goquick.sqlitenow.oversqlite.oversqliteTestConnectionProvider
import dev.goquick.sqlitenow.oversqlite.workerWorkerStorageEvidence
import dev.goquick.sqlitenow.oversqlite.platformsupport.sha256Hex
import dev.goquick.sqlitenow.oversqlite.webRuntimeKind
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.coroutines.async
import kotlinx.serialization.json.*
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.test.runTest

internal class MigratedOversqliteStateTest : PlatformCrossTargetTestSupport() {
    private companion object {
        const val FROZEN_WORKER_REQUEST_HASH =
            "bef80bfb27db809e09cb54709c307e6a51cb967b3dc4ffa27c58204985f235eb"
        const val FROZEN_WORKER_INITIALIZATION_ID =
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
                val ownedStorage = OwnedWorkerStorage()
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
                var coordinatedDatabase: PlatformTestDatabase? = null
                try {
                    val legacyFixture = createAuthenticLegacySqlJsFixture(dbName)
                    val legacy = legacyFixture.connection
                    connection = legacy
                    legacy.execSQL("PRAGMA foreign_keys = ON")
                    createUsersAndPostsTables(legacy)
                    insertUser(legacy, "local-1", "Ada")
                    legacy.execSQL("PRAGMA user_version = 6")
                    val fixedSourceId = "worker-source-fixed"
                    prepareLegacyOversqliteState(legacy, fixedSourceId)
                    val preImportState = assertPendingMigrationBoundary(
                        db = legacy,
                        sourceId = fixedSourceId,
                        expectedUserVersion = 6,
                    )
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
                        val cancellationStage =
                            if (custom == null) "after-integrity" else "after-health"
                        val cancellationHold =
                            cancellingDriver.holdMigrationCancellationForTest(dbName, cancellationStage)
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

                    val storage = workerWorkerStorageEvidence(dbName).split('\t', limit = 4)
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
                    assertEquals("worker-source-fixed", sourceId)
                    worker.close()
                    connection = null
                    coordinatedDatabase = importedDatabase(dbName, workerConfig)
                    val workerHttp = server.newHttpClient()
                    val client = newClient(coordinatedDatabase!!, workerHttp)
                    worker = coordinatedDatabase!!.connection()
                    connection = worker
                    val completedMetrics = try {
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
                                        "attached_user_id = 'worker-user', schema_name = 'main' " +
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
                        client.attach("worker-user").getOrThrow()
                        assertEquals(sourceId, scalarText(worker, "SELECT current_source_id " +
                            "FROM _sync_attachment_state"))
                        server.cancelNextRequestPathPrefix = "/sync/push"
                        kotlin.test.assertFailsWith<kotlinx.coroutines.CancellationException> {
                            client.pushPending()
                        }
                        assertPreparedOutbox(worker, sourceId)
                        val requestHash =
                            scalarText(worker, "SELECT canonical_request_hash FROM _sync_outbox_bundle")
                        assertEquals(FROZEN_WORKER_REQUEST_HASH, requestHash)
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
                        (worker.ref as SqliteWorkerSQLiteConnection).metricsForTest()
                    } finally {
                        worker.beforeTransactionCommitForTest = null
                        workerHttp.close()
                    }
                    assertEquals(0L, completedMetrics.snapshotExports)
                    assertEquals(0, completedMetrics.liveStatements)
                    coordinatedDatabase!!.close()
                    coordinatedDatabase = null
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
                        "default_persistence_default_migrated_matrix runtime=${webRuntimeKind()} source=${scenario.name} " +
                            "bytes=${retainedBefore.size} sha256=$retainedHash " +
                            "requestHash=$FROZEN_WORKER_REQUEST_HASH " +
                            "target=${storage[0]} snapshotExports=0",
                    )
                } finally {
                    coordinatedDatabase?.close()
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

        val ownedStorage = OwnedWorkerStorage()
        val dbName = ownedStorage.newDatabaseName()
        val retained = RecordingSnapshotPersistence()
        val server = MockSyncServer()
        var connection: SafeSQLiteConnection? = null
        var coordinatedDatabase: PlatformTestDatabase? = null
        try {
            val legacyFixture = createAuthenticLegacySqlJsFixture(dbName)
            val legacy = legacyFixture.connection
            connection = legacy
            legacy.execSQL("PRAGMA foreign_keys = ON")
            createUsersAndPostsTables(legacy)
            insertUser(legacy, "local-1", "Ada")
            legacy.execSQL("PRAGMA user_version = 6")
            val sourceId = "worker-source-fixed"
            prepareLegacyOversqliteState(legacy, sourceId)
            val preImportState = assertPendingMigrationBoundary(legacy, sourceId, expectedUserVersion = 6)
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
            worker.close()
            connection = null
            coordinatedDatabase = importedDatabase(dbName, workerConfig)
            val attachedHttp = server.newHttpClient()
            val attachedClient = newClient(coordinatedDatabase!!, attachedHttp)
            worker = coordinatedDatabase!!.connection()
            connection = worker
            attachedClient.attach("worker-user").getOrThrow()
            assertEquals(
                "attached|worker-user|local",
                scalarText(
                    worker,
                    "SELECT binding_state || '|' || attached_user_id || '|' || schema_name " +
                        "FROM _sync_attachment_state WHERE singleton_key = 1",
                ),
            )
            assertEquals(sourceId, scalarText(worker, "SELECT current_source_id FROM _sync_attachment_state"))

            server.rejectPush = true
            assertTrue(attachedClient.pushPending().isFailure)
            val preparedState = assertPreparedOutboxBoundary(worker, sourceId)
            attachedHttp.close()
            coordinatedDatabase!!.close()
            coordinatedDatabase = null
            connection = null

            coordinatedDatabase = importedDatabase(dbName, workerConfig)
            val resumedHttp = server.newHttpClient()
            val resumedClient = newClient(coordinatedDatabase!!, resumedHttp)
            worker = coordinatedDatabase!!.connection()
            connection = worker
            assertEquals(
                preparedState,
                assertPreparedOutboxBoundary(worker, sourceId),
                "prepared outbox rows and all companion control state must survive close/reopen",
            )
            server.rejectPush = false
            resumedClient.attach("worker-user").getOrThrow()
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
            resumedHttp.close()
            coordinatedDatabase!!.close()
            coordinatedDatabase = null
            connection = null

            assertTrue(retainedBefore.contentEquals(assertNotNull(retained.bytes)))
            assertEquals(retainedHash, sha256Hex(assertNotNull(retained.bytes)))
            assertEquals(loadCallsAfterLegacy + 1, retained.loadCalls)
            assertEquals(persistCallsAfterLegacy, retained.persistCalls)
            assertEquals(0, retained.clearCalls)
            println(
                "worker_migrated_oversqlite source=custom bytes=${retainedBefore.size} " +
                    "sha256=$retainedHash loads=${retained.loadCalls} " +
                    "workerSnapshotExports=0 retained=true",
            )
        } finally {
            coordinatedDatabase?.close()
            connection?.close()
            ownedStorage.cleanup()
        }
    }

    @Test
    fun retainedInterruptedApplyCompletesFromCanonicalStageWithoutRebuildOrSourceRotation() = runTest {
        if (webRuntimeKind() !in setOf("js-browser", "wasm-browser")) return@runTest

        val ownedStorage = OwnedWorkerStorage()
        val dbName = ownedStorage.newDatabaseName()
        val retained = RecordingSnapshotPersistence()
        val server = MockSyncServer()
        var connection: SafeSQLiteConnection? = null
        var coordinatedDatabase: PlatformTestDatabase? = null
        try {
            val legacyFixture = createAuthenticLegacySqlJsFixture(dbName)
            val legacy = legacyFixture.connection
            connection = legacy
            createUsersAndPostsTables(legacy)
            legacy.execSQL("PRAGMA user_version = 6")
            val sourceId = "worker-source-fixed"
            prepareLegacyOversqliteState(legacy, sourceId)
            retained.persist(dbName, legacyFixture.exportBytes())
            legacyFixture.close()
            connection = null

            coordinatedDatabase = importedDatabase(dbName, SqliteConnectionConfig(persistence = retained))
            val legacyHttp = server.newHttpClient()
            val legacyClient = newClient(coordinatedDatabase!!, legacyHttp)
            val admitted = coordinatedDatabase!!.connection()
            connection = admitted
            val preImportState = try {
                legacyClient.attach("worker-user").getOrThrow()
                // Seed a post-admission interrupted initial replacement before binding publication.
                admitted.execSQL("UPDATE _sync_attachment_state SET binding_state='anonymous', attached_user_id='', schema_name='' WHERE singleton_key=1")
                admitted.execSQL(
                    """
                    INSERT INTO _sync_snapshot_stage(
                      snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
                    ) VALUES (
                      'worker-interrupted', 1, 'main', 'users', '{"id":"remote-1"}', 7,
                      '{"id":"remote-1","name":"Remote Ada"}'
                    )
                    """.trimIndent(),
                )
                admitted.execSQL(
                    """
                    UPDATE _sync_operation_state
                    SET kind = 'remote_replace',
                        target_user_id = 'worker-user',
                        staged_snapshot_id = 'worker-interrupted',
                        snapshot_bundle_seq = 7,
                        snapshot_row_count = 1,
                        snapshot_byte_count = 39,
                        snapshot_stage_complete = 1
                    WHERE singleton_key = 1
                    """.trimIndent(),
                )
                admitted.execSQL("UPDATE _sync_apply_state SET apply_mode = 1 WHERE singleton_key = 1")
                assertInterruptedMigrationBoundary(
                    db = admitted,
                    sourceId = sourceId,
                    expectedApplyMode = 1,
                )
            } finally {
                legacyHttp.close()
            }
            coordinatedDatabase!!.close()
            coordinatedDatabase = null
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

            worker.close()
            connection = null
            coordinatedDatabase = importedDatabase(dbName, workerConfig)
            val resumedHttp = server.newHttpClient()
            val resumedClient = newClient(coordinatedDatabase!!, resumedHttp)
            worker = coordinatedDatabase!!.connection()
            connection = worker
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
                resumedClient.attach("worker-user")
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
            resumedClient.attach("worker-user").getOrThrow()
            assertEquals("Remote Ada", scalarText(worker, "SELECT name FROM users WHERE id = 'remote-1'"))
            assertEquals(0L, scalarLong(worker, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            assertEquals("none", scalarText(worker, "SELECT kind FROM _sync_operation_state"))
            assertEquals(0L, scalarLong(worker, "SELECT apply_mode FROM _sync_apply_state"))
            assertEquals(
                "attached|worker-user|local|7",
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
            resumedHttp.close()
            coordinatedDatabase!!.close()
            coordinatedDatabase = null
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
            assertEquals(
                loadCallsAfterLegacy,
                retained.loadCalls,
                "the healthy direct target must remain authoritative after interrupted recovery",
            )
            assertEquals(persistCallsAfterLegacy, retained.persistCalls)
            assertEquals(0, retained.clearCalls)
            println(
                "worker_migrated_interrupted_apply source=custom snapshot=worker-interrupted " +
                    "bundleSeq=7 rowOrdinal=1 bytes=${retainedBefore.size} sha256=$retainedHash " +
                    "sourceId=$sourceId retained=true",
            )
        } finally {
            coordinatedDatabase?.close()
            connection?.close()
            ownedStorage.cleanup()
        }
    }

    private fun importedDatabase(
        dbName: String,
        connectionConfig: SqliteConnectionConfig,
    ): PlatformTestDatabase = PlatformTestDatabase(
        dbName = dbName,
        connectionProvider = oversqliteTestConnectionProvider(),
        schemaStatements = mutableListOf(),
        connectionConfig = connectionConfig,
        targetVersion = 6,
    )

    private suspend fun assertPendingMigrationBoundary(
        db: SafeSQLiteConnection,
        sourceId: String,
        expectedUserVersion: Int,
    ): StatementReuseontrolStateSnapshot {
        val state = captureMigrationBoundary(db, expectedUserVersion)
        assertEquals("Ada", scalarText(db, "SELECT name FROM users WHERE id = 'local-1'"))
        assertEquals(listOf("[1,0]"), state.applyState)
        assertNeverPublishedMigrationBaseline(state, attached = false)
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
            listOf("""[1,"jcs_uniform_numeric_strings_v1","none","",0,"","",0]"""),
            state.outboxBundle,
        )
        assertEquals(emptyList(), state.outboxRows)
        assertEquals(listOf("""["local","posts"]""", """["local","users"]"""), state.managedTables)

        assertEquals(
            """local|users|{"id":"local-1"}|INSERT|0|{"id":{"storage":"text","value":"local-1"},"name":{"storage":"text","value":"Ada"}}|1""",
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
        assertWorkerTimestamp(
            scalarText(db, "SELECT updated_at FROM _sync_dirty_rows"),
            "_sync_dirty_rows.updated_at",
        )
        assertWorkerTimestamp(
            scalarText(db, "SELECT created_at FROM _sync_source_state"),
            "_sync_source_state.created_at",
        )
        return state
    }

    private suspend fun assertPreparedOutboxBoundary(
        db: SafeSQLiteConnection,
        sourceId: String,
    ): StatementReuseontrolStateSnapshot {
        val state = captureMigrationBoundary(db, expectedUserVersion = 6)
        assertEquals("Ada", scalarText(db, "SELECT name FROM users WHERE id = 'local-1'"))
        assertEquals(listOf("[1,0]"), state.applyState)
        assertNeverPublishedMigrationBaseline(state, attached = true)
        assertEquals(emptyList(), state.dirtyRows)
        assertEquals(emptyList(), state.snapshotStage)
        assertEquals(1, state.sourceState.size)
        assertEquals(
            listOf(
                """[1,"$sourceId","attached","worker-user","local",0,0,""" +
                    """"$FROZEN_WORKER_INITIALIZATION_ID"]""",
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
        assertWorkerTimestamp(
            scalarText(db, "SELECT created_at FROM _sync_source_state"),
            "_sync_source_state.created_at",
        )
        assertPreparedOutbox(db, sourceId)
        val requestHash = scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle")
        assertEquals(
            "jcs_uniform_numeric_strings_v1|prepared|$sourceId|1|" +
                "$FROZEN_WORKER_INITIALIZATION_ID|$requestHash|1",
            scalarText(
                db,
                "SELECT canonical_json_contract || '|' || state || '|' || source_id || '|' || " +
                    "source_bundle_id || '|' || initialization_id || '|' || canonical_request_hash || " +
                    "'|' || row_count " +
                    "FROM _sync_outbox_bundle WHERE singleton_key = 1",
            ),
        )
        return state
    }

    private fun assertNeverPublishedMigrationBaseline(
        state: StatementReuseontrolStateSnapshot,
        attached: Boolean,
    ) {
        // Initial capture precedes source binding. Admission remaps the schema and binds
        // the generation; attachment binds the account without rewriting capture origin.
        val row = testJson.parseToJsonElement(state.rowState.single()).jsonArray
        val schema = if (attached) "main" else "local"
        assertEquals(10, row.size)
        assertEquals(
            testJson.parseToJsonElement("""["$schema","users","{\"id\":\"local-1\"}",0,"unpublished",null]"""),
            JsonArray(row.take(6)),
        )
        val expectedProvenance = buildJsonObject {
            put("database_id", "platform-sync")
            put("service_id", "platform-mock")
            put("account_scope", buildJsonObject {
                put("binding", if (attached) "attached" else "anonymous")
                put("account_id", if (attached) "worker-user" else "")
            })
            put("generation_id", 1)
            put("generation_fingerprint", if (attached) {
                testGenerationResponse(testRegisteredTableSpecs("users", "posts"))
                    .getValue("schema_generation").jsonObject.getValue("fingerprint")
                    .jsonPrimitive.content
            } else "")
            put("admission", if (attached) "admitted" else "unadmitted")
            put("local_version", 6)
            // Independently authored from createUsersAndPostsTables and the retained
            // physical-shape contract: sorted tables/columns, exact SQLite FK defaults.
            put("physical_shape", """[["posts",["id"],false,false,[["id","TEXT",true,1,0],["title","TEXT",true,0,0],["user_id","TEXT",true,0,0]],[["user_id","id","users","NO ACTION","NO ACTION","NONE",0]]],["users",["id"],false,false,[["id","TEXT",true,1,0],["name","TEXT",true,0,0]],[]]]""")
            put("origin_source_id", "")
            put("origin", "never_published")
            put("bundle_sequence", JsonNull)
            put("authoritative_version", 0)
        }
        assertEquals(expectedProvenance, testJson.parseToJsonElement(row[6].jsonPrimitive.content))
        assertEquals(JsonPrimitive(1), row[7])
        // Insert, then admission's schema/provenance updates, then account binding.
        assertEquals(JsonPrimitive(if (attached) 4 else 1), row[8])
        assertWorkerTimestamp(row[9].jsonPrimitive.content, "_sync_row_state.updated_at")
    }

    private suspend fun assertInterruptedMigrationBoundary(
        db: SafeSQLiteConnection,
        sourceId: String,
        expectedApplyMode: Int,
    ): StatementReuseontrolStateSnapshot {
        val state = captureMigrationBoundary(db, expectedUserVersion = 6)
        assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))
        assertEquals(listOf("[1,$expectedApplyMode]"), state.applyState)
        assertEquals(emptyList(), state.rowState)
        assertEquals(emptyList(), state.dirtyRows)
        assertEquals(
            listOf(
                """["worker-interrupted",1,"main","users","{\"id\":\"remote-1\"}",7,""" +
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
                """[1,"remote_replace","worker-user","worker-interrupted",7,1,39,1,"",""]""",
            ),
            state.operationState,
        )
        assertEquals(
            listOf("""[1,"jcs_uniform_numeric_strings_v1","none","",0,"","",0]"""),
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
        assertWorkerTimestamp(
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
    ): StatementReuseontrolStateSnapshot {
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
                """[1,"$sourceId","attached","worker-user","local",""" +
                    """$expectedLastBundleSeq,0,""]""",
            ),
            state.attachmentState,
        )
        assertEquals(
            listOf("""[1,"none","","",0,0,0,0,"",""]"""),
            state.operationState,
        )
        assertEquals(
            listOf("""[1,"jcs_uniform_numeric_strings_v1","none","",0,"","",0]"""),
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
                """main|users|{"id":"$expectedUserId"}|$expectedRowVersion|live""",
                scalarText(
                    db,
                    "SELECT schema_name || '|' || table_name || '|' || key_json || '|' || " +
                        "row_version || '|' || server_kind FROM _sync_row_state",
                ),
            )
        }
        assertWorkerTimestamp(
            scalarText(db, "SELECT created_at FROM _sync_source_state"),
            "_sync_source_state.created_at",
        )
        if (expectedRowVersion != null) {
            assertWorkerTimestamp(
                scalarText(db, "SELECT updated_at FROM _sync_row_state"),
                "_sync_row_state.updated_at",
            )
        }
        return state
    }

    private suspend fun captureMigrationBoundary(
        db: SafeSQLiteConnection,
        expectedUserVersion: Int,
    ): StatementReuseontrolStateSnapshot {
        assertContinuityBoundary(db, expectedUserVersion)
        return captureStatementReuseontrolState(db)
    }

    /**
     * Builds the historical fixture through the same runtime initializer as the production
     * client. The public client now intentionally requires a coordinator-issued owner, while
     * this fixture is created before a worker import and therefore has no lifecycle owner.
     */
    private suspend fun prepareLegacyOversqliteState(
        db: SafeSQLiteConnection,
        sourceId: String,
    ) {
        val config = OversqliteConfig(
            generationId = 1L,
            databaseId = "platform-sync",
            serviceId = "platform-mock",
            syncTables = listOf(
                SyncTable("users", syncKeyColumnName = "id"),
                SyncTable("posts", syncKeyColumnName = "id"),
            ),
        )
        val initializer = SyncRuntimeInitializer(config, TableInfoCache())
        val validated = initializer.prepareLocalRuntime(db, OversqliteManagedTableStore(db))
        initializer.capturePreexistingAnonymousRows(db, validated)
        OversqliteSourceStateStore(db).ensureSource(sourceId)
        db.execSQL(
            "UPDATE _sync_attachment_state SET current_source_id = '$sourceId' " +
                "WHERE singleton_key = 1",
        )
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
        assertEquals(21L, scalarLong(
            db,
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name LIKE '_sync_%'",
        ))
        assertEquals(
            1L,
            scalarLong(
                db,
                "SELECT COUNT(*) FROM _sync_opening_state " +
                    "WHERE singleton_key=1 AND phase='ready' AND outcome='ready' AND writes_available=1",
            ),
        )
        assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_apply_state"))
        assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_conflict_state"))
        assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_attachment_state"))
        assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_operation_state"))
        assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_bundle"))
        assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM _sync_managed_tables"))
        assertStatementReuseontrolTableSchemasAndIndexes(db)
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
                """{"id":{"storage":"text","value":"local-1"},"name":{"storage":"text","value":"Ada"}}|{"id":"local-1","name":"Ada"}""",
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

internal suspend fun assertStatementReuseontrolTableSchemasAndIndexes(db: SafeSQLiteConnection) {
    val expectedColumns = mapOf(
        "_sync_admitted_contracts" to listOf(
            "generation_id|INTEGER|0|1", "fingerprint|TEXT|1|0", "descriptor|TEXT|1|0",
            "descriptor_hash|TEXT|1|0",
        ),
        "_sync_apply_edges" to listOf(
            "ordinal|INTEGER|0|1", "parent_table|TEXT|1|0", "parent_pk|TEXT|1|0", "child_table|TEXT|1|0",
            "child_pk|TEXT|1|0", "bytes|INTEGER|1|0",
        ),
        "_sync_apply_plan" to listOf(
            "singleton_key|INTEGER|0|1", "unit|TEXT|1|0", "guard|TEXT|1|0", "phase|TEXT|1|0",
            "root_cursor|INTEGER|1|0", "visit_cursor|INTEGER|1|0", "table_cursor|INTEGER|1|0",
            "fk_cursor|INTEGER|1|0", "row_cursor|TEXT|0|0", "entries|INTEGER|1|0", "bytes|INTEGER|1|0",
            "edge_entries|INTEGER|1|0", "edge_bytes|INTEGER|1|0", "operation_kind|TEXT|1|0",
            "preserve_schema|TEXT|1|0", "preserve_table|TEXT|1|0", "preserve_key|TEXT|1|0",
            "atomic_work|INTEGER|1|0", "preservation_complete|INTEGER|1|0",
        ),
        "_sync_apply_rows" to listOf(
            "ordinal|INTEGER|0|1", "table_name|TEXT|1|0", "local_pk|TEXT|1|0", "payload|TEXT|0|0",
            "wire|INTEGER|1|0", "pending|INTEGER|1|0", "bytes|INTEGER|1|0", "role|TEXT|1|0",
            "owner_revision|INTEGER|1|0",
        ),
        "_sync_apply_state" to listOf(
            "singleton_key|INTEGER|1|1", "apply_mode|INTEGER|1|0",
        ),
        "_sync_attachment_state" to listOf(
            "singleton_key|INTEGER|1|1", "attachment_epoch|INTEGER|1|0", "current_source_id|TEXT|1|0",
            "binding_state|TEXT|1|0", "attached_user_id|TEXT|1|0", "schema_name|TEXT|1|0",
            "last_bundle_seq_seen|INTEGER|1|0", "rebuild_required|INTEGER|1|0",
            "pending_initialization_id|TEXT|1|0",
        ),
        "_sync_conflict_state" to listOf(
            "singleton_key|INTEGER|1|1", "dirty_revision|INTEGER|1|0", "baseline_revision|INTEGER|1|0",
            "decision_state|TEXT|1|0", "source_id|TEXT|1|0", "source_bundle_id|INTEGER|1|0",
            "canonical_request_hash|TEXT|1|0", "generation_id|INTEGER|1|0", "generation_fingerprint|TEXT|1|0",
            "conflict_json|TEXT|1|0", "local_payload_json|TEXT|0|0",
        ),
        "_sync_contract_identity" to listOf(
            "singleton_key|INTEGER|0|1", "database_id|TEXT|1|0", "service_id|TEXT|1|0",
            "generation_id|INTEGER|1|0", "local_version|INTEGER|1|0", "physical_shape|TEXT|1|0",
            "fingerprint|TEXT|1|0",
        ),
        "_sync_dirty_rows" to listOf(
            "schema_name|TEXT|1|1", "table_name|TEXT|1|2", "key_json|TEXT|1|3", "op|TEXT|1|0",
            "base_row_version|INTEGER|1|0", "base_kind|TEXT|1|0", "base_payload|TEXT|0|0",
            "base_provenance|TEXT|1|0", "payload|TEXT|0|0", "dirty_ordinal|INTEGER|1|0", "updated_at|TEXT|1|0",
        ),
        "_sync_managed_tables" to listOf(
            "schema_name|TEXT|1|1", "table_name|TEXT|1|2",
        ),
        "_sync_opening_state" to listOf(
            "singleton_key|INTEGER|0|1", "format_version|INTEGER|1|0", "mode|TEXT|1|0", "phase|TEXT|1|0",
            "outcome|TEXT|1|0", "revision|INTEGER|1|0", "plan_digest|TEXT|1|0", "migration_id|TEXT|1|0",
            "database_id|TEXT|1|0", "service_id|TEXT|1|0", "attached_user_id|TEXT|1|0",
            "old_source_id|TEXT|1|0", "new_source_id|TEXT|1|0", "replacement_reason|TEXT|1|0",
            "from_generation_id|INTEGER|1|0", "to_generation_id|INTEGER|1|0", "from_local_version|INTEGER|1|0",
            "to_local_version|INTEGER|1|0", "preserved_tables|TEXT|1|0", "old_fingerprint|TEXT|1|0",
            "target_fingerprint|TEXT|1|0", "preservation_digest|TEXT|1|0", "migration_complete|INTEGER|1|0",
            "contract_confirmed|INTEGER|1|0", "pending_dirty_count|INTEGER|1|0",
            "pending_frozen_count|INTEGER|1|0", "pending_conflict_count|INTEGER|1|0",
            "drain_complete|INTEGER|1|0", "replacement_state|TEXT|1|0", "snapshot_id|TEXT|1|0",
            "snapshot_bundle_seq|INTEGER|1|0", "snapshot_row_count|INTEGER|1|0",
            "snapshot_byte_count|INTEGER|1|0", "staged_row_count|INTEGER|0|0", "staged_byte_count|INTEGER|0|0",
            "target_reads_available|INTEGER|1|0", "writes_available|INTEGER|1|0", "next_action|TEXT|1|0",
            "pending_request_hash|TEXT|1|0", "pending_intent_revision|INTEGER|1|0", "failure_code|TEXT|1|0",
            "failure_message|TEXT|1|0", "initial_old_source_id|TEXT|1|0",
            "old_source_recovery_receipt|TEXT|1|0", "integrity_hash|TEXT|1|0",
        ),
        "_sync_operation_state" to listOf(
            "singleton_key|INTEGER|1|1", "kind|TEXT|1|0", "target_user_id|TEXT|1|0",
            "staged_snapshot_id|TEXT|1|0", "snapshot_bundle_seq|INTEGER|1|0", "snapshot_row_count|INTEGER|1|0",
            "snapshot_byte_count|INTEGER|1|0", "snapshot_stage_complete|INTEGER|1|0", "reason|TEXT|1|0",
            "replacement_source_id|TEXT|1|0", "workspace_context|TEXT|1|0", "reserved_entries|INTEGER|1|0",
            "reserved_bytes|INTEGER|1|0", "snapshot_stage_entries|INTEGER|1|0",
            "snapshot_stage_bytes|INTEGER|1|0", "snapshot_cleanup_ordinal|INTEGER|1|0",
        ),
        "_sync_outbox_bundle" to listOf(
            "singleton_key|INTEGER|1|1", "canonical_json_contract|TEXT|1|0", "state|TEXT|1|0",
            "source_id|TEXT|1|0", "source_bundle_id|INTEGER|1|0", "initialization_id|TEXT|1|0",
            "canonical_request_hash|TEXT|1|0", "row_count|INTEGER|1|0",
        ),
        "_sync_outbox_rows" to listOf(
            "source_bundle_id|INTEGER|1|1", "row_ordinal|INTEGER|1|2", "schema_name|TEXT|1|0",
            "table_name|TEXT|1|0", "key_json|TEXT|1|0", "wire_key_json|TEXT|1|0", "op|TEXT|1|0",
            "base_row_version|INTEGER|1|0", "base_kind|TEXT|1|0", "base_payload|TEXT|0|0",
            "base_provenance|TEXT|1|0", "local_payload|TEXT|0|0", "wire_payload|TEXT|0|0",
        ),
        "_sync_push_edges" to listOf(
            "unit|TEXT|1|1", "dependent|INTEGER|1|2", "prerequisite|INTEGER|1|3", "ordinal|INTEGER|1|0",
            "charge|INTEGER|1|0",
        ),
        "_sync_push_nodes" to listOf(
            "unit|TEXT|1|1", "schema_name|TEXT|1|2", "table_name|TEXT|1|3", "key_json|TEXT|1|4",
            "ordinal|INTEGER|1|0", "intent_revision|INTEGER|1|0", "dirty_ordinal|INTEGER|1|0",
            "status|TEXT|1|0", "adjacency_cursor|TEXT|1|0", "adjacency_phase|INTEGER|1|0",
            "dfs_index|INTEGER|1|0", "lowlink|INTEGER|1|0", "parent|INTEGER|1|0", "on_stack|INTEGER|1|0",
            "stack_ordinal|INTEGER|1|0", "edge_cursor|INTEGER|1|0", "component|INTEGER|1|0",
            "component_rows|INTEGER|1|0", "component_bytes|INTEGER|1|0", "ready_ordinal|INTEGER|1|0",
            "ready_schema|TEXT|1|0", "ready_table|TEXT|1|0", "ready_key|TEXT|1|0", "prerequisites|INTEGER|1|0",
            "blocked|TEXT|1|0", "selected|INTEGER|1|0", "diagnostic_id|TEXT|1|0",
            "diagnostic_ordinal|INTEGER|1|0", "charge|INTEGER|1|0",
        ),
        "_sync_push_plan" to listOf(
            "singleton_key|INTEGER|0|1", "unit|TEXT|1|0", "guard|TEXT|1|0", "phase|TEXT|1|0", "state|TEXT|1|0",
            "entries|INTEGER|1|0", "bytes|INTEGER|1|0",
        ),
        "_sync_relationships" to listOf(
            "view|TEXT|1|1", "child_schema|TEXT|1|2", "child_table|TEXT|1|3", "child_key|TEXT|1|4",
            "fk_column|TEXT|1|5", "parent_schema|TEXT|1|0", "parent_table|TEXT|1|0", "parent_key|TEXT|1|0",
            "owner_revision|INTEGER|1|0",
        ),
        "_sync_row_state" to listOf(
            "schema_name|TEXT|1|1", "table_name|TEXT|1|2", "key_json|TEXT|1|3", "row_version|INTEGER|1|0",
            "server_kind|TEXT|1|0", "server_payload|TEXT|0|0", "server_provenance|TEXT|1|0",
            "relationships_known|INTEGER|1|0", "server_revision|INTEGER|1|0", "updated_at|TEXT|1|0",
        ),
        "_sync_snapshot_stage" to listOf(
            "snapshot_id|TEXT|1|1", "row_ordinal|INTEGER|1|2", "schema_name|TEXT|1|0", "table_name|TEXT|1|0",
            "key_json|TEXT|1|0", "row_version|INTEGER|1|0", "payload|TEXT|1|0", "charge|INTEGER|1|0",
        ),
        "_sync_source_state" to listOf(
            "source_id|TEXT|1|1", "next_source_bundle_id|INTEGER|1|0", "replaced_by_source_id|TEXT|1|0",
            "created_at|TEXT|1|0",
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
        "_sync_admitted_contracts" to emptyList(),
        "_sync_apply_edges" to listOf(
            "sqlite_autoindex__sync_apply_edges_1|1|u|0",
        ),
        "_sync_apply_plan" to emptyList(),
        "_sync_apply_rows" to listOf(
            "_sync_apply_rows_pending_idx|0|c|0", "_sync_apply_rows_role_ordinal_idx|0|c|0",
            "sqlite_autoindex__sync_apply_rows_1|1|u|0",
        ),
        "_sync_apply_state" to emptyList(),
        "_sync_attachment_state" to emptyList(),
        "_sync_conflict_state" to emptyList(),
        "_sync_contract_identity" to emptyList(),
        "_sync_dirty_rows" to listOf(
            "_sync_dirty_rows_order_idx|0|c|0", "sqlite_autoindex__sync_dirty_rows_1|1|pk|0",
        ),
        "_sync_managed_tables" to listOf(
            "sqlite_autoindex__sync_managed_tables_1|1|pk|0",
        ),
        "_sync_opening_state" to emptyList(),
        "_sync_operation_state" to emptyList(),
        "_sync_outbox_bundle" to emptyList(),
        "_sync_outbox_rows" to listOf(
            "sqlite_autoindex__sync_outbox_rows_1|1|pk|0", "sqlite_autoindex__sync_outbox_rows_2|1|u|0",
        ),
        "_sync_push_edges" to listOf(
            "_sync_push_edges_incoming_idx|0|c|0", "_sync_push_edges_outgoing_idx|0|c|0",
            "sqlite_autoindex__sync_push_edges_1|1|pk|0", "sqlite_autoindex__sync_push_edges_2|1|u|0",
        ),
        "_sync_push_nodes" to listOf(
            "_sync_push_nodes_component_diagnostic_idx|0|c|0", "_sync_push_nodes_component_idx|0|c|0",
            "_sync_push_nodes_component_ordinal_idx|0|c|0", "_sync_push_nodes_diagnostic_idx|0|c|0",
            "_sync_push_nodes_frontier_idx|0|c|0", "_sync_push_nodes_ready_idx|0|c|0",
            "_sync_push_nodes_selected_idx|0|c|0", "_sync_push_nodes_stack_idx|0|c|0",
            "sqlite_autoindex__sync_push_nodes_1|1|pk|0", "sqlite_autoindex__sync_push_nodes_2|1|u|0",
        ),
        "_sync_push_plan" to emptyList(),
        "_sync_relationships" to listOf(
            "_sync_relationships_parent_idx|0|c|0", "sqlite_autoindex__sync_relationships_1|1|pk|0",
        ),
        "_sync_row_state" to listOf(
            "_sync_row_state_unknown_idx|0|c|0", "sqlite_autoindex__sync_row_state_1|1|pk|0",
        ),
        "_sync_snapshot_stage" to listOf(
            "sqlite_autoindex__sync_snapshot_stage_1|1|pk|0", "sqlite_autoindex__sync_snapshot_stage_2|1|u|0",
        ),
        "_sync_source_state" to listOf(
            "sqlite_autoindex__sync_source_state_1|1|pk|0",
        ),
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

private data class StatementReuseontrolStateSnapshot(
    val contractIdentity: List<String>,
    val admittedContracts: List<String>,
    val applyState: List<String>,
    val conflictState: List<String>,
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

private suspend fun captureStatementReuseontrolState(
    db: SafeSQLiteConnection,
): StatementReuseontrolStateSnapshot = StatementReuseontrolStateSnapshot(
    contractIdentity = readStatementReuseontrolRows(
        db, "SELECT json_array(singleton_key,database_id,service_id,generation_id,local_version,physical_shape,fingerprint) FROM _sync_contract_identity ORDER BY singleton_key",
    ),
    admittedContracts = readStatementReuseontrolRows(
        db, "SELECT json_array(generation_id,fingerprint,descriptor,descriptor_hash) FROM _sync_admitted_contracts ORDER BY generation_id",
    ),
    conflictState = readStatementReuseontrolRows(
        db,
        "SELECT json_array(singleton_key,dirty_revision,baseline_revision,decision_state,source_id,source_bundle_id," +
            "canonical_request_hash,generation_id,generation_fingerprint,conflict_json,local_payload_json) " +
            "FROM _sync_conflict_state ORDER BY singleton_key",
    ),
    applyState = readStatementReuseontrolRows(
        db,
        "SELECT json_array(singleton_key, apply_mode) FROM _sync_apply_state ORDER BY singleton_key",
    ),
    rowState = readStatementReuseontrolRows(
        db,
        "SELECT json_array(schema_name, table_name, key_json, row_version, server_kind, " +
            "server_payload, server_provenance, relationships_known, server_revision, updated_at) " +
            "FROM _sync_row_state ORDER BY schema_name, table_name, key_json",
    ),
    dirtyRows = readStatementReuseontrolRows(
        db,
        "SELECT json_array(schema_name, table_name, key_json, op, base_row_version, base_kind, " +
            "base_payload, base_provenance, payload, " +
            "dirty_ordinal, updated_at) FROM _sync_dirty_rows " +
            "ORDER BY dirty_ordinal, schema_name, table_name, key_json",
    ),
    snapshotStage = readStatementReuseontrolRows(
        db,
        "SELECT json_array(snapshot_id, row_ordinal, schema_name, table_name, key_json, " +
            "row_version, payload) FROM _sync_snapshot_stage ORDER BY snapshot_id, row_ordinal",
    ),
    sourceState = readStatementReuseontrolRows(
        db,
        "SELECT json_array(source_id, next_source_bundle_id, replaced_by_source_id, created_at) " +
            "FROM _sync_source_state ORDER BY source_id",
    ),
    attachmentState = readStatementReuseontrolRows(
        db,
        "SELECT json_array(singleton_key, current_source_id, binding_state, attached_user_id, " +
            "schema_name, last_bundle_seq_seen, rebuild_required, pending_initialization_id) " +
            "FROM _sync_attachment_state ORDER BY singleton_key",
    ),
    operationState = readStatementReuseontrolRows(
        db,
        "SELECT json_array(singleton_key, kind, target_user_id, staged_snapshot_id, " +
            "snapshot_bundle_seq, snapshot_row_count, snapshot_byte_count, snapshot_stage_complete, " +
            "reason, replacement_source_id) FROM _sync_operation_state ORDER BY singleton_key",
    ),
    outboxBundle = readStatementReuseontrolRows(
        db,
        "SELECT json_array(singleton_key, canonical_json_contract, state, source_id, " +
            "source_bundle_id, initialization_id, canonical_request_hash, row_count) " +
            "FROM _sync_outbox_bundle ORDER BY singleton_key",
    ),
    outboxRows = readStatementReuseontrolRows(
        db,
        "SELECT json_array(source_bundle_id, row_ordinal, schema_name, table_name, key_json, " +
            "wire_key_json, op, base_row_version, local_payload, wire_payload) " +
            "FROM _sync_outbox_rows ORDER BY source_bundle_id, row_ordinal",
    ),
    managedTables = readStatementReuseontrolRows(
        db,
        "SELECT json_array(schema_name, table_name) FROM _sync_managed_tables " +
            "ORDER BY schema_name, table_name",
    ),
)

private suspend fun readStatementReuseontrolRows(
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

private fun assertWorkerTimestamp(
    value: String,
    label: String,
) {
    assertTrue(
        Regex("""\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z""").matches(value),
        "$label must be a canonical UTC millisecond timestamp, but was $value",
    )
}
