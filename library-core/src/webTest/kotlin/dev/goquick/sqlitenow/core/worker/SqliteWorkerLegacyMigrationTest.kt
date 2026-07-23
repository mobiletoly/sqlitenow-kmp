package dev.goquick.sqlitenow.core.worker

import dev.goquick.sqlitenow.core.AUTHENTIC_LEGACY_SQLJS_VERSION
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.DatabaseMigrations
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.SqliteNowDatabase
import dev.goquick.sqlitenow.core.SqlitePersistence
import dev.goquick.sqlitenow.core.createAuthenticLegacySqlJsFixture
import dev.goquick.sqlitenow.core.legacySnapshotPersistenceForTest
import dev.goquick.sqlitenow.core.sqlite.SqliteException
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

class SqliteWorkerLegacyMigrationTest {
    @Test
    fun authenticReleasedFormatFixtureUsesIsolatedSqlJs1130() = runTest {
        val dbName = phase5bName("authentic-sqljs-fixture")
        val fixture = createAuthenticLegacySqlJsFixture(dbName)
        try {
            fixture.connection.execSQL(
                "CREATE TABLE authentic_fixture(value TEXT NOT NULL)",
            )
            fixture.connection.execSQL(
                "INSERT INTO authentic_fixture VALUES ('released-format')",
            )
            fixture.connection.execSQL("PRAGMA user_version = 27")
            val bytes = fixture.exportBytes()
            assertTrue(bytes.size >= 512)
            assertTrue(
                bytes.copyOfRange(0, 16).decodeToString().startsWith("SQLite format 3"),
            )
            println(
                "PHASE7_LEGACY_FIXTURE generator=sql.js " +
                    "version=$AUTHENTIC_LEGACY_SQLJS_VERSION bytes=${bytes.size} " +
                    "userVersion=27",
            )
        } finally {
            fixture.close()
        }
    }

    @Test
    fun ordinaryDefaultCreatesNoLegacySnapshotStore() = runTest {
        if (!browserWorkerAvailable()) return@runTest
        val scenarios = listOf(
            BuiltInScenario("opfs", true),
            BuiltInScenario("indexeddb", false),
        )

        scenarios.forEach { scenario ->
            val dbName = phase5bName("phase7-default-no-store-${scenario.name}")
            try {
                assertFalse(legacySnapshotExistsForTest(dbName, scenario.forceOpfs))
                val database = Phase5BDatabase(dbName)
                assertEquals(null, database.connectionConfig.persistence)
                database.open()
                try {
                    val worker =
                        database.connection().ref as SqliteWorkerSQLiteConnection
                    assertEquals("browser-worker", worker.metricsForTest().runtimeKind)
                    assertEquals("direct-opfs", worker.metricsForTest().storageMode)
                    database.connection().execSQL(
                        "CREATE TABLE phase7_no_store(value TEXT NOT NULL)",
                    )
                    database.connection().execSQL(
                        "INSERT INTO phase7_no_store VALUES ('${scenario.name}')",
                    )
                    database.persistSnapshotNow()
                    assertEquals(0, worker.metricsForTest().snapshotExports)
                } finally {
                    database.close()
                }
                assertFalse(legacySnapshotExistsForTest(dbName, scenario.forceOpfs))
            } finally {
                cleanupMigrationState(dbName)
            }
        }
    }

    @Test
    fun persistenceClassificationAndCustomMigrationAreExact() = runTest {
        val dbName = phase5bName("custom")
        val source = SnapshotPersistence()
        assertEquals(null, Phase5BDatabase("$dbName-default").connectionConfig.persistence)

        seedProductionShapedSnapshot(dbName, source)
        val seededBytes = assertNotNull(source.bytes).copyOf()
        source.resetCounts()

        try {
            val connection = try {
                sqliteWorkerConnectionProvider().openConnection(
                    dbName = dbName,
                    debug = false,
                    config = SqliteConnectionConfig(persistence = source),
                )
            } catch (failure: SqliteException) {
                assertTrue(
                    failure.message.orEmpty().contains("browser-only"),
                    failure.message,
                )
                assertEquals(0, source.loadCalls)
                assertEquals(0, source.persistCalls)
                assertEquals(0, source.clearCalls)
                return@runTest
            }

            try {
                assertProductionFixture(connection)
                val worker = connection.ref as SqliteWorkerSQLiteConnection
                val metrics = worker.metricsForTest()
                assertEquals("custom", metrics.migrationSourceKind)
                assertEquals(seededBytes.size.toLong(), metrics.migrationSourceBytes)
                assertEquals("ok", metrics.migrationIntegrityCheck)
                assertEquals(7, metrics.migrationImportedUserVersion)
                assertTrue(metrics.migrationSourceRetained)
                assertTrue(metrics.migrationTargetFileName.startsWith("sqlitenow-worker-v1-"))
                assertEquals(64, metrics.migrationSourceSha256.length)
                println(
                    "PHASE5B_CUSTOM dbName=$dbName sourceKind=${metrics.migrationSourceKind} " +
                        "sourceBytes=${metrics.migrationSourceBytes} " +
                        "sourceSha256=${metrics.migrationSourceSha256} " +
                        "target=${metrics.migrationTargetFileName} " +
                        "integrity=${metrics.migrationIntegrityCheck} " +
                        "userVersion=${metrics.migrationImportedUserVersion} " +
                        "retained=${metrics.migrationSourceRetained}",
                )

                connection.execSQL("INSERT INTO phase5b_child VALUES (2, 1, 'ordinary')")
                connection.persistSnapshotNow()
                assertEquals(0, worker.metricsForTest().snapshotExports)
            } finally {
                connection.close()
            }

            assertEquals(1, source.loadCalls)
            assertEquals(0, source.persistCalls)
            assertEquals(0, source.clearCalls)

            source.bytes = ByteArray(512)
            val reopened = sqliteWorkerConnectionProvider().openConnection(
                dbName = dbName,
                debug = false,
                config = SqliteConnectionConfig(persistence = source),
            )
            try {
                assertProductionFixture(reopened)
                reopened.prepare(
                    "SELECT value FROM phase5b_child WHERE id = 2",
                ).use { statement ->
                    assertTrue(statement.step())
                    assertEquals("ordinary", statement.getText(0))
                }
            } finally {
                reopened.close()
            }
            assertEquals(1, source.loadCalls)
            assertEquals(0, source.persistCalls)
            assertEquals(0, source.clearCalls)
        } finally {
            cleanupMigrationState(dbName)
        }
    }

    @Test
    fun signedBootstrapUserVersionMigratesAndRemainsHealthy() = runTest {
        if (!browserWorkerAvailable()) return@runTest
        val dbName = phase5bName("signed-bootstrap-user-version")
        val source = SnapshotPersistence()
        try {
            seedProductionShapedSnapshot(
                dbName = dbName,
                persistence = source,
                userVersion = -1,
            )
            source.resetCounts()

            val imported = sqliteWorkerConnectionProvider().openConnection(
                dbName = dbName,
                debug = false,
                config = SqliteConnectionConfig(persistence = source),
            )
            try {
                assertProductionFixture(imported, expectedUserVersion = -1)
                val metrics =
                    (imported.ref as SqliteWorkerSQLiteConnection).metricsForTest()
                assertEquals(-1, metrics.migrationImportedUserVersion)
            } finally {
                imported.close()
            }
            assertEquals(1, source.loadCalls)

            source.bytes = ByteArray(512)
            val reopened = sqliteWorkerConnectionProvider().openConnection(
                dbName = dbName,
                debug = false,
                config = SqliteConnectionConfig(persistence = source),
            )
            try {
                assertProductionFixture(reopened, expectedUserVersion = -1)
            } finally {
                reopened.close()
            }
            assertEquals(1, source.loadCalls)
        } finally {
            cleanupMigrationState(dbName)
        }
    }

    @Test
    fun builtInOpfsAndIndexedDbSnapshotsMigrateAndRemainRetained() = runTest {
        if (!browserWorkerAvailable()) return@runTest
        val scenarios = listOf(
            BuiltInScenario("opfs", true),
            BuiltInScenario("indexeddb", false),
        )
        scenarios.forEach { scenario ->
            val dbName = phase5bName("builtin-${scenario.name}")
            try {
                val persistence =
                    legacySnapshotPersistenceForTest(dbName, scenario.forceOpfs)
                seedProductionShapedSnapshot(dbName, persistence)
                val retainedBefore = assertNotNull(persistence.load(dbName))

                val connection = sqliteWorkerConnectionProvider().openConnection(
                    dbName = dbName,
                    debug = false,
                    config = SqliteConnectionConfig(),
                )
                try {
                    assertProductionFixture(connection)
                    val metrics =
                        (connection.ref as SqliteWorkerSQLiteConnection).metricsForTest()
                    assertEquals(scenario.name, metrics.migrationSourceKind)
                    assertEquals(retainedBefore.size.toLong(), metrics.migrationSourceBytes)
                    assertTrue(metrics.migrationSourceRetained)
                    assertEquals(0, metrics.snapshotExports)
                    println(
                        "PHASE5B_BUILTIN dbName=$dbName " +
                            "sourceKind=${metrics.migrationSourceKind} " +
                            "sourceBytes=${metrics.migrationSourceBytes} " +
                            "sourceSha256=${metrics.migrationSourceSha256} " +
                            "target=${metrics.migrationTargetFileName} " +
                            "integrity=${metrics.migrationIntegrityCheck} " +
                            "userVersion=${metrics.migrationImportedUserVersion} " +
                            "retained=${metrics.migrationSourceRetained}",
                    )
                } finally {
                    connection.close()
                }

                val retainedAfter = assertNotNull(
                    legacySnapshotPersistenceForTest(
                        dbName,
                        scenario.forceOpfs,
                    ).load(dbName),
                )
                assertTrue(retainedBefore.contentEquals(assertNotNull(retainedAfter)))
            } finally {
                cleanupMigrationState(dbName)
            }
        }
    }

    @Test
    fun builtInDualSourceSelectionIsDeterministicAndAmbiguityFailsClosed() = runTest {
        if (!browserWorkerAvailable()) return@runTest
        val identicalName = phase5bName("dual-identical")
        val ambiguousName = phase5bName("dual-ambiguous")
        try {
            val first = SnapshotPersistence()
            val second = SnapshotPersistence()
            seedProductionShapedSnapshot(identicalName, first)
            seedDifferentSnapshot(ambiguousName, first, value = "first")
            seedDifferentSnapshot(ambiguousName, second, value = "second")

            persistBuiltIn(identicalName, forceOpfs = true, assertNotNull(first.bytes))
            persistBuiltIn(identicalName, forceOpfs = false, assertNotNull(first.bytes))
            val identical = sqliteWorkerConnectionProvider().openConnection(
                dbName = identicalName,
                debug = false,
                config = SqliteConnectionConfig(),
            )
            try {
                val metrics =
                    (identical.ref as SqliteWorkerSQLiteConnection).metricsForTest()
                assertEquals("opfs", metrics.migrationSourceKind)
            } finally {
                identical.close()
            }

            persistBuiltIn(ambiguousName, forceOpfs = true, assertNotNull(first.bytes))
            persistBuiltIn(ambiguousName, forceOpfs = false, assertNotNull(second.bytes))
            val failure = assertFailsWith<SqliteException> {
                sqliteWorkerConnectionProvider().openConnection(
                    dbName = ambiguousName,
                    debug = false,
                    config = SqliteConnectionConfig(),
                )
            }
            assertTrue(failure.message.orEmpty().contains("Ambiguous legacy sources"))
        } finally {
            cleanupMigrationState(identicalName)
            cleanupMigrationState(ambiguousName)
        }
    }

    @Test
    fun markerlessDirectTargetWinsOverStaleLegacySnapshot() = runTest {
        if (!browserWorkerAvailable()) return@runTest
        val dbName = phase5bName("markerless-target")
        try {
            val direct = sqliteWorkerConnectionProvider().openConnection(
                dbName = dbName,
                debug = false,
                config = SqliteConnectionConfig(),
            )
            try {
                direct.execSQL("CREATE TABLE authoritative(value TEXT NOT NULL)")
                direct.execSQL("INSERT INTO authoritative VALUES ('direct')")
            } finally {
                direct.close()
            }

            seedDifferentSnapshot(
                dbName,
                legacySnapshotPersistenceForTest(dbName, forceOpfs = false),
                "legacy",
            )

            val reopened = sqliteWorkerConnectionProvider().openConnection(
                dbName = dbName,
                debug = false,
                config = SqliteConnectionConfig(),
            )
            try {
                reopened.prepare("SELECT value FROM authoritative").use { statement ->
                    assertTrue(statement.step())
                    assertEquals("direct", statement.getText(0))
                }
                val metrics =
                    (reopened.ref as SqliteWorkerSQLiteConnection).metricsForTest()
                assertEquals("", metrics.migrationSourceKind)
            } finally {
                reopened.close()
            }
        } finally {
            cleanupMigrationState(dbName)
        }
    }

    @Test
    fun builtInSourceMutationBeforeHealthCommitFailsClosed() = runTest {
        if (!browserWorkerAvailable()) return@runTest
        val dbName = phase5bName("source-mutation")
        try {
            val persistence =
                legacySnapshotPersistenceForTest(dbName, forceOpfs = true)
            seedProductionShapedSnapshot(dbName, persistence)

            val driver = SqliteWorkerSQLiteDriver.create()
            try {
                driver.setMigrationInterruptionForTest(dbName, "after-integrity")
                val interrupted = assertFailsWith<SqliteException> {
                    driver.open(
                        fileName = dbName,
                        legacySourceMode = "built-in",
                        customPersistence = null,
                    )
                }
                assertTrue(interrupted.message.orEmpty().contains("after-integrity"))
            } finally {
                driver.shutdown()
            }

            val mutated = assertNotNull(persistence.load(dbName)).copyOf()
            mutated[mutated.lastIndex] = (mutated.last().toInt() xor 0x01).toByte()
            persistence.persist(dbName, mutated)

            val failure = assertFailsWith<SqliteException> {
                sqliteWorkerConnectionProvider().openConnection(
                    dbName = dbName,
                    debug = false,
                    config = SqliteConnectionConfig(),
                )
            }
            assertTrue(
                failure.message.orEmpty().contains("source changed", ignoreCase = true),
                failure.message,
            )
        } finally {
            cleanupMigrationState(dbName)
        }
    }

    @Test
    fun interruptionBoundariesRetryOnlyIntentOwnedTargets() = runTest {
        if (!browserWorkerAvailable()) return@runTest
        val stages = listOf(
            "before-intent",
            "after-intent",
            "during-import",
            "after-import",
            "after-integrity",
            "after-health",
            "before-intent-cleanup",
        )
        stages.forEach { stage ->
            val dbName = phase5bName("interrupt-$stage")
            val source = SnapshotPersistence()
            try {
                seedProductionShapedSnapshot(dbName, source)
                source.resetCounts()
                val driver = SqliteWorkerSQLiteDriver.create()
                try {
                    driver.setMigrationInterruptionForTest(dbName, stage)
                    val failure = assertFailsWith<SqliteException>(stage) {
                        driver.open(
                            fileName = dbName,
                            legacySourceMode = "custom",
                            customPersistence = source,
                        )
                    }
                    assertTrue(failure.message.orEmpty().contains(stage))
                } finally {
                    driver.shutdown()
                }
                assertEquals(1, source.loadCalls, stage)

                source.resetCounts()
                val retried = sqliteWorkerConnectionProvider().openConnection(
                    dbName = dbName,
                    debug = false,
                    config = SqliteConnectionConfig(persistence = source),
                )
                try {
                    assertProductionFixture(retried)
                } finally {
                    retried.close()
                }
                assertEquals(
                    if (stage in setOf("after-health", "before-intent-cleanup")) 0 else 1,
                    source.loadCalls,
                    stage,
                )
            } finally {
                cleanupMigrationState(dbName)
            }
        }
    }

    @Test
    fun cancellationBeforeIntentWriteLeavesNoMigrationArtifacts() = runTest {
        if (!browserWorkerAvailable()) return@runTest
        val dbName = phase5bName("cancel-before-intent-write")
        val cancelledSource = SnapshotPersistence()
        val replacementSource = SnapshotPersistence()
        try {
            seedProductionShapedSnapshot(dbName, cancelledSource)
            cancelledSource.resetCounts()
            val driver = SqliteWorkerSQLiteDriver.create()
            try {
                val hold = driver.holdMigrationCancellationForTest(
                    dbName,
                    "before-intent-write",
                )
                coroutineScope {
                    val opening = async {
                        driver.open(
                            fileName = dbName,
                            legacySourceMode = "custom",
                            customPersistence = cancelledSource,
                        )
                    }
                    assertEquals(0, driver.awaitCancellationHoldForTest(hold))
                    opening.cancel()
                    assertFailsWith<kotlinx.coroutines.CancellationException> {
                        opening.await()
                    }
                }
            } finally {
                driver.shutdown()
            }
            assertEquals(1, cancelledSource.loadCalls)

            seedDifferentSnapshot(
                dbName = dbName,
                persistence = replacementSource,
                value = "replacement-after-cancellation",
            )
            replacementSource.resetCounts()
            val retried = sqliteWorkerConnectionProvider().openConnection(
                dbName = dbName,
                debug = false,
                config = SqliteConnectionConfig(persistence = replacementSource),
            )
            try {
                retried.prepare("SELECT value FROM legacy_only").use { statement ->
                    assertTrue(statement.step())
                    assertEquals("replacement-after-cancellation", statement.getText(0))
                    assertFalse(statement.step())
                }
                assertEquals(
                    "custom",
                    (retried.ref as SqliteWorkerSQLiteConnection)
                        .metricsForTest()
                        .migrationSourceKind,
                )
            } finally {
                retried.close()
            }
            assertEquals(1, replacementSource.loadCalls)
        } finally {
            cleanupMigrationState(dbName)
        }
    }

    @Test
    fun cancellationAfterBuiltInIntegrityRestoresRetryableIntent() = runTest {
        if (!browserWorkerAvailable()) return@runTest
        val dbName = phase5bName("cancel-built-in-after-integrity")
        try {
            seedProductionShapedSnapshot(
                dbName,
                legacySnapshotPersistenceForTest(dbName, forceOpfs = true),
            )
            val driver = SqliteWorkerSQLiteDriver.create()
            try {
                val hold = driver.holdMigrationCancellationForTest(
                    dbName,
                    "after-integrity",
                )
                coroutineScope {
                    val opening = async {
                        driver.open(
                            fileName = dbName,
                            legacySourceMode = "built-in",
                            customPersistence = null,
                        )
                    }
                    assertEquals(0, driver.awaitCancellationHoldForTest(hold))
                    opening.cancel()
                    assertFailsWith<kotlinx.coroutines.CancellationException> {
                        opening.await()
                    }
                }
            } finally {
                driver.shutdown()
            }

            val retried = sqliteWorkerConnectionProvider().openConnection(
                dbName = dbName,
                debug = false,
                config = SqliteConnectionConfig(),
            )
            try {
                assertProductionFixture(retried)
                assertEquals(
                    "opfs",
                    (retried.ref as SqliteWorkerSQLiteConnection)
                        .metricsForTest()
                        .migrationSourceKind,
                )
            } finally {
                retried.close()
            }
        } finally {
            cleanupMigrationState(dbName)
        }
    }

    @Test
    fun cancellationAfterCustomHealthRestoresRetryableIntent() = runTest {
        if (!browserWorkerAvailable()) return@runTest
        val dbName = phase5bName("cancel-custom-after-health")
        val source = SnapshotPersistence()
        try {
            seedProductionShapedSnapshot(dbName, source)
            source.resetCounts()
            val driver = SqliteWorkerSQLiteDriver.create()
            try {
                val hold = driver.holdMigrationCancellationForTest(dbName, "after-health")
                coroutineScope {
                    val opening = async {
                        driver.open(
                            fileName = dbName,
                            legacySourceMode = "custom",
                            customPersistence = source,
                        )
                    }
                    assertEquals(0, driver.awaitCancellationHoldForTest(hold))
                    opening.cancel()
                    assertFailsWith<kotlinx.coroutines.CancellationException> {
                        opening.await()
                    }
                }
            } finally {
                driver.shutdown()
            }
            assertEquals(1, source.loadCalls)

            source.resetCounts()
            val retried = sqliteWorkerConnectionProvider().openConnection(
                dbName = dbName,
                debug = false,
                config = SqliteConnectionConfig(persistence = source),
            )
            try {
                assertProductionFixture(retried)
                assertEquals(
                    "custom",
                    (retried.ref as SqliteWorkerSQLiteConnection)
                        .metricsForTest()
                        .migrationSourceKind,
                )
            } finally {
                retried.close()
            }
            assertEquals(1, source.loadCalls)
        } finally {
            cleanupMigrationState(dbName)
        }
    }

    @Test
    fun cancellationAfterCompleteOpenResponseRestoresRetryableIntent() = runTest {
        if (!browserWorkerAvailable()) return@runTest
        val dbName = phase5bName("cancel-complete-response")
        val source = SnapshotPersistence()
        try {
            seedProductionShapedSnapshot(dbName, source)
            source.resetCounts()
            val driver = SqliteWorkerSQLiteDriver.create()
            try {
                driver.holdNextResponseForTest("completeOpen")
                coroutineScope {
                    val opening = async {
                        driver.open(
                            fileName = dbName,
                            legacySourceMode = "custom",
                            customPersistence = source,
                        )
                    }
                    awaitCompletedWorkerCommand(driver, "completeOpen")
                    opening.cancel()
                    assertFailsWith<kotlinx.coroutines.CancellationException> {
                        opening.await()
                    }
                }
            } finally {
                driver.shutdown()
            }
            assertEquals(1, source.loadCalls)

            source.resetCounts()
            val retried = sqliteWorkerConnectionProvider().openConnection(
                dbName = dbName,
                debug = false,
                config = SqliteConnectionConfig(persistence = source),
            )
            try {
                assertProductionFixture(retried)
                assertEquals(
                    "custom",
                    (retried.ref as SqliteWorkerSQLiteConnection)
                        .metricsForTest()
                        .migrationSourceKind,
                )
            } finally {
                retried.close()
            }
            assertEquals(1, source.loadCalls)
        } finally {
            cleanupMigrationState(dbName)
        }
    }

    @Test
    fun preDispatchCompleteOpenCancellationClearsPendingOpen() = runTest {
        if (!browserWorkerAvailable()) return@runTest
        val dbName = phase5bName("cancel-complete-predispatch")
        val source = SnapshotPersistence()
        try {
            seedProductionShapedSnapshot(dbName, source)
            source.resetCounts()
            val driver = SqliteWorkerSQLiteDriver.create()
            try {
                val hold = driver.holdNextCompleteOpenCancellationForTest()
                coroutineScope {
                    val opening = async {
                        driver.open(
                            fileName = dbName,
                            legacySourceMode = "custom",
                            customPersistence = source,
                        )
                    }
                    assertEquals(1, driver.awaitCancellationHoldForTest(hold))
                    opening.cancel()
                    assertFailsWith<kotlinx.coroutines.CancellationException> {
                        opening.await()
                    }
                }
                assertEquals(0, driver.pendingOpenCountForTest())
            } finally {
                driver.shutdown()
            }

            source.resetCounts()
            val retried = sqliteWorkerConnectionProvider().openConnection(
                dbName = dbName,
                debug = false,
                config = SqliteConnectionConfig(persistence = source),
            )
            try {
                assertProductionFixture(retried)
            } finally {
                retried.close()
            }
            assertEquals(1, source.loadCalls)
        } finally {
            cleanupMigrationState(dbName)
        }
    }

    @Test
    fun customAbsentMalformedThrownAndCancelledLoadsNeverFallBackSilently() = runTest {
        if (!browserWorkerAvailable()) return@runTest

        val absentName = phase5bName("custom-absent")
        val absent = SnapshotPersistence()
        try {
            val fresh = sqliteWorkerConnectionProvider().openConnection(
                dbName = absentName,
                debug = false,
                config = SqliteConnectionConfig(persistence = absent),
            )
            try {
                fresh.execSQL("CREATE TABLE absent_is_fresh(value TEXT)")
                val metrics = (fresh.ref as SqliteWorkerSQLiteConnection).metricsForTest()
                assertEquals("", metrics.migrationSourceKind)
            } finally {
                fresh.close()
            }
            assertEquals(1, absent.loadCalls)
            assertEquals(0, absent.persistCalls)
            assertEquals(0, absent.clearCalls)
        } finally {
            cleanupMigrationState(absentName)
        }

        val malformedName = phase5bName("custom-malformed")
        try {
            val malformed = SnapshotPersistence().also { it.bytes = ByteArray(512) }
            val failure = assertFailsWith<SqliteException> {
                sqliteWorkerConnectionProvider().openConnection(
                    dbName = malformedName,
                    debug = false,
                    config = SqliteConnectionConfig(persistence = malformed),
                )
            }
            assertTrue(failure.message.orEmpty().contains("plausible SQLite"))
            assertEquals(1, malformed.loadCalls)
            assertEquals(0, malformed.persistCalls)
            assertEquals(0, malformed.clearCalls)

            val structurallyCorrupt = SnapshotPersistence()
            seedProductionShapedSnapshot(malformedName, structurallyCorrupt)
            structurallyCorrupt.bytes = assertNotNull(structurallyCorrupt.bytes).also { bytes ->
                for (index in 100 until bytes.size) bytes[index] = 0
            }
            structurallyCorrupt.resetCounts()
            val corruptFailure = assertFailsWith<SqliteException> {
                sqliteWorkerConnectionProvider().openConnection(
                    dbName = malformedName,
                    debug = false,
                    config = SqliteConnectionConfig(persistence = structurallyCorrupt),
                )
            }
            assertTrue(
                corruptFailure.message.orEmpty().contains("migration failed", ignoreCase = true),
            )
            assertEquals(1, structurallyCorrupt.loadCalls)

            val thrown = ThrowingLoadPersistence()
            val thrownFailure = assertFailsWith<SqliteException> {
                sqliteWorkerConnectionProvider().openConnection(
                    dbName = malformedName,
                    debug = false,
                    config = SqliteConnectionConfig(persistence = thrown),
                )
            }
            assertTrue(thrownFailure.message.orEmpty().contains("controlled custom load failure"))
            assertEquals(1, thrown.loadCalls)
            assertEquals(0, thrown.persistCalls)
            assertEquals(0, thrown.clearCalls)
        } finally {
            cleanupMigrationState(malformedName)
        }

        val cancelledName = phase5bName("custom-cancelled")
        val blocking = BlockingLoadPersistence()
        try {
            coroutineScope {
                val opening = async {
                    sqliteWorkerConnectionProvider().openConnection(
                        dbName = cancelledName,
                        debug = false,
                        config = SqliteConnectionConfig(persistence = blocking),
                    )
                }
                blocking.started.await()
                opening.cancel()
                assertFailsWith<kotlinx.coroutines.CancellationException> {
                    opening.await()
                }
            }
            assertEquals(1, blocking.loadCalls)
            assertEquals(0, blocking.persistCalls)
            assertEquals(0, blocking.clearCalls)

            val valid = SnapshotPersistence()
            seedProductionShapedSnapshot(cancelledName, valid)
            valid.resetCounts()
            val retried = sqliteWorkerConnectionProvider().openConnection(
                dbName = cancelledName,
                debug = false,
                config = SqliteConnectionConfig(persistence = valid),
            )
            try {
                assertProductionFixture(retried)
            } finally {
                retried.close()
            }
            assertEquals(1, valid.loadCalls)
        } finally {
            cleanupMigrationState(cancelledName)
        }
    }

    @Test
    fun integrityFailureAndUnreadableSchemaFailClosed() = runTest {
        if (!browserWorkerAvailable()) return@runTest
        val integrityName = phase5bName("failed-integrity")
        val foreignKeyName = phase5bName("failed-foreign-key")
        val schemaName = phase5bName("unreadable-schema")
        try {
            val failedIntegrity = SnapshotPersistence()
            seedFailedIntegritySnapshot(integrityName, failedIntegrity)
            failedIntegrity.resetCounts()
            val integrityFailure = assertFailsWith<SqliteException> {
                sqliteWorkerConnectionProvider().openConnection(
                    dbName = integrityName,
                    debug = false,
                    config = SqliteConnectionConfig(persistence = failedIntegrity),
                )
            }
            assertTrue(
                integrityFailure.message.orEmpty().contains("integrity_check"),
                integrityFailure.message,
            )
            assertEquals(1, failedIntegrity.loadCalls)
            assertEquals(0, failedIntegrity.persistCalls)
            assertEquals(0, failedIntegrity.clearCalls)

            val failedForeignKey = SnapshotPersistence()
            seedFailedForeignKeySnapshot(foreignKeyName, failedForeignKey)
            failedForeignKey.resetCounts()
            val foreignKeyFailure = assertFailsWith<SqliteException> {
                val unexpected = sqliteWorkerConnectionProvider().openConnection(
                    dbName = foreignKeyName,
                    debug = false,
                    config = SqliteConnectionConfig(persistence = failedForeignKey),
                )
                unexpected.close()
            }
            assertTrue(
                foreignKeyFailure.message.orEmpty().contains("foreign_key_check"),
                foreignKeyFailure.message,
            )
            assertEquals(1, failedForeignKey.loadCalls)
            assertEquals(0, failedForeignKey.persistCalls)
            assertEquals(0, failedForeignKey.clearCalls)
            assertFalse(
                "health" in workerStorageArtifactsForTest(foreignKeyName),
                "A foreign-key-invalid import must not become healthy.",
            )

            val unreadableSchema = SnapshotPersistence()
            seedUnreadableSchemaSnapshot(schemaName, unreadableSchema)
            unreadableSchema.resetCounts()
            val schemaFailure = assertFailsWith<SqliteException> {
                sqliteWorkerConnectionProvider().openConnection(
                    dbName = schemaName,
                    debug = false,
                    config = SqliteConnectionConfig(persistence = unreadableSchema),
                )
            }
            assertTrue(
                schemaFailure.message.orEmpty().contains("schema", ignoreCase = true),
                schemaFailure.message,
            )
            assertEquals(1, unreadableSchema.loadCalls)
            assertEquals(0, unreadableSchema.persistCalls)
            assertEquals(0, unreadableSchema.clearCalls)
        } finally {
            cleanupMigrationState(integrityName)
            cleanupMigrationState(foreignKeyName)
            cleanupMigrationState(schemaName)
        }
    }

    @Test
    fun strictHealthAndIntentMarkersRejectMalformedNoncanonicalAndMismatchedState() = runTest {
        if (!browserWorkerAvailable()) return@runTest
        val modes = listOf(
            "malformed-health",
            "duplicate-health",
            "unknown-health",
            "mismatched-health",
            "orphan-health",
            "malformed-intent",
            "noncanonical-intent",
            "mismatched-intent",
        )
        modes.forEach { mode ->
            val dbName = phase5bName("marker-$mode")
            try {
                val driver = SqliteWorkerSQLiteDriver.create()
                try {
                    driver.seedMigrationMarkerForTest(dbName, mode)
                    val failure = assertFailsWith<SqliteException>(mode) {
                        driver.open(
                            fileName = dbName,
                            legacySourceMode = "built-in",
                            customPersistence = null,
                        )
                    }
                    assertTrue(
                        failure.message.orEmpty().contains(
                            if (mode == "orphan-health") "Orphan health marker" else "marker",
                            ignoreCase = true,
                        ),
                        failure.message,
                    )
                } finally {
                    driver.shutdown()
                }
            } finally {
                cleanupMigrationState(dbName)
            }
        }
    }

    @Test
    fun concurrentSameTargetOpenUsesOneCommittedMigration() = runTest {
        if (!browserWorkerAvailable()) return@runTest
        val dbName = phase5bName("concurrent-same")
        val source = SnapshotPersistence()
        try {
            seedProductionShapedSnapshot(dbName, source)
            source.resetCounts()
            coroutineScope {
                val first = async {
                    sqliteWorkerConnectionProvider().openConnection(
                        dbName = dbName,
                        debug = false,
                        config = SqliteConnectionConfig(persistence = source),
                    )
                }
                val second = async {
                    sqliteWorkerConnectionProvider().openConnection(
                        dbName = dbName,
                        debug = false,
                        config = SqliteConnectionConfig(persistence = source),
                    )
                }
                val connections = listOf(first.await(), second.await())
                try {
                    connections.forEach { assertProductionFixture(it) }
                } finally {
                    for (connection in connections) connection.close()
                }
            }
            assertTrue(source.loadCalls in 1..2)
            assertEquals(0, source.persistCalls)
            assertEquals(0, source.clearCalls)

            source.resetCounts()
            val committed = sqliteWorkerConnectionProvider().openConnection(
                dbName = dbName,
                debug = false,
                config = SqliteConnectionConfig(persistence = source),
            )
            committed.close()
            assertEquals(0, source.loadCalls)
        } finally {
            cleanupMigrationState(dbName)
        }
    }

    @Test
    fun concurrentDifferentTargetOpensRemainIndependent() = runTest {
        if (!browserWorkerAvailable()) return@runTest
        val firstName = phase5bName("concurrent-first")
        val secondName = phase5bName("concurrent-second")
        val firstSource = SnapshotPersistence()
        val secondSource = SnapshotPersistence()
        try {
            seedProductionShapedSnapshot(firstName, firstSource)
            seedProductionShapedSnapshot(secondName, secondSource)
            firstSource.resetCounts()
            secondSource.resetCounts()

            coroutineScope {
                val first = async {
                    sqliteWorkerConnectionProvider().openConnection(
                        dbName = firstName,
                        debug = false,
                        config = SqliteConnectionConfig(persistence = firstSource),
                    )
                }
                val second = async {
                    sqliteWorkerConnectionProvider().openConnection(
                        dbName = secondName,
                        debug = false,
                        config = SqliteConnectionConfig(persistence = secondSource),
                    )
                }
                val connections = listOf(first.await(), second.await())
                try {
                    connections.forEach { assertProductionFixture(it) }
                } finally {
                    for (connection in connections) connection.close()
                }
            }

            assertEquals(1, firstSource.loadCalls)
            assertEquals(1, secondSource.loadCalls)
            assertEquals(0, firstSource.persistCalls)
            assertEquals(0, secondSource.persistCalls)
            assertEquals(0, firstSource.clearCalls)
            assertEquals(0, secondSource.clearCalls)
        } finally {
            cleanupMigrationState(firstName)
            cleanupMigrationState(secondName)
        }
    }

    @Test
    fun literalMemoryNameIsPersistentAndMigratableOnlyInBrowsers() = runTest {
        if (!browserWorkerAvailable()) return@runTest
        val dbName = ":memory:"
        val source = SnapshotPersistence()
        cleanupMigrationState(dbName)
        try {
            seedProductionShapedSnapshot(phase5bName("memory-source"), source)
            assertNotNull(source.bytes)
            source.resetCounts()
            val migrated = sqliteWorkerConnectionProvider().openConnection(
                dbName = dbName,
                debug = false,
                config = SqliteConnectionConfig(persistence = source),
            )
            try {
                assertProductionFixture(migrated)
                assertEquals(
                    "direct-opfs",
                    (migrated.ref as SqliteWorkerSQLiteConnection).metricsForTest().storageMode,
                )
            } finally {
                migrated.close()
            }
            assertEquals(1, source.loadCalls)

            val reopened = sqliteWorkerConnectionProvider().openConnection(
                dbName = dbName,
                debug = false,
                config = SqliteConnectionConfig(persistence = source),
            )
            try {
                assertProductionFixture(reopened)
            } finally {
                reopened.close()
            }
            assertEquals(1, source.loadCalls)
        } finally {
            cleanupMigrationState(dbName)
        }
    }

    @Test
    fun deterministicHeapSamplesReportMiddlePeakAndFinalEndForCustomAndBuiltInImports() = runTest {
        if (!browserWorkerAvailable()) return@runTest
        val customName = phase5bName("heap-custom")
        val builtInName = phase5bName("heap-builtin")
        val customSource = SnapshotPersistence()
        try {
            seedProductionShapedSnapshot(customName, customSource)
            customSource.resetCounts()
            val customDriver = SqliteWorkerSQLiteDriver.create()
            try {
                assertFailsWith<IllegalArgumentException> {
                    customDriver.setMigrationHeapSamplesForTest(listOf(1L, 2L))
                }
                customDriver.setMigrationHeapSamplesForTest(listOf(100L, 700L, 300L))
                val custom = customDriver.open(
                    fileName = customName,
                    legacySourceMode = "custom",
                    customPersistence = customSource,
                )
                try {
                    val metrics = customDriver.metrics()
                    assertTrue(metrics.migrationHeapAvailable)
                    assertEquals(100L, metrics.migrationHeapStartBytes)
                    assertEquals(700L, metrics.migrationHeapPeakBytes)
                    assertEquals(300L, metrics.migrationHeapEndBytes)
                } finally {
                    custom.close()
                }
            } finally {
                customDriver.shutdown()
            }

            val builtInSource = SnapshotPersistence()
            seedProductionShapedSnapshot(builtInName, builtInSource)
            persistBuiltIn(builtInName, forceOpfs = false, assertNotNull(builtInSource.bytes))
            val builtInDriver = SqliteWorkerSQLiteDriver.create()
            try {
                builtInDriver.setMigrationHeapSamplesForTest(
                    listOf(100L, 400L, 900L, 300L),
                )
                val builtIn = builtInDriver.open(
                    fileName = builtInName,
                    legacySourceMode = "built-in",
                    customPersistence = null,
                )
                try {
                    val metrics = builtInDriver.metrics()
                    assertTrue(metrics.migrationHeapAvailable)
                    assertEquals(100L, metrics.migrationHeapStartBytes)
                    assertEquals(900L, metrics.migrationHeapPeakBytes)
                    assertEquals(300L, metrics.migrationHeapEndBytes)
                } finally {
                    builtIn.close()
                }
            } finally {
                builtInDriver.shutdown()
            }
        } finally {
            legacySnapshotPersistenceForTest(
                builtInName,
                forceOpfs = false,
            ).clear(builtInName)
            cleanupMigrationState(customName)
            cleanupMigrationState(builtInName)
        }
    }

    @Test
    fun largeCustomSnapshotUsesBoundedOneTimeTransfer() = runTest {
        if (!browserWorkerAvailable()) return@runTest
        val dbName = phase5bName("large-64mib")
        val source = SnapshotPersistence()
        try {
            seedAuthenticLegacySnapshot(dbName, source) { connection ->
                connection.execSQL("CREATE TABLE large_payload(value BLOB NOT NULL)")
                connection.execSQL(
                    "INSERT INTO large_payload(value) VALUES (zeroblob(${64 * 1024 * 1024}))",
                )
                connection.execSQL("PRAGMA user_version = 11")
            }
            val bytes = assertNotNull(source.bytes)
            assertTrue(bytes.size >= 64 * 1024 * 1024)
            source.resetCounts()

            val connection = sqliteWorkerConnectionProvider().openConnection(
                dbName = dbName,
                debug = false,
                config = SqliteConnectionConfig(persistence = source),
            )
            try {
                connection.prepare("SELECT length(value) FROM large_payload").use { statement ->
                    assertTrue(statement.step())
                    assertEquals(64L * 1024L * 1024L, statement.getLong(0))
                }
                val metrics =
                    (connection.ref as SqliteWorkerSQLiteConnection).metricsForTest()
                assertEquals(bytes.size.toLong(), metrics.migrationSourceBytes)
                assertTrue(metrics.migrationDurationMillis > 0)
                assertTrue(
                    metrics.migrationPeakOwnedBytes <=
                        2L * metrics.migrationSourceBytes + 1024L * 1024L,
                )
                if (metrics.migrationHeapAvailable) {
                    assertTrue(metrics.migrationHeapStartBytes > 0)
                    assertTrue(metrics.migrationHeapPeakBytes >= metrics.migrationHeapStartBytes)
                    assertTrue(metrics.migrationHeapPeakBytes >= metrics.migrationHeapEndBytes)
                } else {
                    assertEquals(0L, metrics.migrationHeapStartBytes)
                    assertEquals(0L, metrics.migrationHeapPeakBytes)
                    assertEquals(0L, metrics.migrationHeapEndBytes)
                }
                println(
                    "PHASE5B_LARGE dbName=$dbName " +
                        "sourceKind=${metrics.migrationSourceKind} " +
                        "sourceBytes=${metrics.migrationSourceBytes} " +
                        "sourceSha256=${metrics.migrationSourceSha256} " +
                        "durationMillis=${metrics.migrationDurationMillis} " +
                        "peakOwnedBytes=${metrics.migrationPeakOwnedBytes} " +
                        "target=${metrics.migrationTargetFileName} " +
                        "integrity=${metrics.migrationIntegrityCheck} " +
                        "userVersion=${metrics.migrationImportedUserVersion} " +
                        "retained=${metrics.migrationSourceRetained} " +
                        "heapAvailable=${metrics.migrationHeapAvailable} " +
                        "heapStart=${metrics.migrationHeapStartBytes} " +
                        "heapPeak=${metrics.migrationHeapPeakBytes} " +
                        "heapEnd=${metrics.migrationHeapEndBytes} " +
                        "snapshotExports=${metrics.snapshotExports}",
                )
                assertEquals(1, source.loadCalls)
                assertEquals(0, source.persistCalls)
                assertEquals(0, source.clearCalls)
            } finally {
                connection.close()
            }
        } finally {
            cleanupMigrationState(dbName)
        }
    }

    private suspend fun browserWorkerAvailable(): Boolean {
        val driver = SqliteWorkerSQLiteDriver.create()
        return try {
            driver.runtimeKind() == "browser-worker"
        } finally {
            driver.shutdown()
        }
    }

    private suspend fun cleanupMigrationState(dbName: String) {
        val driver = SqliteWorkerSQLiteDriver.create()
        try {
            driver.cleanupMigrationStateForTest(dbName)
        } finally {
            driver.shutdown()
        }
    }

    private suspend fun seedProductionShapedSnapshot(
        dbName: String,
        persistence: SqlitePersistence,
        userVersion: Int = 7,
    ) {
        seedAuthenticLegacySnapshot(dbName, persistence) { connection ->
            connection.execSQL("PRAGMA foreign_keys = ON")
            connection.execSQL(
                "CREATE TABLE phase5b_parent(" +
                    "id INTEGER PRIMARY KEY, exact_value INTEGER NOT NULL, " +
                    "label TEXT NOT NULL, payload BLOB NOT NULL, ratio REAL, nullable TEXT)",
            )
            connection.execSQL(
                "CREATE TABLE phase5b_child(" +
                    "id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL, value TEXT NOT NULL, " +
                    "FOREIGN KEY(parent_id) REFERENCES phase5b_parent(id))",
            )
            connection.execSQL(
                "CREATE INDEX phase5b_child_parent_idx ON phase5b_child(parent_id)",
            )
            connection.execSQL(
                "CREATE TRIGGER phase5b_child_trim AFTER INSERT ON phase5b_child " +
                    "BEGIN UPDATE phase5b_child SET value = trim(value) WHERE id = NEW.id; END",
            )
            connection.execSQL(
                "CREATE VIEW phase5b_view AS " +
                    "SELECT p.id, p.exact_value, c.value FROM phase5b_parent p " +
                    "JOIN phase5b_child c ON c.parent_id = p.id",
            )
            connection.execSQL(
                "INSERT INTO phase5b_parent VALUES " +
                    "(1, 9223372036854775807, 'legacy', X'00FF7F80', 12.5, NULL)",
            )
            connection.execSQL("INSERT INTO phase5b_child VALUES (1, 1, ' child ')")
            connection.execSQL(
                "INSERT INTO phase5b_parent VALUES " +
                    "(2, 9223372036854775806, 'pending-local', X'00FF7F80', -0.0, NULL)",
            )
            connection.execSQL(
                """
                CREATE TABLE _sync_apply_state (
                  singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
                  apply_mode INTEGER NOT NULL DEFAULT 0
                )
                """.trimIndent(),
            )
            connection.execSQL(
                """
                CREATE TABLE _sync_row_state (
                  schema_name TEXT NOT NULL,
                  table_name TEXT NOT NULL,
                  key_json TEXT NOT NULL,
                  row_version INTEGER NOT NULL DEFAULT 0,
                  deleted INTEGER NOT NULL DEFAULT 0,
                  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                  PRIMARY KEY (schema_name, table_name, key_json)
                )
                """.trimIndent(),
            )
            connection.execSQL(
                """
                CREATE TABLE _sync_dirty_rows (
                  schema_name TEXT NOT NULL,
                  table_name TEXT NOT NULL,
                  key_json TEXT NOT NULL,
                  op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
                  base_row_version INTEGER NOT NULL DEFAULT 0,
                  payload TEXT,
                  dirty_ordinal INTEGER NOT NULL DEFAULT 0,
                  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                  PRIMARY KEY (schema_name, table_name, key_json)
                )
                """.trimIndent(),
            )
            connection.execSQL(
                "CREATE INDEX idx_sync_dirty_rows_dirty_ordinal " +
                    "ON _sync_dirty_rows(dirty_ordinal)",
            )
            connection.execSQL(
                """
                CREATE TABLE _sync_snapshot_stage (
                  snapshot_id TEXT NOT NULL,
                  row_ordinal INTEGER NOT NULL,
                  schema_name TEXT NOT NULL,
                  table_name TEXT NOT NULL,
                  key_json TEXT NOT NULL,
                  row_version INTEGER NOT NULL,
                  payload TEXT NOT NULL,
                  PRIMARY KEY (snapshot_id, row_ordinal),
                  UNIQUE (snapshot_id, schema_name, table_name, key_json)
                )
                """.trimIndent(),
            )
            connection.execSQL(
                """
                CREATE TABLE _sync_source_state (
                  source_id TEXT NOT NULL PRIMARY KEY,
                  next_source_bundle_id INTEGER NOT NULL DEFAULT 1,
                  replaced_by_source_id TEXT NOT NULL DEFAULT '',
                  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
                )
                """.trimIndent(),
            )
            connection.execSQL(
                """
                CREATE TABLE _sync_attachment_state (
                  singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
                  current_source_id TEXT NOT NULL DEFAULT '',
                  binding_state TEXT NOT NULL DEFAULT 'anonymous'
                    CHECK (binding_state IN ('anonymous', 'attached')),
                  attached_user_id TEXT NOT NULL DEFAULT '',
                  schema_name TEXT NOT NULL DEFAULT '',
                  last_bundle_seq_seen INTEGER NOT NULL DEFAULT 0,
                  rebuild_required INTEGER NOT NULL DEFAULT 0,
                  pending_initialization_id TEXT NOT NULL DEFAULT ''
                )
                """.trimIndent(),
            )
            connection.execSQL(
                """
                CREATE TABLE _sync_operation_state (
                  singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
                  kind TEXT NOT NULL DEFAULT 'none'
                    CHECK (kind IN ('none', 'remote_replace', 'source_recovery')),
                  target_user_id TEXT NOT NULL DEFAULT '',
                  staged_snapshot_id TEXT NOT NULL DEFAULT '',
                  snapshot_bundle_seq INTEGER NOT NULL DEFAULT 0,
                  snapshot_row_count INTEGER NOT NULL DEFAULT 0,
                  snapshot_byte_count INTEGER NOT NULL DEFAULT 0,
                  snapshot_stage_complete INTEGER NOT NULL DEFAULT 0,
                  reason TEXT NOT NULL DEFAULT '',
                  replacement_source_id TEXT NOT NULL DEFAULT ''
                )
                """.trimIndent(),
            )
            connection.execSQL(
                """
                CREATE TABLE _sync_outbox_bundle (
                  singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
                  canonical_json_contract TEXT NOT NULL
                    CHECK (canonical_json_contract = 'jcs_uniform_numeric_strings_v1'),
                  state TEXT NOT NULL DEFAULT 'none'
                    CHECK (state IN ('none', 'prepared', 'committed_remote')),
                  source_id TEXT NOT NULL DEFAULT '',
                  source_bundle_id INTEGER NOT NULL DEFAULT 0,
                  initialization_id TEXT NOT NULL DEFAULT '',
                  canonical_request_hash TEXT NOT NULL DEFAULT '',
                  row_count INTEGER NOT NULL DEFAULT 0,
                  remote_bundle_hash TEXT NOT NULL DEFAULT '',
                  remote_bundle_seq INTEGER NOT NULL DEFAULT 0
                )
                """.trimIndent(),
            )
            connection.execSQL(
                """
                CREATE TABLE _sync_outbox_rows (
                  source_bundle_id INTEGER NOT NULL,
                  row_ordinal INTEGER NOT NULL,
                  schema_name TEXT NOT NULL,
                  table_name TEXT NOT NULL,
                  key_json TEXT NOT NULL,
                  wire_key_json TEXT NOT NULL,
                  op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
                  base_row_version INTEGER NOT NULL DEFAULT 0,
                  local_payload TEXT,
                  wire_payload TEXT,
                  PRIMARY KEY (source_bundle_id, row_ordinal)
                )
                """.trimIndent(),
            )
            connection.execSQL(
                """
                CREATE TABLE _sync_managed_tables (
                  schema_name TEXT NOT NULL,
                  table_name TEXT NOT NULL,
                  PRIMARY KEY (schema_name, table_name)
                )
                """.trimIndent(),
            )
            connection.execSQL("INSERT INTO _sync_apply_state VALUES (1, 0)")
            connection.execSQL(
                "INSERT INTO _sync_row_state VALUES (" +
                    "'main', 'phase5b_parent', '$PHASE5B_KEY_JSON', 11, 0, " +
                    "'2026-07-25T20:15:30.123Z')",
            )
            connection.execSQL(
                "INSERT INTO _sync_dirty_rows VALUES (" +
                    "'main', 'phase5b_parent', '$PHASE5B_KEY_JSON', 'UPDATE', 11, " +
                    "'$PHASE5B_LOCAL_PAYLOAD', 17, '2026-07-25T20:16:00.456Z')",
            )
            connection.execSQL(
                "INSERT INTO _sync_snapshot_stage VALUES (" +
                    "'phase5b-snapshot-1', 3, 'main', 'phase5b_parent', " +
                    "'$PHASE5B_KEY_JSON', 11, '$PHASE5B_SNAPSHOT_PAYLOAD')",
            )
            connection.execSQL(
                "INSERT INTO _sync_source_state VALUES (" +
                    "'phase5b-source-1', 8, '', '2026-07-25T20:00:00.000Z')",
            )
            connection.execSQL(
                "INSERT INTO _sync_attachment_state VALUES (" +
                    "1, 'phase5b-source-1', 'attached', 'phase5b-user-1', 'main', " +
                    "41, 0, 'phase5b-init-1')",
            )
            connection.execSQL(
                "INSERT INTO _sync_operation_state VALUES (1, 'none', '', '', 0, 0, 0, 0, '', '')",
            )
            connection.execSQL(
                "INSERT INTO _sync_outbox_bundle VALUES (" +
                    "1, 'jcs_uniform_numeric_strings_v1', 'prepared', " +
                    "'phase5b-source-1', 7, 'phase5b-init-1', " +
                    "'$PHASE5B_CANONICAL_REQUEST_HASH', 1, '', 0)",
            )
            connection.execSQL(
                "INSERT INTO _sync_outbox_rows VALUES (" +
                    "7, 1, 'main', 'phase5b_parent', '$PHASE5B_KEY_JSON', " +
                    "'$PHASE5B_WIRE_KEY_JSON', 'UPDATE', 11, " +
                    "'$PHASE5B_LOCAL_PAYLOAD', '$PHASE5B_WIRE_PAYLOAD')",
            )
            connection.execSQL(
                "INSERT INTO _sync_managed_tables VALUES " +
                    "('main', 'phase5b_child'), ('main', 'phase5b_parent')",
            )
            connection.execSQL("PRAGMA user_version = $userVersion")
        }
    }

    private suspend fun seedDifferentSnapshot(
        dbName: String,
        persistence: SqlitePersistence,
        value: String,
    ) {
        seedAuthenticLegacySnapshot(dbName, persistence) { connection ->
            connection.execSQL("CREATE TABLE legacy_only(value TEXT NOT NULL)")
            connection.execSQL("INSERT INTO legacy_only VALUES ('$value')")
        }
    }

    private suspend fun seedFailedIntegritySnapshot(
        dbName: String,
        persistence: SqlitePersistence,
    ) {
        seedAuthenticLegacySnapshot(dbName, persistence) { connection ->
            connection.execSQL("CREATE TABLE orphaned_page(id INTEGER PRIMARY KEY, value TEXT)")
            connection.execSQL("INSERT INTO orphaned_page VALUES (1, 'orphaned')")
            connection.execSQL("PRAGMA writable_schema = ON")
            connection.execSQL("DELETE FROM sqlite_schema WHERE name = 'orphaned_page'")
            connection.execSQL("PRAGMA writable_schema = OFF")
        }
    }

    private suspend fun seedFailedForeignKeySnapshot(
        dbName: String,
        persistence: SqlitePersistence,
    ) {
        seedAuthenticLegacySnapshot(dbName, persistence) { connection ->
            connection.execSQL("PRAGMA foreign_keys = OFF")
            connection.execSQL("CREATE TABLE fk_parent(id INTEGER PRIMARY KEY)")
            connection.execSQL(
                "CREATE TABLE fk_child(" +
                    "id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL, " +
                    "FOREIGN KEY(parent_id) REFERENCES fk_parent(id))",
            )
            connection.execSQL("INSERT INTO fk_child VALUES (1, 404)")
        }
    }

    private suspend fun seedUnreadableSchemaSnapshot(
        dbName: String,
        persistence: SqlitePersistence,
    ) {
        seedAuthenticLegacySnapshot(dbName, persistence) { connection ->
            connection.execSQL("CREATE TABLE unreadable_schema(id INTEGER PRIMARY KEY)")
            connection.execSQL("PRAGMA writable_schema = ON")
            connection.execSQL(
                "UPDATE sqlite_schema SET sql = 'CREATE TABLE unreadable_schema(' " +
                    "WHERE name = 'unreadable_schema'",
            )
            connection.execSQL("PRAGMA writable_schema = OFF")
        }
    }

    private suspend fun seedAuthenticLegacySnapshot(
        dbName: String,
        persistence: SqlitePersistence,
        populate: suspend (SafeSQLiteConnection) -> Unit,
    ) {
        val fixture = createAuthenticLegacySqlJsFixture(dbName)
        try {
            populate(fixture.connection)
            persistence.persist(dbName, fixture.exportBytes())
        } finally {
            fixture.close()
        }
    }

    private suspend fun persistBuiltIn(
        dbName: String,
        forceOpfs: Boolean,
        bytes: ByteArray,
    ) {
        legacySnapshotPersistenceForTest(dbName, forceOpfs)
            .persist(dbName, bytes.copyOf())
    }

    private suspend fun assertProductionFixture(
        connection: SafeSQLiteConnection,
        expectedUserVersion: Int = 7,
    ) {
        connection.prepare(
            "SELECT phase5b_view.exact_value, label, hex(payload), ratio, nullable, " +
                "phase5b_view.value " +
                "FROM phase5b_view JOIN phase5b_parent USING(id)",
        ).use { statement ->
            assertTrue(statement.step())
            assertEquals(Long.MAX_VALUE, statement.getLong(0))
            assertEquals("legacy", statement.getText(1))
            assertEquals("00FF7F80", statement.getText(2))
            assertEquals(12.5, statement.getDouble(3))
            assertTrue(statement.isNull(4))
            assertEquals("child", statement.getText(5))
        }
        connection.prepare(
            "SELECT name FROM sqlite_schema WHERE type = 'table' AND name LIKE '_sync_%' " +
                "ORDER BY name",
        ).use { statement ->
            val names = buildList {
                while (statement.step()) add(statement.getText(0))
            }
            assertEquals(
                listOf(
                    "_sync_apply_state",
                    "_sync_attachment_state",
                    "_sync_dirty_rows",
                    "_sync_managed_tables",
                    "_sync_operation_state",
                    "_sync_outbox_bundle",
                    "_sync_outbox_rows",
                    "_sync_row_state",
                    "_sync_snapshot_stage",
                    "_sync_source_state",
                ),
                names,
            )
        }
        assertProductionControlTableSchemas(connection)
        assertProductionSchemaObjects(connection)
        assertSingleTextRow(connection, "SELECT singleton_key || '|' || apply_mode FROM _sync_apply_state", "1|0")
        assertSingleTextRow(
            connection,
            "SELECT schema_name || '|' || table_name || '|' || key_json || '|' || " +
                "row_version || '|' || deleted || '|' || updated_at FROM _sync_row_state",
            "main|phase5b_parent|$PHASE5B_KEY_JSON|11|0|2026-07-25T20:15:30.123Z",
        )
        assertSingleTextRow(
            connection,
            "SELECT schema_name || '|' || table_name || '|' || key_json || '|' || op || '|' || " +
                "base_row_version || '|' || dirty_ordinal || '|' || updated_at " +
                "FROM _sync_dirty_rows",
            "main|phase5b_parent|$PHASE5B_KEY_JSON|UPDATE|11|17|2026-07-25T20:16:00.456Z",
        )
        assertPayloadBytes(connection, "SELECT CAST(payload AS BLOB) FROM _sync_dirty_rows", PHASE5B_LOCAL_PAYLOAD)
        assertSingleTextRow(
            connection,
            "SELECT snapshot_id || '|' || row_ordinal || '|' || schema_name || '|' || " +
                "table_name || '|' || key_json || '|' || row_version FROM _sync_snapshot_stage",
            "phase5b-snapshot-1|3|main|phase5b_parent|$PHASE5B_KEY_JSON|11",
        )
        assertPayloadBytes(
            connection,
            "SELECT CAST(payload AS BLOB) FROM _sync_snapshot_stage",
            PHASE5B_SNAPSHOT_PAYLOAD,
        )
        assertSingleTextRow(
            connection,
            "SELECT source_id || '|' || next_source_bundle_id || '|' || replaced_by_source_id || " +
                "'|' || created_at FROM _sync_source_state",
            "phase5b-source-1|8||2026-07-25T20:00:00.000Z",
        )
        assertSingleTextRow(
            connection,
            "SELECT singleton_key || '|' || current_source_id || '|' || binding_state || '|' || " +
                "attached_user_id || '|' || schema_name || '|' || last_bundle_seq_seen || '|' || " +
                "rebuild_required || '|' || pending_initialization_id FROM _sync_attachment_state",
            "1|phase5b-source-1|attached|phase5b-user-1|main|41|0|phase5b-init-1",
        )
        assertSingleTextRow(
            connection,
            "SELECT singleton_key || '|' || kind || '|' || target_user_id || '|' || " +
                "staged_snapshot_id || '|' || snapshot_bundle_seq || '|' || snapshot_row_count || " +
                "'|' || snapshot_byte_count || '|' || snapshot_stage_complete || '|' || reason || " +
                "'|' || replacement_source_id FROM _sync_operation_state",
            "1|none|||0|0|0|0||",
        )
        assertSingleTextRow(
            connection,
            "SELECT singleton_key || '|' || canonical_json_contract || '|' || state || '|' || " +
                "source_id || '|' || source_bundle_id || '|' || initialization_id || '|' || " +
                "canonical_request_hash || '|' || row_count || '|' || remote_bundle_hash || '|' || " +
                "remote_bundle_seq FROM _sync_outbox_bundle",
            "1|jcs_uniform_numeric_strings_v1|prepared|phase5b-source-1|7|phase5b-init-1|" +
                "$PHASE5B_CANONICAL_REQUEST_HASH|1||0",
        )
        assertSingleTextRow(
            connection,
            "SELECT source_bundle_id || '|' || row_ordinal || '|' || schema_name || '|' || " +
                "table_name || '|' || key_json || '|' || wire_key_json || '|' || op || '|' || " +
                "base_row_version FROM _sync_outbox_rows",
            "7|1|main|phase5b_parent|$PHASE5B_KEY_JSON|$PHASE5B_WIRE_KEY_JSON|UPDATE|11",
        )
        assertPayloadBytes(
            connection,
            "SELECT CAST(local_payload AS BLOB) FROM _sync_outbox_rows",
            PHASE5B_LOCAL_PAYLOAD,
        )
        assertPayloadBytes(
            connection,
            "SELECT CAST(wire_payload AS BLOB) FROM _sync_outbox_rows",
            PHASE5B_WIRE_PAYLOAD,
        )
        connection.prepare(
            "SELECT schema_name || '|' || table_name FROM _sync_managed_tables " +
                "ORDER BY schema_name, table_name",
        ).use { statement ->
            val rows = buildList {
                while (statement.step()) add(statement.getText(0))
            }
            assertEquals(
                listOf("main|phase5b_child", "main|phase5b_parent"),
                rows,
            )
        }
        assertSingleTextRow(
            connection,
            "SELECT exact_value || '|' || label || '|' || hex(payload) || '|' || " +
                "printf('%.1f', ratio) || '|' || ifnull(nullable, '') " +
                "FROM phase5b_parent WHERE id = 2",
            "9223372036854775806|pending-local|00FF7F80|0.0|",
        )
        connection.prepare("PRAGMA user_version").use { statement ->
            assertTrue(statement.step())
            assertEquals(expectedUserVersion.toLong(), statement.getLong(0))
        }
    }

    private suspend fun assertProductionSchemaObjects(connection: SafeSQLiteConnection) {
        connection.prepare("PRAGMA foreign_key_list('phase5b_child')").use { statement ->
            val foreignKeys = buildList {
                while (statement.step()) {
                    add(
                        "${statement.getLong(0)}|${statement.getLong(1)}|" +
                            "${statement.getText(2)}|${statement.getText(3)}|" +
                            "${statement.getText(4)}|${statement.getText(5)}|" +
                            "${statement.getText(6)}|${statement.getText(7)}",
                    )
                }
            }
            assertEquals(
                listOf("0|0|phase5b_parent|parent_id|id|NO ACTION|NO ACTION|NONE"),
                foreignKeys,
            )
        }
        connection.prepare("PRAGMA foreign_key_check").use { statement ->
            assertFalse(statement.step(), "production fixture foreign_key_check")
        }
        assertIndex(
            connection = connection,
            tableName = "phase5b_child",
            indexName = "phase5b_child_parent_idx",
            expectedColumn = "0|1|parent_id",
        )
        assertIndex(
            connection = connection,
            tableName = "_sync_dirty_rows",
            indexName = "idx_sync_dirty_rows_dirty_ordinal",
            expectedColumn = "0|6|dirty_ordinal",
        )
        connection.prepare(
            "SELECT type || '|' || name || '|' || tbl_name FROM sqlite_schema " +
                "WHERE name IN (" +
                "'phase5b_child_parent_idx', " +
                "'idx_sync_dirty_rows_dirty_ordinal', " +
                "'phase5b_child_trim', " +
                "'phase5b_view'" +
                ") AND sql IS NOT NULL ORDER BY type, name",
        ).use { statement ->
            val schemaEntries = buildList {
                while (statement.step()) add(statement.getText(0))
            }
            assertEquals(
                listOf(
                    "index|idx_sync_dirty_rows_dirty_ordinal|_sync_dirty_rows",
                    "index|phase5b_child_parent_idx|phase5b_child",
                    "trigger|phase5b_child_trim|phase5b_child",
                    "view|phase5b_view|phase5b_view",
                ),
                schemaEntries,
            )
        }
    }

    private suspend fun assertIndex(
        connection: SafeSQLiteConnection,
        tableName: String,
        indexName: String,
        expectedColumn: String,
    ) {
        connection.prepare("PRAGMA index_list('$tableName')").use { statement ->
            val properties = buildList {
                while (statement.step()) {
                    if (statement.getText(1) == indexName) {
                        add(
                            "${statement.getLong(2)}|${statement.getText(3)}|" +
                                statement.getLong(4),
                        )
                    }
                }
            }
            assertEquals(listOf("0|c|0"), properties, indexName)
        }
        connection.prepare("PRAGMA index_info('$indexName')").use { statement ->
            val columns = buildList {
                while (statement.step()) {
                    add(
                        "${statement.getLong(0)}|${statement.getLong(1)}|" +
                            statement.getText(2),
                    )
                }
            }
            assertEquals(listOf(expectedColumn), columns, indexName)
        }
    }

    private suspend fun awaitCompletedWorkerCommand(
        driver: SqliteWorkerSQLiteDriver,
        command: String,
    ) {
        repeat(10_000) {
            val completedCommands = sqliteWorkerJson.parseToJsonElement(
                driver.diagnosticsForTest(),
            ).jsonObject.getValue("completedCommands").jsonArray
                .map { it.jsonPrimitive.content }
            if (completedCommands.any { it.startsWith("$command:") }) return
            yield()
        }
        assertTrue(
            false,
            "Worker did not retain the expected completed $command response.",
        )
    }

    private suspend fun assertProductionControlTableSchemas(connection: SafeSQLiteConnection) {
        val expected = mapOf(
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
        expected.forEach { (tableName, columns) ->
            connection.prepare("PRAGMA table_info('$tableName')").use { statement ->
                val actual = buildList {
                    while (statement.step()) {
                        add(
                            "${statement.getText(1)}|${statement.getText(2)}|" +
                                "${statement.getLong(3)}|${statement.getLong(5)}",
                        )
                    }
                }
                assertEquals(columns, actual, tableName)
            }
        }
    }

    private suspend fun assertSingleTextRow(
        connection: SafeSQLiteConnection,
        sql: String,
        expected: String,
    ) {
        connection.prepare(sql).use { statement ->
            assertTrue(statement.step(), sql)
            assertEquals(expected, statement.getText(0), sql)
            assertFalse(statement.step(), sql)
        }
    }

    private suspend fun assertPayloadBytes(
        connection: SafeSQLiteConnection,
        sql: String,
        expected: String,
    ) {
        connection.prepare(sql).use { statement ->
            assertTrue(statement.step(), sql)
            assertTrue(
                statement.getBlob(0).contentEquals(expected.encodeToByteArray()),
                sql,
            )
            assertFalse(statement.step(), sql)
        }
    }
}

private data class BuiltInScenario(
    val name: String,
    val forceOpfs: Boolean,
)

private class Phase5BDatabase(
    dbName: String,
    connectionProvider: dev.goquick.sqlitenow.core.SqliteConnectionProvider =
        BundledSqliteConnectionProvider,
) : SqliteNowDatabase(
    dbName = dbName,
    migration = Phase5BNoopMigrations,
    connectionProvider = connectionProvider,
)

private object Phase5BNoopMigrations : DatabaseMigrations {
    override suspend fun applyMigration(
        conn: SafeSQLiteConnection,
        currentVersion: Int,
    ): Int = currentVersion
}

internal expect suspend fun legacySnapshotExistsForTest(
    dbName: String,
    forceOpfs: Boolean,
): Boolean

internal expect suspend fun workerStorageArtifactsForTest(dbName: String): Set<String>

private const val PHASE5B_KEY_JSON = """{"id":"2"}"""
private const val PHASE5B_WIRE_KEY_JSON = """{"id":"2"}"""
private const val PHASE5B_LOCAL_PAYLOAD =
    """{"exact_value":"9223372036854775806","id":"2","label":"pending-local","nullable":null,"payload":"AP9/gA==","ratio":"-0.0"}"""
private const val PHASE5B_WIRE_PAYLOAD =
    """{"exact_value":"9223372036854775806","id":"2","label":"pending-local","nullable":null,"payload":"AP9/gA==","ratio":"0"}"""
private const val PHASE5B_SNAPSHOT_PAYLOAD =
    """{"exact_value":"9223372036854775805","id":"2","label":"remote-base","nullable":null,"payload":"AP9/gA==","ratio":"0"}"""
private const val PHASE5B_CANONICAL_REQUEST_HASH =
    "4f2d8d9ca566f61250765792f6237cf5105063a0db144a8aa72dd5fda5e3ea1c"

private class SnapshotPersistence : SqlitePersistence {
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

    fun resetCounts() {
        loadCalls = 0
        persistCalls = 0
        clearCalls = 0
    }
}

private class ThrowingLoadPersistence : SqlitePersistence {
    var loadCalls = 0
    var persistCalls = 0
    var clearCalls = 0

    override suspend fun load(dbName: String): ByteArray? {
        loadCalls++
        error("controlled custom load failure")
    }

    override suspend fun persist(dbName: String, bytes: ByteArray) {
        persistCalls++
    }

    override suspend fun clear(dbName: String) {
        clearCalls++
    }
}

private class BlockingLoadPersistence : SqlitePersistence {
    val started = CompletableDeferred<Unit>()
    var loadCalls = 0
    var persistCalls = 0
    var clearCalls = 0

    override suspend fun load(dbName: String): ByteArray? {
        loadCalls++
        started.complete(Unit)
        awaitCancellation()
    }

    override suspend fun persist(dbName: String, bytes: ByteArray) {
        persistCalls++
    }

    override suspend fun clear(dbName: String) {
        clearCalls++
    }
}

private fun phase5bName(scenario: String): String =
    "__sqlitenow_phase5b_${scenario}_${Random.nextInt().toUInt()}"
