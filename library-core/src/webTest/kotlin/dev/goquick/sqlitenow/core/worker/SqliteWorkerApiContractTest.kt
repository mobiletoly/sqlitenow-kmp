package dev.goquick.sqlitenow.core.worker

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.DatabaseMigrations
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.SqliteConnectionProvider
import dev.goquick.sqlitenow.core.SqliteNowDatabase
import dev.goquick.sqlitenow.core.SqlitePersistence
import dev.goquick.sqlitenow.core.persistence.IndexedDbSqlitePersistence
import dev.goquick.sqlitenow.core.sqlite.SqliteException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.random.Random
import kotlinx.coroutines.test.runTest

class SqliteWorkerApiContractTest {
    @Test
    fun acceptedL16PreservesDefaultAndCustomProviderSourceShapes() {
        val config = SqliteConnectionConfig()
        assertNull(config.persistence)
        assertEquals(true, config.autoFlushPersistence)
        assertNull(config.executionContextHook)

        val provider: SqliteConnectionProvider = existingCustomProvider
        assertEquals(existingCustomProvider, provider)
        assertEquals(BundledSqliteConnectionProvider, BundledSqliteConnectionProvider)

        ExistingProviderDatabase(":memory:", NoopMigrations)
        ExistingDefaultDatabase(":memory:", NoopMigrations)

        val indexedDbPersistence: SqlitePersistence = IndexedDbSqlitePersistence()
        assertNotNull(indexedDbPersistence)
        val persistSnapshot:
            suspend SqliteNowDatabase.() -> Unit = SqliteNowDatabase::persistSnapshotNow
        assertNotNull(persistSnapshot)
    }

    @Test
    fun acceptedL16InternalProviderCompilesAndKeepsTheDefaultShapeUnchanged() = runTest {
        val provider: SqliteConnectionProvider = SqliteWorkerConnectionProvider()
        val connection = provider.openConnection(
            dbName = "phase3-api-contract",
            debug = false,
            config = SqliteConnectionConfig(),
        )
        try {
            connection.prepare("SELECT 42").use { statement ->
                assertTrue(statement.step())
                assertEquals(42L, statement.getLong(0))
            }
        } finally {
            connection.close()
        }

        assertEquals(BundledSqliteConnectionProvider, BundledSqliteConnectionProvider)
    }

    @Test
    fun ordinaryDefaultConstructionSelectsObservedWorkerRuntimeAndStorage() = runTest {
        val dbName = ":memory:phase7-default-${Random.nextInt().toUInt()}"
        val tableName = "phase7_default_${Random.nextInt().toUInt()}"
        try {
            val first = ExistingDefaultDatabase(dbName, NoopMigrations)
            first.open()
            val firstWorker = first.connection().ref as SqliteWorkerSQLiteConnection
            val firstMetrics = firstWorker.metricsForTest()
            val expectedStorage = when (firstMetrics.runtimeKind) {
                "js-node-worker" -> "memory"
                "browser-worker" -> "direct-opfs"
                else -> error("Unexpected default runtime: ${firstMetrics.runtimeKind}")
            }
            assertEquals(expectedStorage, firstMetrics.storageMode)
            assertEquals(0, firstMetrics.snapshotExports)
            first.connection().execSQL("CREATE TABLE $tableName(value INTEGER NOT NULL)")
            first.connection().execSQL(
                "INSERT INTO $tableName(value) VALUES (9223372036854775807)",
            )
            first.persistSnapshotNow()
            assertEquals(0, firstWorker.metricsForTest().snapshotExports)
            first.close()

            val reopened = ExistingDefaultDatabase(dbName, NoopMigrations)
            reopened.open()
            try {
                val reopenedWorker =
                    reopened.connection().ref as SqliteWorkerSQLiteConnection
                val reopenedMetrics = reopenedWorker.metricsForTest()
                assertEquals(firstMetrics.runtimeKind, reopenedMetrics.runtimeKind)
                assertEquals(expectedStorage, reopenedMetrics.storageMode)
                assertEquals(0, reopenedMetrics.snapshotExports)
                if (firstMetrics.runtimeKind == "browser-worker") {
                    reopened.connection().prepare(
                        "SELECT value FROM $tableName",
                    ).use { statement ->
                        assertTrue(statement.step())
                        assertEquals(Long.MAX_VALUE, statement.getLong(0))
                    }
                } else {
                    assertFailsWith<SqliteException> {
                        reopened.connection().prepare("SELECT value FROM $tableName")
                    }
                }
            } finally {
                reopened.close()
            }
        } finally {
            cleanupDefaultWorkerState(dbName)
        }
    }

    @Test
    fun missingBrowserCapabilitiesFailClosedWithoutCreatingStorage() = runTest {
        val runtimeProbe = SqliteWorkerSQLiteDriver.create()
        val isBrowser = try {
            runtimeProbe.runtimeKind() == "browser-worker"
        } finally {
            runtimeProbe.shutdown()
        }
        if (!isBrowser) return@runTest

        val scenarios = listOf(
            BrowserCapabilityScenario(
                name = "browser-policy",
                startupMode = "missing-browser-policy",
                expectedCapability = "required browser policy",
                expectedFallback = "no transient fallback was started",
                failsDuringStartup = true,
            ),
            BrowserCapabilityScenario(
                name = "web-crypto",
                startupMode = "missing-web-crypto",
                expectedCapability = "Web Crypto",
            ),
            BrowserCapabilityScenario(
                name = "opfs",
                startupMode = "missing-opfs",
                expectedCapability = "Origin Private File System",
            ),
            BrowserCapabilityScenario(
                name = "web-locks",
                startupMode = "missing-web-locks",
                expectedCapability = "Web Locks",
            ),
            BrowserCapabilityScenario(
                name = "opfs-vfs",
                startupMode = "missing-opfs-vfs",
                expectedCapability = "SQLite OPFS VFS",
            ),
        )

        scenarios.forEach { scenario ->
            val dbName =
                "phase7-capability-${scenario.name}-${Random.nextInt().toUInt()}"
            assertNoWorkerOrLegacyStorage(dbName, scenario.name)
            var driver: SqliteWorkerSQLiteDriver? = null
            try {
                val failure = if (scenario.failsDuringStartup) {
                    assertFails(scenario.name) {
                        SqliteWorkerSQLiteDriver.create(
                            startupModeForTest = scenario.startupMode,
                        )
                    }
                } else {
                    val capabilityDriver = SqliteWorkerSQLiteDriver.create(
                        startupModeForTest = scenario.startupMode,
                    )
                    driver = capabilityDriver
                    assertEquals("browser-worker", capabilityDriver.runtimeKind(), scenario.name)
                    assertFailsWith<SqliteException>(scenario.name) {
                        capabilityDriver.open(dbName)
                    }
                }
                val failureMessages = joinedFailureMessages(failure)
                assertTrue(
                    failureMessages.contains(scenario.expectedCapability),
                    "${scenario.name}: $failureMessages",
                )
                assertTrue(
                    failureMessages.contains(scenario.expectedFallback),
                    "${scenario.name}: $failureMessages",
                )
                driver?.metrics()?.let { metrics ->
                    assertEquals("none", metrics.storageMode, scenario.name)
                    assertEquals(0, metrics.liveDatabases, scenario.name)
                    assertEquals(0, metrics.snapshotExports, scenario.name)
                }
                assertNoWorkerOrLegacyStorage(dbName, scenario.name)
            } finally {
                driver?.shutdown()
                cleanupDefaultWorkerState(dbName)
            }
        }
    }

    @Test
    fun ordinaryDefaultCustomPersistenceIsMigrationLoadOnlyAndRejectedByNode() = runTest {
        val dbName = "phase7-default-custom-${Random.nextInt().toUInt()}"
        val persistence = CountingPersistence()
        val runtimeProbe = SqliteWorkerSQLiteDriver.create()
        val runtimeKind = try {
            runtimeProbe.runtimeKind()
        } finally {
            runtimeProbe.shutdown()
        }

        try {
            val database = ExistingDefaultDatabase(dbName, NoopMigrations)
            database.connectionConfig = SqliteConnectionConfig(
                persistence = persistence,
                autoFlushPersistence = true,
            )
            if (runtimeKind == "js-node-worker") {
                val failure = assertFailsWith<SqliteException> {
                    database.open()
                }
                assertTrue(failure.message.orEmpty().contains("browser-only"))
                assertEquals(0, persistence.loadCalls)
                assertEquals(0, persistence.persistCalls)
                assertEquals(0, persistence.clearCalls)
                return@runTest
            }

            database.open()
            try {
                val worker = database.connection().ref as SqliteWorkerSQLiteConnection
                assertEquals("browser-worker", worker.metricsForTest().runtimeKind)
                assertEquals("direct-opfs", worker.metricsForTest().storageMode)
                assertEquals(1, persistence.loadCalls)
                database.connection().execSQL(
                    "CREATE TABLE phase7_custom(value TEXT NOT NULL)",
                )
                database.connection().execSQL(
                    "INSERT INTO phase7_custom VALUES ('ordinary-write')",
                )
                database.persistSnapshotNow()
                assertEquals(0, worker.metricsForTest().snapshotExports)
            } finally {
                database.close()
            }
            assertEquals(1, persistence.loadCalls)
            assertEquals(0, persistence.persistCalls)
            assertEquals(0, persistence.clearCalls)
        } finally {
            cleanupDefaultWorkerState(dbName)
        }
    }

    @Test
    fun acceptedL17PublicFactorySupportsPackagedAndRelativeWorkerModules() = runTest {
        val scenarios = listOf(
            WorkerFactoryScenario(
                name = "packaged default",
                workerModuleUrl = null,
            ),
            WorkerFactoryScenario(
                name = "relative override",
                workerModuleUrl = "./sqlite-3.53.0-build1/worker.mjs",
            ),
        )

        scenarios.forEach { scenario ->
            val provider: SqliteConnectionProvider =
                sqliteWorkerConnectionProvider(scenario.workerModuleUrl)
            val connection = provider.openConnection(
                dbName = "phase4-api-contract-${scenario.name}",
                debug = false,
                config = SqliteConnectionConfig(),
            )
            try {
                connection.prepare("SELECT 42").use { statement ->
                    assertTrue(statement.step(), scenario.name)
                    assertEquals(42L, statement.getLong(0), scenario.name)
                }
            } finally {
                connection.close()
            }
        }

        assertFailsWith<IllegalArgumentException> {
            sqliteWorkerConnectionProvider(" ")
        }
        assertEquals(BundledSqliteConnectionProvider, BundledSqliteConnectionProvider)
    }

    @Test
    fun optInProviderPersistsOnlyThroughBrowserDirectVfs() = runTest {
        val dbName = ":memory:"
        val tableName = "phase5a_direct_${Random.nextInt().toUInt()}"
        val provider = sqliteWorkerConnectionProvider()
        val first = provider.openConnection(
            dbName = dbName,
            debug = false,
            config = SqliteConnectionConfig(),
        )
        val workerConnection = first.ref as SqliteWorkerSQLiteConnection
        val firstMetrics = workerConnection.metricsForTest()
        val runtimeKind = firstMetrics.runtimeKind
        assertEquals(
            if (runtimeKind == "browser-worker") "direct-opfs" else "memory",
            firstMetrics.storageMode,
        )
        assertEquals(0, firstMetrics.snapshotExports)
        try {
            first.execSQL("CREATE TABLE $tableName(value INTEGER NOT NULL)")
            first.execSQL(
                "INSERT INTO $tableName(value) VALUES (9223372036854775807)",
            )
            first.persistSnapshotNow()
            assertEquals(
                0,
                workerConnection.metricsForTest().snapshotExports,
            )
        } finally {
            first.close()
        }

        val reopened = provider.openConnection(
            dbName = dbName,
            debug = false,
            config = SqliteConnectionConfig(),
        )
        try {
            if (runtimeKind == "browser-worker") {
                reopened.prepare("SELECT value FROM $tableName").use { statement ->
                    assertTrue(statement.step())
                    assertEquals(Long.MAX_VALUE, statement.getLong(0))
                }
            } else {
                assertEquals("js-node-worker", runtimeKind)
                assertFailsWith<SqliteException> {
                    reopened.prepare("SELECT value FROM $tableName")
                }
            }
        } finally {
            reopened.close()
        }
    }

    @Test
    fun acceptedL17PublicFactoryRejectsForbiddenWorkerSchemes() = runTest {
        val scenarios = listOf(
            "blob:https://example.test/sqlitenow-worker.mjs",
            "data:text/javascript,export default null",
        )

        scenarios.forEach { workerModuleUrl ->
            val provider = sqliteWorkerConnectionProvider(workerModuleUrl)
            val failure = assertFailsWith<SqliteException>(workerModuleUrl) {
                provider.openConnection(
                    dbName = "phase4-forbidden-worker-scheme",
                    debug = false,
                    config = SqliteConnectionConfig(),
                )
            }
            assertTrue(failure.message.orEmpty().contains("scheme"))
            assertTrue(failure.message.orEmpty().contains("forbidden"))
        }
    }
}

private suspend fun cleanupDefaultWorkerState(dbName: String) {
    val driver = SqliteWorkerSQLiteDriver.create()
    try {
        if (driver.runtimeKind() == "browser-worker") {
            driver.cleanupMigrationStateForTest(dbName)
        }
    } finally {
        driver.shutdown()
    }
}

private data class WorkerFactoryScenario(
    val name: String,
    val workerModuleUrl: String?,
)

private data class BrowserCapabilityScenario(
    val name: String,
    val startupMode: String,
    val expectedCapability: String,
    val expectedFallback: String =
        "no snapshot or in-memory browser fallback was started",
    val failsDuringStartup: Boolean = false,
)

private suspend fun assertNoWorkerOrLegacyStorage(
    dbName: String,
    scenario: String,
) {
    assertEquals(emptySet(), workerStorageArtifactsForTest(dbName), scenario)
    assertEquals(false, legacySnapshotExistsForTest(dbName, forceOpfs = true), scenario)
    assertEquals(false, legacySnapshotExistsForTest(dbName, forceOpfs = false), scenario)
}

private fun joinedFailureMessages(failure: Throwable): String = buildString {
    var current: Throwable? = failure
    while (current != null) {
        if (isNotEmpty()) append(" | ")
        append(current.message.orEmpty())
        current = current.cause
    }
}

private class CountingPersistence : SqlitePersistence {
    var loadCalls: Int = 0
    var persistCalls: Int = 0
    var clearCalls: Int = 0

    override suspend fun load(dbName: String): ByteArray? {
        loadCalls++
        return null
    }

    override suspend fun persist(dbName: String, bytes: ByteArray) {
        persistCalls++
    }

    override suspend fun clear(dbName: String) {
        clearCalls++
    }
}

private val existingCustomProvider = SqliteConnectionProvider { dbName, debug, config ->
    BundledSqliteConnectionProvider.openConnection(dbName, debug, config)
}

private class ExistingProviderDatabase(
    dbName: String,
    migration: DatabaseMigrations,
) : SqliteNowDatabase(
    dbName = dbName,
    migration = migration,
    connectionProvider = existingCustomProvider,
)

private class ExistingDefaultDatabase(
    dbName: String,
    migration: DatabaseMigrations,
) : SqliteNowDatabase(
    dbName = dbName,
    migration = migration,
)

private object NoopMigrations : DatabaseMigrations {
    override suspend fun applyMigration(
        conn: SafeSQLiteConnection,
        currentVersion: Int,
    ): Int = currentVersion
}
