package dev.goquick.sqlitenow.core.test

import dev.goquick.sqlitenow.core.DatabaseMigrations
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.SqliteNowDatabase
import dev.goquick.sqlitenow.core.SqlitePersistence
import dev.goquick.sqlitenow.core.createAuthenticLegacySqlJsFixture
import dev.goquick.sqlitenow.core.test.migration.db.VersionBasedDatabaseMigrations
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest

class GeneratedWorkerLegacyMigrationTest {
    @Test
    fun generatedUpgradeRunsAfterHealthyImportAndRetriesWithoutReloadingLegacy() = runTest {
        if (!generatedWorkerLegacyMigrationBrowserAvailable()) return@runTest
        val dbName =
            "__sqlitenow_phase5b_generated_${Random.nextInt().toUInt()}"
        val source = GeneratedSnapshotPersistence()
        seedVersionOneSnapshot(dbName, source)
        val sourceBytes = assertNotNull(source.bytes).size
        source.resetCounts()

        val generated = VersionBasedDatabaseMigrations()
        val failOnceAfterGeneratedUpgrade = object : DatabaseMigrations {
            private var fail = true

            override suspend fun applyMigration(
                conn: SafeSQLiteConnection,
                currentVersion: Int,
            ): Int {
                val version = generated.applyMigration(conn, currentVersion)
                if (fail) {
                    fail = false
                    error("controlled failure after generated upgrade")
                }
                return version
            }
        }

        try {
            val first = GeneratedMigrationDatabase(
                dbName = dbName,
                migration = failOnceAfterGeneratedUpgrade,
            )
            first.connectionConfig = SqliteConnectionConfig(persistence = source)
            val failure = assertFailsWith<IllegalStateException> { first.open() }
            assertTrue(failure.message.orEmpty().contains("controlled failure"))
            assertEquals(1, source.loadCalls)
            assertEquals(0, source.persistCalls)
            assertEquals(0, source.clearCalls)

            val retried = GeneratedMigrationDatabase(
                dbName = dbName,
                migration = failOnceAfterGeneratedUpgrade,
            )
            retried.connectionConfig = SqliteConnectionConfig(persistence = source)
            retried.open()
            try {
                val connection = retried.connection()
                assertEquals(3, connection.readUserVersion())
                assertEquals("Ada", connection.scalarText("SELECT name FROM migration_probe"))
                assertEquals(
                    "migrated",
                    connection.scalarText("SELECT created_at FROM migration_probe"),
                )
                assertTrue(connection.columnNames("migration_probe").contains("nickname"))
                assertFalse(
                    connection.scalarLong(
                        "SELECT COUNT(*) FROM migration_meta WHERE key = 'seed'",
                    ) > 0,
                )
                assertEquals(sourceBytes, assertNotNull(source.bytes).size)
            } finally {
                retried.close()
            }

            assertEquals(1, source.loadCalls)
            assertEquals(0, source.persistCalls)
            assertEquals(0, source.clearCalls)
        } finally {
            cleanupGeneratedWorkerMigrationState(dbName)
        }
    }

    private suspend fun seedVersionOneSnapshot(
        dbName: String,
        persistence: GeneratedSnapshotPersistence,
    ) {
        val fixture = createAuthenticLegacySqlJsFixture(dbName)
        val connection = fixture.connection
        try {
            connection.execSQL(
                "CREATE TABLE migration_probe(" +
                    "id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL)",
            )
            connection.execSQL(
                "INSERT INTO migration_probe(id, name) VALUES (1, 'Ada')",
            )
            connection.execSQL("PRAGMA user_version = 1")
            persistence.persist(dbName, fixture.exportBytes())
        } finally {
            fixture.close()
        }
    }
}

internal expect suspend fun cleanupGeneratedWorkerMigrationState(dbName: String)

internal expect fun generatedWorkerLegacyMigrationBrowserAvailable(): Boolean

private class GeneratedMigrationDatabase(
    dbName: String,
    migration: DatabaseMigrations,
) : SqliteNowDatabase(
    dbName = dbName,
    migration = migration,
    debug = false,
)

private class GeneratedSnapshotPersistence : SqlitePersistence {
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

private suspend fun SafeSQLiteConnection.scalarLong(sql: String): Long =
    prepare(sql).use { statement ->
        assertTrue(statement.step())
        statement.getLong(0)
    }

private suspend fun SafeSQLiteConnection.scalarText(sql: String): String =
    prepare(sql).use { statement ->
        assertTrue(statement.step())
        statement.getText(0)
    }

private suspend fun SafeSQLiteConnection.columnNames(tableName: String): List<String> =
    prepare("PRAGMA table_info($tableName)").use { statement ->
        buildList {
            while (statement.step()) add(statement.getText(1))
        }
    }

private suspend fun SafeSQLiteConnection.readUserVersion(): Int =
    scalarLong("PRAGMA user_version").toInt()
