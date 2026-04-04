package dev.goquick.sqlitenow.core.test

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.core.test.migration.db.MigrationFixtureDatabase
import dev.goquick.sqlitenow.core.test.migration.db.VersionBasedDatabaseMigrations
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.assertFailsWith
import kotlinx.coroutines.test.runTest

class MigrationFixtureUpgradeTest {

    @Test
    fun freshBootstrapCreatesLatestSchemaAndRunsInit() = runTest {
        val dbPath = createTempDirectory("migration-fixture-fresh").resolve("fresh.db").toString()
        val database = migrationFixtureDatabase(dbPath)

        try {
            database.open()

            assertEquals(3, database.currentUserVersion())
            assertEquals("fresh-only", database.scalarText("SELECT value FROM migration_meta WHERE key = 'seed'"))
            assertEquals(1L, database.scalarLong("SELECT COUNT(*) FROM migration_meta WHERE key = 'seed'"))
            assertTrue(database.columnNames("migration_probe").contains("created_at"))
        } finally {
            database.close()
        }
    }

    @Test
    fun upgradeFromVersion1PreservesRowsAndSkipsInit() = runTest {
        val dbPath = createTempDirectory("migration-fixture-v1").resolve("upgrade-v1.db").toString()
        seedVersion1Database(dbPath)

        val database = migrationFixtureDatabase(dbPath)
        try {
            database.open()

            assertEquals(3, database.currentUserVersion())
            assertEquals("Ada", database.scalarText("SELECT name FROM migration_probe WHERE id = 1"))
            assertEquals("migrated", database.scalarText("SELECT created_at FROM migration_probe WHERE id = 1"))
            assertEquals(0L, database.scalarLong("SELECT COUNT(*) FROM migration_meta WHERE key = 'seed'"))
        } finally {
            database.close()
        }
    }

    @Test
    fun upgradeFromVersion2PreservesExistingData() = runTest {
        val dbPath = createTempDirectory("migration-fixture-v2").resolve("upgrade-v2.db").toString()
        seedVersion2Database(dbPath)

        val database = migrationFixtureDatabase(dbPath)
        try {
            database.open()

            assertEquals(3, database.currentUserVersion())
            assertEquals("Lovelace", database.scalarText("SELECT nickname FROM migration_probe WHERE id = 1"))
            assertEquals("migrated", database.scalarText("SELECT created_at FROM migration_probe WHERE id = 1"))
            assertEquals(0L, database.scalarLong("SELECT COUNT(*) FROM migration_meta WHERE key = 'seed'"))
        } finally {
            database.close()
        }
    }

    @Test
    fun reopeningUpgradedDatabaseDoesNotRerunInit() = runTest {
        val dbPath = createTempDirectory("migration-fixture-reopen").resolve("reopen.db").toString()

        migrationFixtureDatabase(dbPath).useOpen { database ->
            database.open()
            assertEquals(1L, database.scalarLong("SELECT COUNT(*) FROM migration_meta WHERE key = 'seed'"))
        }

        migrationFixtureDatabase(dbPath).useOpen { database ->
            database.open()
            assertEquals(1L, database.scalarLong("SELECT COUNT(*) FROM migration_meta WHERE key = 'seed'"))
            assertEquals(3, database.currentUserVersion())
        }
    }

    @Test
    fun failingMigrationRollsBackAndLeavesVersionUntouched() = runTest {
        val dbPath = createTempDirectory("migration-fixture-failure").resolve("failure.db").toString()
        seedConflictingVersion2Database(dbPath)

        val database = migrationFixtureDatabase(dbPath)
        val error = assertFailsWith<Throwable> {
            database.open()
        }
        assertTrue(error.message.orEmpty().contains("migration_meta", ignoreCase = true))

        val verificationConnection = BundledSqliteConnectionProvider.openConnection(dbPath, debug = false)
        try {
            assertEquals(2, verificationConnection.readUserVersion())
            assertFalse(columnNames(verificationConnection, "migration_probe").contains("created_at"))
        } finally {
            verificationConnection.close()
        }
    }

    private fun migrationFixtureDatabase(dbPath: String): MigrationFixtureDatabase =
        MigrationFixtureDatabase(
            dbName = dbPath,
            migration = VersionBasedDatabaseMigrations(),
            debug = true,
        )

    private suspend fun seedVersion1Database(dbPath: String) {
        val connection = BundledSqliteConnectionProvider.openConnection(dbPath, debug = false)
        try {
            connection.execSQL(
                """
                    CREATE TABLE migration_probe (
                        id INTEGER PRIMARY KEY NOT NULL,
                        name TEXT NOT NULL
                    );
                """.trimIndent()
            )
            connection.execSQL("INSERT INTO migration_probe (id, name) VALUES (1, 'Ada');")
            connection.execSQL("PRAGMA user_version = 1;")
        } finally {
            connection.close()
        }
    }

    private suspend fun seedVersion2Database(dbPath: String) {
        val connection = BundledSqliteConnectionProvider.openConnection(dbPath, debug = false)
        try {
            connection.execSQL(
                """
                    CREATE TABLE migration_probe (
                        id INTEGER PRIMARY KEY NOT NULL,
                        name TEXT NOT NULL,
                        nickname TEXT
                    );
                """.trimIndent()
            )
            connection.execSQL("INSERT INTO migration_probe (id, name, nickname) VALUES (1, 'Ada', 'Lovelace');")
            connection.execSQL("PRAGMA user_version = 2;")
        } finally {
            connection.close()
        }
    }

    private suspend fun seedConflictingVersion2Database(dbPath: String) {
        val connection = BundledSqliteConnectionProvider.openConnection(dbPath, debug = false)
        try {
            connection.execSQL(
                """
                    CREATE TABLE migration_probe (
                        id INTEGER PRIMARY KEY NOT NULL,
                        name TEXT NOT NULL,
                        nickname TEXT
                    );
                """.trimIndent()
            )
            connection.execSQL(
                """
                    CREATE TABLE migration_meta (
                        key TEXT PRIMARY KEY NOT NULL,
                        value TEXT NOT NULL
                    );
                """.trimIndent()
            )
            connection.execSQL("INSERT INTO migration_probe (id, name, nickname) VALUES (1, 'Ada', 'Lovelace');")
            connection.execSQL("PRAGMA user_version = 2;")
        } finally {
            connection.close()
        }
    }

    private suspend fun MigrationFixtureDatabase.currentUserVersion(): Int = connection().readUserVersion()

    private suspend fun MigrationFixtureDatabase.scalarLong(sql: String): Long =
        connection().prepare(sql).use { statement ->
            statement.step()
            statement.getLong(0)
        }

    private suspend fun MigrationFixtureDatabase.scalarText(sql: String): String =
        connection().prepare(sql).use { statement ->
            statement.step()
            statement.getText(0)
        }

    private suspend fun MigrationFixtureDatabase.columnNames(tableName: String): List<String> =
        columnNames(connection(), tableName)

    private suspend fun columnNames(conn: SafeSQLiteConnection, tableName: String): List<String> =
        conn.prepare("PRAGMA table_info($tableName)").use { statement ->
            buildList {
                while (statement.step()) {
                    add(statement.getText(1))
                }
            }
        }

    private suspend fun SafeSQLiteConnection.readUserVersion(): Int =
        prepare("PRAGMA user_version").use { statement ->
            statement.step()
            statement.getLong(0).toInt()
        }

    private suspend fun MigrationFixtureDatabase.useOpen(block: suspend (MigrationFixtureDatabase) -> Unit) {
        try {
            block(this)
        } finally {
            close()
        }
    }
}
