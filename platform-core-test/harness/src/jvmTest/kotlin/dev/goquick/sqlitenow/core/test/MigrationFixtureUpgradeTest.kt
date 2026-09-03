package dev.goquick.sqlitenow.core.test

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowMigrationStepCallback
import dev.goquick.sqlitenow.core.SqliteNowMigrationConnection
import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.core.test.migration.db.MigrationFixtureDatabase
import dev.goquick.sqlitenow.core.test.migration.db.VersionBasedDatabaseMigrations
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull

class MigrationFixtureUpgradeTest {

    @Test
    fun freshBootstrapCreatesLatestSchemaAndRunsInit() = runTest {
        val dbPath = createTempDirectory("migration-fixture-fresh").resolve("fresh.db").toString()
        val database = migrationFixtureDatabase(dbPath)

        try {
            database.open()

            assertEquals(5, database.currentUserVersion())
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
        seedVersion1Database(dbPath, fullName = null)

        val database = migrationFixtureDatabase(dbPath)
        try {
            database.open()

            assertEquals(5, database.currentUserVersion())
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

            assertEquals(5, database.currentUserVersion())
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
            assertEquals(5, database.currentUserVersion())
        }
    }

    @Test
    fun databaseNewerThanGeneratedTargetIsNotDowngradedOrMigrated() = runTest {
        val dbPath = createTempDirectory("migration-fixture-newer").resolve("newer.db").toString()
        val seedConnection = BundledSqliteConnectionProvider.openConnection(dbPath, debug = false)
        try {
            seedConnection.execSQL("PRAGMA user_version = ${Int.MAX_VALUE};")
        } finally {
            seedConnection.close()
        }

        val database = migrationFixtureDatabase(dbPath) {
            error("a database newer than the generated target must not invoke callbacks")
        }
        try {
            database.open()
            assertEquals(Int.MAX_VALUE, database.currentUserVersion())
        } finally {
            database.close()
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

    @Test
    fun programmaticStepTransformsFullNameBeforeVersion3DropsIt() = runTest {
        val dbPath = createTempDirectory("migration-fixture-callback").resolve("callback.db").toString()
        seedVersion1Database(dbPath, fullName = "Ada Lovelace")
        val boundaries = mutableListOf<String>()
        var retainedConnection: SqliteNowMigrationConnection? = null

        val database = migrationFixtureDatabase(dbPath) { scope ->
            boundaries += "${scope.fromVersion}->${scope.toVersion}"
            assertEquals(1, scope.originalVersion)
            assertEquals(5, scope.targetVersion)
            if (scope.toVersion == 2) {
                retainedConnection = scope.connection
                val rows = scope.connection.usePrepared(
                    "SELECT id, full_name FROM migration_person ORDER BY id"
                ) { statement ->
                    buildList {
                        while (statement.step()) {
                            add(statement.getLong(0) to statement.getText(1))
                        }
                    }
                }
                scope.connection.usePrepared(
                    "UPDATE migration_person SET first_name = ?, last_name = ? WHERE id = ?"
                ) { statement ->
                    rows.forEach { (id, fullName) ->
                        val (firstName, lastName) = fullName.split(" ", limit = 2)
                        statement.bindText(1, firstName)
                        statement.bindText(2, lastName)
                        statement.bindLong(3, id)
                        statement.step()
                        statement.reset()
                    }
                }
            }
        }

        try {
            database.open()
            assertEquals(listOf("1->2", "2->3", "3->4", "4->5"), boundaries)
            assertEquals("Ada", database.scalarText("SELECT first_name FROM migration_person WHERE id = 1"))
            assertEquals("Lovelace", database.scalarText("SELECT last_name FROM migration_person WHERE id = 1"))
            assertFalse(database.columnNames("migration_person").contains("full_name"))
            assertFailsWith<IllegalStateException> {
                requireNotNull(retainedConnection).execSQL("SELECT 1")
            }
        } finally {
            database.close()
        }

        migrationFixtureDatabase(dbPath) { error("reopening must not invoke callbacks") }.useOpen { reopened ->
            reopened.open()
            assertEquals(5, reopened.currentUserVersion())
        }
    }

    @Test
    fun callbackFailureRollsBackSqlDataAndVersion() = runTest {
        val dbPath = createTempDirectory("migration-fixture-callback-failure").resolve("failure.db").toString()
        seedVersion1Database(dbPath, fullName = "Ada Lovelace")
        val database = migrationFixtureDatabase(dbPath) { scope ->
            if (scope.toVersion == 2) {
                scope.connection.execSQL(
                    "UPDATE migration_person SET first_name = 'Ada', last_name = 'Lovelace' WHERE id = 1"
                )
            }
            if (scope.toVersion == 3) error("callback failed")
        }

        val failure = assertFailsWith<Throwable> { database.open() }
        assertTrue(failure.stackTraceToString().contains("callback failed"))
        val verificationConnection = BundledSqliteConnectionProvider.openConnection(dbPath, debug = false)
        try {
            assertEquals(1, verificationConnection.readUserVersion())
            assertFalse(columnNames(verificationConnection, "migration_person").contains("first_name"))
            assertEquals("Ada Lovelace", verificationConnection.scalarText("SELECT full_name FROM migration_person"))
        } finally {
            verificationConnection.close()
        }
    }

    @Test
    fun callbackCancellationRollsBackSqlDataAndVersion() = runTest {
        val dbPath = createTempDirectory("migration-fixture-callback-cancel").resolve("cancel.db").toString()
        seedVersion1Database(dbPath, fullName = "Ada Lovelace")
        val callbackStarted = CompletableDeferred<Unit>()
        val database = migrationFixtureDatabase(dbPath) { scope ->
            if (scope.toVersion == 2) {
                scope.connection.execSQL(
                    "UPDATE migration_person SET first_name = 'Ada', last_name = 'Lovelace' WHERE id = 1"
                )
                callbackStarted.complete(Unit)
                awaitCancellation()
            }
        }

        val opening = launch { database.open() }
        callbackStarted.await()
        opening.cancelAndJoin()

        val verificationConnection = BundledSqliteConnectionProvider.openConnection(dbPath, debug = false)
        try {
            assertEquals(1, verificationConnection.readUserVersion())
            assertFalse(columnNames(verificationConnection, "migration_person").contains("first_name"))
        } finally {
            verificationConnection.close()
        }
    }

    @Test
    fun inFlightPreparedOperationFinishesBeforeNextMigrationBoundary() = runTest {
        val dbPath = createTempDirectory("migration-fixture-callback-drain").resolve("drain.db").toString()
        seedVersion1Database(dbPath, fullName = null)
        val detachedScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val preparedBlockStarted = CompletableDeferred<Unit>()
        val releasePreparedBlock = CompletableDeferred<Unit>()
        val nextBoundaryReached = CompletableDeferred<Unit>()
        var detachedOperation: kotlinx.coroutines.Job? = null
        val database = migrationFixtureDatabase(dbPath) { scope ->
            when (scope.toVersion) {
                2 -> {
                    detachedOperation = detachedScope.launch {
                        scope.connection.usePrepared("SELECT 1") {
                            preparedBlockStarted.complete(Unit)
                            releasePreparedBlock.await()
                        }
                    }
                    preparedBlockStarted.await()
                }
                3 -> nextBoundaryReached.complete(Unit)
            }
        }

        try {
            val opening = launch { database.open() }
            preparedBlockStarted.await()
            assertEquals(
                null,
                withContext(Dispatchers.Default) {
                    withTimeoutOrNull(500) { nextBoundaryReached.await() }
                },
                "the next boundary must wait for already-entered scoped operations",
            )
            releasePreparedBlock.complete(Unit)
            opening.join()
            detachedOperation?.join()
            assertEquals(5, database.currentUserVersion())
        } finally {
            releasePreparedBlock.complete(Unit)
            detachedScope.cancel()
            database.close()
        }
    }

    @Test
    fun cancellationWhileDrainingCancelsAcceptedOperationAndRollsBack() = runTest {
        val dbPath = createTempDirectory("migration-fixture-callback-drain-cancel").resolve("cancel.db").toString()
        seedVersion1Database(dbPath, fullName = "Ada Lovelace")
        val detachedScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val operationStarted = CompletableDeferred<Unit>()
        var detachedOperation: kotlinx.coroutines.Job? = null
        val database = migrationFixtureDatabase(dbPath) { scope ->
            if (scope.toVersion == 2) {
                detachedOperation = detachedScope.launch {
                    scope.connection.usePrepared("SELECT 1") {
                        operationStarted.complete(Unit)
                        awaitCancellation()
                    }
                }
                operationStarted.await()
            }
        }

        val opening = launch { database.open() }
        operationStarted.await()
        opening.cancel()
        try {
            withContext(Dispatchers.Default) {
                withTimeout(2_000) { opening.join() }
            }
            assertTrue(detachedOperation?.isCancelled == true)
        } finally {
            detachedScope.cancel()
            withContext(Dispatchers.Default) {
                withTimeout(2_000) { opening.join() }
            }
            database.close()
        }

        val verificationConnection = BundledSqliteConnectionProvider.openConnection(dbPath, debug = false)
        try {
            assertEquals(1, verificationConnection.readUserVersion())
            assertFalse(columnNames(verificationConnection, "migration_person").contains("first_name"))
        } finally {
            verificationConnection.close()
        }
    }

    @Test
    fun callbackCanCatchSqlFailureCompensateAndCommit() = runTest {
        val dbPath = createTempDirectory("migration-fixture-callback-recovery").resolve("recovery.db").toString()
        seedVersion1Database(dbPath, fullName = "Ada Lovelace")
        var caughtFailure = false
        val database = migrationFixtureDatabase(dbPath) { scope ->
            if (scope.toVersion == 2) {
                try {
                    scope.connection.execSQL(
                        "INSERT INTO migration_person " +
                            "(id, full_name, first_name, last_name) VALUES (1, 'Duplicate', 'Bad', 'Row')"
                    )
                } catch (_: Throwable) {
                    caughtFailure = true
                }
                scope.connection.execSQL(
                    "UPDATE migration_person SET first_name = 'Ada', last_name = 'Lovelace' WHERE id = 1"
                )
            }
        }

        try {
            database.open()
            assertTrue(caughtFailure)
            assertEquals(5, database.currentUserVersion())
            assertEquals("Ada", database.scalarText("SELECT first_name FROM migration_person WHERE id = 1"))
            assertEquals("Lovelace", database.scalarText("SELECT last_name FROM migration_person WHERE id = 1"))
        } finally {
            database.close()
        }
    }

    private fun migrationFixtureDatabase(
        dbPath: String,
        onMigrationStep: SqliteNowMigrationStepCallback = {},
    ): MigrationFixtureDatabase =
        MigrationFixtureDatabase(
            dbName = dbPath,
            migration = VersionBasedDatabaseMigrations(onMigrationStep),
            debug = true,
        )

    private suspend fun seedVersion1Database(dbPath: String, fullName: String? = "Ada Lovelace") {
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
            connection.execSQL(
                "CREATE TABLE migration_person (id INTEGER PRIMARY KEY NOT NULL, full_name TEXT NOT NULL);"
            )
            if (fullName != null) {
                connection.prepare("INSERT INTO migration_person (id, full_name) VALUES (1, ?)").use { statement ->
                    statement.bindText(1, fullName)
                    statement.step()
                }
            }
            connection.execSQL("PRAGMA user_version = 1;")
        } finally {
            connection.close()
        }
    }

    private suspend fun seedVersion2Database(dbPath: String) =
        seedDatabaseAtVersion2(dbPath) { connection ->
            connection.execSQL(
                """
                    CREATE TABLE migration_person (
                        id INTEGER PRIMARY KEY NOT NULL,
                        full_name TEXT NOT NULL,
                        first_name TEXT,
                        last_name TEXT
                    );
                """.trimIndent()
            )
            connection.execSQL(
                "INSERT INTO migration_person (id, full_name, first_name, last_name) " +
                    "VALUES (1, 'Ada Lovelace', 'Ada', 'Lovelace');"
            )
        }

    private suspend fun seedConflictingVersion2Database(dbPath: String) =
        seedDatabaseAtVersion2(dbPath) { connection ->
            connection.execSQL(
                """
                    CREATE TABLE migration_meta (
                        key TEXT PRIMARY KEY NOT NULL,
                        value TEXT NOT NULL
                    );
                """.trimIndent()
            )
        }

    private suspend fun seedDatabaseAtVersion2(
        dbPath: String,
        seedScenario: suspend (SafeSQLiteConnection) -> Unit,
    ) {
        val connection = BundledSqliteConnectionProvider.openConnection(dbPath, debug = false)
        try {
            seedVersion2Probe(connection)
            seedScenario(connection)
            connection.execSQL("PRAGMA user_version = 2;")
        } finally {
            connection.close()
        }
    }

    private suspend fun seedVersion2Probe(connection: SafeSQLiteConnection) {
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

    private suspend fun SafeSQLiteConnection.scalarText(sql: String): String =
        prepare(sql).use { statement ->
            statement.step()
            statement.getText(0)
        }

    private suspend fun MigrationFixtureDatabase.useOpen(block: suspend (MigrationFixtureDatabase) -> Unit) {
        try {
            block(this)
        } finally {
            close()
        }
    }
}
