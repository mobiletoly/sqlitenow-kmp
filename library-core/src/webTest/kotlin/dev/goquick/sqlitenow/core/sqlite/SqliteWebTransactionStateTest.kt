package dev.goquick.sqlitenow.core.sqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.SqlitePersistence
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlinx.coroutines.test.runTest

class SqliteWebTransactionStateTest {

    @Test
    fun transactionControlStatementsUpdateInTransactionState() = runTest {
        val scenarios = listOf(
            BeginScenario("deferred", "BEGIN"),
            BeginScenario("immediate", "BEGIN IMMEDIATE"),
            BeginScenario("exclusive-lowercase", "begin exclusive"),
        )

        for (scenario in scenarios) {
            withConnection("web-tx-state-${scenario.name}") { connection ->
                assertFalse(connection.inTransaction(), "initial state should not be in transaction")

                connection.execSQL(scenario.sql)
                assertTrue(connection.inTransaction(), "${scenario.sql} should enter a transaction")
                connection.execSQL("COMMIT")
                assertFalse(connection.inTransaction(), "COMMIT should leave the transaction")

                connection.execSQL(scenario.sql)
                assertTrue(connection.inTransaction(), "${scenario.sql} should enter a transaction again")
                connection.execSQL("ROLLBACK")
                assertFalse(connection.inTransaction(), "ROLLBACK should leave the transaction")
            }
        }
    }

    @Test
    fun autoFlushIsDeferredDuringManualTransactionAndManualFlushRejects() = runTest {
        val persistence = RecordingPersistence()

        withConnection("web-tx-persistence", persistence) { connection ->
            connection.execSQL("CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
            val persistedAfterSetup = persistence.persistCalls
            assertTrue(persistedAfterSetup > 0, "setup statement should persist outside a transaction")

            connection.execSQL("BEGIN IMMEDIATE")
            val persistedAfterBegin = persistence.persistCalls
            assertEquals(
                persistedAfterSetup,
                persistedAfterBegin,
                "BEGIN should not flush while the connection is in a transaction",
            )

            connection.execSQL("INSERT INTO notes(id, body) VALUES (1, 'inside transaction')")
            assertEquals(
                persistedAfterBegin,
                persistence.persistCalls,
                "writes inside a manual transaction should not auto-flush",
            )

            assertFailsWithType<IllegalStateException> {
                connection.persistSnapshotNow()
            }

            connection.execSQL("COMMIT")
            assertFalse(connection.inTransaction(), "COMMIT should clear transaction state")
            assertTrue(
                persistence.persistCalls > persistedAfterBegin,
                "COMMIT should allow persistence once the transaction is closed",
            )
        }
    }

    @Test
    fun failedStatementKeepsManualTransactionActiveUntilRollback() = runTest {
        val persistence = RecordingPersistence()

        withConnection("web-tx-failed-statement", persistence) { connection ->
            connection.execSQL("CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
            val persistedAfterSetup = persistence.persistCalls

            connection.execSQL("BEGIN")
            connection.execSQL("INSERT INTO notes(id, body) VALUES (1, 'pending')")
            assertEquals(
                persistedAfterSetup,
                persistence.persistCalls,
                "successful write inside a manual transaction should not auto-flush",
            )

            assertFailsWithType<SqliteException> {
                connection.execSQL("INSERT INTO notes(id, body) VALUES (1, 'duplicate')")
            }

            assertTrue(connection.inTransaction(), "failed statement should not clear transaction state")
            assertEquals(
                persistedAfterSetup,
                persistence.persistCalls,
                "failed statement inside a manual transaction should not auto-flush",
            )
            assertFailsWithType<IllegalStateException> {
                connection.persistSnapshotNow()
            }

            connection.execSQL("ROLLBACK")
            assertFalse(connection.inTransaction(), "ROLLBACK should clear transaction state")

            connection.prepare("SELECT COUNT(*) FROM notes").use { statement ->
                assertTrue(statement.step(), "count query should return one row")
                assertEquals(0L, statement.getLong(0), "ROLLBACK should discard the pending insert")
            }
        }
    }

    private suspend fun withConnection(
        dbNamePrefix: String,
        persistence: RecordingPersistence? = null,
        block: suspend (SafeSQLiteConnection) -> Unit,
    ) {
        val connection = BundledSqliteConnectionProvider.openConnection(
            dbName = "$dbNamePrefix-${Random.nextInt()}.db",
            debug = false,
            config = SqliteConnectionConfig(persistence = persistence),
        )
        try {
            block(connection)
        } finally {
            connection.close()
        }
    }

    private data class BeginScenario(
        val name: String,
        val sql: String,
    )

    private class RecordingPersistence : SqlitePersistence {
        var persistCalls: Int = 0
            private set

        private var stored: ByteArray? = null

        override suspend fun load(dbName: String): ByteArray? = stored

        override suspend fun persist(dbName: String, bytes: ByteArray) {
            persistCalls++
            stored = bytes.copyOf()
        }

        override suspend fun clear(dbName: String) {
            stored = null
        }
    }
}

private suspend inline fun <reified T : Throwable> assertFailsWithType(block: suspend () -> Unit): T {
    try {
        block()
    } catch (t: Throwable) {
        if (t is T) return t
        fail("Expected ${T::class.simpleName}, but caught ${t::class.simpleName}: ${t.message}")
    }
    fail("Expected ${T::class.simpleName} to be thrown")
}
