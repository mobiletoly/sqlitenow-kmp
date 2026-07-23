package dev.goquick.sqlitenow.core.sqlite

import androidx.sqlite.SQLITE_DATA_BLOB
import androidx.sqlite.SQLITE_DATA_FLOAT
import androidx.sqlite.SQLITE_DATA_INTEGER
import androidx.sqlite.SQLITE_DATA_NULL
import androidx.sqlite.SQLITE_DATA_TEXT
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlinx.coroutines.test.runTest

class SqliteWebTransactionStateTest {

    @Test
    fun batchStopsAtThePrimaryStatementFailure() = runTest {
        withConnection("web-batch-cleanup-suppression") { connection ->
            connection.execSQL("CREATE TABLE cleanup_failure(id INTEGER PRIMARY KEY)")
            connection.execSQL("INSERT INTO cleanup_failure(id) VALUES (1)")

            val failure = assertFailsWithType<SqliteException> {
                connection.execSQL(
                    """
                    INSERT INTO cleanup_failure(id) VALUES (1);
                    THIS IS NOT VALID SQL;
                    """.trimIndent(),
                )
            }

            assertThrowableMessageContains(failure, "constraint", "UNIQUE")
            assertTrue(
                failure.suppressedExceptions.isEmpty(),
                "the worker must stop before executing later batch statements",
            )
        }
    }

    @Test
    fun preparedSavepointObservesOncePerExecutionCycle() = runTest {
        withConnection("web-prepared-savepoint") { connection ->
            connection.prepare("SAVEPOINT prepared_cycle").use { statement ->
                assertFalse(statement.step())
                assertTrue(connection.inTransaction())

                assertFalse(statement.step(), "a second public step should execute one underlying step")
                connection.execSQL("RELEASE prepared_cycle")
                assertFalse(
                    connection.inTransaction(),
                    "the completed statement must not have been observed twice",
                )

                statement.reset()
                assertFalse(statement.step())
                assertTrue(connection.inTransaction(), "reset should rearm statement observation")
                connection.execSQL("RELEASE prepared_cycle")
                assertFalse(connection.inTransaction())

                statement.clearBindings()
                assertFalse(statement.step())
                assertTrue(connection.inTransaction(), "clearBindings should rearm statement observation")
                connection.execSQL("RELEASE prepared_cycle")
                assertFalse(connection.inTransaction())
            }
        }
    }

    @Test
    fun columnTypesPreserveSqliteStorageClassesAndExactIntegers() = runTest {
        withConnection("web-column-storage-types") { connection ->
            connection.prepare(
                "SELECT 9223372036854775807, 1.25, 'text-value', X'0001FF', NULL",
            ).use { statement ->
                assertTrue(statement.step())
                assertEquals(SQLITE_DATA_INTEGER, statement.getColumnType(0))
                assertEquals(Long.MAX_VALUE, statement.getLong(0))
                assertEquals(SQLITE_DATA_FLOAT, statement.getColumnType(1))
                assertEquals(1.25, statement.getDouble(1))
                assertEquals(SQLITE_DATA_TEXT, statement.getColumnType(2))
                assertEquals("text-value", statement.getText(2))
                assertEquals(SQLITE_DATA_BLOB, statement.getColumnType(3))
                assertContentEquals(byteArrayOf(0, 1, -1), statement.getBlob(3))
                assertEquals(SQLITE_DATA_NULL, statement.getColumnType(4))
                assertTrue(statement.isNull(4))
                assertFalse(statement.step())
            }
        }
    }

    @Test
    fun signed64IntegerReadsRemainExactBeyondJavaScriptSafeRange() = runTest {
        val scenarios = listOf(
            Signed64Scenario("minimum", Long.MIN_VALUE),
            Signed64Scenario("above-javascript-safe-range", 9_007_199_254_740_993L),
            Signed64Scenario("maximum", Long.MAX_VALUE),
        )

        withConnection("web-signed64-read") { connection ->
            connection.execSQL("CREATE TABLE exact_integers(name TEXT PRIMARY KEY, value INTEGER NOT NULL)")
            scenarios.forEach { scenario ->
                connection.execSQL(
                    "INSERT INTO exact_integers(name, value) VALUES ('${scenario.name}', ${scenario.value})",
                )
            }

            connection.prepare("SELECT name, value FROM exact_integers ORDER BY rowid").use { statement ->
                scenarios.forEach { scenario ->
                    assertTrue(statement.step(), "Expected row for ${scenario.name}")
                    assertEquals(scenario.name, statement.getText(0))
                    assertEquals(scenario.value, statement.getLong(1), scenario.name)
                }
                assertFalse(statement.step(), "Only the signed-64 scenarios should be present")
            }
        }
    }

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
    fun failedStatementKeepsManualTransactionActiveUntilRollback() = runTest {
        withConnection("web-tx-failed-statement") { connection ->
            connection.execSQL("CREATE TABLE notes(id INTEGER PRIMARY KEY, body TEXT NOT NULL)")

            connection.execSQL("BEGIN")
            connection.execSQL("INSERT INTO notes(id, body) VALUES (1, 'pending')")

            assertFailsWithType<SqliteException> {
                connection.execSQL("INSERT INTO notes(id, body) VALUES (1, 'duplicate')")
            }

            assertTrue(connection.inTransaction(), "failed statement should not clear transaction state")

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
        block: suspend (SafeSQLiteConnection) -> Unit,
    ) {
        val connection = openConnection(dbName = "$dbNamePrefix-${Random.nextInt()}.db")
        try {
            block(connection)
        } finally {
            connection.close()
        }
    }

    private suspend fun openConnection(
        dbName: String,
    ): SafeSQLiteConnection =
        BundledSqliteConnectionProvider.openConnection(
            dbName = dbName,
            debug = false,
        )

    private data class BeginScenario(
        val name: String,
        val sql: String,
    )

    private data class Signed64Scenario(
        val name: String,
        val value: Long,
    )

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

private fun assertThrowableMessageContains(error: Throwable, vararg candidates: String) {
    var current: Throwable? = error
    while (current != null) {
        val message = current.message.orEmpty().lowercase()
        if (candidates.any { message.contains(it.lowercase()) }) return
        current = current.cause
    }
    fail(
        "Expected ${error::class.simpleName} message chain to contain one of " +
            "${candidates.toList()}, but got ${error.message}",
    )
}
