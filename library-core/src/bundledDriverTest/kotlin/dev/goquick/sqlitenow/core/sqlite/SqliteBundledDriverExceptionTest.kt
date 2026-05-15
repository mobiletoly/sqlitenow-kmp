package dev.goquick.sqlitenow.core.sqlite

import androidx.sqlite.SQLiteException as DriverSQLiteException
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest

class SqliteBundledDriverExceptionTest {

    @Test
    fun invalidPrepareSqlIsWrappedAsSqliteException() = runTest {
        withBundledConnection { connection ->
            val error = assertFailsWithType<SqliteException> {
                connection.prepare("SELECT * FROM missing_table")
            }

            assertHasCause<DriverSQLiteException>(error)
            assertContains(error.message.orEmpty().lowercase(), "missing_table")
        }
    }

    @Test
    fun constraintFailureFromExecSqlIsWrappedAsSqliteException() = runTest {
        withBundledConnection { connection ->
            connection.execSQL("CREATE TABLE exception_probe(id INTEGER PRIMARY KEY, label TEXT NOT NULL)")
            connection.execSQL("INSERT INTO exception_probe(id, label) VALUES (1, 'first')")

            val error = assertFailsWithType<SqliteException> {
                connection.execSQL("INSERT INTO exception_probe(id, label) VALUES (1, 'duplicate')")
            }

            assertHasCause<DriverSQLiteException>(error)
            assertTrue(
                error.message.orEmpty().contains("UNIQUE", ignoreCase = true) ||
                    error.message.orEmpty().contains("constraint", ignoreCase = true),
                "Expected constraint failure message, got: ${error.message}",
            )
        }
    }
}

private suspend fun withBundledConnection(block: suspend (SafeSQLiteConnection) -> Unit) {
    val connection = BundledSqliteConnectionProvider.openConnection(
        dbName = ":memory:",
        debug = false,
    )
    try {
        block(connection)
    } finally {
        connection.close()
    }
}

private suspend inline fun <reified T : Throwable> assertFailsWithType(block: suspend () -> Unit): T {
    try {
        block()
    } catch (t: Throwable) {
        assertIs<T>(t)
        return t
    }
    throw AssertionError("Expected ${T::class.simpleName} to be thrown")
}

private inline fun <reified T : Throwable> assertHasCause(error: Throwable) {
    var cause = error.cause
    while (cause != null) {
        if (cause is T) {
            return
        }
        cause = cause.cause
    }
    throw AssertionError("Expected ${error::class.simpleName} cause chain to contain ${T::class.simpleName}")
}
