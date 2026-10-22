package dev.goquick.sqlitenow.core.test

import dev.goquick.sqlitenow.common.SqliteNowLogger
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.core.test.db.PersonQuery
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDate
import kotlin.test.*

class GeneratedQueryDiagnosticsTest {
    @Test fun debugGeneratedQueriesEmitNoParameters() = exercise(false)
    @Test fun throwingDebugSinkPreservesQueriesCancellationAndCleanup() = exercise(true)

    private fun exercise(throws: Boolean) = runBlocking {
        val old = sqliteNowLogger; val events = mutableListOf<String>()
        sqliteNowLogger = object : SqliteNowLogger {
            private fun write(message: () -> String) { if (throws) error("synthetic sink failure"); events += message() }
            override fun e(throwable: Throwable?, message: () -> String) = write(message)
            override fun w(throwable: Throwable?, message: () -> String) = write(message)
            override fun i(throwable: Throwable?, message: () -> String) = write(message)
            override fun d(throwable: Throwable?, message: () -> String) = write(message)
        }
        val db = createLibraryTestDatabase(":memory:", debug = true)
        try {
            db.open()
            val person = db.person.add.one(PersonQuery.Add.Params(
                email = "sensitive@example.invalid", firstName = "synthetic-password", lastName = "synthetic-token",
                phone = null, birthDate = LocalDate(1990, 1, 1)))
            db.connection().prepare("SELECT first_name FROM person").use { st ->
                assertTrue(st.step()); assertEquals("synthetic-password", st.getText(0))
            }
            db.connection().execSQL("CREATE TEMP TRIGGER reject_synthetic_insert BEFORE INSERT ON person WHEN NEW.first_name='reject' BEGIN SELECT RAISE(ABORT, 'synthetic rejection'); END")
            assertFails { db.person.add.one(PersonQuery.Add.Params(
                email = "sensitive@example.invalid", firstName = "reject", lastName = "duplicate",
                phone = null, birthDate = LocalDate(1990, 1, 1))) }
            val cancellation = CancellationException("caller")
            val cancelled = assertFailsWith<CancellationException> {
                db.connection().withContextAndTrace { throw cancellation }
            }
            assertTrue(cancelled === cancellation || cancelled.cause === cancellation)
            db.person.deleteByIds(PersonQuery.DeleteByIds.Params(listOf(person.id)))
            assertTrue(events.none { it.contains("sensitive") || it.contains("synthetic-") || it.contains("params:") || it.contains("SafeSQLiteConnection.") })
            db.close(); db.open()
        } finally {
            if (db.isOpen()) db.close()
            sqliteNowLogger = old
        }
    }
}
