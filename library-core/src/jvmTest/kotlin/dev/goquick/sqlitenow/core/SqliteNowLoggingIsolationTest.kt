package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.common.SqliteNowLogger
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import kotlin.test.*

class SqliteNowLoggingIsolationTest {
    private class Sink(val events: MutableList<String>, val throws: Boolean = false) : SqliteNowLogger {
        private fun write(message: () -> String) { if (throws) error("sink"); events += message() }
        override fun e(throwable: Throwable?, message: () -> String) = write(message)
        override fun w(throwable: Throwable?, message: () -> String) = write(message)
        override fun i(throwable: Throwable?, message: () -> String) = write(message)
        override fun d(throwable: Throwable?, message: () -> String) = write(message)
    }
    @Test fun debugConstructionNeverChangesLoggerOrEnablesSql() = runBlocking {
        val old = sqliteNowLogger; val events = mutableListOf<String>()
        try {
            sqliteNowLogger = Sink(events); val configured = sqliteNowLogger
            for (order in listOf(listOf(true, false), listOf(false, true))) {
                for (debug in order) {
                    val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug, SqliteConnectionConfig())
                    try { db.execSQL("CREATE TABLE private_data(secret TEXT)"); db.prepare("SELECT 'synthetic-secret'").use { assertTrue(it.step()) } }
                    finally { db.close() }
                    assertSame(configured, sqliteNowLogger)
                }
            }
            assertTrue(events.isEmpty(), events.toString())
        } finally { sqliteNowLogger = old }
    }
    @Test fun connectionTracePolicyIsIndependentInBothOrders() = runBlocking {
        for (traceFirst in listOf(true, false)) {
            val events = mutableListOf<String>(); val traced = SqliteConnectionConfig(sqlTrace = SqlTracePolicy(events::add))
            val configs = if (traceFirst) listOf(traced, SqliteConnectionConfig()) else listOf(SqliteConnectionConfig(), traced)
            val connections = configs.map { BundledSqliteConnectionProvider.openConnection(":memory:", true, it) }
            try {
                connections.forEach { it.execSQL("CREATE TABLE secret(value TEXT)") }
                assertEquals(1, events.size); assertFalse("secret" in events.single())
            } finally { connections.forEach { it.close() } }
        }
    }
    @Test fun throwingDiagnosticsPreserveSuccessSqlFailureCancellationAndRollback() = runBlocking {
        val old = sqliteNowLogger; var traceCalls = 0
        sqliteNowLogger = Sink(mutableListOf(), throws = true)
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", true,
            SqliteConnectionConfig(sqlTrace = SqlTracePolicy({ traceCalls++; error("trace") })))
        try {
            db.execSQL("CREATE TABLE item(id INTEGER)")
            assertFails { db.execSQL("INVALID SQL") }
            val cancelled = CancellationException("caller")
            val observed = assertFailsWith<CancellationException> {
                db.transaction(TransactionMode.IMMEDIATE) { db.execSQL("INSERT INTO item VALUES(1)"); throw cancelled }
            }
            // Coroutine stack recovery may copy CancellationException across the dispatcher hop.
            assertTrue(observed === cancelled || observed.cause === cancelled)
            db.prepare("SELECT count(*) FROM item").use { assertTrue(it.step()); assertEquals(0L, it.getLong(0)) }
            assertEquals(1, traceCalls)
        } finally { db.close(); sqliteNowLogger = old }
    }
}
