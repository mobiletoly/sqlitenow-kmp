package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.TransactionMode
import dev.goquick.sqlitenow.core.sqlite.SqliteException
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue

class StatementCacheTest {
    @Test
    fun statementCache_rebindsWithoutLeakingPriorBindings() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        db.execSQL("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")

        val cache = StatementCache(db)
        try {
            val first = cache.get("INSERT INTO items(id, value) VALUES(?, ?)")
            first.bindLong(1, 1)
            first.bindText(2, "one")
            first.step()
            cache.release(first)

            val second = cache.get("INSERT INTO items(id, value) VALUES(?, ?)")
            assertSame(first, second)
            second.bindLong(1, 2)
            second.bindText(2, "two")
            second.step()
            cache.release(second)

            val values = mutableListOf<String>()
            db.prepare("SELECT value FROM items ORDER BY id").use { st ->
                while (st.step()) {
                    values += st.getText(0)
                }
            }
            assertEquals(listOf("one", "two"), values)
        } finally {
            cache.close()
            db.close()
        }
    }

    @Test
    fun statementCache_failedExecutionMakesLaterReuseFailClosed() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        db.execSQL("CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")

        val cache = StatementCache(db)
        try {
            val insert = cache.get("INSERT INTO items(id, value) VALUES(?, ?)")
            insert.bindLong(1, 1)
            insert.bindText(2, "one")
            insert.step()
            cache.release(insert)

            val duplicate = cache.get("INSERT INTO items(id, value) VALUES(?, ?)")
            duplicate.bindLong(1, 1)
            duplicate.bindText(2, "duplicate")
            assertFailsWith<SqliteException> {
                duplicate.step()
            }

            val resetFailure = assertFailsWith<SqliteException> {
                cache.release(duplicate)
            }
            assertTrue(resetFailure.message.orEmpty().contains("UNIQUE constraint failed"))
        } finally {
            runCatching { cache.close() }
        }

        assertTrue(runCatching { db.execSQL("SELECT 1") }.isFailure)
        db.close()
    }

    @Test
    fun statementCache_closeOwnsStatementLifetime() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        val cache = StatementCache(db)
        val statement = cache.get("SELECT 1")
        assertSame(statement, cache.get("SELECT 1"))

        cache.close()

        assertFailsWith<IllegalStateException> {
            cache.get("SELECT 1")
        }
        assertFailsWith<Throwable> {
            statement.reset()
        }

        db.close()
    }

    @Test
    fun statementCache_resetAndBindingClearFailuresAreNotSilentlyReplaced() = runBlocking {
        for (failurePoint in listOf("reset", "clearBindings")) {
            val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
            val failure = IllegalStateException("$failurePoint-failure")
            var closeCalls = 0
            val cache = StatementCache(
                db = db,
                operations = StatementCacheOperations(
                    reset = { statement ->
                        if (failurePoint == "reset") throw failure
                        statement.reset()
                    },
                    clearBindings = { statement ->
                        if (failurePoint == "clearBindings") throw failure
                        statement.clearBindings()
                    },
                    close = { statement ->
                        closeCalls++
                        statement.close()
                    },
                ),
            )
            val statement = cache.get("SELECT ?")

            val observed = assertFailsWith<IllegalStateException> {
                cache.release(statement)
            }

            val originalFailure = observed.originalRecoveredFailure()
            assertSame(failure, originalFailure)
            assertEquals(0, closeCalls)
            cache.close()
            assertEquals(1, closeCalls)
            assertFailsWith<Throwable> { statement.reset() }
            db.close()
        }
    }

    @Test
    fun statementCache_releaseAttemptsClearAfterResetFailureAndDoesNotResetOnGet() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        val resetFailure = IllegalStateException("RESET_SENTINEL")
        val clearFailure = IllegalStateException("CLEAR_SENTINEL")
        var resetCalls = 0
        var clearCalls = 0
        val cache = StatementCache(
            db = db,
            operations = StatementCacheOperations(
                reset = {
                    resetCalls++
                    throw resetFailure
                },
                clearBindings = {
                    clearCalls++
                    throw clearFailure
                },
            ),
        )
        val statement = cache.get("SELECT ?")
        assertSame(statement, cache.get("SELECT ?"))
        assertEquals(0, resetCalls, "get must return the already-clean cached statement")
        assertEquals(0, clearCalls)

        val observed = assertFailsWith<IllegalStateException> { cache.release(statement) }
            .originalRecoveredFailure()

        assertSame(resetFailure, observed)
        assertEquals(listOf(clearFailure), observed.suppressed.toList())
        assertEquals(1, resetCalls)
        assertEquals(1, clearCalls)
        runCatching { cache.close() }
        db.close()
    }

    @Test
    fun statementCacheResetFailure_usesPlatformObserverAndFatallyDisposesConnection() = runBlocking {
        assertStatementCacheObserverFailure(JvmCleanupFailureMode.RESET_THEN_CLEAR)
    }

    @Test
    fun statementCacheClearFailure_usesPlatformObserverAndFatallyDisposesConnection() = runBlocking {
        assertStatementCacheObserverFailure(JvmCleanupFailureMode.CLEAR_ONLY)
    }

    @Test
    fun statementCache_closeAttemptsEveryStatementAndSuppressesLaterFailures() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        var closeCalls = 0
        val cache = StatementCache(
            db = db,
            operations = StatementCacheOperations(
                close = {
                    closeCalls++
                    throw IllegalStateException("close-failure-$closeCalls")
                },
            ),
        )
        cache.get("SELECT 1")
        cache.get("SELECT 2")

        val error = assertFailsWith<IllegalStateException> { cache.close() }

        assertEquals(2, closeCalls)
        val originalFailure = error.originalRecoveredFailure()
        assertEquals("close-failure-1", originalFailure.message)
        assertEquals(listOf("close-failure-2"), originalFailure.suppressed.map { it.message })
        db.close()
    }

    @Test
    fun transactionScopedCachePreservesBodyFailureAndSuppressesCloseFailureOnce() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        val bodyFailure = IllegalStateException("BODY_SENTINEL")
        val closeFailure = IllegalStateException("CLOSE_SENTINEL")
        var closeCalls = 0

        val observed = assertFailsWith<IllegalStateException> {
            db.transaction(TransactionMode.IMMEDIATE) {
                db.withStatementCache(
                    operations = StatementCacheOperations(
                        close = {
                            closeCalls++
                            throw closeFailure
                        },
                    ),
                ) { cache ->
                    cache.get("SELECT 1")
                    throw bodyFailure
                }
            }
        }.originalRecoveredFailure()

        assertSame(bodyFailure, observed)
        assertEquals(1, observed.suppressed.size)
        assertSame(closeFailure, observed.suppressed.single().originalRecoveredFailure())
        assertEquals(1, closeCalls)
        assertEquals(1L, db.scalarLong("SELECT 1"))
        db.close()
    }

    private suspend fun assertStatementCacheObserverFailure(mode: JvmCleanupFailureMode) {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        val fault = JvmStatementCleanupFault.install(
            db = db,
            mode = mode,
            sqlMatcher = { sql -> sql.trim() == "SELECT ?" },
        )
        val cache = StatementCache(db)
        val statement = cache.get("SELECT ?")
        statement.bindLong(1, 1L)

        val observed = assertFailsWith<Throwable> { cache.release(statement) }

        fault.assertFailureGraph(observed)
        val prepareCallsBeforeRejection = fault.prepareCalls
        assertTrue(runCatching { db.execSQL("SELECT rejected-after-cache-cleanup") }.isFailure)
        assertEquals(prepareCallsBeforeRejection, fault.prepareCalls)
        assertEquals(1, fault.rawConnectionCloseCalls)
        assertEquals(1, fault.selectedStatement.closeCalls)
        assertEquals(0, fault.selectedStatement.bindOrStepCallsAfterFailure)
        runCatching { cache.close() }
        db.close()
    }

    private fun Throwable.originalRecoveredFailure(): Throwable =
        generateSequence(this) { it.cause }.last()
}
