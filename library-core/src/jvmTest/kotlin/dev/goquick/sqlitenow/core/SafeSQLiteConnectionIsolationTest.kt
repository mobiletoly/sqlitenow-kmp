package dev.goquick.sqlitenow.core

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.common.SqliteNowLogger
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.sqlite.SqliteConnection
import dev.goquick.sqlitenow.core.sqlite.SqliteStatement
import dev.goquick.sqlitenow.core.sqlite.use
import java.nio.file.Files
import java.util.concurrent.Executors
import kotlin.coroutines.ContinuationInterceptor
import kotlin.io.path.deleteIfExists
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull

class SafeSQLiteConnectionIsolationTest {
    @Test
    fun sameOwnerClose_isRejectedBeforeTouchingResources() = runBlocking {
        val fixture = recordingConnection()

        fixture.connection.withExclusiveAccess {
            val error = assertFailsWith<IllegalStateException> {
                fixture.connection.close()
            }
            assertEquals("Cannot close SQLite connection from its active owner context", error.message)
            fixture.connection.execSQL("SELECT exclusive-owner-still-open")
        }

        fixture.connection.transaction(TransactionMode.IMMEDIATE) {
            val statement = fixture.connection.prepare("SELECT live-statement")
            try {
                val error = assertFailsWith<IllegalStateException> {
                    fixture.connection.close()
                }
                assertEquals("Cannot close SQLite connection from its active owner context", error.message)
                fixture.connection.execSQL("SELECT transaction-owner-still-open")
            } finally {
                statement.close()
            }
        }

        fixture.connection.withExclusiveAccess {
            coroutineScope {
                val childError = async { runCatching { fixture.connection.close() }.exceptionOrNull() }.await()
                assertTrue(childError is IllegalStateException)
                assertEquals(
                    "Cannot close SQLite connection from its active owner context",
                    childError.message,
                )
            }
        }

        assertEquals(0, fixture.persistence.closeCalls)
        assertEquals(0, fixture.raw.closeCalls)
        assertEquals(0, fixture.execution.closeCalls)
        fixture.connection.close()
        assertEquals(1, fixture.persistence.closeCalls)
        assertEquals(1, fixture.raw.closeCalls)
        assertEquals(1, fixture.execution.closeCalls)
    }

    @Test
    fun successfulCommit_isNeverRolledBackForNonForcedPersistenceFailure() = runBlocking {
        val failure = IllegalStateException("POST_COMMIT_PERSISTENCE_SENTINEL")
        val fixture = recordingConnection(
            persistence = RecordingPersistenceController(commitFailure = failure),
        )

        val result = fixture.connection.transaction(TransactionMode.IMMEDIATE) {
            fixture.connection.execSQL("INSERT durable-row")
            "committed"
        }

        assertEquals("committed", result)
        assertEquals(listOf("BEGIN IMMEDIATE", "INSERT durable-row", "COMMIT"), fixture.raw.executedSql)
        assertFalse(fixture.raw.executedSql.contains("ROLLBACK"))
        fixture.connection.execSQL("SELECT reusable-after-contained-persistence-failure")
    }

    @Test
    fun postCommitCancellation_propagatesWithoutRollbackOrFatalClose() = runBlocking {
        val cancellation = CancellationException("POST_COMMIT_CANCELLATION_SENTINEL")
        val fixture = recordingConnection(
            persistence = RecordingPersistenceController(commitFailure = cancellation),
        )

        val thrown = assertFailsWith<CancellationException> {
            fixture.connection.transaction(TransactionMode.IMMEDIATE) {
                fixture.connection.execSQL("INSERT durable-before-cancellation")
            }
        }

        assertSame(cancellation, thrown)
        assertEquals(
            listOf("BEGIN IMMEDIATE", "INSERT durable-before-cancellation", "COMMIT"),
            fixture.raw.executedSql,
        )
        assertEquals(0, fixture.raw.closeCalls)
        fixture.connection.execSQL("SELECT reusable-after-post-commit-cancellation")
    }

    @Test
    fun cancellationDeliveredDuringPreCommitCleanupRollsBackInsteadOfCommitting() = runBlocking {
        lateinit var fixture: RecordingFixture
        val cancellation = CancellationException("PRE_COMMIT_CLEANUP_CANCELLATION_SENTINEL")
        supervisorScope {
            val child = launch {
                val childJob = currentCoroutineContext()[Job] ?: error("missing child job")
                fixture = recordingConnection(
                    statements = ArrayDeque(
                        listOf(
                            RecordingRawStatement(
                                name = "cancel-during-close",
                                closeAction = { childJob.cancel(cancellation) },
                            ),
                        ),
                    ),
                )
                assertFailsWith<CancellationException> {
                    fixture.connection.transaction(TransactionMode.IMMEDIATE) {
                        fixture.connection.prepare("live-until-pre-commit")
                    }
                }
            }
            child.join()
            assertTrue(child.isCancelled)
        }

        assertEquals(listOf("BEGIN IMMEDIATE", "ROLLBACK"), fixture.raw.executedSql)
        assertFalse(fixture.raw.executedSql.contains("COMMIT"))
    }

    @Test
    fun successfulOperation_isNotReportedFailedByNonForcedPersistence() = runBlocking {
        val fixture = recordingConnection(
            persistence = RecordingPersistenceController(
                operationFailure = IllegalStateException("OPERATION_PERSISTENCE_SENTINEL"),
            ),
        )

        fixture.connection.execSQL("INSERT non-transactional-success")

        assertEquals(listOf("INSERT non-transactional-success"), fixture.raw.executedSql)
        assertEquals(0, fixture.raw.closeCalls)
        fixture.connection.execSQL("SELECT reusable-after-operation-persistence-failure")
        fixture.connection.persistSnapshotNow()
        assertEquals(1, fixture.persistence.flushCalls)
    }

    @Test
    fun close_attemptsPersistenceRawAndExecutionIndependentlyInOrder() = runBlocking {
        val persistenceFailure = IllegalStateException("CLOSE_PERSISTENCE_SENTINEL")
        val rawFailure = IllegalStateException("RAW_CLOSE_SENTINEL")
        val executionFailure = IllegalStateException("EXECUTION_CLOSE_SENTINEL")
        val fixture = recordingConnection(
            persistence = RecordingPersistenceController(closeFailure = persistenceFailure),
            rawCloseFailure = rawFailure,
            executionCloseFailure = executionFailure,
        )

        val thrown = assertFailsWith<IllegalStateException> { fixture.connection.close() }

        assertSame(persistenceFailure, thrown)
        assertEquals(listOf(rawFailure, executionFailure), thrown.suppressed.toList())
        assertEquals(1, fixture.persistence.closeCalls)
        assertEquals(1, fixture.raw.closeCalls)
        assertEquals(1, fixture.execution.closeCalls)
        fixture.connection.close()
        assertEquals(1, fixture.persistence.closeCalls)
        assertEquals(1, fixture.raw.closeCalls)
        assertEquals(1, fixture.execution.closeCalls)
    }

    @Test
    fun concurrentClose_attemptsEveryOwnedResourceExactlyOnce() = runBlocking {
        val fixture = recordingConnection()

        coroutineScope {
            List(8) {
                async(Dispatchers.Default) { fixture.connection.close() }
            }.forEach { it.await() }
        }

        assertEquals(1, fixture.persistence.closeCalls)
        assertEquals(1, fixture.raw.closeCalls)
        assertEquals(1, fixture.execution.closeCalls)
    }

    @Test
    fun statementUse_suppressesCloseFailureOntoBodyFailure() {
        val bodyFailure = IllegalStateException("BODY_SENTINEL")
        val closeFailure = IllegalStateException("STATEMENT_CLOSE_SENTINEL")
        val statement = SqliteStatement(RecordingRawStatement(closeFailure = closeFailure))

        val thrown = assertFailsWith<IllegalStateException> {
            statement.use { throw bodyFailure }
        }

        assertSame(bodyFailure, thrown)
        assertEquals(listOf(closeFailure), thrown.suppressed.toList())
    }

    @Test
    fun cleanupFailure_marksConnectionFatalAndClosesLiveStatementsInReverseOrder() = runBlocking {
        val cleanupFailure = IllegalStateException("RESET_SENTINEL")
        val closeOrder = mutableListOf<String>()
        val fixture = recordingConnection(
            statements = ArrayDeque(
                listOf(
                    RecordingRawStatement(name = "first", closeOrder = closeOrder),
                    RecordingRawStatement(
                        name = "second",
                        closeOrder = closeOrder,
                        resetFailure = cleanupFailure,
                    ),
                    RecordingRawStatement(name = "third", closeOrder = closeOrder),
                ),
            ),
        )

        val thrown = assertFailsWith<IllegalStateException> {
            fixture.connection.withExclusiveAccess {
                fixture.connection.prepare("first")
                val second = fixture.connection.prepare("second")
                fixture.connection.prepare("third")
                second.reset()
            }
        }

        assertSame(cleanupFailure, thrown)
        assertEquals(listOf("third", "second", "first"), closeOrder)
        assertEquals(1, fixture.raw.closeCalls)
        assertEquals(1, fixture.execution.closeCalls)
        assertTrue(runCatching { fixture.connection.execSQL("SELECT rejected-after-fatal") }.isFailure)
    }

    @Test
    fun bodyFailureWithRawCloseFailure_preservesPrimaryAndSuppressedIdentityOrder() = runBlocking {
        val bodyFailure = IllegalStateException("BODY_FAILURE")
        val resetFailure = IllegalStateException("RESET_FAILURE")
        val rawCloseFailure = IllegalStateException("RAW_CLOSE_FAILURE")
        val fixture = recordingConnection(
            rawCloseFailure = rawCloseFailure,
            statements = ArrayDeque(listOf(RecordingRawStatement(resetFailure = resetFailure))),
        )

        val observed = assertFailsWith<IllegalStateException> {
            fixture.connection.withExclusiveAccess {
                val statement = fixture.connection.prepare("body-raw-close")
                assertSame(resetFailure, runCatching { statement.reset() }.exceptionOrNull())
                throw bodyFailure
            }
        }

        assertSame(bodyFailure, observed)
        assertEquals(listOf(resetFailure, rawCloseFailure), observed.suppressed.toList())
        assertEquals(1, observed.countIdentity(resetFailure))
        assertEquals(1, observed.countIdentity(rawCloseFailure))
        assertEquals(1, fixture.raw.closeCalls)
        assertEquals(1, fixture.execution.closeCalls)
    }

    @Test
    fun bodyFailureWithExecutionCloseFailure_preservesPrimaryAndSuppressedIdentityOrder() = runBlocking {
        val bodyFailure = IllegalStateException("BODY_FAILURE")
        val resetFailure = IllegalStateException("RESET_FAILURE")
        val executionCloseFailure = IllegalStateException("EXECUTION_CLOSE_FAILURE")
        val fixture = recordingConnection(
            executionCloseFailure = executionCloseFailure,
            statements = ArrayDeque(listOf(RecordingRawStatement(resetFailure = resetFailure))),
        )

        val observed = assertFailsWith<IllegalStateException> {
            fixture.connection.withExclusiveAccess {
                val statement = fixture.connection.prepare("body-execution-close")
                assertSame(resetFailure, runCatching { statement.reset() }.exceptionOrNull())
                throw bodyFailure
            }
        }

        assertSame(bodyFailure, observed)
        assertEquals(listOf(resetFailure, executionCloseFailure), observed.suppressed.toList())
        assertEquals(1, observed.countIdentity(resetFailure))
        assertEquals(1, observed.countIdentity(executionCloseFailure))
        assertEquals(1, fixture.raw.closeCalls)
        assertEquals(1, fixture.execution.closeCalls)
    }

    @Test
    fun containedPersistenceFailure_logsOnlyFixedRedactedMessage() = runBlocking {
        val unsafeFailure = IllegalStateException("UNSAFE_THROWABLE_CONTENT")
        val fixture = recordingConnection(
            persistence = RecordingPersistenceController(operationFailure = unsafeFailure),
        )
        val previousLogger = sqliteNowLogger
        val logger = ErrorOnlyRecordingLogger()
        sqliteNowLogger = logger
        try {
            fixture.connection.execSQL("UNSAFE_SQL_AND_DATABASE_SENTINEL")
        } finally {
            sqliteNowLogger = previousLogger
            fixture.connection.close()
        }

        assertEquals(listOf("Failed to persist database snapshot"), logger.messages)
        assertEquals(listOf<Throwable?>(null), logger.throwables)
        val captured = logger.messages.joinToString("|") + logger.throwables.joinToString("|")
        assertFalse(captured.contains("UNSAFE_THROWABLE_CONTENT"))
        assertFalse(captured.contains("UNSAFE_SQL_AND_DATABASE_SENTINEL"))
    }

    @Test
    fun nestedExclusiveAccess_reusesOwnedDispatcherContext() = runBlocking {
        withTimeout(10_000) {
            withTestDatabase { database ->
                database.open()
                val connection = database.connection()

                connection.withExclusiveAccess {
                    val outerContext = currentCoroutineContext()
                    assertSame(connection.dispatcher, outerContext[ContinuationInterceptor])

                    connection.withExclusiveAccess {
                        assertSame(outerContext, currentCoroutineContext())
                        assertSame(connection.dispatcher, currentCoroutineContext()[ContinuationInterceptor])
                    }
                }
            }
        }
    }

    @Test
    fun inheritedOwnership_onAnotherDispatcher_returnsToConnectionDispatcher() = runBlocking {
        val foreignDispatcher = Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, "SafeSQLiteConnection-foreign-dispatcher").apply { isDaemon = true }
        }.asCoroutineDispatcher()
        try {
            withTimeout(10_000) {
                withTestDatabase { database ->
                    database.open()
                    val connection = database.connection()

                    connection.withExclusiveAccess {
                        val connectionThread = Thread.currentThread()
                        withContext(foreignDispatcher) {
                            val foreignThread = Thread.currentThread()
                            assertTrue(foreignThread !== connectionThread)

                            connection.withExclusiveAccess {
                                assertSame(connection.dispatcher, currentCoroutineContext()[ContinuationInterceptor])
                                assertSame(connectionThread, Thread.currentThread())
                            }

                            assertSame(foreignThread, Thread.currentThread())
                        }
                    }
                }
            }
        } finally {
            foreignDispatcher.close()
        }
    }

    @Test
    fun nestedExclusiveAccess_preservesCancellationPromptness() = runBlocking {
        withTimeout(10_000) {
            withTestDatabase { database ->
                database.open()
                val connection = database.connection()
                var nestedBlockRan = false

                supervisorScope {
                    val cancelled = async {
                        connection.withExclusiveAccess {
                            currentCoroutineContext()[Job]!!.cancel(CancellationException("cancel nested access"))
                            connection.withExclusiveAccess {
                                nestedBlockRan = true
                            }
                        }
                    }

                    val failure = runCatching { cancelled.await() }.exceptionOrNull()
                    assertTrue(failure is CancellationException)
                    assertEquals("cancel nested access", failure.message)
                }

                assertFalse(nestedBlockRan)
                assertFalse(connection.inTransaction(), "cancelled owner must release exclusive access")
            }
        }
    }

    @Test
    fun nestedExclusiveAccess_preservesExecutionContextHook() = runBlocking {
        withTimeout(10_000) {
            val hook = RecordingContextHook()
            withTestDatabase(
                configure = { connectionConfig = SqliteConnectionConfig(executionContextHook = hook) },
            ) { database ->
                database.open()
                hook.reset()
                val connection = database.connection()

                connection.withExclusiveAccess {
                    connection.withExclusiveAccess { Unit }
                }

                assertEquals(2, hook.captured.size)
                assertEquals(hook.captured, hook.restored)
                assertEquals(2, hook.restoreThreads.size)
                assertTrue(hook.restoreThreads.all { it.startsWith("SqliteNow-") })
            }
        }
    }

    @Test
    fun suspendedTransaction_blocksOtherCoroutinesFromUsingSameConnection() = runBlocking {
        withTimeout(10_000) {
            withTestDatabase { database ->
                database.open()
                database.connection().execSQL(
                    """
                    CREATE TABLE items (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL
                    )
                    """.trimIndent(),
                )

                val transactionStarted = CompletableDeferred<Unit>()
                val releaseTransaction = CompletableDeferred<Unit>()
                val transactionJob = launch {
                    database.transaction(TransactionMode.IMMEDIATE) {
                        database.connection().execSQL("INSERT INTO items(id, name) VALUES (1, 'first')")
                        transactionStarted.complete(Unit)
                        releaseTransaction.await()
                        database.connection().execSQL("INSERT INTO items(id, name) VALUES (2, 'second')")
                    }
                }

                transactionStarted.await()

                val concurrentRead = async {
                    database.connection().prepare("SELECT COUNT(*) FROM items").use { statement ->
                        check(statement.step())
                        statement.getLong(0)
                    }
                }

                val prematureResult = withTimeoutOrNull(250) {
                    concurrentRead.await()
                }

                releaseTransaction.complete(Unit)
                transactionJob.join()

                assertNull(
                    prematureResult,
                    "another coroutine used the same connection while a transaction was suspended; " +
                        "leaked read result=$prematureResult",
                )

                assertEquals(
                    2L,
                    withTimeout(5_000) { concurrentRead.await() },
                )
            }
        }
    }

    @Test
    fun rollbackFailureIsSuppressedAndDiscardsConnection() = runBlocking {
        withTimeout(10_000) {
            withTestDatabase { database ->
                database.open()
                val connection = database.connection()
                val primary = IllegalStateException("primary-transaction-failure")
                val rollback = IllegalStateException("rollback-failure")
                connection.beforeTransactionRollbackForTest = { throw rollback }

                val error = assertFailsWith<IllegalStateException> {
                    connection.transaction(TransactionMode.IMMEDIATE) {
                        throw primary
                    }
                }

                // Coroutine stack recovery may wrap the original exception at a dispatcher
                // boundary; the original remains the terminal cause with its cleanup chain.
                val originalError = generateSequence<Throwable>(error) { it.cause }.last()
                assertSame(primary, originalError)
                assertSame(rollback, originalError.suppressed.single())
                assertTrue(runCatching { connection.execSQL("SELECT 1") }.isFailure)
            }
        }
    }

    private suspend fun withTestDatabase(
        configure: SqliteNowDatabase.() -> Unit = {},
        block: suspend (SqliteNowDatabase) -> Unit,
    ) {
        val dbPath = Files.createTempFile("sqlitenow-isolation", ".db")
        val database = SqliteNowDatabase(dbPath.toString(), NoopMigration()).apply(configure)
        try {
            block(database)
        } finally {
            if (database.isOpen()) {
                database.close()
            }
            dbPath.deleteIfExists()
        }
    }

    private class NoopMigration : DatabaseMigrations {
        override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
            return if (currentVersion == -1) 0 else currentVersion
        }
    }

    private class RecordingContextHook : SqliteNowContextHook {
        val captured = mutableListOf<CapturedContext>()
        val restored = mutableListOf<CapturedContext>()
        val restoreThreads = mutableListOf<String>()

        override fun capture(): Any {
            return CapturedContext(captured.size, Thread.currentThread().name).also(captured::add)
        }

        override suspend fun <T> withCaptured(captured: Any?, block: suspend () -> T): T {
            restored += captured as CapturedContext
            restoreThreads += Thread.currentThread().name
            return block()
        }

        fun reset() {
            captured.clear()
            restored.clear()
            restoreThreads.clear()
        }
    }

    private data class CapturedContext(
        val sequence: Int,
        val threadName: String,
    )

    private fun recordingConnection(
        persistence: RecordingPersistenceController = RecordingPersistenceController(),
        rawCloseFailure: Throwable? = null,
        executionCloseFailure: Throwable? = null,
        statements: ArrayDeque<RecordingRawStatement> = ArrayDeque(),
    ): RecordingFixture {
        val raw = RecordingRawConnection(
            closeFailure = rawCloseFailure,
            statements = statements,
        )
        val execution = RecordingExecutionContext(executionCloseFailure)
        return RecordingFixture(
            connection = SafeSQLiteConnection(
                ref = SqliteConnection(raw),
                persistenceController = persistence,
                executionContext = execution,
            ),
            raw = raw,
            persistence = persistence,
            execution = execution,
        )
    }

    private data class RecordingFixture(
        val connection: SafeSQLiteConnection,
        val raw: RecordingRawConnection,
        val persistence: RecordingPersistenceController,
        val execution: RecordingExecutionContext,
    )

    private class ErrorOnlyRecordingLogger : SqliteNowLogger {
        val messages = mutableListOf<String>()
        val throwables = mutableListOf<Throwable?>()

        override fun e(throwable: Throwable?, message: () -> String) {
            throwables += throwable
            messages += message()
        }

        override fun w(throwable: Throwable?, message: () -> String) = Unit
        override fun i(throwable: Throwable?, message: () -> String) = Unit
        override fun d(throwable: Throwable?, message: () -> String) = Unit
    }

    private class RecordingPersistenceController(
        private val operationFailure: Throwable? = null,
        private val commitFailure: Throwable? = null,
        private val closeFailure: Throwable? = null,
    ) : PersistenceController {
        override val restoredFromSnapshot: Boolean = false
        var closeCalls = 0
            private set
        var flushCalls = 0
            private set

        override suspend fun onOperationComplete(connection: SqliteConnection, inTransaction: Boolean) {
            if (!inTransaction) operationFailure?.let { throw it }
        }

        override suspend fun onTransactionCommitted(connection: SqliteConnection) {
            commitFailure?.let { throw it }
        }

        override suspend fun flush(connection: SqliteConnection) {
            flushCalls++
        }

        override suspend fun onClose(connection: SqliteConnection) {
            closeCalls++
            closeFailure?.let { throw it }
        }
    }

    private class RecordingExecutionContext(
        private val closeFailure: Throwable?,
    ) : SqliteConnectionExecutionContext {
        override val dispatcher = Dispatchers.Unconfined
        var closeCalls = 0
            private set

        override fun close() {
            closeCalls++
            closeFailure?.let { throw it }
        }

    }

    private class RecordingRawConnection(
        private val closeFailure: Throwable?,
        private val statements: ArrayDeque<RecordingRawStatement>,
    ) : SQLiteConnection {
        val executedSql = mutableListOf<String>()
        var closeCalls = 0
            private set
        private var inTransaction = false

        override fun inTransaction(): Boolean = inTransaction

        override fun prepare(sql: String): SQLiteStatement {
            val isTransactionControl = sql == "BEGIN" || sql == "BEGIN IMMEDIATE" ||
                sql == "BEGIN EXCLUSIVE" || sql == "COMMIT" || sql == "ROLLBACK"
            return if (statements.isEmpty() || isTransactionControl) {
                RecordingRawStatement(name = sql, onStep = { recordExecution(sql) })
            } else {
                statements.removeFirst()
            }
        }

        override fun close() {
            closeCalls++
            closeFailure?.let { throw it }
        }

        private fun recordExecution(sql: String) {
            executedSql += sql
            when (sql) {
                "BEGIN", "BEGIN IMMEDIATE", "BEGIN EXCLUSIVE" -> inTransaction = true
                "COMMIT", "ROLLBACK" -> inTransaction = false
            }
        }
    }

    private class RecordingRawStatement(
        private val name: String = "statement",
        private val closeOrder: MutableList<String>? = null,
        private val resetFailure: Throwable? = null,
        private val closeFailure: Throwable? = null,
        private val closeAction: (() -> Unit)? = null,
        private val onStep: (() -> Unit)? = null,
    ) : SQLiteStatement {
        override fun bindBlob(index: Int, value: ByteArray) = Unit
        override fun bindDouble(index: Int, value: Double) = Unit
        override fun bindLong(index: Int, value: Long) = Unit
        override fun bindText(index: Int, value: String) = Unit
        override fun bindNull(index: Int) = Unit
        override fun getBlob(index: Int): ByteArray = byteArrayOf()
        override fun getDouble(index: Int): Double = 0.0
        override fun getLong(index: Int): Long = 0L
        override fun getText(index: Int): String = ""
        override fun isNull(index: Int): Boolean = true
        override fun getColumnCount(): Int = 0
        override fun getColumnName(index: Int): String = ""
        override fun getColumnType(index: Int): Int = 5
        override fun step(): Boolean {
            onStep?.invoke()
            return false
        }
        override fun reset() {
            resetFailure?.let { throw it }
        }
        override fun clearBindings() = Unit
        override fun close() {
            closeOrder?.add(name)
            closeAction?.invoke()
            closeFailure?.let { throw it }
        }
    }

    private fun Throwable.countIdentity(
        target: Throwable,
        visited: MutableList<Throwable> = mutableListOf(),
    ): Int {
        if (visited.any { it === this }) return 0
        visited += this
        return (if (this === target) 1 else 0) +
            (cause?.countIdentity(target, visited) ?: 0) +
            suppressed.sumOf { it.countIdentity(target, visited) }
    }
}
