package dev.goquick.sqlitenow.core.worker

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.SqliteNowInternalApi
import dev.goquick.sqlitenow.core.createSqliteConnectionExecutionContext
import dev.goquick.sqlitenow.core.SqliteNowLifecycleBusyException
import dev.goquick.sqlitenow.core.sqlite.SqliteException
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertNotNull
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonPrimitive

class SqliteWorkerDriverTest {
    @Test
    fun exactTaggedValuesAndRuntimeIdentity() = runTest {
        withWorker { driver, connection ->
            execute(
                connection,
                "CREATE TABLE exact_integers(ordinal INTEGER PRIMARY KEY, value INTEGER)",
            )
            sqliteWorkerIntegerScenarios.forEachIndexed { ordinal, scenario ->
                val insert = connection.prepare(
                    "INSERT INTO exact_integers(ordinal, value) VALUES (?1, ?2)",
                )
                insert.bindLong(1, ordinal.toLong())
                insert.bindLong(2, scenario.value)
                assertFalse(insert.step(), scenario.name)
                insert.close()
            }
            val integers = connection.prepare(
                "SELECT value FROM exact_integers ORDER BY ordinal",
            )
            sqliteWorkerIntegerScenarios.forEach { scenario ->
                assertTrue(integers.step(), scenario.name)
                assertEquals(scenario.value, integers.getLong(0), scenario.name)
                assertEquals(scenario.value.toString(), integers.getText(0), scenario.name)
            }
            assertFalse(integers.step())
            integers.close()

            val statement = connection.prepare("SELECT ?1, ?2, ?3, ?4")
            statement.bindDouble(1, 1.25)
            statement.bindText(2, "worker-text")
            statement.bindBlob(3, byteArrayOf(0, 1, 127, -1))
            statement.bindNull(4)
            assertTrue(statement.step())
            assertEquals(1.25, statement.getDouble(0))
            assertEquals("worker-text", statement.getText(1))
            assertContentEquals(byteArrayOf(0, 1, 127, -1), statement.getBlob(2))
            assertTrue(statement.isNull(3))
            assertFalse(statement.step())
            statement.close()

            val metrics = driver.metrics()
            assertEquals("3.53.0", metrics.sqliteVersion)
            assertTrue(metrics.runtimeKind in setOf("js-node-worker", "browser-worker"))
            assertEquals(0, metrics.integerNumberViolations)
            assertTrue(metrics.integerBindingsAsStrings >= sqliteWorkerIntegerScenarios.size * 2)
            assertTrue(metrics.integerResultsAsStrings >= sqliteWorkerIntegerScenarios.size)
        }
    }

    @Test
    fun taggedRealBindingsPreserveFiniteValuesAndStorageClass() = runTest {
        withWorker { _, connection ->
            sqliteWorkerRealScenarios.forEach { scenario ->
                val statement = connection.prepare("SELECT ?1, typeof(?1)")
                statement.bindDouble(1, scenario.value)
                assertTrue(statement.step(), scenario.name)
                assertEquals(scenario.value, statement.getDouble(0), scenario.name)
                assertEquals("real", statement.getText(1), scenario.name)
                assertFalse(statement.step(), scenario.name)
                statement.close()
            }
        }
    }

    @Test
    fun taggedRealBindingsRejectNonFiniteValues() = runTest {
        withWorker { _, connection ->
            val statement = connection.prepare("SELECT ?1")
            sqliteWorkerNonFiniteRealScenarios.forEach { scenario ->
                assertFailsWith<IllegalArgumentException>(scenario.name) {
                    statement.bindDouble(1, scenario.value)
                }
            }
            statement.close()
        }
    }

    @Test
    fun gettersPreserveSqliteConversionSemanticsWithoutLosingIntegers() = runTest {
        withWorker { _, connection ->
            val statement = connection.prepare(
                "SELECT 1, 1.75, '42', '2.5', 9223372036854775807, NULL, X'01'",
            )
            assertTrue(statement.step())
            assertEquals(1.0, statement.getDouble(0))
            assertEquals("1", statement.getText(0))
            assertEquals(1L, statement.getLong(1))
            assertEquals("1.75", statement.getText(1))
            assertEquals(42L, statement.getLong(2))
            assertEquals(2.5, statement.getDouble(3))
            assertEquals(Long.MAX_VALUE.toString(), statement.getText(4))
            assertFailsWith<SqliteException> { statement.getText(5) }
            assertFailsWith<SqliteException> { statement.getLong(6) }
            statement.close()
        }
    }

    @Test
    fun hundredThousandRowsEarlyCloseAndFullDrainStayBounded() = runTest(timeout = 180.seconds) {
        withWorker { driver, connection ->
            val sql =
                """
                WITH RECURSIVE values_100k(value) AS (
                    SELECT 1
                    UNION ALL
                    SELECT value + 1 FROM values_100k WHERE value < 100000
                )
                SELECT value FROM values_100k
                """.trimIndent()

            val early = connection.prepare(sql)
            assertTrue(early.step())
            assertEquals(1L, early.getLong(0))
            early.close()
            val afterEarlyClose = driver.metrics()
            assertEquals(64L, afterEarlyClose.steppedRows)
            assertEquals(64L, afterEarlyClose.encodedRows)
            assertEquals(64L, afterEarlyClose.transferredRows)
            assertEquals(64, afterEarlyClose.maxPageRows)
            assertEquals(0, afterEarlyClose.liveStatements)
            println(
                "WORKER_EARLY_EXIT rows=100000 stepped=${afterEarlyClose.steppedRows} " +
                    "encoded=${afterEarlyClose.encodedRows} " +
                    "transferred=${afterEarlyClose.transferredRows} " +
                    "maxPageRows=${afterEarlyClose.maxPageRows} " +
                    "maxPageBytes=${afterEarlyClose.maxPageBytes}",
            )

            val full = connection.prepare(sql)
            var rows = 0
            var last = 0L
            while (full.step()) {
                rows++
                last = full.getLong(0)
            }
            full.close()
            assertEquals(100_000, rows)
            assertEquals(100_000L, last)

            val afterFullDrain = driver.metrics()
            assertTrue(afterFullDrain.maxPageRows <= SQLITE_WORKER_DEFAULT_PAGE_ROWS)
            assertTrue(afterFullDrain.maxPageBytes <= SQLITE_WORKER_DEFAULT_PAGE_BYTES)
        }
    }

    @Test
    fun bytePagingAndBothOversizedRowCasesAreDeterministic() = runTest(timeout = 180.seconds) {
        withWorker(pageBytes = 2048) { driver, connection ->
            val bounded = connection.prepare(
                """
                WITH RECURSIVE values_100(value) AS (
                    SELECT 1
                    UNION ALL
                    SELECT value + 1 FROM values_100 WHERE value < 100
                )
                SELECT printf('%.*c', 700, 'x') FROM values_100
                """.trimIndent(),
            )
            var rows = 0
            while (bounded.step()) rows++
            bounded.close()
            assertEquals(100, rows)
            assertTrue(driver.metrics().maxPageBytes <= 2048)

            val soft = connection.prepare("SELECT printf('%.*c', 4096, 's')")
            assertTrue(soft.step())
            assertEquals(4096, soft.getText(0).length)
            assertFalse(soft.step())
            soft.close()
            assertTrue(driver.metrics().oversizedRows >= 1)

            execute(connection, "BEGIN")
            val hard = connection.prepare("SELECT printf('%.*c', 1048577, 'h')")
            val failure = assertFailsWith<SqliteException> { hard.step() }
            assertTrue(failure.message.orEmpty().contains("hard cap"))
            assertTrue(failure.cause is SqliteWorkerException)
            assertEquals(0, driver.metrics().liveStatements)
            assertTrue(connection.inTransaction())
            execute(connection, "ROLLBACK")
            assertFalse(connection.inTransaction())

            val reusable = connection.prepare("SELECT 7")
            assertTrue(reusable.step())
            assertEquals(7L, reusable.getLong(0))
            reusable.close()
        }
    }

    @Test
    fun resetClearAndCloseDiscardPrefetchedRowsWithoutDraining() = runTest {
        withWorker { driver, connection ->
            val statement = connection.prepare(
                """
                WITH RECURSIVE values_100k(value) AS (
                    SELECT ?1
                    UNION ALL
                    SELECT value + 1 FROM values_100k WHERE value < 100000
                )
                SELECT value FROM values_100k
                """.trimIndent(),
            )
            statement.bindLong(1, 1)
            assertTrue(statement.step())
            assertEquals(1L, statement.getLong(0))

            statement.reset()
            statement.bindLong(1, 5)
            assertTrue(statement.step())
            assertEquals(5L, statement.getLong(0))

            statement.clearBindings()
            assertTrue(statement.step())
            assertTrue(statement.isNull(0))
            statement.close()

            val metrics = driver.metrics()
            assertEquals(0, metrics.liveStatements)
            assertTrue(metrics.transferredRows <= 3L * SQLITE_WORKER_DEFAULT_PAGE_ROWS)
        }
    }

    @Test
    fun transactionsRollbackAndConnectionRemainReusableAfterActiveCancellation() =
        runTest(timeout = 180.seconds) {
            withWorker { driver, connection ->
                execute(connection, "CREATE TABLE values_table(value INTEGER)")
                execute(connection, "BEGIN")
                execute(connection, "INSERT INTO values_table VALUES (1)")
                assertTrue(connection.inTransaction())

                // This hold precedes step(): prove page-boundary cleanup, not VM interruption.
                val statement = connection.prepare("SELECT value FROM values_table")
                driver.holdNextActivePageForTest()
                val active = async(start = CoroutineStart.UNDISPATCHED) { statement.step() }
                driver.awaitActivePageForTest()
                active.cancelAndJoin()

                assertFalse(connection.inTransaction())
                val count = connection.prepare("SELECT COUNT(*) FROM values_table")
                assertTrue(count.step())
                assertEquals(0L, count.getLong(0))
                count.close()

                val metrics = driver.metrics()
                assertTrue(metrics.requestsCancelled >= 1)
                assertEquals(1L, metrics.transactionsRolledBackOnCancel)
                assertEquals(0, metrics.liveStatements)
            }
        }

    @Test
    fun safeTransactionCancellationRollsBackOnceAndKeepsConnectionReusable() =
        runTest(timeout = 180.seconds) {
            val connection = SqliteWorkerConnectionProvider().openConnection(
                dbName = "safe-worker-cancellation",
                debug = false,
                config = SqliteConnectionConfig(),
            )
            val workerConnection = connection.ref as SqliteWorkerSQLiteConnection
            try {
                connection.execSQL("CREATE TABLE values_table(value INTEGER)")
                val active = async(start = CoroutineStart.UNDISPATCHED) {
                    connection.transaction {
                        connection.execSQL("INSERT INTO values_table VALUES (1)")
                        // Arm after BEGIN and the write, so cancellation reaches the
                        // transaction body. This remains a pre-step boundary witness.
                        val statement = connection.prepare("SELECT value FROM values_table")
                        workerConnection.holdNextActivePageForTest()
                        try {
                            statement.step()
                        } finally {
                            statement.close()
                        }
                    }
                }
                workerConnection.awaitActivePageForTest()
                active.cancelAndJoin()

                assertFalse(connection.inTransaction())
                val count = connection.prepare("SELECT COUNT(*) FROM values_table")
                assertTrue(count.step())
                assertEquals(0L, count.getLong(0))
                count.close()
                val metrics = workerConnection.metricsForTest()
                assertEquals(1L, metrics.transactionsRolledBackOnCancel)
                assertEquals(0, metrics.liveStatements)
                connection.execSQL("INSERT INTO values_table VALUES (2)")
            } finally {
                connection.close()
            }
        }

    @Test
    @OptIn(SqliteNowInternalApi::class)
    fun coordinatedProviderRetainsFailedInspectionHandleUntilExplicitCloseRetry() =
        runTest(timeout = 180.seconds) {
            val databaseName = nextWorkerDriverDatabaseName()
            var driver: SqliteWorkerSQLiteDriver? = null
            val session = openSqliteWorkerCoordinatedSession(
                dbName = databaseName,
                debug = false,
                config = SqliteConnectionConfig(),
                workerModuleUrl = null,
                pageRows = SQLITE_WORKER_DEFAULT_PAGE_ROWS,
                pageBytes = SQLITE_WORKER_DEFAULT_PAGE_BYTES,
                startupModeForTest = "normal",
                cleanupTimeoutMillis = 100,
                inspectionRefForTest = { openedDriver ->
                    driver = openedDriver
                    openedDriver.open(databaseName)
                },
            )
            val inspection = assertNotNull(session.inspectionConnection)
            assertFalse(session.preparationRequired)
            val raw = inspection.ref
            val closeFailure = IllegalStateException("PROVIDER_RAW_CLOSE_RETRY_SENTINEL")
            var rawCloseCalls = 0
            inspection.closeRawDriverForTest = {
                rawCloseCalls++
                if (rawCloseCalls == 1) throw closeFailure
                raw.close()
            }

            assertSame(closeFailure, assertFailsWith<IllegalStateException> { session.prepareConnection() })
            assertEquals(1, rawCloseCalls)
            assertFailsWith<IllegalStateException> { session.prepareConnection() }
            assertSame(raw, inspection.ref)
            val retainedDriver = assertNotNull(driver)
            if (retainedDriver.runtimeKind() == "browser-worker") {
                val contender = SqliteWorkerSQLiteDriver.create()
                try {
                    assertFailsWith<SqliteNowLifecycleBusyException> {
                        contender.inspect(databaseName, "built-in")
                    }
                } finally { contender.shutdown() }
            }

            session.close()
            assertEquals(2, rawCloseCalls)
            session.close()
            assertEquals(2, rawCloseCalls)
            cleanupWorkerDriverDatabase(databaseName)
        }

    @Test
    fun coordinatedProviderFinishesContextAfterPhysicalTerminationWithStickyShutdownFailure() =
        runTest(timeout = 180.seconds) {
            data class Scenario(val name: String, val prepared: Boolean, val failContext: Boolean)
            for (scenario in listOf(
                Scenario("unprepared", false, false),
                Scenario("prepared", true, false),
                Scenario("unprepared context retry", false, true),
                Scenario("prepared context retry", true, true),
            )) {
                val before = globalWorkerDiagnostics()
                val databaseName = nextWorkerDriverDatabaseName()
                val delegate = createSqliteConnectionExecutionContext(databaseName)
                var contextCloses = 0
                val context = object : dev.goquick.sqlitenow.core.SqliteConnectionExecutionContext {
                    override val dispatcher = delegate.dispatcher
                    override fun close() {
                        contextCloses++
                        if (scenario.failContext && contextCloses == 1) error("context retry")
                        delegate.close()
                    }
                }
                val driver = SqliteWorkerSQLiteDriver.create(cleanupTimeoutMillis = 100)
                val session = SqliteWorkerCoordinatedProviderSession(
                    dbName = databaseName,
                    debug = false,
                    config = SqliteConnectionConfig(),
                    initialDriver = driver,
                    createDriver = { error("must not replace retained driver") },
                    executionContext = context,
                    inspectionConnection = null,
                )
                if (scenario.prepared) session.prepareConnection()
                driver.forceCleanup()
                assertTrue(driver.terminationConfirmed, scenario.name)
                val failure = runCatching { session.close() }.exceptionOrNull()
                assertNotNull(failure, scenario.name)
                assertEquals(1, contextCloses, scenario.name)
                assertFailsWith<IllegalStateException>(scenario.name) { session.prepareConnection() }
                session.close()
                session.close()
                assertEquals(if (scenario.failContext) 2 else 1, contextCloses, scenario.name)
                val after = globalWorkerDiagnostics()
                assertEquals(before.workersCreated + 1, after.workersCreated, scenario.name)
                assertEquals(before.workersTerminated + 1, after.workersTerminated, scenario.name)
                assertEquals(before.activeWorkers, after.activeWorkers, scenario.name)
                cleanupWorkerDriverDatabase(databaseName)
            }
        }

    @Test
    fun providerPreparationFailureRetainsIndependentShutdownDiagnosticAndFinishesContext() =
        runTest(timeout = 180.seconds) {
            val before = globalWorkerDiagnostics()
            val databaseName = nextWorkerDriverDatabaseName()
            val driver = SqliteWorkerSQLiteDriver.create(cleanupTimeoutMillis = 100)
            // Keep an actual worker-owned database for the shutdown fault after preparation fails.
            driver.open(databaseName)
            driver.failShutdownCleanupForTest("preparation finalize diagnostic", "preparation rollback diagnostic", "preparation close diagnostic")
            val execution = createSqliteConnectionExecutionContext(databaseName)
            var contextsClosed = 0
            val context = object : dev.goquick.sqlitenow.core.SqliteConnectionExecutionContext {
                override val dispatcher = execution.dispatcher
                override fun close() { contextsClosed++; execution.close() }
            }
            val session = SqliteWorkerCoordinatedProviderSession("", false,
                SqliteConnectionConfig(), driver, { error("must not replace failed driver during close") }, context, null)
            val failure = assertFailsWith<IllegalArgumentException> { session.prepareConnection() }
            assertTrue(failure.message.orEmpty().contains("non-empty"))
            val shutdown = failure.suppressedExceptions.single()
            assertTrue(shutdown.message.orEmpty().contains("preparation finalize diagnostic"))
            assertEquals(listOf("preparation rollback diagnostic", "preparation close diagnostic"),
                shutdown.suppressedExceptions.map { it.message })
            assertTrue(driver.terminationConfirmed)
            assertEquals(0, contextsClosed)
            session.close()
            session.close()
            assertEquals(1, contextsClosed)
            val after = globalWorkerDiagnostics()
            assertEquals(before.workersCreated + 1, after.workersCreated)
            assertEquals(before.workersTerminated + 1, after.workersTerminated)
            assertEquals(before.activeWorkers, after.activeWorkers)
            cleanupWorkerDriverDatabase(databaseName)
        }

    @Test
    fun providerCloseJoinsCancelledPreparationWithoutReplacingTheDriver() =
        runTest(timeout = 180.seconds) {
            supervisorScope {
                val before = globalWorkerDiagnostics()
                val databaseName = nextWorkerDriverDatabaseName()
                val driver = SqliteWorkerSQLiteDriver.create(cleanupTimeoutMillis = 100)
                val execution = createSqliteConnectionExecutionContext(databaseName)
                var contextsClosed = 0
                val context = object : dev.goquick.sqlitenow.core.SqliteConnectionExecutionContext {
                    override val dispatcher = execution.dispatcher
                    override fun close() { contextsClosed++; execution.close() }
                }
                val session = SqliteWorkerCoordinatedProviderSession(databaseName, false,
                    SqliteConnectionConfig(), driver, { error("must not replace a preparing driver") }, context, null)
                driver.holdNextResponseForTest("open")
                val preparing = async(start = CoroutineStart.UNDISPATCHED) { session.prepareConnection() }
                awaitClientDiagnostic(driver, "completedResponses", 1)
                val first = async(start = CoroutineStart.UNDISPATCHED) { session.close() }
                val second = async(start = CoroutineStart.UNDISPATCHED) { session.close() }
                assertFailsWith<IllegalStateException> { session.prepareConnection() }
                assertFailsWith<CancellationException> { preparing.await() }
                first.await(); second.await()
                assertTrue(driver.terminationConfirmed)
                assertEquals(1, contextsClosed)
                session.close()
                val after = globalWorkerDiagnostics()
                assertEquals(before.workersCreated + 1, after.workersCreated)
                assertEquals(before.workersTerminated + 1, after.workersTerminated)
                assertEquals(before.activeWorkers, after.activeWorkers)
                cleanupWorkerDriverDatabase(databaseName)
            }
        }

    @Test
    fun lateSuccessfulCommitCancellationKeepsCommittedDataAndConnectionReusable() =
        runTest(timeout = 180.seconds) {
            val connection = SqliteWorkerConnectionProvider().openConnection(
                dbName = "safe-worker-late-commit-cancellation",
                debug = false,
                config = SqliteConnectionConfig(),
            )
            val workerConnection = connection.ref as SqliteWorkerSQLiteConnection
            try {
                connection.execSQL("CREATE TABLE values_table(value INTEGER)")
                connection.beforeTransactionCommitForTest = {
                    workerConnection.setAcknowledgementModeForTest("drop-page-confirmation")
                }
                val committing = async(start = CoroutineStart.UNDISPATCHED) {
                    connection.transaction {
                        connection.execSQL("INSERT INTO values_table VALUES (1)")
                    }
                }
                awaitCompletedCommand(workerConnection, "page:COMMIT")
                awaitClientDiagnostic(workerConnection, "acknowledgementRequests", 1)
                val held = sqliteWorkerJson.parseToJsonElement(
                    workerConnection.diagnosticsForTest(),
                ).jsonObject
                val heldCommands = held.getValue("completedCommands").jsonArray.map {
                    it.jsonPrimitive.content
                }
                val heldTransactionStates =
                    held.getValue("completedTransactionStates").jsonArray.map {
                        it.jsonPrimitive.content
                    }
                committing.cancel()
                assertFailsWith<CancellationException> { committing.await() }

                assertEquals(listOf("page:COMMIT"), heldCommands)
                assertEquals(listOf("false"), heldTransactionStates)
                assertEquals(0, sqliteWorkerClientDiagnostic(
                    workerConnection.diagnosticsForTest(),
                    "acknowledgementRequests",
                ))
                assertFalse(connection.inTransaction())
                assertEquals(
                    0L,
                    workerConnection.metricsForTest().transactionsRolledBackOnCancel,
                )
                val count = connection.prepare("SELECT COUNT(*) FROM values_table")
                assertTrue(count.step())
                assertEquals(1L, count.getLong(0))
                count.close()
                connection.execSQL("INSERT INTO values_table VALUES (2)")
            } finally {
                connection.close()
            }
        }

    @Test
    fun cancellationKeepsCleanupFailuresOrderedAndNormalized() =
        runTest(timeout = 180.seconds) {
            withWorker { driver, connection ->
                execute(connection, "CREATE TABLE values_table(value INTEGER)")
                execute(connection, "BEGIN")
                val long = connection.prepare(longRunningSql())
                driver.failCancellationCleanupForNextRequestForTest()
                driver.holdNextActivePageForTest()
                val active = async(start = CoroutineStart.UNDISPATCHED) { long.step() }
                driver.awaitActivePageForTest()
                active.cancel()
                val cancelled = assertFailsWith<CancellationException> { active.await() }
                assertEquals(
                    listOf("statement finalization failed", "rollback failed"),
                    cancelled.suppressedExceptions.map { it.message },
                )
                assertTrue(cancelled.suppressedExceptions.all { it is SqliteException })

                assertFalse(connection.inTransaction())
                val inactiveRollback = assertFailsWith<SqliteException> {
                    execute(connection, "ROLLBACK")
                }
                assertTrue(inactiveRollback.message.orEmpty().contains("transaction"))
                assertFalse(connection.inTransaction())
                execute(connection, "SELECT 1")
            }
        }

    @Test
    fun queuedCancellationBeforeDispatchAndActiveCancellationUseDistinctCleanupPaths() =
        runTest(timeout = 180.seconds) {
        withWorker { driver, connection ->
            val first = connection.prepare(longRunningSql())
            val second = connection.prepare("SELECT 2")
            driver.holdNextActivePageForTest()
            val active = async(start = CoroutineStart.UNDISPATCHED) { first.step() }
            driver.awaitActivePageForTest()
            driver.failCancellationCleanupForNextRequestForTest()
            val queued = async(start = CoroutineStart.UNDISPATCHED) { second.step() }
            queued.cancel()
            active.cancel()
            val queuedCancellation = assertFailsWith<CancellationException> { queued.await() }
            active.join()
            assertEquals(
                listOf("statement finalization failed", "rollback failed"),
                queuedCancellation.suppressedExceptions.map { it.message },
            )

            val reusable = connection.prepare("SELECT 42")
            assertTrue(reusable.step())
            assertEquals(42L, reusable.getLong(0))
            reusable.close()

            val metrics = driver.metrics()
            assertTrue(metrics.requestsCancelled >= 2)
            assertEquals(0, metrics.pendingRequests)
            assertEquals(0, metrics.liveStatements)
        }
    }

    @Test
    fun cancellationReclaimsACompletedPrepareBeforeKotlinDelivery() =
        runTest(timeout = 180.seconds) {
            withWorker { driver, connection ->
                cancelAfterCompletedResponse(driver, "prepare") {
                    connection.prepare("SELECT 41")
                }

                val metrics = driver.metrics()
                assertEquals(0, metrics.liveStatements)
                awaitClientDiagnostic(driver, "completedResponses", 0)

                val reusable = connection.prepare("SELECT 42")
                assertTrue(reusable.step())
                assertEquals(42L, reusable.getLong(0))
                reusable.close()
            }
        }

    @Test
    fun cancellationReclaimsACompletedOpenBeforeKotlinDelivery() =
        runTest(timeout = 180.seconds) {
            val driver = SqliteWorkerSQLiteDriver.create()
            val databaseName = nextWorkerDriverDatabaseName()
            try {
                cancelAfterCompletedResponse(driver, "open") {
                    driver.open(databaseName)
                }

                assertEquals(0, driver.metrics().liveDatabases)
                awaitClientDiagnostic(driver, "completedResponses", 0)
            } finally {
                assertZeroResources(driver.shutdown())
                cleanupWorkerDriverDatabase(databaseName)
            }
        }

    @Test
    fun cancellationReclaimsACompletedPageAndRollsBackItsManualTransaction() =
        runTest(timeout = 180.seconds) {
            withWorker { driver, connection ->
                execute(connection, "BEGIN")
                assertTrue(connection.inTransaction())
                val statement = connection.prepare("SELECT 1")
                cancelAfterCompletedResponse(driver, "page") { statement.step() }

                assertFalse(connection.inTransaction())
                val metrics = driver.metrics()
                assertEquals(0, metrics.liveStatements)
                assertEquals(1L, metrics.transactionsRolledBackOnCancel)
                awaitClientDiagnostic(driver, "completedResponses", 0)

                val reusable = connection.prepare("SELECT 43")
                assertTrue(reusable.step())
                assertEquals(43L, reusable.getLong(0))
                reusable.close()
            }
        }

    @Test
    fun nonCancellationPageFailurePreservesManualTransactionUntilRollback() =
        runTest(timeout = 180.seconds) {
            withWorker { _, connection ->
                execute(connection, "BEGIN")
                val statement = connection.prepare("SELECT abs(-9223372036854775808)")

                val failure = assertFailsWith<SqliteException> { statement.step() }

                assertTrue(failure.message.orEmpty().contains("page"))
                assertTrue(connection.inTransaction())
                execute(connection, "ROLLBACK")
                assertFalse(connection.inTransaction())
                val reusable = connection.prepare("SELECT 45")
                assertTrue(reusable.step())
                assertEquals(45L, reusable.getLong(0))
                reusable.close()
            }
        }

    @Test
    fun cancelledFlushRetainsOrderedOneWayFailuresForTheCaller() =
        runTest(timeout = 180.seconds) {
            withWorker { driver, connection ->
                val long = connection.prepare(longRunningSql())
                val active = async(start = CoroutineStart.UNDISPATCHED) { long.step() }
                yield()
                driver.sendOneWay(
                    SqliteWorkerRequest(
                        protocol = SQLITE_WORKER_PROTOCOL,
                        command = "closeStatement",
                        databaseId = Int.MAX_VALUE,
                        statementId = Int.MAX_VALUE,
                    ),
                )
                val barrier = async(start = CoroutineStart.UNDISPATCHED) { driver.metrics() }
                yield()
                barrier.cancel()
                active.cancelAndJoin()

                val cancelled = assertFailsWith<CancellationException> { barrier.await() }
                assertTrue(cancelled.suppressedExceptions.single() is SqliteException)
                assertTrue(
                    cancelled.suppressedExceptions.single().message.orEmpty()
                        .contains("Unknown SQLite worker database"),
                )
                assertEquals(0, driver.metrics().pendingRequests)
            }
        }

    @Test
    fun oneWayFailuresRemainOrderedAcrossStickyTerminalTransition() =
        runTest(timeout = 180.seconds) {
            val driver = SqliteWorkerSQLiteDriver.create(cleanupTimeoutMillis = 50)
            driver.open(":memory:")
            driver.sendOneWay(
                SqliteWorkerRequest(
                    protocol = SQLITE_WORKER_PROTOCOL,
                    command = "closeStatement",
                    databaseId = Int.MAX_VALUE,
                    statementId = Int.MAX_VALUE,
                ),
            )
            driver.sendOneWay(
                SqliteWorkerRequest(
                    protocol = SQLITE_WORKER_PROTOCOL,
                    command = "closeStatement",
                    databaseId = Int.MAX_VALUE - 1,
                    statementId = Int.MAX_VALUE - 1,
                ),
            )
            awaitClientDiagnostic(driver, "completedResponses", 2)
            driver.failWorkerForTest("terminal after queued one-way failures")

            val queuedFailure = assertFailsWith<SqliteException> { driver.metrics() }

            val orderedFailures = listOf(queuedFailure) + queuedFailure.suppressedExceptions
            assertTrue(orderedFailures[0].message.orEmpty().contains(Int.MAX_VALUE.toString()))
            assertTrue(
                orderedFailures[1].message.orEmpty()
                    .contains((Int.MAX_VALUE - 1).toString()),
            )
            val terminal = assertFailsWith<SqliteException> { driver.metrics() }
            assertTrue(
                terminal.message.orEmpty().contains("terminal after queued one-way failures"),
                terminal.stackTraceToString(),
            )
            runCatching { driver.shutdown() }
        }

    @Test
    fun malformedOneWayResponsesBecomeStickyBeforeAcknowledgement() =
        runTest(timeout = 180.seconds) {
            listOf("reset", "clearBindings", "closeStatement", "closeDatabase").forEach { command ->
                val before = globalWorkerDiagnostics()
                val driver = SqliteWorkerSQLiteDriver.create(cleanupTimeoutMillis = 50)
                val connection = driver.open(":memory:") as SqliteWorkerSQLiteConnection
                val statement = connection.prepare("SELECT 1")
                driver.malformNextResponseForTest(command)
                when (command) {
                    "reset" -> statement.reset()
                    "clearBindings" -> statement.clearBindings()
                    "closeStatement" -> statement.close()
                    "closeDatabase" -> connection.close()
                }

                assertFailsWith<SqliteException>(command) {
                    driver.metrics()
                }
                awaitClientDiagnostic(driver, "terminationConfirmed", 1)
                assertEquals(0, clientDiagnostic(driver, "pendingRequests"), command)
                assertEquals(0, clientDiagnostic(driver, "completedResponses"), command)
                assertEquals(0, clientDiagnostic(driver, "acknowledgementRequests"), command)
                runCatching { driver.shutdown() }

                val after = globalWorkerDiagnostics()
                assertEquals(before.activeWorkers, after.activeWorkers, command)
                assertEquals(before.workersCreated + 1, after.workersCreated, command)
                assertEquals(before.workersTerminated + 1, after.workersTerminated, command)
            }
        }

    @Test
    fun negativeCancellationReconciliationIsStickyAndTerminatesOnce() =
        runTest(timeout = 180.seconds) {
            val before = globalWorkerDiagnostics()
            val driver = SqliteWorkerSQLiteDriver.create(cleanupTimeoutMillis = 50)
            val connection = driver.open(":memory:")
            driver.holdNextResponseForTest("prepare")
            val preparing = async(start = CoroutineStart.UNDISPATCHED) {
                connection.prepare("SELECT 73")
            }
            awaitClientDiagnostic(driver, "completedResponses", 1)
            driver.failCancellationReconciliationForNextRequestForTest()
            preparing.cancel(CancellationException("cancel before negative reconciliation"))
            val cancellation = assertFailsWith<CancellationException> { preparing.await() }
            val terminal = cancellation.suppressedExceptions.single()

            assertTrue(terminal is SqliteException)
            assertTrue(
                terminal.message.orEmpty().contains("negative cancellation reconciliation"),
            )
            awaitClientDiagnostic(driver, "terminationConfirmed", 1)
            assertEquals(0, clientDiagnostic(driver, "pendingRequests"))
            assertEquals(0, clientDiagnostic(driver, "completedResponses"))
            assertEquals(0, clientDiagnostic(driver, "reconciliationRequests"))
            assertEquals(0, clientDiagnostic(driver, "acknowledgementRequests"))
            assertEquals(0, clientDiagnostic(driver, "releaseRequests"))
            val futureFailure = assertFailsWith<SqliteException> { driver.metrics() }
            assertSame(terminal, futureFailure)
            assertEquals(1, clientDiagnostic(driver, "terminationAttempts"))
            runCatching { driver.shutdown() }

            val after = globalWorkerDiagnostics()
            assertEquals(before.activeWorkers, after.activeWorkers)
            assertEquals(before.workersCreated + 1, after.workersCreated)
            assertEquals(before.workersTerminated + 1, after.workersTerminated)
        }

    @Test
    fun shutdownPreservesPrimaryAndEveryInjectedStageFailureInOrder() =
        runTest(timeout = 180.seconds) {
            val driver = SqliteWorkerSQLiteDriver.create(cleanupTimeoutMillis = 50)
            val databaseName = nextWorkerDriverDatabaseName()
            try {
                val databaseId = driver.request(
                    SqliteWorkerRequest(
                        protocol = SQLITE_WORKER_PROTOCOL,
                        command = "open",
                        fileName = databaseName,
                        legacySourceMode =
                            if (driver.runtimeKind() == "browser-worker") "built-in" else "none",
                    ),
                ) { checkNotNull(it.databaseId) }

                suspend fun prepare(sql: String): Int =
                    driver.request(
                        SqliteWorkerRequest(
                            protocol = SQLITE_WORKER_PROTOCOL,
                            command = "prepare",
                            databaseId = databaseId,
                            sql = sql,
                        ),
                    ) { checkNotNull(it.statementId) }

                suspend fun executePage(statementId: Int, sql: String) {
                    driver.request(
                        SqliteWorkerRequest(
                            protocol = SQLITE_WORKER_PROTOCOL,
                            command = "page",
                            databaseId = databaseId,
                            statementId = statementId,
                            sql = sql,
                            pageRows = 1,
                            pageBytes = SQLITE_WORKER_DEFAULT_PAGE_BYTES,
                        ),
                    ) { Unit }
                }

                val begin = prepare("BEGIN")
                executePage(begin, "BEGIN")
                prepare("SELECT 1")
                driver.failShutdownCleanupForTest(
                    finalize = "shutdown finalize failed",
                    rollback = "shutdown rollback failed",
                    close = "shutdown close failed",
                )

                val failure = assertFailsWith<SqliteException> { driver.shutdown() }
                assertTrue(failure.message.orEmpty().contains("shutdown finalize failed"))
                assertEquals(
                    listOf("shutdown rollback failed", "shutdown close failed"),
                    failure.suppressedExceptions.map { it.message },
                )
                assertEquals(1, clientDiagnostic(driver, "terminationConfirmed"))
            } finally {
                runCatching { driver.shutdown() }
                cleanupWorkerDriverDatabase(databaseName)
            }
        }

    @Test
    fun preDispatchFlushFailureLeavesStatementReusable() = runTest(timeout = 180.seconds) {
        withWorker { driver, connection ->
            val statement = connection.prepare("SELECT 48")
            driver.sendOneWay(
                SqliteWorkerRequest(
                    protocol = SQLITE_WORKER_PROTOCOL,
                    command = "closeStatement",
                    databaseId = Int.MAX_VALUE,
                    statementId = Int.MAX_VALUE,
                ),
            )

            val flushFailure = assertFailsWith<SqliteException> { statement.step() }
            assertTrue(flushFailure.message.orEmpty().contains("Unknown SQLite worker database"))
            assertTrue(statement.step())
            assertEquals(48L, statement.getLong(0))
            statement.close()
        }
    }

    @Test
    fun contextualErrorsLiveStatementCloseAndBlankOpenRejectionLeaveNoResources() = runTest {
        val driver = SqliteWorkerSQLiteDriver.create()
        var connection: SQLiteConnection? = null
        try {
            val failedOpen = assertFailsWith<IllegalArgumentException> {
                driver.open(" ")
            }
            assertTrue(failedOpen.message.orEmpty().contains("non-empty"))
            assertEquals(0, driver.metrics().liveDatabases)

            connection = driver.open(":memory:")
            val prepareFailure = assertFailsWith<SqliteException> {
                connection.prepare("THIS IS NOT SQL")
            }
            assertTrue(prepareFailure.message.orEmpty().contains("prepare"))
            assertTrue(prepareFailure.message.orEmpty().contains("THIS IS NOT SQL"))
            assertTrue(prepareFailure.cause is SqliteWorkerException)

            connection.prepare("SELECT 1")
            connection.prepare("SELECT 2")
            connection.close()
            connection = null
            val final = driver.shutdown()
            assertZeroResources(final)
        } finally {
            connection?.close()
            runCatching { driver.shutdown() }
        }
    }

    @Test
    fun startupMismatchTerminatesTheWorkerWithoutRetainingResources() = runTest {
        listOf(
            "protocol-mismatch-reject-once",
            "protocol-mismatch-hang-once",
        ).forEach { mode ->
            val before = globalWorkerDiagnostics()
            val failure = assertFailsWith<SqliteException>(mode) {
                SqliteWorkerConnectionProvider().openConnectionForTest(
                    dbName = "protocol-mismatch",
                    debug = false,
                    config = SqliteConnectionConfig(),
                    startupModeForTest = mode,
                    cleanupTimeoutMillis = 50,
                )
            }
            assertTrue(failure.message.orEmpty().contains("startup mismatch"), mode)
            assertTrue(failure.cause !is SqliteException, mode)
            assertTrue(failure.cause?.message.orEmpty().contains("startup mismatch"), mode)
            val after = globalWorkerDiagnostics()
            assertEquals(before.activeWorkers, after.activeWorkers, mode)
            assertEquals(before.pendingStartups, after.pendingStartups, mode)
            assertEquals(before.workersCreated + 1, after.workersCreated, mode)
            assertEquals(before.workersTerminated + 1, after.workersTerminated, mode)
        }
    }

    @Test
    fun startupCancellationTerminatesTheWorkerBeforeProviderCleanupReturns() =
        runTest(timeout = 180.seconds) {
            listOf("hold-ready-reject-once", "hold-ready-hang-once").forEach { mode ->
                val before = globalWorkerDiagnostics()
                val opening = async(start = CoroutineStart.UNDISPATCHED) {
                    SqliteWorkerConnectionProvider().openConnectionForTest(
                        dbName = "cancelled-worker-startup",
                        debug = false,
                        config = SqliteConnectionConfig(),
                        startupModeForTest = mode,
                        cleanupTimeoutMillis = 50,
                    )
                }
                var startupObserved = false
                repeat(1_000) {
                    if (globalWorkerDiagnostics().pendingStartups > before.pendingStartups) {
                        startupObserved = true
                        return@repeat
                    }
                    yield()
                }
                assertTrue(startupObserved, mode)
                opening.cancelAndJoin()

                val after = globalWorkerDiagnostics()
                assertEquals(before.activeWorkers, after.activeWorkers, mode)
                assertEquals(before.pendingStartups, after.pendingStartups, mode)
                assertEquals(before.workersCreated + 1, after.workersCreated, mode)
                assertEquals(before.workersTerminated + 1, after.workersTerminated, mode)
            }
        }

    @Test
    fun cancelledSafeCloseStillTerminatesTheWorkerBeforeReturningCancellation() =
        runTest(timeout = 180.seconds) {
            val before = globalWorkerDiagnostics()
            val connection = SqliteWorkerConnectionProvider().openConnectionForTest(
                dbName = "cancelled-safe-close",
                debug = false,
                config = SqliteConnectionConfig(),
                startupModeForTest = "normal",
                cleanupTimeoutMillis = 50,
            )
            val workerConnection = connection.ref as SqliteWorkerSQLiteConnection
            workerConnection.holdNextResponseForTest("shutdown")
            supervisorScope {
                val cancellation = CancellationException("cancel safe close")
                val observedFailure = CompletableDeferred<Throwable>()
                val closing = launch(start = CoroutineStart.UNDISPATCHED) {
                    observedFailure.complete(
                        runCatching { connection.close() }
                            .exceptionOrNull()
                            ?: error("cancelled close unexpectedly succeeded"),
                    )
                }
                awaitClientDiagnostic(workerConnection, "completedResponses", 1)
                closing.cancel(cancellation)
                val observed = observedFailure.await()
                closing.join()
                assertTrue(observed is CancellationException)
                assertEquals(cancellation.message, observed.message)
                assertTrue(
                    observed.suppressedExceptions.any {
                        it.message.orEmpty().contains("cleanup deadline")
                    },
                )
            }
            val after = globalWorkerDiagnostics()
            assertEquals(before.activeWorkers, after.activeWorkers)
            assertEquals(before.workersCreated + 1, after.workersCreated)
            assertEquals(before.workersTerminated + 1, after.workersTerminated)
        }

    @Test
    fun cancelledSafeCloseForcesTerminationWhenAnotherOwnerKeepsTheMutex() =
        runTest(timeout = 180.seconds) {
            val before = globalWorkerDiagnostics()
            val connection = SqliteWorkerConnectionProvider().openConnectionForTest(
                dbName = "cancelled-safe-close-held-owner",
                debug = false,
                config = SqliteConnectionConfig(),
                startupModeForTest = "normal",
                cleanupTimeoutMillis = 50,
            )
            val ownerEntered = CompletableDeferred<Unit>()
            val releaseOwner = CompletableDeferred<Unit>()
            val owner = async(start = CoroutineStart.UNDISPATCHED) {
                connection.withExclusiveAccess {
                    ownerEntered.complete(Unit)
                    releaseOwner.await()
                }
            }
            ownerEntered.await()

            supervisorScope {
                val cancellation = CancellationException("cancel held-owner close")
                val observedFailure = CompletableDeferred<Throwable>()
                val closing = launch(start = CoroutineStart.UNDISPATCHED) {
                    observedFailure.complete(
                        runCatching { connection.close() }
                            .exceptionOrNull()
                            ?: error("cancelled close unexpectedly succeeded"),
                    )
                }
                closing.cancel(cancellation)
                val observed = observedFailure.await()
                closing.join()
                assertTrue(observed is CancellationException)
                assertEquals(cancellation.message, observed.message)
                assertTrue(
                    observed.suppressedExceptions.any {
                        it.message.orEmpty().contains("cleanup deadline")
                    },
                )
            }
            releaseOwner.complete(Unit)
            owner.await()

            val after = globalWorkerDiagnostics()
            assertEquals(before.activeWorkers, after.activeWorkers)
            assertEquals(before.workersCreated + 1, after.workersCreated)
            assertEquals(before.workersTerminated + 1, after.workersTerminated)
        }

    @Test
    fun droppedShutdownResponseHitsDeadlineAndForcesTermination() =
        runTest(timeout = 180.seconds) {
            val before = globalWorkerDiagnostics()
            val driver = SqliteWorkerSQLiteDriver.create(cleanupTimeoutMillis = 50)
            val connection = driver.open(":memory:")
            connection.close()
            driver.dropNextResponseForTest("shutdown")
            val failure = assertFailsWith<SqliteException> { driver.shutdown() }
            assertTrue(failure.message.orEmpty().contains("cleanup deadline"))
            val after = globalWorkerDiagnostics()
            assertEquals(before.activeWorkers, after.activeWorkers)
            assertEquals(before.workersCreated + 1, after.workersCreated)
            assertEquals(before.workersTerminated + 1, after.workersTerminated)
        }

    @Test
    fun responseHandoffFailureModesBecomeStickyAndTerminateWithoutRetainedResponses() =
        runTest(timeout = 180.seconds) {
            val scenarios = listOf(
                HandoffFailureScenario(
                    name = "dropped acknowledgement confirmation",
                    mode = "drop-confirmation",
                    failureOccursBeforeAcceptance = true,
                    expectedMessageFragment = "acknowledgement",
                ),
                HandoffFailureScenario(
                    name = "failed acknowledgement",
                    mode = "throw",
                    failureOccursBeforeAcceptance = true,
                    expectedMessageFragment = "controlled",
                ),
                HandoffFailureScenario(
                    name = "dropped release confirmation",
                    mode = "drop-release-confirmation",
                    failureOccursBeforeAcceptance = false,
                    expectedMessageFragment = "release",
                ),
                HandoffFailureScenario(
                    name = "failed release",
                    mode = "throw-release",
                    failureOccursBeforeAcceptance = false,
                    expectedMessageFragment = "controlled",
                ),
            )

            scenarios.forEach { scenario ->
                val before = globalWorkerDiagnostics()
                val driver = SqliteWorkerSQLiteDriver.create(cleanupTimeoutMillis = 50)
                driver.open(":memory:")
                driver.setAcknowledgementModeForTest(scenario.mode)

                val failure =
                    if (scenario.failureOccursBeforeAcceptance) {
                        assertFailsWith<SqliteException>(scenario.name) { driver.metrics() }
                    } else {
                        driver.metrics()
                        awaitClientDiagnostic(driver, "terminationConfirmed", 1)
                        assertFailsWith<SqliteException>(scenario.name) { driver.metrics() }
                    }
                assertTrue(
                    failure.message.orEmpty().contains(scenario.expectedMessageFragment),
                    scenario.name,
                )
                assertEquals(0, clientDiagnostic(driver, "pendingRequests"), scenario.name)
                assertEquals(0, clientDiagnostic(driver, "completedResponses"), scenario.name)
                assertEquals(0, clientDiagnostic(driver, "acknowledgementRequests"), scenario.name)
                assertEquals(0, clientDiagnostic(driver, "releaseRequests"), scenario.name)
                runCatching { driver.shutdown() }

                val after = globalWorkerDiagnostics()
                assertEquals(before.activeWorkers, after.activeWorkers, scenario.name)
                assertEquals(before.workersCreated + 1, after.workersCreated, scenario.name)
                assertEquals(before.workersTerminated + 1, after.workersTerminated, scenario.name)
            }
        }

    @Test
    fun commandSpecificMalformedResponsesTerminateAllocatedResources() =
        runTest(timeout = 180.seconds) {
            val scenarios = listOf(
                MalformedResponseScenario("open") { driver ->
                    driver.malformNextResponseForTest("open")
                    driver.open(":memory:")
                },
                MalformedResponseScenario("prepare") { driver ->
                    val connection = driver.open(":memory:")
                    driver.malformNextResponseForTest("prepare")
                    connection.prepare("SELECT 1")
                },
                MalformedResponseScenario("page") { driver ->
                    val connection = driver.open(":memory:")
                    val statement = connection.prepare("SELECT 1")
                    driver.malformNextResponseForTest("page")
                    statement.step()
                },
                MalformedResponseScenario("metrics") { driver ->
                    driver.open(":memory:")
                    driver.malformNextResponseForTest("metrics")
                    driver.metrics()
                },
                MalformedResponseScenario("prepare missing columnNames") { driver ->
                    val connection = driver.open(":memory:")
                    driver.omitNextResponseFieldForTest("prepare", "columnNames")
                    connection.prepare("SELECT 1")
                },
                MalformedResponseScenario("page missing rows") { driver ->
                    val connection = driver.open(":memory:")
                    val statement = connection.prepare("SELECT 1")
                    driver.omitNextResponseFieldForTest("page", "rows")
                    statement.step()
                },
                MalformedResponseScenario("metrics missing workerStops") { driver ->
                    driver.open(":memory:")
                    driver.omitNextResponseFieldForTest("metrics", "workerStops")
                    driver.metrics()
                },
                MalformedResponseScenario("error missing cancelled") { driver ->
                    val connection = driver.open(":memory:") as SqliteWorkerSQLiteConnection
                    driver.omitNextResponseFieldForTest("page", "cancelled", error = true)
                    val accepted = driver.request(
                        SqliteWorkerRequest(
                            protocol = SQLITE_WORKER_PROTOCOL,
                            command = "page",
                            databaseId = connection.databaseId,
                            statementId = Int.MAX_VALUE,
                            sql = "SELECT 1",
                            pageRows = 1,
                            pageBytes = SQLITE_WORKER_DEFAULT_PAGE_BYTES,
                        ),
                    ) { true }
                    assertTrue(accepted)
                },
            )

            scenarios.forEach { scenario ->
                val before = globalWorkerDiagnostics()
                val driver = SqliteWorkerSQLiteDriver.create(cleanupTimeoutMillis = 50)
                val failure = assertFailsWith<SqliteException>(scenario.command) {
                    scenario.action(driver)
                }
                assertTrue(failure.cause != null, scenario.command)
                assertTrue(failure.cause !is SqliteException, scenario.command)
                assertEquals(0, clientDiagnostic(driver, "pendingRequests"), scenario.command)
                assertEquals(0, clientDiagnostic(driver, "completedResponses"), scenario.command)
                assertEquals(1, clientDiagnostic(driver, "terminationConfirmed"), scenario.command)

                val after = globalWorkerDiagnostics()
                assertEquals(before.activeWorkers, after.activeWorkers, scenario.command)
                assertEquals(before.workersCreated + 1, after.workersCreated, scenario.command)
                assertEquals(before.workersTerminated + 1, after.workersTerminated, scenario.command)
            }
        }

    @Test
    fun terminationFailureModesGetOneBoundedRetryAndTruthfulDiagnostics() =
        runTest(timeout = 180.seconds) {
            listOf("reject-once", "hang-once").forEach { mode ->
                val before = globalWorkerDiagnostics()
                val driver = SqliteWorkerSQLiteDriver.create(cleanupTimeoutMillis = 50)
                driver.open(":memory:")
                driver.setTerminationModeForTest(mode)

                driver.forceCleanup()

                assertEquals(2, clientDiagnostic(driver, "terminationAttempts"), mode)
                assertEquals(1, clientDiagnostic(driver, "terminationConfirmed"), mode)
                val after = globalWorkerDiagnostics()
                assertEquals(before.activeWorkers, after.activeWorkers, mode)
                assertEquals(before.workersCreated + 1, after.workersCreated, mode)
                assertEquals(before.workersTerminated + 1, after.workersTerminated, mode)
            }
        }

    @Test
    fun postReadyWorkerFailureTerminatesAndFailsCurrentAndFutureRequests() =
        runTest(timeout = 180.seconds) {
            supervisorScope {
                listOf("reject-once", "hang-once").forEach { mode ->
                    val before = globalWorkerDiagnostics()
                    val driver = SqliteWorkerSQLiteDriver.create(cleanupTimeoutMillis = 50)
                    val connection = driver.open(":memory:")
                    driver.setTerminationModeForTest(mode)
                    driver.holdNextResponseForTest("prepare")
                    val current = async(start = CoroutineStart.UNDISPATCHED) {
                        connection.prepare("SELECT 44")
                    }
                    awaitClientDiagnostic(driver, "completedResponses", 1)
                    driver.failWorkerForTest("controlled post-ready failure")

                    val currentFailure = assertFailsWith<SqliteException>(mode) { current.await() }
                    assertTrue(
                        currentFailure.message.orEmpty().contains("controlled post-ready failure"),
                        mode,
                    )
                    val futureFailure = assertFailsWith<SqliteException>(mode) { driver.metrics() }
                    assertTrue(
                        futureFailure.message.orEmpty().contains("controlled post-ready failure"),
                        mode,
                    )
                    assertEquals(
                        currentFailure.cause?.message,
                        futureFailure.cause?.message,
                        mode,
                    )
                    assertEquals(2, clientDiagnostic(driver, "terminationAttempts"), mode)
                    assertEquals(1, clientDiagnostic(driver, "terminationConfirmed"), mode)
                    runCatching { driver.shutdown() }

                    val after = globalWorkerDiagnostics()
                    assertEquals(before.activeWorkers, after.activeWorkers, mode)
                    assertEquals(before.workersCreated + 1, after.workersCreated, mode)
                    assertEquals(before.workersTerminated + 1, after.workersTerminated, mode)
                }
            }
        }

    @Test
    fun completedRequestStateStaysConstantAndPositiveIdsDoNotWrap() =
        runTest(timeout = 180.seconds) {
            val driver = SqliteWorkerSQLiteDriver.create()
            val connection = driver.open(":memory:")
            try {
                repeat(200) { driver.metrics() }
                awaitClientDiagnostic(driver, "completedResponses", 0)
                assertEquals(0, clientDiagnostic(driver, "pendingRequests"))
                assertEquals(0, clientDiagnostic(driver, "releaseRequests"))

                driver.setNextRequestIdForTest(Int.MAX_VALUE - 1)
                driver.metrics()
                val exhausted = assertFailsWith<SqliteException> { driver.metrics() }
                assertTrue(exhausted.message.orEmpty().contains("exhausted"))
            } finally {
                connection.close()
                assertZeroResources(driver.shutdown())
            }
        }

    @Test
    fun browserLifecycleBusyIsTypedAndWorkerTerminationReleasesOwnership() =
        runTest(timeout = 180.seconds) {
            val databaseName = nextWorkerDriverDatabaseName()
            val first = SqliteWorkerSQLiteDriver.create()
            if (first.runtimeKind() != "browser-worker") {
                first.shutdown()
                return@runTest
            }

            val contender = SqliteWorkerSQLiteDriver.create()
            try {
                assertNull(first.inspect(databaseName, "built-in"))
                val busy = assertFailsWith<SqliteNowLifecycleBusyException> {
                    contender.inspect(databaseName, "built-in")
                }
                assertTrue(busy.databaseIdentity.contains("lifecycle lock is busy"))
            } finally {
                contender.shutdown()
                first.forceCleanup()
            }

            val successor = SqliteWorkerSQLiteDriver.create()
            try {
                assertNull(successor.inspect(databaseName, "built-in"))
            } finally {
                successor.forceCleanup()
            }
        }

    @Test
    fun browserInspectionCancellationTerminatesStartupOwnerAndReleasesLifecycleLock() =
        runTest(timeout = 180.seconds) {
            val databaseName = nextWorkerDriverDatabaseName()
            val cancelledOwner = SqliteWorkerSQLiteDriver.create()
            if (cancelledOwner.runtimeKind() != "browser-worker") {
                cancelledOwner.shutdown()
                return@runTest
            }

            cancelledOwner.holdNextResponseForTest("inspect")
            val inspection = async(start = CoroutineStart.UNDISPATCHED) {
                cancelledOwner.inspect(databaseName, "built-in")
            }
            awaitClientDiagnostic(cancelledOwner, "completedResponses", 1)
            inspection.cancel(CancellationException("cancel coordinated inspection"))
            assertFailsWith<CancellationException> { inspection.await() }

            val successor = SqliteWorkerSQLiteDriver.create()
            try {
                assertNull(successor.inspect(databaseName, "built-in"))
            } finally {
                successor.forceCleanup()
            }
        }

    // Cancel only after the worker has produced the resource and before Kotlin receives it.
    private suspend fun <T> cancelAfterCompletedResponse(
        driver: SqliteWorkerSQLiteDriver,
        command: String,
        operation: suspend () -> T,
    ) = coroutineScope {
        driver.holdNextResponseForTest(command)
        val pending = async(start = CoroutineStart.UNDISPATCHED) { operation() }
        try {
            awaitClientDiagnostic(driver, "completedResponses", 1)
            pending.cancel()
            assertFailsWith<CancellationException> { pending.await() }
        } finally {
            pending.cancelAndJoin()
        }
    }

    private suspend fun withWorker(
        pageRows: Int = SQLITE_WORKER_DEFAULT_PAGE_ROWS,
        pageBytes: Int = SQLITE_WORKER_DEFAULT_PAGE_BYTES,
        block: suspend (SqliteWorkerSQLiteDriver, SQLiteConnection) -> Unit,
    ) {
        val driver = SqliteWorkerSQLiteDriver.create(
            SqliteWorkerConfig(pageRows = pageRows, pageBytes = pageBytes),
        )
        var connection: SQLiteConnection? = null
        var primary: Throwable? = null
        try {
            connection = driver.open(nextWorkerDriverDatabaseName())
            block(driver, connection)
        } catch (failure: Throwable) {
            primary = failure
            throw failure
        } finally {
            connection?.close()
            try {
                val final = driver.shutdown()
                assertZeroResources(final)
            } catch (cleanupFailure: Throwable) {
                if (primary == null) throw cleanupFailure
                if (cleanupFailure !== primary) primary.addSuppressed(cleanupFailure)
            }
        }
    }

    private suspend fun execute(connection: SQLiteConnection, sql: String) {
        val statement = connection.prepare(sql)
        try {
            while (statement.step()) {
                // Drain statement results so transaction state is synchronized.
            }
        } finally {
            statement.close()
        }
    }

    private suspend fun awaitClientDiagnostic(
        connection: SqliteWorkerSQLiteConnection,
        name: String,
        expected: Int,
    ) {
        repeat(10_000) {
            if (sqliteWorkerClientDiagnostic(connection.diagnosticsForTest(), name) == expected) {
                return
            }
            yield()
        }
        error("Timed out waiting for $name=$expected")
    }

    private suspend fun awaitCompletedCommand(
        connection: SqliteWorkerSQLiteConnection,
        expected: String,
    ) {
        repeat(10_000) {
            val commands = sqliteWorkerJson.parseToJsonElement(
                connection.diagnosticsForTest(),
            ).jsonObject.getValue("completedCommands").jsonArray
                .map { it.jsonPrimitive.content }
            if (expected in commands) return
            yield()
        }
        error("Timed out waiting for completed command $expected")
    }

    private fun longRunningSql(): String =
        """
        WITH RECURSIVE values_long(value) AS (
            SELECT 1
            UNION ALL
            SELECT value + 1 FROM values_long WHERE value < 100000000
        )
        SELECT SUM(value) FROM values_long
        """.trimIndent()

    private fun assertZeroResources(metrics: SqliteWorkerMetrics) {
        assertEquals(0, metrics.pendingRequests)
        assertEquals(0, metrics.liveStatements)
        assertEquals(0, metrics.liveDatabases)
        assertEquals(1, metrics.workerStarts)
        assertEquals(1, metrics.workerStops)
    }

    private suspend fun awaitClientDiagnostic(
        driver: SqliteWorkerSQLiteDriver,
        name: String,
        expected: Int,
    ) {
        repeat(10_000) {
            if (clientDiagnostic(driver, name) == expected) return
            yield()
        }
        assertEquals(expected, clientDiagnostic(driver, name))
    }

    private fun clientDiagnostic(driver: SqliteWorkerSQLiteDriver, name: String): Int =
        sqliteWorkerClientDiagnostic(driver.diagnosticsForTest(), name)

    private fun globalWorkerDiagnostics(): WorkerGlobalDiagnostics {
        val values = sqliteWorkerJson.parseToJsonElement(
            SqliteWorkerTransport.globalDiagnosticsForTest(),
        ).jsonObject
        return WorkerGlobalDiagnostics(
            workersCreated = values.getValue("workersCreated").jsonPrimitive.content.toInt(),
            workersTerminated = values.getValue("workersTerminated").jsonPrimitive.content.toInt(),
            activeWorkers = values.getValue("activeWorkers").jsonPrimitive.content.toInt(),
            pendingStartups = values.getValue("pendingStartups").jsonPrimitive.content.toInt(),
        )
    }
}

private var workerDriverDatabaseSequence = 0

private fun nextWorkerDriverDatabaseName(): String =
    "worker-driver-test-${workerDriverDatabaseSequence++}"

private suspend fun cleanupWorkerDriverDatabase(databaseName: String) {
    val driver = SqliteWorkerSQLiteDriver.create()
    try {
        driver.cleanupMigrationStateForTest(databaseName)
    } finally {
        driver.shutdown()
    }
}

private data class WorkerGlobalDiagnostics(
    val workersCreated: Int,
    val workersTerminated: Int,
    val activeWorkers: Int,
    val pendingStartups: Int,
)

private data class SqliteWorkerRealScenario(
    val name: String,
    val value: Double,
)

private val sqliteWorkerRealScenarios = listOf(
    SqliteWorkerRealScenario("maximum finite", Double.MAX_VALUE),
    SqliteWorkerRealScenario("negative maximum finite", -Double.MAX_VALUE),
    SqliteWorkerRealScenario("positive value above signed 64-bit range", 1.0e20),
    SqliteWorkerRealScenario("negative value below signed 64-bit range", -1.0e20),
    SqliteWorkerRealScenario("positive JavaScript safe boundary plus one", 9_007_199_254_740_992.0),
    SqliteWorkerRealScenario("negative JavaScript safe boundary minus one", -9_007_199_254_740_992.0),
    SqliteWorkerRealScenario("integral real", 1.0),
    SqliteWorkerRealScenario("ordinary fraction", 6.57111473696007),
    SqliteWorkerRealScenario("positive zero", 0.0),
    SqliteWorkerRealScenario("minimum positive subnormal", Double.MIN_VALUE),
    SqliteWorkerRealScenario("maximum negative subnormal", -Double.MIN_VALUE),
)

private val sqliteWorkerNonFiniteRealScenarios = listOf(
    SqliteWorkerRealScenario("NaN", Double.NaN),
    SqliteWorkerRealScenario("positive infinity", Double.POSITIVE_INFINITY),
    SqliteWorkerRealScenario("negative infinity", Double.NEGATIVE_INFINITY),
)

private data class MalformedResponseScenario(
    val command: String,
    val action: suspend (SqliteWorkerSQLiteDriver) -> Unit,
)

private data class HandoffFailureScenario(
    val name: String,
    val mode: String,
    val failureOccursBeforeAcceptance: Boolean,
    val expectedMessageFragment: String,
)
