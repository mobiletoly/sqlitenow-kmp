package dev.goquick.sqlitenow.core

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteException as DriverSQLiteException
import androidx.sqlite.SQLiteStatement
import androidx.sqlite.async.step as asyncStep
import dev.goquick.sqlitenow.common.SqliteNowLogger
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.sqlite.SqliteException
import dev.goquick.sqlitenow.core.sqlite.trackSQLiteStatement
import dev.goquick.sqlitenow.core.sqlite.use
import java.nio.file.Files
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext
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
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.yield

class SafeSQLiteConnectionIsolationTest {
    @Test
    fun tracedContextPreservesCancellationAndSqliteExceptionClassification() = runBlocking {
        val fixture = recordingConnection()
        val cancellation = CancellationException("TRACE_CANCELLATION_SENTINEL")
        val observedCancellation = assertFailsWith<CancellationException> {
            fixture.connection.withContextAndTrace { throw cancellation }
        }
        assertSame(cancellation, observedCancellation)

        val workerCause = IllegalStateException("WORKER_CAUSE_SENTINEL")
        val cleanupFailure = IllegalStateException("WORKER_CLEANUP_SENTINEL")
        val sqliteFailure = SqliteException("SQLITE_FAILURE_SENTINEL", workerCause).also {
            it.addSuppressed(cleanupFailure)
        }
        val observedSqlite = assertFailsWith<SqliteException> {
            fixture.connection.withContextAndTrace { throw sqliteFailure }
        }
        assertSame(workerCause, observedSqlite.cause)
        assertEquals(listOf(cleanupFailure), observedSqlite.suppressed.toList())
        fixture.connection.close()
    }

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
    fun closeContextFailuresDoNotBypassOwnedCleanup() = runBlocking {
        listOf(
            CloseContextFailureScenario(
                name = "capture failure",
                captureFailure = IllegalStateException("CLOSE_CONTEXT_CAPTURE_SENTINEL"),
            ),
            CloseContextFailureScenario(
                name = "restore failure",
                restoreFailure = IllegalStateException("CLOSE_CONTEXT_RESTORE_SENTINEL"),
            ),
        ).forEach { scenario ->
            val fixture = recordingConnection(
                executionContextHook = FailingCloseContextHook(
                    captureFailure = scenario.captureFailure,
                    restoreFailure = scenario.restoreFailure,
                ),
            )

            val thrown = assertFailsWith<IllegalStateException>(scenario.name) {
                fixture.connection.close()
            }

            assertSame(scenario.failure, thrown, scenario.name)
            assertEquals(1, fixture.persistence.closeCalls, scenario.name)
            assertEquals(1, fixture.raw.closeCalls, scenario.name)
            assertEquals(1, fixture.raw.cleanupCalls, scenario.name)
            assertEquals(1, fixture.execution.closeCalls, scenario.name)
        }
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
    fun cancelledOrdinaryCloseWhileMutexIsHeldHasNoCloseSideEffect() = runBlocking {
        val fixture = recordingOrdinaryConnection()
        val ownerEntered = CompletableDeferred<Unit>()
        val releaseOwner = CompletableDeferred<Unit>()
        val holder = async(start = kotlinx.coroutines.CoroutineStart.UNDISPATCHED) {
            fixture.connection.withExclusiveAccess {
                ownerEntered.complete(Unit)
                releaseOwner.await()
            }
        }
        ownerEntered.await()

        val closeStarted = CompletableDeferred<Unit>()
        val closing = launch(Dispatchers.Default) {
            closeStarted.complete(Unit)
            fixture.connection.close()
        }
        closeStarted.await()
        closing.cancel(CancellationException("cancel ordinary close before ownership"))
        withTimeout(1_000) { closing.join() }

        assertEquals(0, fixture.raw.closeCalls)
        assertEquals(0, fixture.execution.closeCalls)

        releaseOwner.complete(Unit)
        holder.await()
        fixture.connection.close()
        assertEquals(1, fixture.raw.closeCalls)
        assertEquals(1, fixture.execution.closeCalls)
    }

    @Test
    fun cancelledOrdinaryCloseDuringContextRestorationPreservesCleanupOutcome() = runBlocking {
        listOf(
            CancelledOrdinaryCloseScenario(
                name = "successful fallback",
                cancellationMessage = "cancel ordinary close after ownership",
            ),
            CancelledOrdinaryCloseScenario(
                name = "fallback cleanup failures",
                cancellationMessage = "cancel before fallback failure",
                rawCloseFailure = IllegalStateException("ORDINARY_RAW_CLOSE_SENTINEL"),
                executionCloseFailure =
                    IllegalStateException("ORDINARY_EXECUTION_CLOSE_SENTINEL"),
            ),
        ).forEach { scenario ->
            val hook = SuspendingCloseContextHook()
            val fixture = recordingOrdinaryConnection(
                executionContextHook = hook,
                rawCloseFailure = scenario.rawCloseFailure,
                executionCloseFailure = scenario.executionCloseFailure,
                executionDispatcher = Dispatchers.Default,
            )
            hook.suspendNextRestore()
            val observed = CompletableDeferred<Throwable>()

            supervisorScope {
                val closing = launch {
                    observed.complete(
                        runCatching { fixture.connection.close() }
                            .exceptionOrNull()
                            ?: error("cancelled ordinary close unexpectedly succeeded"),
                    )
                }
                hook.restoreEntered.await()
                closing.cancel(CancellationException(scenario.cancellationMessage))
                withTimeout(1_000) { closing.join() }
            }

            val cancellation = observed.await()
            assertTrue(cancellation is CancellationException, scenario.name)
            assertEquals(scenario.cancellationMessage, cancellation.message, scenario.name)
            assertEquals(
                scenario.expectedSuppressedFailures,
                cancellation.suppressedExceptions.toList(),
                scenario.name,
            )
            assertEquals(1, fixture.raw.closeCalls, scenario.name)
            assertEquals(1, fixture.execution.closeCalls, scenario.name)
            assertTrue(
                runCatching { fixture.connection.withExclusiveAccess { Unit } }.isFailure,
                scenario.name,
            )

            fixture.connection.close()
            assertEquals(1, fixture.raw.closeCalls, scenario.name)
            assertEquals(1, fixture.execution.closeCalls, scenario.name)
        }
    }

    @Test
    fun cancelledOrdinaryCloseAfterCleanupStartsRetainsCleanupFailures() = runBlocking {
        val persistenceEntered = CompletableDeferred<Unit>()
        val releasePersistence = CompletableDeferred<Unit>()
        val rawCloseFailure = IllegalStateException("STARTED_RAW_CLOSE_SENTINEL")
        val executionCloseFailure = IllegalStateException("STARTED_EXECUTION_CLOSE_SENTINEL")
        val fixture = recordingOrdinaryConnection(
            persistence = RecordingPersistenceController(
                closeAction = {
                    persistenceEntered.complete(Unit)
                    releasePersistence.await()
                },
            ),
            rawCloseFailure = rawCloseFailure,
            executionCloseFailure = executionCloseFailure,
            executionDispatcher = Dispatchers.Default,
        )
        val observed = CompletableDeferred<Throwable>()

        supervisorScope {
            val closing = launch {
                observed.complete(
                    runCatching { fixture.connection.close() }
                        .exceptionOrNull()
                        ?: error("cancelled ordinary close unexpectedly succeeded"),
                )
            }
            persistenceEntered.await()
            closing.cancel(CancellationException("cancel after cleanup starts"))
            releasePersistence.complete(Unit)
            withTimeout(1_000) { closing.join() }
        }

        val cancellation = observed.await()
        assertTrue(cancellation is CancellationException)
        assertEquals(
            listOf(rawCloseFailure, executionCloseFailure),
            cancellation.suppressedExceptions.toList(),
        )
        assertEquals(1, fixture.persistence.closeCalls)
        assertEquals(1, fixture.raw.closeCalls)
        assertEquals(1, fixture.execution.closeCalls)
    }

    @Test
    fun cancelledOrdinaryCloseBoundsSuspendingFallbackPersistence() = runBlocking {
        val hook = SuspendingCloseContextHook()
        val persistenceEntered = CompletableDeferred<Unit>()
        val fixture = recordingOrdinaryConnection(
            executionContextHook = hook,
            persistence = RecordingPersistenceController(
                closeAction = {
                    persistenceEntered.complete(Unit)
                    awaitCancellation()
                },
            ),
            executionDispatcher = Dispatchers.Default,
            ordinaryCloseCleanupTimeoutMillis = 100,
        )
        hook.suspendNextRestore()
        val observed = CompletableDeferred<Throwable>()

        supervisorScope {
            val closing = launch {
                observed.complete(
                    runCatching { fixture.connection.close() }
                        .exceptionOrNull()
                        ?: error("cancelled ordinary close unexpectedly succeeded"),
                )
            }
            hook.restoreEntered.await()
            closing.cancel(CancellationException("cancel before hanging fallback"))
            withTimeout(1_000) { persistenceEntered.await() }
            withTimeout(2_000) { closing.join() }
        }

        val cancellation = observed.await()
        assertTrue(cancellation is CancellationException)
        val deadlineFailure = cancellation.suppressedExceptions.single()
        assertTrue(deadlineFailure is SqliteException)
        assertTrue(deadlineFailure.message.orEmpty().contains("100ms cleanup deadline"))
        assertTrue(deadlineFailure.cause is TimeoutCancellationException)
        assertEquals(1, fixture.persistence.closeCalls)
        assertEquals(1, fixture.raw.closeCalls)
        assertEquals(1, fixture.execution.closeCalls)

        fixture.connection.close()
        assertEquals(1, fixture.persistence.closeCalls)
        assertEquals(1, fixture.raw.closeCalls)
        assertEquals(1, fixture.execution.closeCalls)
    }

    @Test
    fun ordinaryFallbackDispatchFailureLeavesCleanupRetryable() = runBlocking {
        val dispatcher = RejectNextDispatchDispatcher()
        val hook = CancelOnceCloseContextHook {
            dispatcher.rejectNextDispatch()
        }
        val fixture = recordingOrdinaryConnection(
            executionContextHook = hook,
            executionDispatcher = dispatcher,
        )

        val cancellation = assertFailsWith<CancellationException> {
            fixture.connection.close()
        }

        assertEquals("cancel before fallback dispatch", cancellation.message)
        assertTrue(
            cancellation.suppressedExceptions.single() is RejectedExecutionException,
        )
        assertEquals(0, fixture.raw.closeCalls)
        assertEquals(0, fixture.execution.closeCalls)

        fixture.connection.close()
        assertEquals(1, fixture.raw.closeCalls)
        assertEquals(1, fixture.execution.closeCalls)
    }

    @Test
    fun cancelledCloseCompletesSuspendCleanupAndSuppressesItsFailureOntoCancellation() =
        runBlocking {
            val cancellation = CancellationException("CANCELLED_CLOSE_SENTINEL")
            val cleanupFailure = IllegalStateException("SUSPEND_CLEANUP_SENTINEL")
            lateinit var fixture: RecordingFixture
            val observed = CompletableDeferred<Throwable>()

            supervisorScope {
                val closing = launch {
                    val callerJob = currentCoroutineContext()[Job] ?: error("missing caller job")
                    fixture = recordingConnection(
                        cleanupAction = {
                            callerJob.cancel(cancellation)
                            throw cleanupFailure
                        },
                    )
                    observed.complete(
                        runCatching { fixture.connection.close() }
                            .exceptionOrNull()
                            ?: error("cancelled close unexpectedly succeeded"),
                    )
                }
                closing.join()
            }

            val thrown = observed.await()
            assertTrue(thrown is CancellationException)
            assertEquals(cancellation.message, thrown.message)
            assertEquals(listOf(cleanupFailure), thrown.suppressed.toList())
            assertEquals(1, fixture.raw.closeCalls)
            assertEquals(1, fixture.raw.cleanupCalls)
            assertEquals(1, fixture.execution.closeCalls)
        }

    @Test
    fun cancellationAfterCloseStartsRequestsOneSharedBoundedForceCleanup() = runBlocking {
        supervisorScope {
            val fixture = recordingConnection()
            val ownerEntered = CompletableDeferred<Unit>()
            val releaseOwner = CompletableDeferred<Unit>()
            val holder = async(start = kotlinx.coroutines.CoroutineStart.UNDISPATCHED) {
                fixture.connection.withExclusiveAccess {
                    ownerEntered.complete(Unit)
                    releaseOwner.await()
                }
            }
            ownerEntered.await()

            val closeOwner = async(start = kotlinx.coroutines.CoroutineStart.UNDISPATCHED) {
                fixture.connection.close()
            }
            val observedWaiterFailure = CompletableDeferred<Throwable>()
            val cancelledWaiter = launch(start = kotlinx.coroutines.CoroutineStart.UNDISPATCHED) {
                observedWaiterFailure.complete(
                    runCatching { fixture.connection.close() }
                        .exceptionOrNull()
                        ?: error("cancelled close unexpectedly succeeded"),
                )
            }
            val cancellation = CancellationException("ACTIVE_CLOSE_CANCELLATION_SENTINEL")
            cancelledWaiter.cancel(cancellation)

            val observedCancellation = observedWaiterFailure.await()
            cancelledWaiter.join()
            val ownerFailure = assertFailsWith<SqliteException> {
                closeOwner.await()
            }

            assertTrue(observedCancellation is CancellationException)
            assertEquals(cancellation.message, observedCancellation.message)
            assertEquals(
                ownerFailure.message,
                observedCancellation.suppressed.single().message,
            )
            assertTrue(ownerFailure.message.orEmpty().contains("cleanup deadline"))
            assertEquals(1, fixture.raw.forceCleanupCalls)
            assertEquals(1, fixture.execution.closeCalls)

            releaseOwner.complete(Unit)
            holder.await()
            fixture.connection.close()
            assertEquals(1, fixture.raw.forceCleanupCalls)
            assertEquals(1, fixture.execution.closeCalls)
        }
    }

    @Test
    fun workerCloseOwnerCancellationDuringContextRestorationReachesDeadline() = runBlocking {
        val hook = SuspendingCloseContextHook()
        val fixture = recordingConnection(executionContextHook = hook)
        hook.suspendNextRestore()
        val observed = CompletableDeferred<Throwable>()

        supervisorScope {
            val closing = launch {
                observed.complete(
                    runCatching { fixture.connection.close() }
                        .exceptionOrNull()
                        ?: error("cancelled close unexpectedly succeeded"),
                )
            }
            hook.restoreEntered.await()
            closing.cancel(CancellationException("cancel owner during context restoration"))
            withTimeout(2_000) { closing.join() }
        }

        val cancellation = observed.await()
        assertTrue(cancellation is CancellationException)
        assertTrue(
            cancellation.suppressedExceptions.single().message.orEmpty()
                .contains("cleanup deadline"),
        )
        assertEquals(0, fixture.raw.closeCalls)
        assertEquals(0, fixture.raw.cleanupCalls)
        assertEquals(1, fixture.raw.forceCleanupCalls)
        assertEquals(1, fixture.execution.closeCalls)
        assertTrue(runCatching { fixture.connection.withExclusiveAccess { Unit } }.isFailure)
    }

    @Test
    fun workerCloseWaiterCancellationDuringContextRestorationReachesSharedDeadline() =
        runBlocking {
            val hook = SuspendingCloseContextHook()
            val fixture = recordingConnection(executionContextHook = hook)
            hook.suspendNextRestore()
            supervisorScope {
                val owner = async { fixture.connection.close() }
                hook.restoreEntered.await()
                val waiterFailure = CompletableDeferred<Throwable>()
                val waiter = launch(start = kotlinx.coroutines.CoroutineStart.UNDISPATCHED) {
                    waiterFailure.complete(
                        runCatching { fixture.connection.close() }
                            .exceptionOrNull()
                            ?: error("cancelled close unexpectedly succeeded"),
                    )
                }
                waiter.cancel(CancellationException("cancel waiter during context restoration"))
                withTimeout(2_000) { waiter.join() }

                val ownerFailure = assertFailsWith<SqliteException> { owner.await() }
                val waiterCancellation = waiterFailure.await()
                assertTrue(waiterCancellation is CancellationException)
                assertEquals(
                    ownerFailure.message,
                    waiterCancellation.suppressedExceptions.single().message,
                )
                assertTrue(ownerFailure.message.orEmpty().contains("cleanup deadline"))
                assertEquals(0, fixture.raw.closeCalls)
                assertEquals(0, fixture.raw.cleanupCalls)
                assertEquals(1, fixture.raw.forceCleanupCalls)
                assertEquals(1, fixture.execution.closeCalls)
            }
        }

    @Test
    fun workerContextCancellationForcesCleanupAndCompletesTheSharedAttempt() = runBlocking {
        val cancellation = CancellationException("CONTEXT_RESTORATION_CANCELLATION_SENTINEL")
        val fixture = recordingConnection(
            executionContextHook = FailingCloseContextHook(restoreFailure = cancellation),
        )

        val thrown = assertFailsWith<CancellationException> {
            fixture.connection.close()
        }

        assertSame(cancellation, thrown)
        assertEquals(0, fixture.raw.closeCalls)
        assertEquals(0, fixture.raw.cleanupCalls)
        assertEquals(1, fixture.raw.forceCleanupCalls)
        assertEquals(1, fixture.execution.closeCalls)
        fixture.connection.close()
        assertEquals(1, fixture.raw.forceCleanupCalls)
        assertEquals(1, fixture.execution.closeCalls)
    }

    @Test
    fun statementUse_suppressesCloseFailureOntoBodyFailure() = runBlocking {
        val bodyFailure = IllegalStateException("BODY_SENTINEL")
        val closeFailure = IllegalStateException("STATEMENT_CLOSE_SENTINEL")
        val statement: SQLiteStatement = RecordingRawStatement(closeFailure = closeFailure)

        val thrown = assertFailsWith<IllegalStateException> {
            statement.use { throw bodyFailure }
        }

        assertSame(bodyFailure, thrown)
        assertEquals(listOf(closeFailure), thrown.suppressed.toList())
    }

    @Test
    fun statementUse_normalizesDriverCloseFailure() = runBlocking {
        val driverFailure = DriverSQLiteException("STATEMENT_DRIVER_CLOSE_SENTINEL")
        val statement: SQLiteStatement = RecordingRawStatement(closeFailure = driverFailure)

        val thrown = assertFailsWith<SqliteException> {
            statement.use { Unit }
        }

        assertSame(driverFailure, thrown.cause)
    }

    @Test
    fun statementUse_preservesBodyAndSuppressesNormalizedDriverCloseFailure() = runBlocking {
        val bodyFailure = IllegalStateException("BODY_SENTINEL")
        val driverFailure = DriverSQLiteException("STATEMENT_DRIVER_CLOSE_SENTINEL")
        val statement: SQLiteStatement = RecordingRawStatement(closeFailure = driverFailure)

        val thrown = assertFailsWith<IllegalStateException> {
            statement.use { throw bodyFailure }
        }

        assertSame(bodyFailure, thrown)
        val suppressed = thrown.suppressed.single()
        assertTrue(suppressed is SqliteException)
        assertSame(driverFailure, suppressed.cause)
    }

    @Test
    fun connectionClose_normalizesDriverFailure() = runBlocking {
        val driverFailure = DriverSQLiteException("RAW_DRIVER_CLOSE_SENTINEL")
        val fixture = recordingConnection(rawCloseFailure = driverFailure)

        val thrown = assertFailsWith<SqliteException> {
            fixture.connection.close()
        }

        assertSame(driverFailure, thrown.cause)
        assertEquals(1, fixture.raw.closeCalls)
        assertEquals(1, fixture.execution.closeCalls)
    }

    @Test
    fun connectionClose_preservesPersistenceFailureAndSuppressesNormalizedDriverFailure() = runBlocking {
        val persistenceFailure = IllegalStateException("CLOSE_PERSISTENCE_SENTINEL")
        val driverFailure = DriverSQLiteException("RAW_DRIVER_CLOSE_SENTINEL")
        val fixture = recordingConnection(
            persistence = RecordingPersistenceController(closeFailure = persistenceFailure),
            rawCloseFailure = driverFailure,
        )

        val thrown = assertFailsWith<IllegalStateException> {
            fixture.connection.close()
        }

        assertSame(persistenceFailure, thrown)
        val suppressed = thrown.suppressed.single()
        assertTrue(suppressed is SqliteException)
        assertSame(driverFailure, suppressed.cause)
        assertEquals(1, fixture.raw.closeCalls)
        assertEquals(1, fixture.execution.closeCalls)
    }

    @Test
    fun asyncStep_invokesExactlyOneUnderlyingNonWebStep() = runBlocking {
        val raw = RecordingRawStatement()
        val statement = trackSQLiteStatement(
            statement = raw,
            cleanupFailureObserver = {},
            beforeCloseObserver = {},
            closeSuccessObserver = {},
        )

        assertFalse(statement.asyncStep())
        assertEquals(1, raw.stepCalls)
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
    fun fatalDisposal_normalizesDriverFailuresAndPreservesSuppressionOrder() = runBlocking {
        val bodyFailure = IllegalStateException("BODY_FAILURE")
        val resetDriverFailure = DriverSQLiteException("RESET_DRIVER_FAILURE")
        val rawCloseDriverFailure = DriverSQLiteException("RAW_CLOSE_DRIVER_FAILURE")
        val fixture = recordingConnection(
            rawCloseFailure = rawCloseDriverFailure,
            statements = ArrayDeque(listOf(RecordingRawStatement(resetFailure = resetDriverFailure))),
        )

        val observed = assertFailsWith<IllegalStateException> {
            fixture.connection.withExclusiveAccess {
                val statement = fixture.connection.prepare("fatal-driver-cleanup")
                val resetFailure = runCatching { statement.reset() }.exceptionOrNull()
                assertTrue(resetFailure is SqliteException)
                assertSame(resetDriverFailure, resetFailure.cause)
                throw bodyFailure
            }
        }

        assertSame(bodyFailure, observed)
        assertEquals(2, observed.suppressed.size)
        assertTrue(observed.suppressed[0] is SqliteException)
        assertSame(resetDriverFailure, observed.suppressed[0].cause)
        assertTrue(observed.suppressed[1] is SqliteException)
        assertSame(rawCloseDriverFailure, observed.suppressed[1].cause)
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
    fun migrationOwnerOperationFinalizesBeforeReturningToCallerDispatcher() = runBlocking {
        withTimeout(10_000) {
            withTestDatabase { database ->
                database.open()
                val connection = database.connection()
                val callerDispatcher = QueuedDispatcher()
                val expectedFailure = IllegalStateException("migration operation failed")

                connection.withExclusiveAccess {
                    val access = connection.captureMigrationOwnerAccess()
                    val releaseOperation = CompletableDeferred<Unit>()
                    val operationFinishing = CompletableDeferred<Unit>()

                    supervisorScope {
                        val operation = async(callerDispatcher, start = CoroutineStart.UNDISPATCHED) {
                            connection.withMigrationOwnerAccess(access) {
                                releaseOperation.await()
                                operationFinishing.complete(Unit)
                                throw expectedFailure
                            }
                        }

                        releaseOperation.complete(Unit)
                        operationFinishing.await()
                        yield()

                        access.expire()
                        access.expireAndDrain(
                            cancelOperations = false,
                            propagateOperationFailure = true,
                        )

                        callerDispatcher.runAll()
                        val observedFailure = assertFailsWith<IllegalStateException> { operation.await() }
                        assertEquals(expectedFailure.message, observedFailure.message)
                    }
                }
            }
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

    private class FailingCloseContextHook(
        private val captureFailure: Throwable? = null,
        private val restoreFailure: Throwable? = null,
    ) : SqliteNowContextHook {
        override fun capture(): Any {
            captureFailure?.let { throw it }
            return Unit
        }

        override suspend fun <T> withCaptured(captured: Any?, block: suspend () -> T): T {
            restoreFailure?.let { throw it }
            return block()
        }
    }

    private class SuspendingCloseContextHook : SqliteNowContextHook {
        val restoreEntered = CompletableDeferred<Unit>()
        private var suspendNext = false

        fun suspendNextRestore() {
            suspendNext = true
        }

        override fun capture(): Any = Unit

        override suspend fun <T> withCaptured(captured: Any?, block: suspend () -> T): T {
            if (suspendNext) {
                suspendNext = false
                restoreEntered.complete(Unit)
                kotlinx.coroutines.awaitCancellation()
            }
            return block()
        }
    }

    private class CancelOnceCloseContextHook(
        private val beforeCancellation: () -> Unit,
    ) : SqliteNowContextHook {
        private var cancelNextRestore = true

        override fun capture(): Any = Unit

        override suspend fun <T> withCaptured(captured: Any?, block: suspend () -> T): T {
            if (cancelNextRestore) {
                cancelNextRestore = false
                beforeCancellation()
                throw CancellationException("cancel before fallback dispatch")
            }
            return block()
        }
    }

    private class RejectNextDispatchDispatcher : CoroutineDispatcher() {
        private var rejectNextDispatch = false

        fun rejectNextDispatch() {
            rejectNextDispatch = true
        }

        override fun isDispatchNeeded(context: CoroutineContext): Boolean = true

        override fun dispatch(context: CoroutineContext, block: Runnable) {
            if (rejectNextDispatch) {
                rejectNextDispatch = false
                throw RejectedExecutionException("ordinary fallback dispatch rejected")
            }
            block.run()
        }
    }

    private class QueuedDispatcher : CoroutineDispatcher() {
        private val queued = ConcurrentLinkedQueue<Runnable>()

        override fun isDispatchNeeded(context: CoroutineContext): Boolean = true

        override fun dispatch(context: CoroutineContext, block: Runnable) {
            queued.add(block)
        }

        fun runAll() {
            while (true) {
                queued.poll()?.run() ?: return
            }
        }
    }

    private data class CloseContextFailureScenario(
        val name: String,
        val captureFailure: Throwable? = null,
        val restoreFailure: Throwable? = null,
    ) {
        val failure: Throwable
            get() = checkNotNull(captureFailure ?: restoreFailure)
    }

    private data class CancelledOrdinaryCloseScenario(
        val name: String,
        val cancellationMessage: String,
        val rawCloseFailure: Throwable? = null,
        val executionCloseFailure: Throwable? = null,
    ) {
        val expectedSuppressedFailures: List<Throwable>
            get() = listOfNotNull(rawCloseFailure, executionCloseFailure)
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
        cleanupAction: (suspend () -> Unit)? = null,
        executionContextHook: SqliteNowContextHook? = null,
    ): RecordingFixture {
        val raw = RecordingRawConnection(
            closeFailure = rawCloseFailure,
            statements = statements,
            cleanupAction = cleanupAction,
        )
        val execution = RecordingExecutionContext(executionCloseFailure)
        return RecordingFixture(
            connection = SafeSQLiteConnection(
                ref = raw,
                persistenceController = persistence,
                executionContextHook = executionContextHook,
                executionContext = execution,
            ),
            raw = raw,
            persistence = persistence,
            execution = execution,
        )
    }

    private fun recordingOrdinaryConnection(
        executionContextHook: SqliteNowContextHook? = null,
        persistence: RecordingPersistenceController = RecordingPersistenceController(),
        rawCloseFailure: Throwable? = null,
        executionCloseFailure: Throwable? = null,
        executionDispatcher: CoroutineDispatcher = Dispatchers.Unconfined,
        ordinaryCloseCleanupTimeoutMillis: Long = 5_000,
    ): RecordingOrdinaryFixture {
        val raw = RecordingOrdinaryRawConnection(closeFailure = rawCloseFailure)
        val execution = RecordingExecutionContext(
            closeFailure = executionCloseFailure,
            dispatcher = executionDispatcher,
        )
        return RecordingOrdinaryFixture(
            connection = SafeSQLiteConnection(
                ref = raw,
                persistenceController = persistence,
                executionContextHook = executionContextHook,
                executionContext = execution,
                ordinaryCloseCleanupTimeoutMillis = ordinaryCloseCleanupTimeoutMillis,
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

    private data class RecordingOrdinaryFixture(
        val connection: SafeSQLiteConnection,
        val raw: RecordingOrdinaryRawConnection,
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
        private val closeAction: suspend () -> Unit = {},
    ) : PersistenceController {
        override val restoredFromSnapshot: Boolean = false
        var closeCalls = 0
            private set
        var flushCalls = 0
            private set

        override suspend fun onOperationComplete(connection: SQLiteConnection, inTransaction: Boolean) {
            if (!inTransaction) operationFailure?.let { throw it }
        }

        override suspend fun onTransactionCommitted(connection: SQLiteConnection) {
            commitFailure?.let { throw it }
        }

        override suspend fun flush(connection: SQLiteConnection) {
            flushCalls++
        }

        override suspend fun onClose(connection: SQLiteConnection) {
            closeCalls++
            closeAction()
            closeFailure?.let { throw it }
        }
    }

    private class RecordingExecutionContext(
        private val closeFailure: Throwable?,
        override val dispatcher: CoroutineDispatcher = Dispatchers.Unconfined,
    ) : SqliteConnectionExecutionContext {
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
        private val cleanupAction: (suspend () -> Unit)?,
    ) : SQLiteConnection, SuspendSQLiteConnectionCleanup {
        override val cleanupTimeoutMillis: Int = 100
        val executedSql = mutableListOf<String>()
        var closeCalls = 0
            private set
        var cleanupCalls = 0
            private set
        var forceCleanupCalls = 0
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

        override suspend fun awaitCleanup() {
            cleanupCalls++
            cleanupAction?.invoke()
        }

        override suspend fun awaitCleanupDeadline() {
            delay(cleanupTimeoutMillis.toLong())
        }

        override suspend fun forceCleanup() {
            forceCleanupCalls++
        }

        private fun recordExecution(sql: String) {
            executedSql += sql
            when (sql) {
                "BEGIN", "BEGIN IMMEDIATE", "BEGIN EXCLUSIVE" -> inTransaction = true
                "COMMIT", "ROLLBACK" -> inTransaction = false
            }
        }
    }

    private class RecordingOrdinaryRawConnection(
        private val closeFailure: Throwable?,
    ) : SQLiteConnection {
        var closeCalls = 0
            private set

        override fun inTransaction(): Boolean = false

        override fun prepare(sql: String): SQLiteStatement = RecordingRawStatement(name = sql)

        override fun close() {
            closeCalls++
            closeFailure?.let { throw it }
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
        var stepCalls: Int = 0
            private set

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
            stepCalls++
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
