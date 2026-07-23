/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.core

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import androidx.sqlite.async.executeSQL
import androidx.sqlite.async.prepare
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.sqlite.clearTrackedSQLiteStatementObservers
import dev.goquick.sqlitenow.core.sqlite.executeSqliteNowSql
import dev.goquick.sqlitenow.core.sqlite.SqliteException
import dev.goquick.sqlitenow.core.sqlite.trackSQLiteStatement
import dev.goquick.sqlitenow.core.sqlite.wrapAndroidxSqliteAsyncCall
import dev.goquick.sqlitenow.core.sqlite.wrapAndroidxSqliteCall
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull

/**
 * A thread-safe wrapper around SQLiteConnection that ensures that only one coroutine
 * can access the connection at a time.
 *
 * This is necessary because many SQLite driver implementations are not thread-safe and can cause issues when used concurrently
 * from multiple coroutines.
 */
class SafeSQLiteConnection internal constructor(
    val ref: SQLiteConnection,
    val debug: Boolean = false,
    private val persistenceController: PersistenceController = NoopPersistenceController(),
    private val executionContextHook: SqliteNowContextHook? = null,
    private val executionContext: SqliteConnectionExecutionContext,
    private val ordinaryCloseCleanupTimeoutMillis: Long =
        DEFAULT_ORDINARY_CLOSE_CLEANUP_TIMEOUT_MILLIS,
) {
    val dispatcher: CoroutineDispatcher = executionContext.dispatcher
    private var activeTransactionDepth: Int = 0
    private var activeTransactionToken: Any? = null
    private var tableInvalidationListener: ((Set<String>) -> Unit)? = null
    private val connectionMutex = Mutex()
    private val closeCoordinationMutex = Mutex()
    @kotlin.concurrent.Volatile
    private var state = ConnectionState.OPEN
    private var executionContextClosed = false
    private var closeAttempt: CloseAttempt? = null
    private var fatalFailure: Throwable? = null
    private val liveStatements = mutableListOf<LiveStatement>()
    internal var beforeTransactionCommitForTest: (() -> Unit)? = null
    internal var beforeTransactionRollbackForTest: (() -> Unit)? = null

    internal val restoredFromSnapshot: Boolean
        get() = persistenceController.restoredFromSnapshot

    private fun isInTransaction(): Boolean =
        activeTransactionDepth > 0 || wrapAndroidxSqliteCall { ref.inTransaction() }

    internal suspend fun <T> withDispatcherContext(block: suspend () -> T): T {
        val hook = executionContextHook
        val captured = hook?.capture()
        val currentOwner = coroutineContext[ConnectionOwnerContext]
        val alreadyOwnsConnection = currentOwner?.connection === this
        val alreadyOnDispatcher = coroutineContext[ContinuationInterceptor] === dispatcher
        val ownerToken = currentOwner?.token ?: Any()
        if (alreadyOwnsConnection && alreadyOnDispatcher) {
            coroutineContext.ensureActive()
            return if (hook != null) {
                hook.withCaptured(captured, block)
            } else {
                block()
            }
        }
        if (alreadyOwnsConnection) {
            return withContext(dispatcher + ConnectionOwnerContext(this, ownerToken)) {
                if (hook != null) {
                    hook.withCaptured(captured, block)
                } else {
                    block()
                }
            }
        }

        connectionMutex.lock()
        try {
            if (state != ConnectionState.OPEN) {
                if (state == ConnectionState.FATAL) {
                    val failure = withContext(dispatcher + ConnectionOwnerContext(this, ownerToken)) {
                        withContext(NonCancellable) { disposeFatalConnection(null) }
                    }
                    throw failure ?: closedConnectionFailure()
                }
                throw closedConnectionFailure()
            }
            return withContext(dispatcher + ConnectionOwnerContext(this, ownerToken)) {
                runOwnedBlock(hook, captured, block)
            }
        } finally {
            connectionMutex.unlock()
        }
    }

    private suspend fun <T> runOwnedBlock(
        hook: SqliteNowContextHook?,
        captured: Any?,
        block: suspend () -> T,
    ): T {
        var primaryFailure: Throwable? = null
        try {
            return if (hook != null) hook.withCaptured(captured, block) else block()
        } catch (t: Throwable) {
            primaryFailure = t
            throw t
        } finally {
            if (state == ConnectionState.FATAL) {
                val disposalFailure = withContext(NonCancellable) {
                    disposeFatalConnection(primaryFailure)
                }
                if (primaryFailure == null && disposalFailure != null) throw disposalFailure
            }
        }
    }

    suspend fun <T> withContextAndTrace(block: suspend () -> T): T {
        val creationTrace = Throwable().stackTraceToString().replace("\n\n", "\n")
        return try {
            withDispatcherContext {
                val result = block()
                notifyOperationComplete()
                result
            }
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (failure: SqliteException) {
            val combinedMessage = buildString {
                appendLine(failure.message ?: "")
                appendLine(creationTrace)
                appendLine("Original exception stack:")
                appendLine(failure.stackTraceToString())
            }
            throw SqliteException(combinedMessage, failure.cause ?: failure).also { traced ->
                failure.suppressedExceptions.forEach(traced::addSuppressed)
            }
        } catch (failure: Exception) {
            val combinedMessage = buildString {
                appendLine(failure.message ?: "")
                appendLine(creationTrace)
                appendLine("Original exception stack:")
                appendLine(failure.stackTraceToString())
            }
            throw SqliteNowException(combinedMessage, failure)
        }
    }

    /**
     * Runs a block with exclusive access to this connection for the full suspend block.
     * Generated queries and handwritten multi-statement reads should prefer this over
     * using [dispatcher] directly so active statements cannot overlap transactions.
     */
    suspend fun <T> withExclusiveAccess(block: suspend () -> T): T {
        return withDispatcherContext(block)
    }

    suspend fun execSQL(sql: String) {
        sqliteNowLogger.d { "SafeSQLiteConnection.execSQL: $sql" }
        withDispatcherContext {
            wrapAndroidxSqliteAsyncCall { executeSqliteNowSql(ref, sql) }
            notifyOperationComplete()
        }
    }

    suspend fun prepare(sql: String): SQLiteStatement {
        sqliteNowLogger.d { "SafeSQLiteConnection.prepare: $sql" }
        return withDispatcherContext {
            registerStatement(wrapAndroidxSqliteAsyncCall { ref.prepare(sql) })
        }
    }

    suspend fun close() {
        if (coroutineContext[ConnectionOwnerContext]?.connection === this) {
            throw IllegalStateException("Cannot close SQLite connection from its active owner context")
        }

        val cleanupController = ref as? SuspendSQLiteConnectionCleanup
        if (cleanupController == null) {
            closeOrdinaryConnection()
            return
        }

        val callerContext = coroutineContext
        val cancellationSignal = callerContext[Job]?.let(::Job)
        val (attempt, isOwner) = withContext(NonCancellable) {
            claimCloseAttempt()
        }
        val primaryFailure = try {
            withContext(NonCancellable) {
                if (isOwner) {
                    performWorkerClose(attempt, cancellationSignal, cleanupController)
                } else {
                    awaitCloseAttempt(attempt, cancellationSignal)
                }
            }
        } finally {
            cancellationSignal?.complete()
        }
        try {
            callerContext.ensureActive()
        } catch (cancellation: CancellationException) {
            primaryFailure?.let { cancellation.addSuppressed(it) }
            throw cancellation
        }
        primaryFailure?.let { throw it }
    }

    private suspend fun closeOrdinaryConnection() {
        val hook = executionContextHook
        var captured: Any? = null
        var captureSucceeded = hook == null
        var primaryFailure: Throwable? = null
        if (hook != null) {
            try {
                captured = hook.capture()
                captureSucceeded = true
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (captureFailure: Throwable) {
                primaryFailure = appendFailure(primaryFailure, captureFailure)
            }
        }

        val ownerToken = Any()
        connectionMutex.lock()
        var performedClose = false
        try {
            if (state == ConnectionState.CLOSED) return
            performedClose = true
            var cleanupInvoked = false
            val closeOutcome = OrdinaryCloseOutcome()
            try {
                withContext(
                    dispatcher + ConnectionOwnerContext(this@SafeSQLiteConnection, ownerToken),
                ) {
                    val closeBlock: suspend () -> Unit = {
                        cleanupInvoked = true
                        withContext(NonCancellable) {
                            runOrdinaryCloseWithDeadline(closeOutcome) {
                                closeOwnedResources(closeOutcome)
                            }
                        }
                    }
                    if (hook != null && captureSucceeded) {
                        hook.withCaptured(captured, closeBlock)
                    } else {
                        closeBlock()
                    }
                }
            } catch (cancellation: CancellationException) {
                primaryFailure = appendFailure(cancellation, primaryFailure)
            } catch (hookOrCloseFailure: Throwable) {
                primaryFailure = appendFailure(primaryFailure, hookOrCloseFailure)
            }
            primaryFailure = appendFailure(primaryFailure, closeOutcome.failure)
            if (!cleanupInvoked) {
                val fallbackOutcome = OrdinaryCloseOutcome()
                try {
                    withContext(NonCancellable) {
                        runOrdinaryCloseWithDeadline(fallbackOutcome) {
                            withContext(
                                dispatcher +
                                    ConnectionOwnerContext(
                                        this@SafeSQLiteConnection,
                                        ownerToken,
                                    ),
                            ) {
                                closeOwnedResources(fallbackOutcome)
                            }
                        }
                    }
                } catch (cleanupFailure: Throwable) {
                    primaryFailure = appendFailure(primaryFailure, cleanupFailure)
                }
                primaryFailure = appendFailure(primaryFailure, fallbackOutcome.failure)
            }
        } finally {
            if (performedClose && state == ConnectionState.CLOSED) {
                try {
                    closeExecutionContextOnce()
                } catch (executionFailure: Throwable) {
                    primaryFailure = appendFailure(primaryFailure, executionFailure)
                }
            }
            connectionMutex.unlock()
        }
        primaryFailure?.let { throw it }
    }

    private suspend fun runOrdinaryCloseWithDeadline(
        outcome: OrdinaryCloseOutcome,
        block: suspend () -> Unit,
    ) {
        val completed = withTimeoutOrNull(ordinaryCloseCleanupTimeoutMillis) {
            block()
            true
        } ?: false
        if (!completed) {
            val cleanupFailure = outcome.failure
            outcome.failure = SqliteException(
                "SQLite ordinary connection close exceeded the " +
                    "${ordinaryCloseCleanupTimeoutMillis}ms cleanup deadline.",
                cleanupFailure,
            ).also { deadlineFailure ->
                cleanupFailure?.suppressedExceptions?.forEach {
                    appendFailure(deadlineFailure, it)
                }
            }
        }
    }

    private suspend fun claimCloseAttempt(): Pair<CloseAttempt, Boolean> =
        closeCoordinationMutex.withLock {
            closeAttempt?.takeUnless { it.completed.isCompleted }?.let {
                return@withLock it to false
            }
            if (state == ConnectionState.CLOSED) {
                val completedAttempt = CloseAttempt()
                completedAttempt.completed.complete(null)
                return@withLock completedAttempt to false
            }
            val attempt = CloseAttempt()
            closeAttempt = attempt
            attempt to true
        }

    private suspend fun performWorkerClose(
        attempt: CloseAttempt,
        cancellationSignal: Job?,
        cleanupController: SuspendSQLiteConnectionCleanup,
    ): Throwable? = coroutineScope {
        var primaryFailure: Throwable? = null
        val cleanup = async {
            try {
                performCooperativeWorkerClose()
            } catch (cancellation: CancellationException) {
                CloseWorkResult(
                    failure = cancellation,
                    performedClose = false,
                    forceRequired = true,
                )
            }
        }
        try {
            val completedBeforeForce = if (cancellationSignal == null) {
                select<CloseWorkResult?> {
                    cleanup.onAwait { it }
                    attempt.forceRequested.onAwait { null }
                }
            } else {
                select<CloseWorkResult?> {
                    cleanup.onAwait { it }
                    attempt.forceRequested.onAwait { null }
                    cancellationSignal.onJoin {
                        attempt.forceRequested.complete(Unit)
                        null
                    }
                }
            }

            val result = if (completedBeforeForce != null) {
                completedBeforeForce
            } else {
                val deadline = async {
                    runCatching { cleanupController.awaitCleanupDeadline() }.exceptionOrNull()
                }
                try {
                    select<CloseWorkResult?> {
                        cleanup.onAwait { it }
                        deadline.onAwait { deadlineFailure ->
                            deadlineFailure?.let {
                                primaryFailure = appendFailure(primaryFailure, it)
                            }
                            null
                        }
                    }
                } finally {
                    if (!deadline.isCompleted) deadline.cancel()
                }
            }

            if (result != null) {
                primaryFailure = appendFailure(primaryFailure, result.failure)
                if (result.forceRequired) {
                    primaryFailure = forceWorkerClose(
                        primaryFailure = primaryFailure,
                        cleanupController = cleanupController,
                    )
                } else if (result.performedClose) {
                    try {
                        closeExecutionContextOnce()
                    } catch (executionFailure: Throwable) {
                        primaryFailure = appendFailure(primaryFailure, executionFailure)
                    }
                }
            } else {
                cleanup.cancelAndJoin()
                primaryFailure = forceWorkerClose(
                    primaryFailure = appendFailure(
                        primaryFailure,
                        SqliteException(
                            "SQLite connection close exceeded the " +
                                "${cleanupController.cleanupTimeoutMillis}ms cleanup deadline.",
                        ),
                    ),
                    cleanupController = cleanupController,
                )
            }
        } catch (cleanupCancellation: CancellationException) {
            if (!cleanup.isCompleted) cleanup.cancelAndJoin()
            primaryFailure = forceWorkerClose(
                primaryFailure = appendFailure(primaryFailure, cleanupCancellation),
                cleanupController = cleanupController,
            )
        } catch (cleanupFailure: Throwable) {
            if (!cleanup.isCompleted) cleanup.cancelAndJoin()
            primaryFailure = forceWorkerClose(
                primaryFailure = appendFailure(primaryFailure, cleanupFailure),
                cleanupController = cleanupController,
            )
        } finally {
            attempt.completed.complete(primaryFailure)
        }
        primaryFailure
    }

    private suspend fun forceWorkerClose(
        primaryFailure: Throwable?,
        cleanupController: SuspendSQLiteConnectionCleanup,
    ): Throwable? {
        var failure = primaryFailure
        state = ConnectionState.CLOSING
        try {
            cleanupController.forceCleanup()
        } catch (forceFailure: Throwable) {
            failure = appendFailure(failure, forceFailure)
        } finally {
            state = ConnectionState.CLOSED
            fatalFailure = null
            clearLiveStatementObservers()
        }
        try {
            closeExecutionContextOnce()
        } catch (executionFailure: Throwable) {
            failure = appendFailure(failure, executionFailure)
        }
        return failure
    }

    private suspend fun performCooperativeWorkerClose(): CloseWorkResult {
        val hook = executionContextHook
        var captured: Any? = null
        val ownerToken = Any()
        var primaryFailure: Throwable? = null
        var captureSucceeded = hook == null
        if (hook != null) {
            try {
                captured = hook.capture()
                captureSucceeded = true
            } catch (cancellation: CancellationException) {
                throw cancellation
            } catch (captureFailure: Throwable) {
                primaryFailure = appendFailure(primaryFailure, captureFailure)
            }
        }

        var lockAcquired = false
        var performedClose = false
        try {
            connectionMutex.lock()
            lockAcquired = true
            if (state != ConnectionState.CLOSED) {
                performedClose = true
                var cleanupInvoked = false
                try {
                    val closeFailure = withContext(
                        dispatcher + ConnectionOwnerContext(this@SafeSQLiteConnection, ownerToken),
                    ) {
                        val closeBlock: suspend () -> Throwable? = {
                            cleanupInvoked = true
                            closeOwnedResources()
                        }
                        if (hook != null && captureSucceeded) {
                            hook.withCaptured(captured, closeBlock)
                        } else {
                            closeBlock()
                        }
                    }
                    primaryFailure = appendFailure(primaryFailure, closeFailure)
                } catch (cancellation: CancellationException) {
                    throw cancellation
                } catch (hookOrCloseFailure: Throwable) {
                    primaryFailure = appendFailure(primaryFailure, hookOrCloseFailure)
                }
                if (!cleanupInvoked) {
                    try {
                        val fallbackFailure = withContext(
                            dispatcher +
                                ConnectionOwnerContext(this@SafeSQLiteConnection, ownerToken),
                        ) {
                            closeOwnedResources()
                        }
                        primaryFailure = appendFailure(primaryFailure, fallbackFailure)
                    } catch (cancellation: CancellationException) {
                        throw cancellation
                    } catch (cleanupFailure: Throwable) {
                        primaryFailure = appendFailure(primaryFailure, cleanupFailure)
                    }
                }
            }
        } finally {
            if (lockAcquired) connectionMutex.unlock()
        }
        return CloseWorkResult(primaryFailure, performedClose)
    }

    private suspend fun awaitCloseAttempt(
        attempt: CloseAttempt,
        cancellationSignal: Job?,
    ): Throwable? {
        if (cancellationSignal == null) return attempt.completed.await()
        if (cancellationSignal.isCompleted) {
            attempt.forceRequested.complete(Unit)
            return attempt.completed.await()
        }
        return select {
            attempt.completed.onAwait { it }
            cancellationSignal.onJoin {
                attempt.forceRequested.complete(Unit)
                attempt.completed.await()
            }
        }
    }

    private suspend fun closeExecutionContextOnce() {
        val shouldClose = closeCoordinationMutex.withLock {
            if (executionContextClosed) {
                false
            } else {
                executionContextClosed = true
                true
            }
        }
        if (shouldClose) executionContext.close()
    }

    suspend fun inTransaction(): Boolean {
        return withDispatcherContext {
            wrapAndroidxSqliteCall { ref.inTransaction() }
        }
    }

    /**
     * Executes the given block within a database transaction, avoiding nested BEGIN/COMMIT.
     * If a transaction is already active on this connection, the block runs as-is inside
     * the existing transaction. Otherwise, a new transaction is started and properly
     * committed or rolled back.
     */
    suspend fun <T> transaction(mode: TransactionMode = TransactionMode.DEFERRED, block: suspend () -> T): T {
        return withDispatcherContext {
            val alreadyInTransaction =
                activeTransactionDepth > 0 || wrapAndroidxSqliteCall { ref.inTransaction() }
            val transactionToken = activeTransactionToken ?: Any()
            if (!alreadyInTransaction) {
                when (mode) {
                    TransactionMode.DEFERRED -> wrapAndroidxSqliteAsyncCall { ref.executeSQL("BEGIN") }
                    TransactionMode.IMMEDIATE ->
                        wrapAndroidxSqliteAsyncCall { ref.executeSQL("BEGIN IMMEDIATE") }
                    TransactionMode.EXCLUSIVE ->
                        wrapAndroidxSqliteAsyncCall { ref.executeSQL("BEGIN EXCLUSIVE") }
                }
                activeTransactionToken = transactionToken
            }
            activeTransactionDepth++
            var transactionEnded = alreadyInTransaction
            try {
                val result = block()
                if (!alreadyInTransaction) {
                    coroutineContext.ensureActive()
                    val statementFailure = withContext(NonCancellable) {
                        closeLiveStatements(primary = null, transactionToken = transactionToken)
                    }
                    if (statementFailure != null) throw statementFailure
                    coroutineContext.ensureActive()
                    beforeTransactionCommitForTest?.invoke()
                    wrapAndroidxSqliteAsyncCall { ref.executeSQL("COMMIT") }
                    transactionEnded = true
                    notifyTransactionCommitted()
                }
                result
            } catch (e: Throwable) {
                if (!alreadyInTransaction && !transactionEnded) {
                    withContext(NonCancellable) {
                        closeLiveStatements(primary = e, transactionToken = transactionToken)
                        val rollbackNeeded = try {
                            wrapAndroidxSqliteCall { ref.inTransaction() }
                        } catch (stateFailure: Throwable) {
                            appendFailure(e, stateFailure)
                            markFatal(stateFailure)
                            false
                        }
                        if (rollbackNeeded) {
                            try {
                                beforeTransactionRollbackForTest?.invoke()
                                wrapAndroidxSqliteAsyncCall { ref.executeSQL("ROLLBACK") }
                                transactionEnded = true
                            } catch (rollbackFailure: Throwable) {
                                appendFailure(e, rollbackFailure)
                                markFatal(rollbackFailure)
                            }
                        } else if (state != ConnectionState.FATAL) {
                            transactionEnded = true
                        }
                    }
                }
                if (debug) {
                    sqliteNowLogger.e(e) { "Transaction failed: ${e.message}" }
                }
                throw e
            } finally {
                activeTransactionDepth--
                if (!alreadyInTransaction) activeTransactionToken = null
            }
        }
    }

    private suspend fun notifyOperationComplete() {
        try {
            persistenceController.onOperationComplete(ref, isInTransaction())
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (_: Throwable) {
            sqliteNowLogger.e { "Failed to persist database snapshot" }
        }
    }

    private suspend fun notifyTransactionCommitted() {
        try {
            persistenceController.onTransactionCommitted(ref)
        } catch (cancellation: CancellationException) {
            throw cancellation
        } catch (_: Throwable) {
            sqliteNowLogger.e { "Failed to persist database snapshot" }
        }
    }

    private fun registerStatement(statement: SQLiteStatement): SQLiteStatement {
        lateinit var record: LiveStatement
        val trackedStatement = trackSQLiteStatement(
            statement = statement,
            cleanupFailureObserver = ::markFatal,
            beforeCloseObserver = { record.closeAttempted = true },
            closeSuccessObserver = {
                liveStatements.remove(record)
                clearTrackedSQLiteStatementObservers(record.statement)
            },
        )
        record = LiveStatement(
            statement = trackedStatement,
            transactionToken = activeTransactionToken,
        )
        liveStatements += record
        return trackedStatement
    }

    private fun markFatal(failure: Throwable) {
        if (state == ConnectionState.OPEN) state = ConnectionState.FATAL
        fatalFailure = appendFailure(fatalFailure, failure)
    }

    private fun closeLiveStatements(
        primary: Throwable?,
        transactionToken: Any? = null,
    ): Throwable? {
        var failure = primary
        val records = liveStatements
            .filter { transactionToken == null || it.transactionToken === transactionToken }
            .asReversed()
        for (record in records) {
            if (record.closeAttempted) continue
            try {
                record.statement.close()
            } catch (closeFailure: Throwable) {
                failure = appendFailure(failure, closeFailure)
            }
        }
        return failure
    }

    private suspend fun closeOwnedResources(
        outcome: OrdinaryCloseOutcome? = null,
    ): Throwable? {
        val fatal = state == ConnectionState.FATAL
        state = ConnectionState.CLOSING
        var failure = if (fatal) fatalFailure else null
        failure = closeLiveStatements(failure)
        if (!fatal && failure == null) {
            try {
                persistenceController.onClose(ref)
            } catch (persistenceFailure: Throwable) {
                failure = appendFailure(failure, persistenceFailure)
            }
        }
        try {
            wrapAndroidxSqliteCall { ref.close() }
        } catch (rawCloseFailure: Throwable) {
            failure = appendFailure(failure, rawCloseFailure)
        }
        try {
            (ref as? SuspendSQLiteConnectionCleanup)?.awaitCleanup()
        } catch (cleanupFailure: Throwable) {
            failure = appendFailure(failure, cleanupFailure)
        } finally {
            state = ConnectionState.CLOSED
            fatalFailure = null
            clearLiveStatementObservers()
            outcome?.failure = failure
        }
        return failure
    }

    private suspend fun disposeFatalConnection(primary: Throwable?): Throwable? {
        if (state == ConnectionState.CLOSED) return primary
        state = ConnectionState.CLOSING
        var failure = appendFailure(primary, fatalFailure)
        failure = closeLiveStatements(failure)
        try {
            wrapAndroidxSqliteCall { ref.close() }
        } catch (rawCloseFailure: Throwable) {
            failure = appendFailure(failure, rawCloseFailure)
        }
        try {
            (ref as? SuspendSQLiteConnectionCleanup)?.awaitCleanup()
        } catch (cleanupFailure: Throwable) {
            failure = appendFailure(failure, cleanupFailure)
        } finally {
            state = ConnectionState.CLOSED
            fatalFailure = null
            clearLiveStatementObservers()
            try {
                closeExecutionContextOnce()
            } catch (executionFailure: Throwable) {
                failure = appendFailure(failure, executionFailure)
            }
        }
        return failure
    }

    private fun clearLiveStatementObservers() {
        liveStatements.forEach { clearTrackedSQLiteStatementObservers(it.statement) }
        liveStatements.clear()
    }

    private fun closedConnectionFailure(): IllegalStateException =
        IllegalStateException("SQLite connection is closed")

    private class LiveStatement(
        val statement: SQLiteStatement,
        val transactionToken: Any?,
        var closeAttempted: Boolean = false,
    )

    private class CloseAttempt {
        val forceRequested = CompletableDeferred<Unit>()
        val completed = CompletableDeferred<Throwable?>()
    }

    private data class CloseWorkResult(
        val failure: Throwable?,
        val performedClose: Boolean,
        val forceRequired: Boolean = false,
    )

    private class OrdinaryCloseOutcome {
        var failure: Throwable? = null
    }

    private enum class ConnectionState { OPEN, FATAL, CLOSING, CLOSED }

    /**
     * Forces the current database snapshot to be persisted when persistence is configured.
     * Throws if invoked while a transaction is active to avoid flushing inconsistent state.
     * This call makes sense only for JS target and does nothing on other targets, so it
     * is safe to call unconditionally.
     */
    internal suspend fun persistSnapshotNow() {
        withDispatcherContext {
            if (activeTransactionDepth > 0 || wrapAndroidxSqliteCall { ref.inTransaction() }) {
                throw IllegalStateException("Cannot flush persistence while a transaction is active")
            }
            persistenceController.flush(ref)
        }
    }

    /**
     * Reports out-of-band table changes so reactive queries backed by this connection can re-run.
     *
     * SQLiteNow-managed execute helpers already notify invalidation automatically. This lower-level
     * hook exists for cross-module integrations such as oversqlite and for advanced callers that
     * legitimately mutate tables outside generated helpers.
     */
    fun reportExternalTableChanges(affectedTables: Set<String>) {
        if (affectedTables.isEmpty()) return
        val normalized = affectedTables.mapTo(linkedSetOf()) { it.lowercase() }
        if (normalized.isEmpty()) return
        tableInvalidationListener?.invoke(normalized)
    }

    internal fun setTableInvalidationListener(listener: ((Set<String>) -> Unit)?) {
        tableInvalidationListener = listener
    }

    private fun appendFailure(primary: Throwable?, additional: Throwable?): Throwable? {
        if (additional == null) return primary
        if (primary == null) return additional
        if (primary.containsThrowableIdentity(additional)) return primary
        primary.addSuppressed(additional)
        return primary
    }

    private fun Throwable.containsThrowableIdentity(
        target: Throwable,
        visited: MutableList<Throwable> = mutableListOf(),
    ): Boolean {
        if (this === target) return true
        if (visited.any { it === this }) return false
        visited += this
        if (cause?.containsThrowableIdentity(target, visited) == true) return true
        return suppressedExceptions.any { it.containsThrowableIdentity(target, visited) }
    }
}

private class ConnectionOwnerContext(
    val connection: SafeSQLiteConnection,
    val token: Any,
) : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<ConnectionOwnerContext>
}

internal expect fun exportConnectionBytes(connection: SQLiteConnection): ByteArray?

private const val DEFAULT_ORDINARY_CLOSE_CLEANUP_TIMEOUT_MILLIS = 5_000L

/**
 * Transaction modes supported by SQLite.
 * - DEFERRED: default; locks are acquired lazily when first needed.
 * - IMMEDIATE: acquires a RESERVED lock immediately; prevents other writers.
 * - EXCLUSIVE: acquires an EXCLUSIVE lock; prevents other readers and writers.
 */
enum class TransactionMode { DEFERRED, IMMEDIATE, EXCLUSIVE }
