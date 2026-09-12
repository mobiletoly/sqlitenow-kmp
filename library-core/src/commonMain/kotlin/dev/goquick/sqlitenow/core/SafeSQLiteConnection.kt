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
import dev.goquick.sqlitenow.core.sqlite.OwnedStatementClose
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
import kotlinx.coroutines.currentCoroutineContext
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
    @SqliteNowInternalApi
    val activeClientIdentity: Any = Any(),
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
    @kotlin.concurrent.Volatile
    private var publicAccessRevoked = false
    private var executionContextClosed = false
    private var closeAttempt: CloseAttempt? = null
    private var fatalFailure: Throwable? = null
    private val statementsCloseComplete: Boolean
        get() = liveStatements.all { (it.statement as OwnedStatementClose).closeProgress.physicallyComplete }
    private var persistenceCloseComplete = false
    private var rawDriverCloseComplete = false
    private var providerCleanupComplete = false
    private var migrationSqlValidator: ((String) -> Unit)? = null
    private val liveStatements = mutableListOf<LiveStatement>()
    internal var beforeTransactionCommitForTest: (() -> Unit)? = null
    internal var beforeTransactionRollbackForTest: (() -> Unit)? = null
    internal var closeRawDriverForTest: (() -> Unit)? = null
    internal var closeAttemptJoinedForTest: (() -> Unit)? = null

    internal val restoredFromSnapshot: Boolean
        get() = persistenceController.restoredFromSnapshot

    internal val ownedCloseComplete: Boolean
        get() = state == ConnectionState.CLOSED && executionContextClosed

    internal val isPublicAccessRevoked: Boolean
        get() = publicAccessRevoked

    @SqliteNowInternalApi
    fun revokePublicAccess() {
        revokeApplicationAccess()
    }

    private fun revokeApplicationAccess() {
        publicAccessRevoked = true
        tableInvalidationListener = null
    }

    internal suspend fun beginApplicationClose() {
        if (coroutineContext[ConnectionOwnerContext]?.connection === this) {
            throw IllegalStateException("Cannot close SQLite connection from its active owner context")
        }
        revokeApplicationAccess()
        if (state == ConnectionState.OPEN) state = ConnectionState.CLOSING
    }

    private fun requireApplicationAccess(owner: ConnectionOwnerContext?) {
        val ownsPrivateCleanup =
            owner?.connection === this && owner.capability == ConnectionOwnerCapability.PRIVATE_CLEANUP
        if (publicAccessRevoked && !ownsPrivateCleanup) throw closedConnectionFailure()
    }

    private fun isInTransaction(): Boolean =
        activeTransactionDepth > 0 || wrapAndroidxSqliteCall { ref.inTransaction() }

    internal suspend fun <T> withDispatcherContext(block: suspend () -> T): T {
        val currentOwner = coroutineContext[ConnectionOwnerContext]
        val alreadyOwnsConnection = currentOwner?.connection === this
        requireApplicationAccess(currentOwner)
        val hook = executionContextHook
        val captured = hook?.capture()
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
            requireApplicationAccess(currentOwner)
            if (state != ConnectionState.OPEN) {
                if (state == ConnectionState.FATAL) {
                    val failure = withContext(
                        dispatcher + ConnectionOwnerContext(
                            this,
                            ownerToken,
                            ConnectionOwnerCapability.PRIVATE_CLEANUP,
                        ),
                    ) {
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

    internal suspend fun captureMigrationOwnerAccess(): MigrationOwnerAccess {
        val owner = coroutineContext[ConnectionOwnerContext]
        require(owner?.connection === this) {
            "Migration scope must be created while the connection is exclusively owned"
        }
        return MigrationOwnerAccess(owner.token)
    }

    internal suspend fun <T> withMigrationOwnerAccess(
        access: MigrationOwnerAccess,
        block: suspend () -> T,
    ): T {
        return withContext(dispatcher + ConnectionOwnerContext(this, access.ownerToken)) {
            access.withOperation {
                block()
            }
        }
    }

    @OptIn(SqliteNowInternalApi::class)
    internal suspend fun <T> withMigrationSqlValidation(
        policy: SqliteNowCoordinatedMigrationSqlPolicy?,
        block: suspend () -> T,
    ): T {
        if (policy == null) return block()
        check(migrationSqlValidator == null) { "Migration SQL validation is already active" }
        migrationSqlValidator = policy::requireAllowed
        return try {
            block()
        } finally {
            migrationSqlValidator = null
        }
    }

    suspend fun execSQL(sql: String) {
        migrationSqlValidator?.invoke(sql)
        sqliteNowLogger.d { "SafeSQLiteConnection.execSQL: $sql" }
        withDispatcherContext {
            wrapAndroidxSqliteAsyncCall { executeSqliteNowSql(ref, sql) }
            notifyOperationComplete()
        }
    }

    suspend fun prepare(sql: String): SQLiteStatement {
        migrationSqlValidator?.invoke(sql)
        sqliteNowLogger.d { "SafeSQLiteConnection.prepare: $sql" }
        return withDispatcherContext {
            registerStatement(wrapAndroidxSqliteAsyncCall { ref.prepare(sql) })
        }
    }

    suspend fun close() {
        beginApplicationClose()

        val callerContext = coroutineContext
        val cancellationSignal = callerContext[Job]?.let(::Job)
        val (attempt, isOwner) = withContext(NonCancellable) {
            claimCloseAttempt()
        }
        if (!isOwner && !attempt.completed.isCompleted) closeAttemptJoinedForTest?.invoke()
        val cleanupController = ref as? SuspendSQLiteConnectionCleanup
        val primaryFailure = try {
            if (cleanupController == null && isOwner) {
                // Mutex acquisition remains cancellable for the first ordinary close owner.
                performOrdinaryClose(attempt)
            } else {
                withContext(NonCancellable) {
                    if (cleanupController == null) {
                        attempt.completed.await()
                    } else if (isOwner) {
                        performWorkerClose(attempt, cancellationSignal, cleanupController)
                    } else {
                        awaitCloseAttempt(attempt, cancellationSignal)
                    }
                }
            }
        } finally {
            cancellationSignal?.complete()
        }
        try {
            callerContext.ensureActive()
        } catch (cancellation: CancellationException) {
            when (primaryFailure) {
                null -> Unit
                is CancellationException -> primaryFailure.suppressedExceptions.forEach {
                    if (!cancellation.containsThrowableIdentity(it)) cancellation.addSuppressed(it)
                }
                else -> if (!cancellation.containsThrowableIdentity(primaryFailure)) {
                    cancellation.addSuppressed(primaryFailure)
                }
            }
            throw cancellation
        }
        primaryFailure?.let { throw it }
    }

    /**
     * Closes only the underlying inspection handle so its provider can reopen writable on the
     * same execution context (and, on web, in the same lifetime-owning worker).
     */
    internal suspend fun closeInspectionHandleForProviderPreparation() {
        connectionMutex.withLock {
            check(state == ConnectionState.OPEN) { "SQLite inspection connection is not open" }
            withContext(
                dispatcher + ConnectionOwnerContext(
                    this@SafeSQLiteConnection,
                    Any(),
                    ConnectionOwnerCapability.PRIVATE_CLEANUP,
                ),
            ) {
                check(liveStatements.all { (it.statement as OwnedStatementClose).closeProgress.physicallyComplete }) {
                    "SQLite inspection connection still owns prepared statements"
                }
                wrapAndroidxSqliteCall { closeRawDriverForTest?.invoke() ?: ref.close() }
                state = ConnectionState.CLOSED
                tableInvalidationListener = null
                clearLiveStatementObservers()
            }
        }
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
                    dispatcher + ConnectionOwnerContext(
                        this@SafeSQLiteConnection,
                        ownerToken,
                        ConnectionOwnerCapability.PRIVATE_CLEANUP,
                    ),
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
                                        ConnectionOwnerCapability.PRIVATE_CLEANUP,
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
                    state = ConnectionState.CLOSING
                    primaryFailure = appendFailure(primaryFailure, executionFailure)
                }
            }
            connectionMutex.unlock()
        }
        primaryFailure?.let { throw it }
    }

    private suspend fun performOrdinaryClose(attempt: CloseAttempt): Throwable? {
        var failure: Throwable? = null
        try {
            closeOrdinaryConnection()
        } catch (closeFailure: Throwable) {
            failure = closeFailure
        } finally {
            attempt.completed.complete(failure)
        }
        return failure
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
                } else if (result.performedClose && state == ConnectionState.CLOSED) {
                    try {
                        closeExecutionContextOnce()
                    } catch (executionFailure: Throwable) {
                        state = ConnectionState.CLOSING
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
        var forceComplete = false
        try {
            cleanupController.forceCleanup()
            forceComplete = true
        } catch (forceFailure: Throwable) {
            failure = appendFailure(failure, forceFailure)
            forceComplete = cleanupController.physicalCleanupComplete
        }
        if (forceComplete) {
            rawDriverCloseComplete = true
            providerCleanupComplete = true
            persistenceCloseComplete = true
            state = ConnectionState.CLOSED
            fatalFailure = null
            clearLiveStatementObservers()
            try {
                closeExecutionContextOnce()
            } catch (executionFailure: Throwable) {
                state = ConnectionState.CLOSING
                failure = appendFailure(failure, executionFailure)
            }
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
                        dispatcher + ConnectionOwnerContext(
                            this@SafeSQLiteConnection,
                            ownerToken,
                            ConnectionOwnerCapability.PRIVATE_CLEANUP,
                        ),
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
                                ConnectionOwnerContext(
                                    this@SafeSQLiteConnection,
                                    ownerToken,
                                    ConnectionOwnerCapability.PRIVATE_CLEANUP,
                                ),
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
        if (closeCoordinationMutex.withLock { executionContextClosed }) return
        executionContext.close()
        closeCoordinationMutex.withLock { executionContextClosed = true }
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
        liveStatements.removeAll { (it.statement as OwnedStatementClose).closeProgress.physicallyComplete }
        val trackedStatement = trackSQLiteStatement(
            statement = statement,
            cleanupFailureObserver = { failure ->
                markFatal(failure)
            },
            beforeCloseObserver = {},
            closeSuccessObserver = {},
        )
        val record = LiveStatement(
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

    private suspend fun closeLiveStatements(
        primary: Throwable?,
        transactionToken: Any? = null,
    ): Throwable? {
        var failure = primary
        val records = liveStatements
            .filter { transactionToken == null || it.transactionToken === transactionToken }
            .asReversed()
        for (record in records) {
            try {
                (record.statement as OwnedStatementClose).closeFromOwner()
            } catch (closeFailure: Throwable) {
                failure = appendFailure(failure, closeFailure)
            }
        }
        liveStatements.removeAll { (it.statement as OwnedStatementClose).closeProgress.physicallyComplete }
        return failure
    }

    private suspend fun closeOwnedResources(
        outcome: OrdinaryCloseOutcome? = null,
    ): Throwable? {
        val fatal = state == ConnectionState.FATAL
        state = ConnectionState.CLOSING
        var failure = if (fatal) fatalFailure else null
        if (!statementsCloseComplete) {
            failure = closeLiveStatements(failure)
            if (!statementsCloseComplete) {
                outcome?.failure = failure
                return failure
            }
        }
        if (!fatal && !persistenceCloseComplete) {
            try {
                persistenceController.onClose(ref)
                persistenceCloseComplete = true
            } catch (persistenceFailure: Throwable) {
                failure = appendFailure(failure, persistenceFailure)
            }
        } else if (fatal) {
            persistenceCloseComplete = true
        }
        if (!rawDriverCloseComplete) {
            try {
                wrapAndroidxSqliteCall { closeRawDriverForTest?.invoke() ?: ref.close() }
                rawDriverCloseComplete = true
            } catch (rawCloseFailure: Throwable) {
                failure = appendFailure(failure, rawCloseFailure)
            }
            if (!rawDriverCloseComplete) {
                outcome?.failure = failure
                return failure
            }
        }
        if (!providerCleanupComplete) {
            try {
                (ref as? SuspendSQLiteConnectionCleanup)?.awaitCleanup()
                providerCleanupComplete = true
            } catch (cleanupFailure: Throwable) {
                failure = appendFailure(failure, cleanupFailure)
                providerCleanupComplete = (ref as? SuspendSQLiteConnectionCleanup)?.physicalCleanupComplete == true
            }
        }
        if (
            statementsCloseComplete && persistenceCloseComplete && rawDriverCloseComplete &&
            providerCleanupComplete
        ) {
            state = ConnectionState.CLOSED
            fatalFailure = null
            clearLiveStatementObservers()
        }
        outcome?.failure = failure
        return failure
    }

    private suspend fun disposeFatalConnection(primary: Throwable?): Throwable? {
        if (state == ConnectionState.CLOSED) return primary
        val retainedFatalFailure = fatalFailure
        fatalFailure = null
        var failure = appendFailure(primary, retainedFatalFailure)
        state = ConnectionState.FATAL
        failure = appendFailure(failure, closeOwnedResources())
        if (state == ConnectionState.CLOSED) {
            try {
                closeExecutionContextOnce()
            } catch (executionFailure: Throwable) {
                state = ConnectionState.CLOSING
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
        if (publicAccessRevoked || state != ConnectionState.OPEN) throw closedConnectionFailure()
        if (affectedTables.isEmpty()) return
        val normalized = affectedTables.mapTo(linkedSetOf()) { it.lowercase() }
        if (normalized.isEmpty()) return
        tableInvalidationListener?.invoke(normalized)
    }

    internal fun setTableInvalidationListener(listener: ((Set<String>) -> Unit)?) {
        if (listener != null && publicAccessRevoked) throw closedConnectionFailure()
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
    val capability: ConnectionOwnerCapability = ConnectionOwnerCapability.APPLICATION,
) : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<ConnectionOwnerContext>
}

private enum class ConnectionOwnerCapability {
    APPLICATION,
    PRIVATE_CLEANUP,
}

internal class MigrationOwnerAccess internal constructor(
    internal val ownerToken: Any,
) {
    private val stateMutex = Mutex()
    @kotlin.concurrent.Volatile
    private var acceptingOperations = true
    private val operations = mutableMapOf<Any, Job?>()
    private var drained = CompletableDeferred(Unit)
    private var firstDrainedOperationFailure: Throwable? = null

    internal suspend fun <T> withOperation(block: suspend () -> T): T {
        val marker = Any()
        val operationJob = currentCoroutineContext()[Job]
        stateMutex.withLock {
            check(acceptingOperations) { "Migration-step connection is no longer active" }
            if (operations.isEmpty()) drained = CompletableDeferred()
            operations[marker] = operationJob
        }
        var operationFailure: Throwable? = null
        try {
            return block()
        } catch (failure: Throwable) {
            operationFailure = failure
            throw failure
        } finally {
            withContext(NonCancellable) {
                stateMutex.withLock {
                    if (
                        !acceptingOperations &&
                        firstDrainedOperationFailure == null &&
                        operationFailure != null
                    ) {
                        firstDrainedOperationFailure = operationFailure
                    }
                    operations.remove(marker)
                    if (operations.isEmpty()) drained.complete(Unit)
                }
            }
        }
    }

    internal fun expire() {
        acceptingOperations = false
    }

    internal suspend fun expireAndDrain(
        cancelOperations: Boolean,
        propagateOperationFailure: Boolean,
    ) {
        val currentJob = currentCoroutineContext()[Job]
        val (drainSignal, operationJobs) = withContext(NonCancellable) {
            stateMutex.withLock {
                check(!acceptingOperations) { "Migration-step connection is still active" }
                drained to operations.values.filterNotNull().filterNot { it === currentJob }
            }
        }
        if (cancelOperations) {
            withContext(NonCancellable) {
                operationJobs.forEach { job ->
                    job.cancel(CancellationException("Migration-step callback did not complete"))
                }
                drainSignal.await()
            }
            return
        }

        try {
            drainSignal.await()
        } catch (cancellation: CancellationException) {
            withContext(NonCancellable) {
                operationJobs.forEach { job ->
                    job.cancel(CancellationException("Migration-step owner was cancelled"))
                }
                drainSignal.await()
            }
            throw cancellation
        }

        if (propagateOperationFailure) {
            val operationFailure = withContext(NonCancellable) {
                stateMutex.withLock { firstDrainedOperationFailure }
            }
            operationFailure?.let { throw it }
        }
    }
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
