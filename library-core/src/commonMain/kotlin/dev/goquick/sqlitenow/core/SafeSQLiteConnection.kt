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

import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.sqlite.SqliteConnection
import dev.goquick.sqlitenow.core.sqlite.SqliteStatement
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.withContext

/**
 * A thread-safe wrapper around SQLiteConnection that ensures that only one coroutine
 * can access the connection at a time.
 *
 * This is necessary because many SQLite driver implementations are not thread-safe and can cause issues when used concurrently
 * from multiple coroutines.
 */
class SafeSQLiteConnection internal constructor(
    val ref: SqliteConnection,
    val debug: Boolean = false,
    private val persistenceController: PersistenceController = NoopPersistenceController(),
    private val executionContextHook: SqliteNowContextHook? = null,
    private val executionContext: SqliteConnectionExecutionContext,
) {
    val dispatcher: CoroutineDispatcher = executionContext.dispatcher
    private var activeTransactionDepth: Int = 0
    private var activeTransactionToken: Any? = null
    private var tableInvalidationListener: ((Set<String>) -> Unit)? = null
    private val connectionMutex = Mutex()
    @kotlin.concurrent.Volatile
    private var state = ConnectionState.OPEN
    private var fatalFailure: Throwable? = null
    private val liveStatements = mutableListOf<LiveStatement>()
    internal var beforeTransactionRollbackForTest: (() -> Unit)? = null

    internal val restoredFromSnapshot: Boolean
        get() = persistenceController.restoredFromSnapshot

    private fun isInTransaction(): Boolean = activeTransactionDepth > 0 || ref.inTransaction()

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
                        disposeFatalConnection(null)
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
                val disposalFailure = disposeFatalConnection(primaryFailure)
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
        } catch (e: Exception) {
            // Combine the original creation trace and the actual exception trace
            val combinedMessage = buildString {
                appendLine(e.message ?: "")
                appendLine(creationTrace)
                appendLine("Original exception stack:")
                appendLine(e.stackTraceToString())
            }
            // Rethrow so your test harness prints full detail
            throw SqliteNowException(combinedMessage, e)
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
            ref.execSQL(sql)
            notifyOperationComplete()
        }
    }

    suspend fun prepare(sql: String): SqliteStatement {
        sqliteNowLogger.d { "SafeSQLiteConnection.prepare: $sql" }
        return withDispatcherContext {
            ref.prepare(sql).also(::registerStatement)
        }
    }

    suspend fun close() {
        if (state == ConnectionState.CLOSED) return
        if (coroutineContext[ConnectionOwnerContext]?.connection === this) {
            throw IllegalStateException("Cannot close SQLite connection from its active owner context")
        }

        val hook = executionContextHook
        val captured = hook?.capture()
        val ownerToken = Any()
        connectionMutex.lock()
        var primaryFailure: Throwable? = null
        var performedClose = false
        try {
            if (state == ConnectionState.CLOSED) return
            performedClose = true
            primaryFailure = withContext(dispatcher + ConnectionOwnerContext(this, ownerToken)) {
                val closeBlock: suspend () -> Throwable? = { closeOwnedResources() }
                if (hook != null) hook.withCaptured(captured, closeBlock) else closeBlock()
            }
        } finally {
            if (performedClose) {
                try {
                    executionContext.close()
                } catch (executionFailure: Throwable) {
                    primaryFailure = appendFailure(primaryFailure, executionFailure)
                }
            }
            connectionMutex.unlock()
        }
        primaryFailure?.let { throw it }
    }

    suspend fun inTransaction(): Boolean {
        return withDispatcherContext {
            ref.inTransaction()
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
            val alreadyInTransaction = activeTransactionDepth > 0 || ref.inTransaction()
            val transactionToken = activeTransactionToken ?: Any()
            if (!alreadyInTransaction) {
                when (mode) {
                    TransactionMode.DEFERRED -> ref.execSQL("BEGIN")
                    TransactionMode.IMMEDIATE -> ref.execSQL("BEGIN IMMEDIATE")
                    TransactionMode.EXCLUSIVE -> ref.execSQL("BEGIN EXCLUSIVE")
                }
                activeTransactionToken = transactionToken
            }
            activeTransactionDepth++
            var committed = false
            try {
                val result = block()
                if (!alreadyInTransaction) {
                    coroutineContext.ensureActive()
                    val statementFailure = withContext(NonCancellable) {
                        closeLiveStatements(primary = null, transactionToken = transactionToken)
                    }
                    if (statementFailure != null) throw statementFailure
                    coroutineContext.ensureActive()
                    ref.execSQL("COMMIT")
                    committed = true
                    notifyTransactionCommitted()
                }
                result
            } catch (e: Throwable) {
                if (!alreadyInTransaction && !committed) {
                    withContext(NonCancellable) {
                        closeLiveStatements(primary = e, transactionToken = transactionToken)
                        try {
                            beforeTransactionRollbackForTest?.invoke()
                            ref.execSQL("ROLLBACK")
                        } catch (rollbackFailure: Throwable) {
                            appendFailure(e, rollbackFailure)
                            markFatal(rollbackFailure)
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

    private fun registerStatement(statement: SqliteStatement) {
        val record = LiveStatement(
            statement = statement,
            transactionToken = activeTransactionToken,
        )
        liveStatements += record
        statement.cleanupFailureObserver = ::markFatal
        statement.beforeCloseObserver = { record.closeAttempted = true }
        statement.closeSuccessObserver = {
            liveStatements.remove(record)
            clearStatementObservers(statement)
        }
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

    private suspend fun closeOwnedResources(): Throwable? {
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
            ref.close()
        } catch (rawCloseFailure: Throwable) {
            failure = appendFailure(failure, rawCloseFailure)
        } finally {
            state = ConnectionState.CLOSED
            fatalFailure = null
            clearLiveStatementObservers()
        }
        return failure
    }

    private fun disposeFatalConnection(primary: Throwable?): Throwable? {
        if (state == ConnectionState.CLOSED) return primary
        state = ConnectionState.CLOSING
        var failure = appendFailure(primary, fatalFailure)
        failure = closeLiveStatements(failure)
        try {
            ref.close()
        } catch (rawCloseFailure: Throwable) {
            failure = appendFailure(failure, rawCloseFailure)
        } finally {
            state = ConnectionState.CLOSED
            fatalFailure = null
            clearLiveStatementObservers()
            try {
                executionContext.close()
            } catch (executionFailure: Throwable) {
                failure = appendFailure(failure, executionFailure)
            }
        }
        return failure
    }

    private fun clearLiveStatementObservers() {
        liveStatements.forEach { clearStatementObservers(it.statement) }
        liveStatements.clear()
    }

    private fun clearStatementObservers(statement: SqliteStatement) {
        statement.cleanupFailureObserver = null
        statement.beforeCloseObserver = null
        statement.closeSuccessObserver = null
    }

    private fun closedConnectionFailure(): IllegalStateException =
        IllegalStateException("SQLite connection is closed")

    private class LiveStatement(
        val statement: SqliteStatement,
        val transactionToken: Any?,
        var closeAttempted: Boolean = false,
    )

    private enum class ConnectionState { OPEN, FATAL, CLOSING, CLOSED }

    /**
     * Forces the current database snapshot to be persisted when persistence is configured.
     * Throws if invoked while a transaction is active to avoid flushing inconsistent state.
     * This call makes sense only for JS target and does nothing on other targets, so it
     * is safe to call unconditionally.
     */
    internal suspend fun persistSnapshotNow() {
        withDispatcherContext {
            if (activeTransactionDepth > 0 || ref.inTransaction()) {
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

internal expect fun exportConnectionBytes(connection: SqliteConnection): ByteArray?

/**
 * Transaction modes supported by SQLite.
 * - DEFERRED: default; locks are acquired lazily when first needed.
 * - IMMEDIATE: acquires a RESERVED lock immediately; prevents other writers.
 * - EXCLUSIVE: acquires an EXCLUSIVE lock; prevents other readers and writers.
 */
enum class TransactionMode { DEFERRED, IMMEDIATE, EXCLUSIVE }
