/*
 * Copyright 2025 Anatoliy Pochkin
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
import kotlinx.coroutines.CoroutineDispatcher
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
) {
    val dispatcher: CoroutineDispatcher = sqliteConnectionDispatcher()
    private var activeTransactionDepth: Int = 0

    internal val restoredFromSnapshot: Boolean
        get() = persistenceController.restoredFromSnapshot

    private fun isInTransaction(): Boolean = activeTransactionDepth > 0 || ref.inTransaction()

    suspend fun <T> withContextAndTrace(block: suspend () -> T): T {
        val creationTrace = Throwable().stackTraceToString().replace("\n\n", "\n")
        return try {
            withContext(dispatcher) {
                val result = block()
                persistenceController.onOperationComplete(ref, isInTransaction())
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

    suspend fun execSQL(sql: String) {
        sqliteNowLogger.d { "SafeSQLiteConnection.execSQL: $sql" }
        withContext(dispatcher) {
            ref.execSQL(sql)
            persistenceController.onOperationComplete(ref, isInTransaction())
        }
    }

    suspend fun prepare(sql: String): SqliteStatement {
        sqliteNowLogger.d { "SafeSQLiteConnection.prepare: $sql" }
        return withContext(dispatcher) {
            val result = ref.prepare(sql)
            result
        }
    }

    suspend fun close() {
        withContext(dispatcher) {
            sqliteNowLogger.d { "SafeSQLiteConnection.close" }
            persistenceController.onClose(ref)
            ref.close()
        }
    }

    suspend fun inTransaction(): Boolean {
        return withContext(dispatcher) {
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
        return withContext(dispatcher) {
            val alreadyInTransaction = activeTransactionDepth > 0 || ref.inTransaction()
            if (!alreadyInTransaction) {
                when (mode) {
                    TransactionMode.DEFERRED -> ref.execSQL("BEGIN")
                    TransactionMode.IMMEDIATE -> ref.execSQL("BEGIN IMMEDIATE")
                    TransactionMode.EXCLUSIVE -> ref.execSQL("BEGIN EXCLUSIVE")
                }
            }
            activeTransactionDepth++
            try {
                val result = block()
                if (!alreadyInTransaction) {
                    ref.execSQL("COMMIT")
                    persistenceController.onTransactionCommitted(ref)
                }
                result
            } catch (e: Exception) {
                if (!alreadyInTransaction) {
                    try {
                        ref.execSQL("ROLLBACK")
                    } catch (_: Exception) {
                        // ignore rollback errors
                    }
                }
                sqliteNowLogger.e(e) { "Transaction failed: ${e.message}" }
                throw e
            } finally {
                activeTransactionDepth--
            }
        }
    }

    /**
     * Forces the current database snapshot to be persisted when persistence is configured.
     * Throws if invoked while a transaction is active to avoid flushing inconsistent state.
     * This call makes sense only for JS target and does nothing on other targets, so it
     * is safe to call unconditionally.
     */
    internal suspend fun persistSnapshotNow() {
        withContext(dispatcher) {
            if (activeTransactionDepth > 0 || ref.inTransaction()) {
                throw IllegalStateException("Cannot flush persistence while a transaction is active")
            }
            persistenceController.flush(ref)
        }
    }
}

internal expect fun exportConnectionBytes(connection: SqliteConnection): ByteArray?

/**
 * Transaction modes supported by SQLite.
 * - DEFERRED: default; locks are acquired lazily when first needed.
 * - IMMEDIATE: acquires a RESERVED lock immediately; prevents other writers.
 * - EXCLUSIVE: acquires an EXCLUSIVE lock; prevents other readers and writers.
 */
enum class TransactionMode { DEFERRED, IMMEDIATE, EXCLUSIVE }
