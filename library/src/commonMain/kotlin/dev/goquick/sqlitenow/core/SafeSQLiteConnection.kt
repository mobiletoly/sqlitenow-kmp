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
class SafeSQLiteConnection(
    val ref: SqliteConnection,
    val debug: Boolean = false,
    private val dbName: String? = null,
    private val persistence: SqlitePersistence? = null,
    private val autoFlushPersistence: Boolean = true,
) {
    val dispatcher: CoroutineDispatcher = sqliteConnectionDispatcher()

    suspend fun <T> withContextAndTrace(block: suspend () -> T): T {
        val creationTrace = Throwable().stackTraceToString().replace("\n\n", "\n")
        return try {
            withContext(dispatcher) {
                block()
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
            persistSnapshotIfNeeded()
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
            persistSnapshot(force = true)
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
            if (ref.inTransaction()) {
                // Already in a transaction: just run the block safely on the same dispatcher
                block()
            } else {
                when (mode) {
                    TransactionMode.DEFERRED -> ref.execSQL("BEGIN")
                    TransactionMode.IMMEDIATE -> ref.execSQL("BEGIN IMMEDIATE")
                    TransactionMode.EXCLUSIVE -> ref.execSQL("BEGIN EXCLUSIVE")
                }
                try {
                    val result = block()
                    ref.execSQL("COMMIT")
                    result
                } catch (e: Exception) {
                    try {
                        ref.execSQL("ROLLBACK")
                    } catch (_: Exception) {
                        // ignore rollback errors
                    }
                    sqliteNowLogger.e(e) { "Transaction failed: ${e.message}" }
                    throw e
                }
            }
        }
    }

    private suspend fun persistSnapshotIfNeeded() {
        if (!autoFlushPersistence) return
        if (persistence == null || dbName == null) return
        if (ref.inTransaction()) return
        persistSnapshot(force = false)
    }

private suspend fun persistSnapshot(force: Boolean) {
        val persistence = persistence ?: return
        val dbName = dbName ?: return
        if (!force && (!autoFlushPersistence || ref.inTransaction())) return
        val bytes = exportConnectionBytes(ref) ?: return
        try {
            persistence.persist(dbName, bytes)
        } catch (t: Throwable) {
            sqliteNowLogger.e(t) { "Failed to persist database snapshot for $dbName" }
            if (force) throw t
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
