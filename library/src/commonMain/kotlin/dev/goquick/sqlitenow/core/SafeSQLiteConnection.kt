package dev.goquick.sqlitenow.core

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import androidx.sqlite.execSQL
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.withContext

/**
 * A thread-safe wrapper around SQLiteConnection that ensures that only one coroutine
 * can access the connection at a time.
 *
 * This is necessary because SQLite connections are not thread-safe in
 * bundled androidx.sqlite driver and can cause issues when used concurrently
 * from multiple coroutines.
 */
class SafeSQLiteConnection(
    val ref: SQLiteConnection
) {
    val dispatcher = Dispatchers.IO.limitedParallelism(1)

    init {
        println("SafeSQLiteConnection created")
    }

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
        withContext(dispatcher) {
            ref.execSQL(sql)
        }
    }

    suspend fun prepare(sql: String): SQLiteStatement {
        return withContext(dispatcher) {
            val result = ref.prepare(sql)
            result
        }
    }

    suspend fun close() {
        withContext(dispatcher) {
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
    suspend fun <T> transaction(block: suspend () -> T): T {
        return withContext(dispatcher) {
            if (ref.inTransaction()) {
                // Already in a transaction: just run the block safely on the same dispatcher
                block()
            } else {
                ref.execSQL("BEGIN")
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
                    throw e
                }
            }
        }
    }
}
