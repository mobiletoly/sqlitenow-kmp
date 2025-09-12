package dev.goquick.sqlitenow.core

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import androidx.sqlite.execSQL
import dev.goquick.sqlitenow.common.logger
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
        logger.d { "TRACK/ execSQL begin 1 >>> $sql" }
        withContext(dispatcher) {
            logger.d { "TRACK/ execSQL begin 2 >>> $sql" }
            ref.execSQL(sql)
            logger.d { "TRACK/execSQL end <<< $sql" }
        }
    }

    suspend fun prepare(sql: String): SQLiteStatement {
        logger.d { "TRACK/ prepare begin 1 >>> $sql" }
        return withContext(dispatcher) {
            logger.d { "TRACK/ prepare begin 2 >>> $sql" }
            val result = ref.prepare(sql)
            logger.d { "TRACK/ prepare end <<< $sql" }
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
}
