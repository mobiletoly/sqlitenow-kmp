package dev.goquick.sqlitenow.core

import androidx.sqlite.SQLiteConnection
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

    suspend fun <T> withContextAndTrace(block: suspend () -> T): T {
        val creationTrace = Throwable().stackTraceToString().replace("\n\n", "\n")
        return try {
            withContext<T>(dispatcher) {
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
}
