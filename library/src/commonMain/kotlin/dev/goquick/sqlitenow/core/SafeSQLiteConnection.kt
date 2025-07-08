package dev.goquick.sqlitenow.core

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.execSQL
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * A thread-safe wrapper around SQLiteConnection that uses a Mutex to ensure
 * only one coroutine can access the connection at a time.
 * 
 * This is necessary because SQLite connections are not thread-safe in
 * bundled androidx.sqlite driver and can cause issues when used concurrently
 * from multiple coroutines.
 */
class SafeSQLiteConnection(
    val ref: SQLiteConnection
) {
    val mutex = Mutex()
    val dispatcher = Dispatchers.IO.limitedParallelism(1)

    /**
     * Executes a block of code with exclusive access to the SQLite connection.
     * This ensures thread safety when using the connection from multiple coroutines.
     */
    suspend inline fun <T> withLock(crossinline block: suspend (SQLiteConnection) -> T): T {
        return mutex.withLock {
            block(ref)
        }
    }

    suspend fun execSQL(sql: String) {
        withLock {
            ref.execSQL(sql)
        }
    }
}
