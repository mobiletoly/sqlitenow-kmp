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
package dev.goquick.sqlitenow.core.sqlite

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteException
import androidx.sqlite.SQLiteStatement

/**
 * Convenience helper mirroring the androidx API.
 */
@Suppress("EXTENSION_SHADOWED_BY_MEMBER")
fun SQLiteStatement.getColumnNames(): List<String> {
    val count = getColumnCount()
    if (count <= 0) return emptyList()
    return (0 until count).map { getColumnName(it) }
}

class SqliteException(message: String? = null, cause: Throwable? = null) : RuntimeException(message, cause)

suspend inline fun <T> SQLiteStatement.use(block: suspend (SQLiteStatement) -> T): T {
    var exception: Throwable? = null
    try {
        return block(this)
    } catch (t: Throwable) {
        exception = t
        throw t
    } finally {
        try {
            close()
        } catch (closeError: Throwable) {
            val normalizedCloseError = if (closeError is SQLiteException) {
                SqliteException(closeError.message, closeError)
            } else {
                closeError
            }
            if (exception == null) {
                throw normalizedCloseError
            }
            if (
                normalizedCloseError !== exception &&
                exception.suppressedExceptions.none { it === normalizedCloseError }
            ) {
                exception.addSuppressed(normalizedCloseError)
            }
        }
    }
}

internal expect suspend fun executeSqliteNowSql(connection: SQLiteConnection, sql: String)

internal expect fun trackSQLiteStatement(
    statement: SQLiteStatement,
    cleanupFailureObserver: (Throwable) -> Unit,
    beforeCloseObserver: () -> Unit,
    closeSuccessObserver: () -> Unit,
): SQLiteStatement

internal expect fun clearTrackedSQLiteStatementObservers(statement: SQLiteStatement)

internal inline fun <T> wrapAndroidxSqliteCall(block: () -> T): T {
    return try {
        block()
    } catch (t: Throwable) {
        if (t is SQLiteException) {
            throw SqliteException(t.message, t)
        }
        throw t
    }
}

internal suspend inline fun <T> wrapAndroidxSqliteAsyncCall(
    crossinline block: suspend () -> T,
): T {
    return try {
        block()
    } catch (t: Throwable) {
        if (t is SQLiteException) {
            throw SqliteException(t.message, t)
        }
        throw t
    }
}
