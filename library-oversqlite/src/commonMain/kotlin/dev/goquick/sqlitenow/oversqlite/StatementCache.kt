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
package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.SqliteStatement
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext

internal class StatementCacheOperations(
    val prepare: suspend (SafeSQLiteConnection, String) -> SqliteStatement = { db, sql -> db.prepare(sql) },
    val reset: (SqliteStatement) -> Unit = { statement -> statement.reset() },
    val clearBindings: (SqliteStatement) -> Unit = { statement -> statement.clearBindings() },
    val close: suspend (SqliteStatement) -> Unit = { statement -> statement.close() },
) {
    fun resetAndClear(statement: SqliteStatement, primary: Throwable? = null) {
        resetAndClearReusableStatement(
            statement = statement,
            primary = primary,
            reset = reset,
            clearBindings = clearBindings,
        )
    }
}

internal fun resetAndClearReusableStatement(
    statement: SqliteStatement,
    primary: Throwable? = null,
    reset: (SqliteStatement) -> Unit = { it.reset() },
    clearBindings: (SqliteStatement) -> Unit = { it.clearBindings() },
) {
    var cleanupFailure: Throwable? = null
    try {
        reset(statement)
    } catch (resetFailure: Throwable) {
        cleanupFailure = resetFailure
    }
    try {
        clearBindings(statement)
    } catch (clearFailure: Throwable) {
        val first = cleanupFailure
        if (first == null) {
            cleanupFailure = clearFailure
        } else {
            attachSuppressedOnce(first, clearFailure)
        }
    }
    cleanupFailure?.let { failure ->
        if (primary == null) throw failure
        attachSuppressedOnce(primary, failure)
    }
}

internal typealias ReusableStatementCleanup = (SqliteStatement) -> Unit

internal val DefaultReusableStatementCleanup: ReusableStatementCleanup = { statement ->
    resetAndClearReusableStatement(statement)
}

internal class StatementCache(
    private val db: SafeSQLiteConnection,
    private val operations: StatementCacheOperations = StatementCacheOperations(),
) {
    private val statements = linkedMapOf<String, SqliteStatement>()
    private var closed = false

    suspend fun get(sql: String): SqliteStatement {
        return db.withExclusiveAccess {
            check(!closed) { "statement cache is already closed" }

            statements[sql]
                ?: operations.prepare(db, sql).also { statements[sql] = it }
        }
    }

    suspend fun release(statement: SqliteStatement) {
        db.withExclusiveAccess {
            check(!closed) { "statement cache is already closed" }
            operations.resetAndClear(statement)
        }
    }

    suspend fun close() {
        db.withExclusiveAccess {
            if (closed) return@withExclusiveAccess
            closed = true

            var failure: Throwable? = null
            val openStatements = statements.values.toList().asReversed()
            statements.clear()
            for (statement in openStatements) {
                try {
                    operations.close(statement)
                } catch (closeError: Throwable) {
                    if (failure == null) {
                        failure = closeError
                    } else {
                        attachSuppressedOnce(failure, closeError)
                    }
                }
            }
            if (failure != null) {
                throw failure
            }
        }
    }
}

internal suspend fun <T> SafeSQLiteConnection.withStatementCache(
    operations: StatementCacheOperations = StatementCacheOperations(),
    block: suspend (StatementCache) -> T,
): T {
    val cache = StatementCache(this, operations)
    var primaryFailure: Throwable? = null
    try {
        return block(cache)
    } catch (failure: Throwable) {
        primaryFailure = failure
        throw failure
    } finally {
        try {
            withContext(NonCancellable) { cache.close() }
        } catch (closeFailure: Throwable) {
            val primary = primaryFailure
            if (primary == null) {
                throw closeFailure
            }
            attachSuppressedOnce(primary, closeFailure)
        }
    }
}

internal fun attachSuppressedOnce(primary: Throwable, additional: Throwable) {
    if (!primary.containsThrowableIdentity(additional)) primary.addSuppressed(additional)
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
