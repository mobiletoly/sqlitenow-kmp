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

internal class StatementCache(
    private val db: SafeSQLiteConnection,
) : AutoCloseable {
    private val statements = linkedMapOf<String, SqliteStatement>()
    private var closed = false

    suspend fun get(sql: String): SqliteStatement {
        check(!closed) { "statement cache is already closed" }

        val cached = statements[sql]
        if (cached == null) {
            return db.prepare(sql).also { statements[sql] = it }
        }

        return try {
            cached.reset()
            cached.clearBindings()
            cached
        } catch (_: Throwable) {
            runCatching { cached.close() }
            val replacement = db.prepare(sql)
            statements[sql] = replacement
            replacement
        }
    }

    override fun close() {
        if (closed) return
        closed = true

        var failure: Throwable? = null
        val openStatements = statements.values.toList().asReversed()
        statements.clear()
        for (statement in openStatements) {
            try {
                statement.close()
            } catch (closeError: Throwable) {
                if (failure == null) {
                    failure = closeError
                }
            }
        }
        if (failure != null) {
            throw failure
        }
    }
}
