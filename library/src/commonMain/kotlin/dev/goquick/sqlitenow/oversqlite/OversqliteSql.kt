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

internal suspend inline fun <T> SafeSQLiteConnection.withPreparedStatement(
    sql: String,
    statementCache: StatementCache? = null,
    block: suspend (SqliteStatement) -> T,
): T {
    val cached = statementCache?.get(sql)
    if (cached != null) {
        return block(cached)
    }

    val statement = prepare(sql)
    var failure: Throwable? = null
    try {
        return block(statement)
    } catch (t: Throwable) {
        failure = t
        throw t
    } finally {
        try {
            statement.close()
        } catch (closeError: Throwable) {
            if (failure == null) {
                throw closeError
            }
        }
    }
}
