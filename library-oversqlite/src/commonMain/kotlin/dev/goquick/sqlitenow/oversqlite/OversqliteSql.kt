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
import dev.goquick.sqlitenow.core.sqlite.use

internal suspend inline fun <T> SafeSQLiteConnection.withPreparedStatement(
    sql: String,
    statementCache: StatementCache? = null,
    crossinline block: suspend (SqliteStatement) -> T,
): T {
    return withExclusiveAccess {
        val cached = statementCache?.get(sql)
        if (cached != null) {
            return@withExclusiveAccess block(cached)
        }

        val statement = prepare(sql)
        var failure: Throwable? = null
        try {
            block(statement)
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
}

internal suspend fun SafeSQLiteConnection.scalarLong(sql: String): Long {
    return withExclusiveAccess {
        prepare(sql).use { st ->
            check(st.step())
            st.getLong(0)
        }
    }
}

internal suspend inline fun <T> SafeSQLiteConnection.queryRequiredSingle(
    sql: String,
    missingMessage: String,
    crossinline map: (SqliteStatement) -> T,
): T {
    return withExclusiveAccess {
        prepare(sql).use { st ->
            check(st.step()) { missingMessage }
            map(st)
        }
    }
}

internal suspend inline fun <T> SafeSQLiteConnection.queryList(
    sql: String,
    crossinline bind: (SqliteStatement) -> Unit = {},
    crossinline map: suspend (SqliteStatement) -> T,
): List<T> {
    return withExclusiveAccess {
        prepare(sql).use { st ->
            bind(st)
            buildList {
                while (st.step()) {
                    add(map(st))
                }
            }
        }
    }
}

internal fun buildJsonObjectExprHexAware(
    tableInfo: TableInfo,
    prefix: String,
): String {
    val pairs = tableInfo.columns.map { column ->
        val name = column.name.lowercase()
        val valueExpr = when {
            column.kind.isBlobKind() ->
                "CASE WHEN $prefix.${quoteIdent(column.name)} IS NULL THEN NULL ELSE lower(hex($prefix.${quoteIdent(column.name)})) END"
            column.kind == ColumnKind.INTEGER ->
                "CASE WHEN $prefix.${quoteIdent(column.name)} IS NULL THEN NULL ELSE CAST($prefix.${quoteIdent(column.name)} AS TEXT) END"
            column.kind == ColumnKind.REAL ->
                "CASE WHEN $prefix.${quoteIdent(column.name)} IS NULL THEN NULL ELSE printf('%!.17g', $prefix.${quoteIdent(column.name)}) END"
            else -> "$prefix.${quoteIdent(column.name)}"
        }
        "'$name', $valueExpr"
    }
    return "json_object(${pairs.joinToString(", ")})"
}

internal fun buildKeyJsonObjectExprHexAware(
    tableInfo: TableInfo,
    keyColumn: String,
    prefix: String,
): String {
    val column = tableInfo.columns.firstOrNull { it.name.equals(keyColumn, ignoreCase = true) }
        ?: error("table ${tableInfo.table} is missing sync key column $keyColumn")
    val keyName = column.name.lowercase()
    val valueExpr = when {
        column.kind.isBlobKind() -> "lower(hex($prefix.${quoteIdent(column.name)}))"
        column.kind == ColumnKind.INTEGER ->
            "CAST($prefix.${quoteIdent(column.name)} AS TEXT)"
        else -> "$prefix.${quoteIdent(column.name)}"
    }
    return "json_object('$keyName', $valueExpr)"
}

internal fun quoteIdent(identifier: String): String = "\"${identifier.replace("\"", "\"\"")}\""
