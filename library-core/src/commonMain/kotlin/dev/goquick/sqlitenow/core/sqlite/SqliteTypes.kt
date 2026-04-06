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

/**
 * Minimal cross-platform SQLite connection interface used by the SQLiteNow runtime
 * and generated code. Platform source sets provide actual implementations backed by
 * the corresponding native driver (androidx, SQL.js, etc).
 */
expect class SqliteConnection {
    fun execSQL(sql: String)
    fun prepare(sql: String): SqliteStatement
    fun inTransaction(): Boolean
    fun close()
}

/**
 * Minimal SQLite statement contract consumed by generated bindings.
 */
expect class SqliteStatement {
    fun bindBlob(index: Int, value: ByteArray)
    fun bindDouble(index: Int, value: Double)
    fun bindLong(index: Int, value: Long)
    fun bindText(index: Int, value: String)
    fun bindNull(index: Int)
    fun bindInt(index: Int, value: Int)

    fun getBlob(index: Int): ByteArray
    fun getDouble(index: Int): Double
    fun getLong(index: Int): Long
    fun getText(index: Int): String
    fun isNull(index: Int): Boolean
    fun getInt(index: Int): Int

    fun getColumnCount(): Int
    fun getColumnName(index: Int): String

    fun step(): Boolean
    fun reset()
    fun clearBindings()
    fun close()
}

/**
 * Convenience helper mirroring the androidx API.
 */
fun SqliteStatement.getColumnNames(): List<String> {
    val count = getColumnCount()
    if (count <= 0) return emptyList()
    return (0 until count).map { getColumnName(it) }
}


class SqliteException(message: String? = null, cause: Throwable? = null) : RuntimeException(message, cause)

inline fun <T> SqliteStatement.use(block: (SqliteStatement) -> T): T {
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
            if (exception == null) throw closeError
        }
    }
}
