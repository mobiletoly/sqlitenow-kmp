/*
 * Copyright 2025 Anatoliy Pochkin
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

import androidx.sqlite.SQLiteConnection as JvmSQLiteConnection
import androidx.sqlite.SQLiteException as JvmSQLiteException
import androidx.sqlite.SQLiteStatement as JvmSQLiteStatement
import androidx.sqlite.execSQL

actual class SqliteConnection internal constructor(
    internal val delegate: JvmSQLiteConnection,
) {
    actual fun execSQL(sql: String) = wrapSqliteCall { delegate.execSQL(sql) }
    actual fun prepare(sql: String): SqliteStatement = wrapSqliteCall { delegate.prepare(sql) }.let(::SqliteStatement)
    actual fun inTransaction(): Boolean = wrapSqliteCall { delegate.inTransaction() }
    actual fun close() = delegate.close()
}

actual class SqliteStatement internal constructor(
    internal val delegate: JvmSQLiteStatement,
) {
    actual fun bindBlob(index: Int, value: ByteArray) = wrapSqliteCall { delegate.bindBlob(index, value) }
    actual fun bindDouble(index: Int, value: Double) = wrapSqliteCall { delegate.bindDouble(index, value) }
    actual fun bindLong(index: Int, value: Long) = wrapSqliteCall { delegate.bindLong(index, value) }
    actual fun bindText(index: Int, value: String) = wrapSqliteCall { delegate.bindText(index, value) }
    actual fun bindNull(index: Int) = wrapSqliteCall { delegate.bindNull(index) }
    actual fun bindInt(index: Int, value: Int) = wrapSqliteCall { delegate.bindLong(index, value.toLong()) }
    actual fun getBlob(index: Int): ByteArray = wrapSqliteCall { delegate.getBlob(index) }
    actual fun getDouble(index: Int): Double = wrapSqliteCall { delegate.getDouble(index) }
    actual fun getLong(index: Int): Long = wrapSqliteCall { delegate.getLong(index) }
    actual fun getText(index: Int): String = wrapSqliteCall { delegate.getText(index) }
    actual fun isNull(index: Int): Boolean = wrapSqliteCall { delegate.isNull(index) }
    actual fun getInt(index: Int): Int = wrapSqliteCall { delegate.getLong(index).toInt() }
    actual fun getColumnCount(): Int = wrapSqliteCall { delegate.getColumnCount() }
    actual fun getColumnName(index: Int): String = wrapSqliteCall { delegate.getColumnName(index) }
    actual fun step(): Boolean = wrapSqliteCall { delegate.step() }
    actual fun reset() = wrapSqliteCall { delegate.reset() }
    actual fun clearBindings() = wrapSqliteCall { delegate.clearBindings() }
    actual fun close() = delegate.close()
}

private inline fun <T> wrapSqliteCall(block: () -> T): T {
    return try {
        block()
    } catch (t: Throwable) {
        if (t is JvmSQLiteException) {
            throw SqliteException(t.message, t)
        } else {
            throw t
        }
    }
}
