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
package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.sqlite.SqlJsDatabaseHandle
import dev.goquick.sqlitenow.core.sqlite.SqliteConnection
import dev.goquick.sqlitenow.core.sqlite.dbCreate
import dev.goquick.sqlitenow.core.sqlite.dbOpen
import dev.goquick.sqlitenow.core.sqlite.ensureSqlJsLoaded
import dev.goquick.sqlitenow.core.sqlite.toSqlJsArray
import dev.goquick.sqlitenow.core.sqlite.wrapSqlite

internal actual suspend fun openBundledSqliteConnection(
    dbName: String,
    debug: Boolean,
    initialBytes: ByteArray?,
    config: SqliteConnectionConfig,
): SqliteConnection {
    ensureSqlJsLoaded()

    val handle = wrapSqlite {
        if (initialBytes != null) {
            sqliteNowLogger.i { "[SqlJs][Wasm] Kotlin opening $dbName with snapshot bytes=${initialBytes.size}" }
        }
        val id = if (initialBytes != null && initialBytes.isNotEmpty()) {
            dbOpen(initialBytes.toSqlJsArray())
        } else {
            sqliteNowLogger.i { "[SqlJs][Wasm] Kotlin opening $dbName without snapshot (new database)" }
            dbCreate()
        }
        SqlJsDatabaseHandle(id)
    }

    if (debug) {
        if (initialBytes != null) {
            sqliteNowLogger.d { "[SqlJs][Wasm] Opening $dbName from snapshot (${initialBytes.size} bytes)" }
        } else {
            sqliteNowLogger.d { "[SqlJs][Wasm] Opening $dbName with empty in-memory database" }
        }
    }

    return SqliteConnection(handle)
}
