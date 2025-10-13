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
package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.core.sqlite.SqlJsDatabase
import dev.goquick.sqlitenow.core.sqlite.SqliteConnection
import dev.goquick.sqlitenow.core.sqlite.loadSqlJsModule
import dev.goquick.sqlitenow.core.sqlite.toUint8Array
import kotlin.js.console

internal actual suspend fun openBundledSqliteConnection(
    dbName: String,
    debug: Boolean,
    config: SqliteConnectionConfig,
): SqliteConnection {
    val module = loadSqlJsModule()
    val persistence = config.persistence
    val restoredBytes = if (persistence != null) {
        try {
            persistence.load(dbName)
        } catch (t: Throwable) {
            console.warn("[SqlJs] Failed to load persisted snapshot for $dbName", t)
            null
        }
    } else {
        null
    }

    val dataArg = restoredBytes?.toUint8Array()
    if (dataArg != null) {
        console.log("[SqlJs] Opening $dbName from persisted snapshot (${restoredBytes.size} bytes)")
    } else {
        console.log("[SqlJs] Opening $dbName with empty in-memory database")
    }

    @Suppress("UNCHECKED_CAST_TO_EXTERNAL_INTERFACE")
    val database = if (dataArg != null) {
        js("new module.Database(dataArg)") as SqlJsDatabase
    } else {
        js("new module.Database()") as SqlJsDatabase
    }

    return SqliteConnection(database)
}
