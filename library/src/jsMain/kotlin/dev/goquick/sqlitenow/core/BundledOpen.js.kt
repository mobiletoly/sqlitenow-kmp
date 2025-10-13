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
