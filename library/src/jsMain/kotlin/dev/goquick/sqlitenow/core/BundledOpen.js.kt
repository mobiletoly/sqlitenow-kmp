package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.core.sqlite.SqlJsDatabase
import dev.goquick.sqlitenow.core.sqlite.SqliteConnection
import dev.goquick.sqlitenow.core.sqlite.loadSqlJsModule
import kotlin.js.console

internal actual suspend fun openBundledSqliteConnection(dbName: String, debug: Boolean): SqliteConnection {
    val module = loadSqlJsModule()
    console.log("[SqlJs] module keys", js("Object.keys(module)") as Array<String>)
    val database = js("new module.Database()") as SqlJsDatabase
    return SqliteConnection(database)
}
