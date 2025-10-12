package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.core.sqlite.SqliteConnection

/**
 * Provides platform-specific [SafeSQLiteConnection] instances.
 */
fun interface SqliteConnectionProvider {
    suspend fun openConnection(dbName: String, debug: Boolean): SafeSQLiteConnection
}

/**
 * Default provider backed by the bundled SQLite driver for non-JS targets.
 * JS actual provides a stub connection for now.
 */
object BundledSqliteConnectionProvider : SqliteConnectionProvider {
    override suspend fun openConnection(dbName: String, debug: Boolean): SafeSQLiteConnection {
        val connection = openBundledSqliteConnection(dbName, debug)
        return SafeSQLiteConnection(connection, debug)
    }
}

internal expect suspend fun openBundledSqliteConnection(dbName: String, debug: Boolean): SqliteConnection
