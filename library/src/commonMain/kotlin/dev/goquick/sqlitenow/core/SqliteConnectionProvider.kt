package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.core.sqlite.SqliteConnection

/**
 * Provides platform-specific [SafeSQLiteConnection] instances.
 */
fun interface SqliteConnectionProvider {
    suspend fun openConnection(
        dbName: String,
        debug: Boolean,
        config: SqliteConnectionConfig,
    ): SafeSQLiteConnection

    suspend fun openConnection(
        dbName: String,
        debug: Boolean,
    ): SafeSQLiteConnection = openConnection(dbName, debug, SqliteConnectionConfig())
}

/**
 * Default provider backed by the bundled SQLite driver for non-JS targets.
 * JS actual provides a stub connection for now.
 */
object BundledSqliteConnectionProvider : SqliteConnectionProvider {
    override suspend fun openConnection(
        dbName: String,
        debug: Boolean,
        config: SqliteConnectionConfig,
    ): SafeSQLiteConnection {
        val persistence = config.persistence?.takeUnless { dbName.isInMemoryPath() }
        val connection = openBundledSqliteConnection(dbName, debug, config.copy(persistence = persistence))
        return SafeSQLiteConnection(
            ref = connection,
            debug = debug,
            dbName = dbName,
            persistence = persistence,
            autoFlushPersistence = config.autoFlushPersistence,
        )
    }
}

internal expect suspend fun openBundledSqliteConnection(
    dbName: String,
    debug: Boolean,
    config: SqliteConnectionConfig,
): SqliteConnection

private fun String.isInMemoryPath(): Boolean {
    if (isEmpty()) return true
    return this == ":memory:" || startsWith(":memory:") || startsWith(":temp:")
}
