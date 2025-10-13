package dev.goquick.sqlitenow.core

import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.sqlite.driver.bundled.SQLITE_OPEN_CREATE
import androidx.sqlite.driver.bundled.SQLITE_OPEN_READWRITE
import dev.goquick.sqlitenow.core.sqlite.SqliteConnection

internal actual suspend fun openBundledSqliteConnection(
    dbName: String,
    debug: Boolean,
    config: SqliteConnectionConfig,
): SqliteConnection {
    val delegate = BundledSQLiteDriver().open(
        fileName = dbName,
        flags = SQLITE_OPEN_CREATE or SQLITE_OPEN_READWRITE,
    )
    return SqliteConnection(delegate)
}
