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

import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.persistence.SnapshotPersistenceController
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
        val restoredBytes = if (persistence != null) {
            try {
                persistence.load(dbName)
            } catch (t: Throwable) {
                sqliteNowLogger.w(t) { "Failed to load persisted snapshot for $dbName" }
                null
            }
        } else {
            null
        }
        val connection = openBundledSqliteConnection(
            dbName = dbName,
            debug = debug,
            initialBytes = restoredBytes,
            config = config.copy(persistence = persistence),
        )
        val persistenceController = if (persistence != null) {
            SnapshotPersistenceController(
                persistence = persistence,
                dbName = dbName,
                autoFlush = config.autoFlushPersistence,
                restoredFromSnapshot = restoredBytes != null,
            )
        } else {
            NoopPersistenceController()
        }
        return SafeSQLiteConnection(
            ref = connection,
            debug = debug,
            persistenceController = persistenceController,
        )
    }
}

internal expect suspend fun openBundledSqliteConnection(
    dbName: String,
    debug: Boolean,
    initialBytes: ByteArray?,
    config: SqliteConnectionConfig,
): SqliteConnection

private fun String.isInMemoryPath(): Boolean {
    if (isEmpty()) return true
    return this == ":memory:" || startsWith(":memory:") || startsWith(":temp:")
}
