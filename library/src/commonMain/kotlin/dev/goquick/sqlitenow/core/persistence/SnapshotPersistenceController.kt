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
package dev.goquick.sqlitenow.core.persistence

import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.PersistenceController
import dev.goquick.sqlitenow.core.SqlitePersistence
import dev.goquick.sqlitenow.core.exportConnectionBytes
import dev.goquick.sqlitenow.core.sqlite.SqliteConnection

/**
 * Persists database snapshots using a [SqlitePersistence] implementation.
 */
internal class SnapshotPersistenceController(
    private val persistence: SqlitePersistence,
    private val dbName: String,
    private val autoFlush: Boolean,
    override val restoredFromSnapshot: Boolean,
) : PersistenceController {

    override suspend fun onOperationComplete(connection: SqliteConnection, inTransaction: Boolean) {
        if (!autoFlush || inTransaction) return
        persistSnapshot(connection, force = false)
    }

    override suspend fun onTransactionCommitted(connection: SqliteConnection) {
        if (!autoFlush) return
        persistSnapshot(connection, force = false)
    }

    override suspend fun flush(connection: SqliteConnection) {
        persistSnapshot(connection, force = true)
    }

    override suspend fun onClose(connection: SqliteConnection) {
        persistSnapshot(connection, force = true)
    }

    private suspend fun persistSnapshot(connection: SqliteConnection, force: Boolean) {
        val bytes = exportConnectionBytes(connection) ?: return
        try {
            persistence.persist(dbName, bytes)
        } catch (t: Throwable) {
            sqliteNowLogger.e(t) { "Failed to persist database snapshot for $dbName" }
            if (force) throw t
        }
    }
}
