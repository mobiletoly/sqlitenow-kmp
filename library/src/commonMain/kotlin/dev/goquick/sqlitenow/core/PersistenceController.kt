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

import dev.goquick.sqlitenow.core.sqlite.SqliteConnection

/**
 * Coordinates persistence-related lifecycle hooks for a [SafeSQLiteConnection].
 */
internal interface PersistenceController {
    /**
     * Indicates whether the connection was restored from a previously persisted snapshot.
     */
    val restoredFromSnapshot: Boolean

    /**
     * Called after an operation completes (e.g., execSQL or a statement-driven block).
     * Implementations can use [inTransaction] to decide whether to flush.
     */
    suspend fun onOperationComplete(connection: SqliteConnection, inTransaction: Boolean)

    /**
     * Called after the outermost transaction successfully commits.
     */
    suspend fun onTransactionCommitted(connection: SqliteConnection)

    /**
     * Forces persistence to flush any pending changes.
     */
    suspend fun flush(connection: SqliteConnection)

    /**
     * Invoked before the connection is closed to allow final flushing or cleanup.
     */
    suspend fun onClose(connection: SqliteConnection)
}

internal class NoopPersistenceController(
    override val restoredFromSnapshot: Boolean = false,
) : PersistenceController {
    override suspend fun onOperationComplete(connection: SqliteConnection, inTransaction: Boolean) = Unit
    override suspend fun onTransactionCommitted(connection: SqliteConnection) = Unit
    override suspend fun flush(connection: SqliteConnection) = Unit
    override suspend fun onClose(connection: SqliteConnection) = Unit
}
