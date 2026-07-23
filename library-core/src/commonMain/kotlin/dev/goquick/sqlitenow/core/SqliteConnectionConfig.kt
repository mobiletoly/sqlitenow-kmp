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

/**
 * Configuration applied when opening a database connection.
 *
 * On JS/Wasm, a non-null [persistence] is an optional source for one-time legacy byte import into
 * the direct worker database. The worker may call only [SqlitePersistence.load] after requesting
 * that source; direct writes do not call [SqlitePersistence.persist] or [SqlitePersistence.clear],
 * and [autoFlushPersistence] has no snapshot-export role. Other targets retain their existing
 * persistence behavior.
 */
data class SqliteConnectionConfig(
    val persistence: SqlitePersistence? = null,
    val autoFlushPersistence: Boolean = true,
    /**
     * Optional hook that restores contextual state (trace/span/MDC) across the dispatcher hop
     * used by SQLiteNow connections.
     */
    val executionContextHook: SqliteNowContextHook? = null,
)

/**
 * Provides a storage mechanism for persisting a SQLite database.
 *
 * This source-compatible contract has target-specific semantics. On the JS/Wasm direct worker,
 * an explicitly configured implementation is a one-time legacy import source: only [load] may be
 * requested. The worker never calls [persist] or [clear]. Non-web targets retain their existing
 * persistence behavior.
 */
interface SqlitePersistence {
    /**
     * Reads the previously persisted database bytes for [dbName], or null if none exist.
     */
    suspend fun load(dbName: String): ByteArray?

    /**
     * Persists the provided [bytes] for [dbName], overwriting any existing snapshot.
     */
    suspend fun persist(dbName: String, bytes: ByteArray)

    /**
     * Removes any stored database snapshot for [dbName].
     *
     * Optional to implement; default implementation is a no-op.
     */
    suspend fun clear(dbName: String) {}
}
