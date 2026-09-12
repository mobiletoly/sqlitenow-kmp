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

import androidx.sqlite.SQLiteConnection
import dev.goquick.sqlitenow.common.PlatformType
import dev.goquick.sqlitenow.common.platform
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.persistence.SnapshotPersistenceController
import kotlinx.coroutines.withContext

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

    /**
     * Creates the internal provider session used by coordinated opening.
     *
     * The default deliberately fails before calling [openConnection]: an ordinary provider open
     * may restore bytes, create storage, or otherwise mutate provider state and therefore cannot
     * be used for the inspection-only `openForSync` boundary.
     */
    @SqliteNowInternalApi
    suspend fun openCoordinatedSession(
        dbName: String,
        debug: Boolean,
        config: SqliteConnectionConfig,
    ): SqliteNowCoordinatedProviderSession = throw UnsupportedOperationException(
        "The selected SQLite connection provider does not support coordinated-opening inspection",
    )
}

/** Provider-owned connection lifecycle spanning inspection, preparation, and publication. */
@SqliteNowInternalApi
interface SqliteNowCoordinatedProviderSession {
    /** A side-effect-free inspection connection, or null until explicit preparation is run. */
    val inspectionConnection: SafeSQLiteConnection?

    /** True when no complete local inspection was possible without provider preparation. */
    val preparationRequired: Boolean

    /** Performs writable provider preparation after explicit `advance()` consent. */
    suspend fun prepareConnection(): SafeSQLiteConnection

    /** Transfers the prepared connection to the published database lifetime. */
    fun publish(connection: SafeSQLiteConnection)

    /** Joins cleanup of all unpublished provider resources. */
    suspend fun close()
}

/**
 * Default provider backed by the bundled SQLite driver.
 *
 * Web targets use SQLiteNow's packaged worker. Other targets retain their platform driver and
 * persistence behavior.
 */
object BundledSqliteConnectionProvider : SqliteConnectionProvider {
    @OptIn(SqliteNowInternalApi::class)
    override suspend fun openCoordinatedSession(
        dbName: String,
        debug: Boolean,
        config: SqliteConnectionConfig,
    ): SqliteNowCoordinatedProviderSession = createBundledCoordinatedOpeningProviderSession(
        dbName = dbName,
        debug = debug,
        config = config,
    )

    override suspend fun openConnection(
        dbName: String,
        debug: Boolean,
        config: SqliteConnectionConfig,
    ): SafeSQLiteConnection {
        val usesDirectWebWorker = platform() == PlatformType.JS
        val persistence = if (usesDirectWebWorker) {
            null
        } else {
            config.persistence?.takeUnless { dbName.isInMemoryPath() }
        }
        val restoredBytes = if (persistence != null) {
            try {
                val bytes = persistence.load(dbName)
                if (bytes != null) {
                    println("[SqliteNow] Restored ${bytes.size} bytes for $dbName")
                } else {
                    println("[SqliteNow] No persisted snapshot for $dbName")
                }
                bytes
            } catch (t: Throwable) {
                sqliteNowLogger.w(t) { "Failed to load persisted snapshot for $dbName" }
                null
            }
        } else {
            null
        }
        val executionContext = createSqliteConnectionExecutionContext(dbName)
        try {
            val connection = withContext(executionContext.dispatcher) {
                openBundledSqliteConnection(
                    dbName = dbName,
                    debug = debug,
                    initialBytes = restoredBytes,
                    config = if (usesDirectWebWorker) {
                        config
                    } else {
                        config.copy(persistence = persistence)
                    },
                )
            }
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
                activeClientIdentity = sqliteNowActiveClientIdentity(dbName, connection),
                persistenceController = persistenceController,
                executionContextHook = config.executionContextHook,
                executionContext = executionContext,
            )
        } catch (t: Throwable) {
            runCatching { executionContext.close() }
            throw t
        }
    }
}

@OptIn(SqliteNowInternalApi::class)
internal expect suspend fun createBundledCoordinatedOpeningProviderSession(
    dbName: String,
    debug: Boolean,
    config: SqliteConnectionConfig,
): SqliteNowCoordinatedProviderSession

internal expect suspend fun openBundledSqliteConnection(
    dbName: String,
    debug: Boolean,
    initialBytes: ByteArray?,
    config: SqliteConnectionConfig,
): SQLiteConnection

private fun String.isInMemoryPath(): Boolean {
    if (isEmpty()) return true
    return this == ":memory:" || hasPrefix(":memory:") || hasPrefix(":temp:")
}

private fun String.hasPrefix(prefix: String): Boolean {
    if (length < prefix.length) return false
    for (index in prefix.indices) {
        if (this[index] != prefix[index]) return false
    }
    return true
}
