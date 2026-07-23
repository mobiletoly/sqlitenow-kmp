/*
 * Copyright 2026 Toly Pochkin
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
package dev.goquick.sqlitenow.core.worker

import androidx.sqlite.SQLiteConnection
import dev.goquick.sqlitenow.core.NoopPersistenceController
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.SqliteConnectionProvider
import dev.goquick.sqlitenow.core.createSqliteConnectionExecutionContext
import dev.goquick.sqlitenow.core.sqlite.addSuppressedIfAbsent
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext

/**
 * Creates the explicit SQLite WASM worker connection provider for Kotlin/JS and Kotlin/Wasm.
 *
 * When [workerModuleUrl] is `null`, the provider uses the versioned worker module packaged with
 * SQLiteNow Core. A relative override is resolved against SQLiteNow's packaged client module;
 * `blob:` and `data:` overrides are rejected.
 *
 * Browser workers store each database through SQLite's direct OPFS VFS under a deterministic,
 * versioned target derived from the SQLiteNow database name. Missing OPFS capability fails the
 * open instead of silently selecting snapshot or in-memory browser persistence. JS Node workers
 * remain transient and in-memory. SQLiteNow's bundled web provider uses the same implementation;
 * this factory exists for resource overrides and focused testing.
 */
public fun sqliteWorkerConnectionProvider(
    workerModuleUrl: String? = null,
): SqliteConnectionProvider {
    require(workerModuleUrl == null || workerModuleUrl.isNotBlank()) {
        "SQLite worker module URL must be non-empty when provided."
    }
    return SqliteWorkerConnectionProvider(workerModuleUrl = workerModuleUrl)
}

internal class SqliteWorkerConnectionProvider(
    private val workerModuleUrl: String? = null,
    private val pageRows: Int = SQLITE_WORKER_DEFAULT_PAGE_ROWS,
    private val pageBytes: Int = SQLITE_WORKER_DEFAULT_PAGE_BYTES,
) : SqliteConnectionProvider {
    override suspend fun openConnection(
        dbName: String,
        debug: Boolean,
        config: SqliteConnectionConfig,
    ): SafeSQLiteConnection = openConnection(
        dbName = dbName,
        debug = debug,
        config = config,
        startupModeForTest = "normal",
        cleanupTimeoutMillis = SQLITE_WORKER_CLEANUP_TIMEOUT_MILLIS,
    )

    internal suspend fun openConnectionForTest(
        dbName: String,
        debug: Boolean,
        config: SqliteConnectionConfig,
        startupModeForTest: String,
        cleanupTimeoutMillis: Int = SQLITE_WORKER_CLEANUP_TIMEOUT_MILLIS,
    ): SafeSQLiteConnection = openConnection(
        dbName = dbName,
        debug = debug,
        config = config,
        startupModeForTest = startupModeForTest,
        cleanupTimeoutMillis = cleanupTimeoutMillis,
    )

    private suspend fun openConnection(
        dbName: String,
        debug: Boolean,
        config: SqliteConnectionConfig,
        startupModeForTest: String,
        cleanupTimeoutMillis: Int,
    ): SafeSQLiteConnection {
        val executionContext = createSqliteConnectionExecutionContext(dbName)
        try {
            val connection = withContext(executionContext.dispatcher) {
                openSqliteWorkerConnection(
                    dbName = dbName,
                    config = config,
                    workerModuleUrl = workerModuleUrl,
                    pageRows = pageRows,
                    pageBytes = pageBytes,
                    startupModeForTest = startupModeForTest,
                    cleanupTimeoutMillis = cleanupTimeoutMillis,
                )
            }
            return SafeSQLiteConnection(
                ref = connection,
                debug = debug,
                persistenceController = NoopPersistenceController(),
                executionContextHook = config.executionContextHook,
                executionContext = executionContext,
            )
        } catch (failure: Throwable) {
            val primary = failure.normalizeWorkerFailure()
            withContext(NonCancellable) {
                try {
                    executionContext.close()
                } catch (contextFailure: Throwable) {
                    primary.addSuppressedIfAbsent(contextFailure.normalizeWorkerFailure())
                }
            }
            throw primary
        }
    }
}

internal suspend fun openSqliteWorkerConnection(
    dbName: String,
    config: SqliteConnectionConfig,
    workerModuleUrl: String? = null,
    pageRows: Int = SQLITE_WORKER_DEFAULT_PAGE_ROWS,
    pageBytes: Int = SQLITE_WORKER_DEFAULT_PAGE_BYTES,
    startupModeForTest: String = "normal",
    cleanupTimeoutMillis: Int = SQLITE_WORKER_CLEANUP_TIMEOUT_MILLIS,
): SQLiteConnection {
    val workerConfig = SqliteWorkerConfig(pageRows = pageRows, pageBytes = pageBytes)
    workerConfig.validate()
    var driver: SqliteWorkerSQLiteDriver? = null
    try {
        val openedDriver = SqliteWorkerSQLiteDriver.create(
            config = workerConfig,
            workerModuleUrl = workerModuleUrl,
            startupModeForTest = startupModeForTest,
            cleanupTimeoutMillis = cleanupTimeoutMillis,
        ).also { driver = it }
        val customPersistence = config.persistence
        val legacySourceMode = when {
            openedDriver.runtimeKind() == "js-node-worker" && customPersistence == null -> "none"
            customPersistence != null -> "custom"
            else -> "built-in"
        }
        return openedDriver.open(
            fileName = dbName,
            legacySourceMode = legacySourceMode,
            customPersistence = customPersistence,
        )
    } catch (failure: Throwable) {
        val primary = failure.normalizeWorkerFailure()
        withContext(NonCancellable) {
            driver?.let { openedDriver ->
                try {
                    openedDriver.shutdown()
                } catch (cleanupFailure: Throwable) {
                    primary.addSuppressedIfAbsent(cleanupFailure.normalizeWorkerFailure())
                }
            }
        }
        throw primary
    }
}
