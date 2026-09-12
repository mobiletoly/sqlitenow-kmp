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
import dev.goquick.sqlitenow.core.SqliteNowCoordinatedProviderSession
import dev.goquick.sqlitenow.core.SqliteNowInternalApi
import dev.goquick.sqlitenow.core.createSqliteConnectionExecutionContext
import dev.goquick.sqlitenow.core.sqliteNowActiveClientIdentity
import dev.goquick.sqlitenow.core.sqlite.addSuppressedIfAbsent
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
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
    @OptIn(SqliteNowInternalApi::class)
    override suspend fun openCoordinatedSession(
        dbName: String,
        debug: Boolean,
        config: SqliteConnectionConfig,
    ): SqliteNowCoordinatedProviderSession = openSqliteWorkerCoordinatedSession(
        dbName = dbName,
        debug = debug,
        config = config,
        workerModuleUrl = workerModuleUrl,
        pageRows = pageRows,
        pageBytes = pageBytes,
        startupModeForTest = "normal",
        cleanupTimeoutMillis = SQLITE_WORKER_CLEANUP_TIMEOUT_MILLIS,
    )

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
                activeClientIdentity = sqliteNowActiveClientIdentity(dbName, connection),
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
        return openedDriver.openConfigured(dbName, config)
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

private suspend fun SqliteWorkerSQLiteDriver.openConfigured(
    dbName: String,
    config: SqliteConnectionConfig,
): SQLiteConnection {
    val customPersistence = config.persistence
    val legacySourceMode = when {
        runtimeKind() == "js-node-worker" && customPersistence == null -> "none"
        customPersistence != null -> "custom"
        else -> "built-in"
    }
    return open(
        fileName = dbName,
        legacySourceMode = legacySourceMode,
        customPersistence = customPersistence,
    )
}

@OptIn(SqliteNowInternalApi::class)
internal suspend fun openSqliteWorkerCoordinatedSession(
    dbName: String,
    debug: Boolean,
    config: SqliteConnectionConfig,
    workerModuleUrl: String?,
    pageRows: Int,
    pageBytes: Int,
    startupModeForTest: String,
    cleanupTimeoutMillis: Int,
    inspectionRefForTest: (suspend (SqliteWorkerSQLiteDriver) -> SQLiteConnection?)? = null,
): SqliteNowCoordinatedProviderSession {
    val workerConfig = SqliteWorkerConfig(pageRows = pageRows, pageBytes = pageBytes)
    workerConfig.validate()
    val executionContext = createSqliteConnectionExecutionContext("$dbName:coordinated")
    var driver: SqliteWorkerSQLiteDriver? = null
    try {
        val openedDriver = withContext(executionContext.dispatcher) {
            SqliteWorkerSQLiteDriver.create(
                config = workerConfig,
                workerModuleUrl = workerModuleUrl,
                startupModeForTest = startupModeForTest,
                cleanupTimeoutMillis = cleanupTimeoutMillis,
            )
        }.also { driver = it }
        val inspectionSourceMode = when {
            openedDriver.runtimeKind() == "js-node-worker" && config.persistence == null -> "none"
            config.persistence != null -> "custom"
            else -> "built-in"
        }
        val inspectedRef = withContext(executionContext.dispatcher) {
            inspectionRefForTest?.invoke(openedDriver)
                ?: openedDriver.inspect(dbName, inspectionSourceMode)
        }
        val inspection = inspectedRef?.let { ref ->
            SafeSQLiteConnection(
                ref = ref,
                debug = debug,
                activeClientIdentity = sqliteNowActiveClientIdentity(dbName, ref),
                persistenceController = NoopPersistenceController(),
                executionContextHook = config.executionContextHook,
                executionContext = executionContext,
            )
        }
        return SqliteWorkerCoordinatedProviderSession(
            dbName = dbName,
            debug = debug,
            config = config,
            initialDriver = openedDriver,
            createDriver = {
                withContext(executionContext.dispatcher) {
                    SqliteWorkerSQLiteDriver.create(
                        config = workerConfig,
                        workerModuleUrl = workerModuleUrl,
                        startupModeForTest = startupModeForTest,
                        cleanupTimeoutMillis = cleanupTimeoutMillis,
                    )
                }
            },
            executionContext = executionContext,
            inspectionConnection = inspection,
        )
    } catch (failure: Throwable) {
        val primary = failure.normalizeWorkerFailure()
        withContext(NonCancellable) {
            try {
                driver?.shutdown()
            } catch (cleanupFailure: Throwable) {
                primary.addSuppressedIfAbsent(cleanupFailure.normalizeWorkerFailure())
            }
            try {
                executionContext.close()
            } catch (cleanupFailure: Throwable) {
                primary.addSuppressedIfAbsent(cleanupFailure.normalizeWorkerFailure())
            }
        }
        throw primary
    }
}

@OptIn(SqliteNowInternalApi::class)
internal class SqliteWorkerCoordinatedProviderSession(
    private val dbName: String,
    private val debug: Boolean,
    private val config: SqliteConnectionConfig,
    initialDriver: SqliteWorkerSQLiteDriver,
    private val createDriver: suspend () -> SqliteWorkerSQLiteDriver,
    private val executionContext: dev.goquick.sqlitenow.core.SqliteConnectionExecutionContext,
    override val inspectionConnection: SafeSQLiteConnection?,
) : SqliteNowCoordinatedProviderSession {
    override val preparationRequired: Boolean = inspectionConnection == null
    private var prepared: SafeSQLiteConnection? = null
    private var driver: SqliteWorkerSQLiteDriver? = initialDriver
    private var inspectionHandleClosed = false
    private var published = false
    private val closed: Boolean get() = driver == null && executionContextCloseComplete
    private var cleanupOnly = false
    private var executionContextCloseComplete = false
    private val closeMutex = Mutex()
    private var closeAttempt: ProviderCloseAttempt? = null
    private var preparation: CompletableDeferred<Unit>? = null
    private var preparationJob: Job? = null

    override suspend fun prepareConnection(): SafeSQLiteConnection {
        check(!closed && !published && !cleanupOnly && closeAttempt == null) {
            "Coordinated worker provider session is not active"
        }
        prepared?.let { return it }
        check(preparation == null) { "Coordinated worker preparation is already in progress" }
        val completed = CompletableDeferred<Unit>()
        preparation = completed
        preparationJob = currentCoroutineContext()[Job]
        try {
            return prepareOwnedConnection()
        } finally {
            preparationJob = null
            preparation = null
            completed.complete(Unit)
        }
    }

    private suspend fun prepareOwnedConnection(): SafeSQLiteConnection {
        if (!inspectionHandleClosed) {
            try {
                inspectionConnection?.closeInspectionHandleForProviderPreparation()
            } catch (failure: Throwable) {
                cleanupOnly = true
                throw failure
            }
            inspectionHandleClosed = true
        }
        val activeDriver = driver ?: createDriver().also { driver = it }
        val ref = try {
            withContext(executionContext.dispatcher) { activeDriver.openConfigured(dbName, config) }
        } catch (failure: Throwable) {
            withContext(NonCancellable) {
                try {
                    activeDriver.shutdown()
                } catch (cleanupFailure: Throwable) {
                    failure.addSuppressedIfAbsent(cleanupFailure.normalizeWorkerFailure())
                }
            }
            if (activeDriver.terminationConfirmed) {
                driver = null
            } else {
                cleanupOnly = true
            }
            throw failure
        }
        return SafeSQLiteConnection(
            ref = ref,
            debug = debug,
            activeClientIdentity = sqliteNowActiveClientIdentity(dbName, ref),
            persistenceController = NoopPersistenceController(),
            executionContextHook = config.executionContextHook,
            executionContext = executionContext,
        ).also {
            prepared = it
            check(!cleanupOnly) { "Coordinated worker preparation was revoked by close" }
        }
    }

    override fun publish(connection: SafeSQLiteConnection) {
        check(!closed && !published && !cleanupOnly && closeAttempt == null) {
            "Coordinated worker provider session is not active"
        }
        check(prepared === connection) { "Only the prepared worker connection can be published" }
        published = true
        prepared = null
    }

    override suspend fun close() {
        check(preparation == null || preparationJob !== currentCoroutineContext()[Job]) {
            "Cannot close a coordinated worker from its own preparation"
        }
        val (attempt, owner) = closeMutex.withLock {
            if (closed || published) return
            closeAttempt?.let { return@withLock it to false }
            cleanupOnly = true
            ProviderCloseAttempt().also { closeAttempt = it } to true
        }
        if (!owner) {
            attempt.completed.await()?.let { throw it }
            return
        }
        val failure = withContext(NonCancellable) {
            try { closeOwnedResources() } catch (failure: Throwable) { failure }
        }
        attempt.completed.complete(failure)
        closeMutex.withLock {
            if (closeAttempt === attempt) closeAttempt = null
        }
        failure?.let { throw it }
    }

    private suspend fun closeOwnedResources(): Throwable? {
        preparation?.let { pending ->
            preparationJob?.cancel()
            pending.await()
        }
        val active = prepared ?: inspectionConnection?.takeUnless { inspectionHandleClosed }
        if (active != null) {
            val failure = runCatching { active.close() }.exceptionOrNull()
            if (active.ownedCloseComplete) {
                prepared = null
                inspectionHandleClosed = true
                driver = null
                executionContextCloseComplete = true
            } else {
                cleanupOnly = true
            }
            return failure
        }
        var failure: Throwable? = null
        val activeDriver = driver
        if (activeDriver != null) {
            try {
                activeDriver.shutdown()
            } catch (t: Throwable) {
                failure = t
            }
            // Sticky protocol diagnostics do not imply a live worker.
            if (activeDriver.terminationConfirmed) driver = null
        }
        if (driver == null && !executionContextCloseComplete) {
            try {
                executionContext.close()
                executionContextCloseComplete = true
            } catch (t: Throwable) {
                if (failure == null) failure = t else failure.addSuppressedIfAbsent(t)
            }
        }
        return failure
    }

    private class ProviderCloseAttempt {
        val completed = CompletableDeferred<Throwable?>()
    }
}
