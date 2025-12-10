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

import dev.goquick.sqlitenow.common.KermitSqliteNowLogger
import dev.goquick.sqlitenow.common.LogLevel
import dev.goquick.sqlitenow.common.originalSqliteNowLogger
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.concurrent.Volatile

/**
 * Base class for generated database classes.
 *
 * The generated types build on this implementation across every supported Kotlin target:
 * Android/JVM, iOS/Native, JavaScript, and Kotlin/Wasm. On browser targets the runtime
 * uses SQL.js under the hood (with optional IndexedDB snapshots) while native targets rely
 * on the bundled SQLite driver.
 */
open class SqliteNowDatabase private constructor(
    private val dbName: String,
    private val migration: DatabaseMigrations,
    private val debug: Boolean,
    private val connectionProvider: SqliteConnectionProvider,
    private val adjustLogger: Boolean,
) {
    @Volatile
    private var _conn: SafeSQLiteConnection? = null
    private val conn: SafeSQLiteConnection
        get() = _conn ?: error("Database connection not initialized or already closed. Call open() first.")

    private val openCloseMutex = Mutex()

    var connectionConfig: SqliteConnectionConfig = SqliteConnectionConfig(
        persistence = sqliteDefaultPersistence(dbName),
    )
        set(value) {
            if (_conn != null || openCloseMutex.isLocked) {
                throw IllegalStateException("Cannot update connectionConfig after open() has started")
            }
            field = value
        }

    // Table change notification system
    private val tableChangeFlows = mutableMapOf<String, MutableSharedFlow<Unit>>()
    private val tableChangesFlowMutex = Mutex()
    private val tableChangeScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    @Volatile
    private var enableTableChangeNotifications = false

    /**
     * Constructor for the database.
     * @param dbName The name of the database file
     * @param migration The migration object to apply migrations, in most cases you must pass
     *                  generated migration class VersionBasedDatabaseMigrations() here.
     */
    constructor(
        dbName: String,
        migration: DatabaseMigrations,
        connectionProvider: SqliteConnectionProvider = BundledSqliteConnectionProvider,
    ) : this(
        dbName = dbName,
        migration = migration,
        debug = false,
        connectionProvider = connectionProvider,
        adjustLogger = false,
    )

    /**
     * Optional constructor that allows enabling verbose debug logging.
     * When debug is true, increases logger verbosity to Debug; otherwise uses default Info level.
     */
    constructor(
        dbName: String,
        migration: DatabaseMigrations,
        debug: Boolean,
        connectionProvider: SqliteConnectionProvider = BundledSqliteConnectionProvider,
    ) : this(
        dbName = dbName,
        migration = migration,
        debug = debug,
        connectionProvider = connectionProvider,
        adjustLogger = true,
    )

    init {
        if (adjustLogger) {
            sqliteNowLogger = if (originalSqliteNowLogger === sqliteNowLogger) {
                KermitSqliteNowLogger(if (debug) LogLevel.Debug else LogLevel.Info)
            } else {
                sqliteNowLogger
            }
        }
    }

    /**
     * @return The database connection
     * @throws IllegalStateException if the database is not open
     */
    fun connection(): SafeSQLiteConnection = conn

    /**
     * Opens a database connection and applies migrations.
     *
     * @param preInit Optional suspend function to run before migrations are applied.
     *  this will be executed outside of initialization/migration transaction, so it
     *  is useful for operation such as setting PRAGMAs that must be set before initialization
     *  or migration.
     */
    suspend fun open(preInit: suspend (conn: SafeSQLiteConnection) -> Unit = {}) {
        openCloseMutex.withLock {
            if (_conn != null) {
                throw IllegalStateException("Database connection already initialized")
            }

            val c = connectionProvider.openConnection(dbName, debug, connectionConfig)
            try {
                preInit(c)
                ensureBootstrapUserVersion(c)

                c.transaction {
                    val currentVersion = c.readUserVersion()
                    val newVersion = migration.applyMigration(c, currentVersion)
                    if (newVersion != currentVersion) {
                        c.execSQL("PRAGMA user_version = $newVersion;")
                    }
                }

                // Publish only after successful initialization/migrations
                _conn = c
            } catch (t: Throwable) {
                c.close()
                throw t
            }
        }
    }

    /**
     * Gets the current user_version from the database.
     */
    internal suspend fun getUserVersion(): Int {
        val connection = conn
        val statement = connection.prepare("PRAGMA user_version;")
        return try {
            statement.step()
            statement.getLong(0).toInt()
        } finally {
            statement.close()
        }
    }

    /**
     * Sets the user_version in the database.
     */
    internal suspend fun setUserVersion(version: Int) {
        conn.execSQL("PRAGMA user_version = $version;")
    }

    /**
     * Executes the given block within a database transaction.
     * The transaction will be committed if the block completes successfully,
     * or rolled back if an exception is thrown.
     *
     * @param block The suspend function to execute within the transaction
     * @return The result of the block
     * @throws Exception Any exception thrown by the block
     */
    suspend fun <T> transaction(
        mode: TransactionMode = TransactionMode.DEFERRED,
        block: suspend () -> T
    ): T {
        // Delegate to SafeSQLiteConnection to ensure nested transactions are handled safely
        return conn.transaction(mode, block)
    }

    /**
     * Disables table change notifications. Disabling notifications can improve performance
     * or UI conflicts when perform initial data imports or bulk updates.
     */
    fun disableTableChangeNotifications() {
        this.enableTableChangeNotifications = false
    }

    /**
     * Enables table change notifications.
     */
    fun enableTableChangeNotifications() {
        this.enableTableChangeNotifications = true
    }

    /**
     * Closes the database connection.
     */
    suspend fun close() = openCloseMutex.withLock {
        _conn?.let { c ->
            _conn = null
            c.close()
            tableChangeScope.cancel()
        }
    }

    /**
     * Check if database is open.
     */
    fun isOpen(): Boolean = _conn != null

    /**
     * Forces any external persistence layer to store the current database snapshot.
     *
     * Targets that back databases with a real file system (Android/iOS/JVM) persist changes
     * automatically, so calling this function is a no-op and safe to call.
     * It is primarily useful for the Kotlin/JS runtime when an external persistence
     * implementation (for example, `IndexedDbSqlitePersistence`) is configured and
     * `autoFlushPersistence` is disabled.
     */
    suspend fun persistSnapshotNow() {
        conn.persistSnapshotNow()
    }

    private suspend fun ensureBootstrapUserVersion(connection: SafeSQLiteConnection) {
        val currentVersion = connection.readUserVersion()
        if (currentVersion != 0) return
        if (connection.hasUserTables() || connection.restoredFromSnapshot) return
        connection.execSQL("PRAGMA user_version = -1;")
    }


    /**
     * Notifies listeners that the specified tables have been modified.
     * This should be called after any EXECUTE statement (INSERT/UPDATE/DELETE).
     *
     * @param affectedTables Set of table names that were modified
     */
    protected fun notifyTablesChanged(affectedTables: Set<String>) {
        if (!enableTableChangeNotifications || affectedTables.isEmpty()) return

        tableChangeScope.launch {
            val flowsToNotify = tableChangesFlowMutex.withLock {
                affectedTables.asSequence()
                    .map { tableChangeFlows[it.lowercase()] }
                    .filterNotNull()
                    .toList()
            }

            flowsToNotify.forEach { flow ->
                flow.emit(Unit)
            }
        }
    }

    /**
     * Creates a Flow that emits when any of the specified tables change.
     * This is used internally by generated listen() functions.
     *
     * @param affectedTables Set of table names to listen for changes
     * @return Flow that emits Unit when any of the tables change
     */
    protected suspend fun createTableChangeFlow(affectedTables: Set<String>): Flow<Unit> {
        if (affectedTables.isEmpty()) {
            return flow { } // Empty flow for queries with no affected tables
        }

        // Get or create SharedFlows for each table
        val flows = tableChangesFlowMutex.withLock {
            affectedTables.map { tableName ->
                tableChangeFlows.getOrPut(tableName.lowercase()) {
                    MutableSharedFlow(replay = 0, extraBufferCapacity = 1)
                }.asSharedFlow()
            }
        }
        return merge(*flows.toTypedArray())
    }

    /**
     * Creates a reactive Flow that re-executes a query whenever its affected tables change.
     * This is used by generated listen() functions.
     *
     * @param affectedTables Set of table names that affect this query
     * @param queryExecutor Suspend function that executes the query and returns the result
     * @return Flow that emits query results whenever the data changes
     */
    protected fun <T> createReactiveQueryFlow(
        affectedTables: Set<String>,
        queryExecutor: suspend () -> T
    ): Flow<T> = flow {
        enableTableChangeNotifications()
        // Emit initial result
        emit(queryExecutor())

        // Listen for table changes and re-execute query
        createTableChangeFlow(affectedTables).collect {
            emit(queryExecutor())
        }
    }
}

interface DatabaseMigrations {
    /**
     * Applies the necessary migrations to the database.
     *
     * @param conn The database connection
     * @param currentVersion The current version of the database
     * @return The new version of the database
     */
    suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int
}

private suspend fun SafeSQLiteConnection.readUserVersion(): Int {
    val statement = prepare("PRAGMA user_version;")
    statement.use {
        it.step()
        return it.getLong(0).toInt()
    }
}

private suspend fun SafeSQLiteConnection.hasUserTables(): Boolean {
    val statement = prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1",
    )
    statement.use {
        return it.step()
    }
}
