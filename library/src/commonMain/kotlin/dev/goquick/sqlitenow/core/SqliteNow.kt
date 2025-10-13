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

import co.touchlab.kermit.Severity
import dev.goquick.sqlitenow.common.SqliteNowLogger
import dev.goquick.sqlitenow.common.originalSqliteNowLogger
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.common.validateFileExists
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
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
 */
open class SqliteNowDatabase private constructor(
    private val dbName: String,
    private val migration: DatabaseMigrations,
    private val debug: Boolean,
    private val connectionProvider: SqliteConnectionProvider,
    private val adjustLogger: Boolean,
) {
    private lateinit var conn: SafeSQLiteConnection
    var connectionConfig: SqliteConnectionConfig = SqliteConnectionConfig()
        set(value) {
            if (::conn.isInitialized) {
                throw IllegalStateException("Cannot update connectionConfig after open()")
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
            sqliteNowLogger = if (originalSqliteNowLogger == sqliteNowLogger) {
                SqliteNowLogger(if (debug) Severity.Debug else Severity.Info)
            } else {
                sqliteNowLogger
            }
        }
    }

    /**
     * @return The database connection
     * @throws IllegalStateException if the database is not open
     */
    fun connection(): SafeSQLiteConnection {
        if (!::conn.isInitialized) {
            throw IllegalStateException("Database connection not initialized. Call open() first.")
        }
        return conn
    }

    /**
     * Opens a database connection and applies migrations.
     *
     * @param preInit Optional suspend function to run before migrations are applied.
     *  this will be executed outside of initialization/migration transaction, so it
     *  is useful for operation such as setting PRAGMAs that must be set before initialization
     *  or migration.
     */
    suspend fun open(preInit: suspend (conn: SafeSQLiteConnection) -> Unit = {}) {
        if (::conn.isInitialized) {
            throw IllegalStateException("Database connection already initialized")
        }

        val dbFileExists = validateFileExists(dbName)

        conn = connectionProvider.openConnection(dbName, debug, connectionConfig)
        preInit(conn)

        transaction {
            val currentVersion = if (!dbFileExists) {
                setUserVersion(-1)
                -1
            } else {
                getUserVersion()
            }

            // Apply migrations starting from the current version
            val newVersion = migration.applyMigration(conn, currentVersion)
            if (newVersion != currentVersion) {
                setUserVersion(newVersion)
            }
        }
    }

    /**
     * Gets the current user_version from the database.
     */
    internal suspend fun getUserVersion(): Int {
        if (!::conn.isInitialized) {
            throw IllegalStateException("Database connection not initialized. Call open() first.")
        }

        val statement = conn.prepare("PRAGMA user_version;")
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
        if (!::conn.isInitialized) {
            throw IllegalStateException("Database connection not initialized. Call open() first.")
        }

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
    suspend fun <T> transaction(mode: TransactionMode = TransactionMode.DEFERRED, block: suspend () -> T): T {
        if (!::conn.isInitialized) {
            throw IllegalStateException("Database connection not initialized. Call open() first.")
        }
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
    suspend fun close() {
        if (::conn.isInitialized) {
            conn.close()
        }
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
