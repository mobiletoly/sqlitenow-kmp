package dev.goquick.sqlitenow.core

import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.sqlite.driver.bundled.SQLITE_OPEN_CREATE
import androidx.sqlite.driver.bundled.SQLITE_OPEN_READWRITE
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
open class SqliteNowDatabase {
    private val dbName: String
    private val migration: DatabaseMigrations
    private lateinit var conn: SafeSQLiteConnection

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
    constructor(dbName: String, migration: DatabaseMigrations) {
        this.dbName = dbName
        this.migration = migration
    }

    /**
     * Optional constructor that allows enabling verbose debug logging.
     * When debug is true, increases logger verbosity to Debug; otherwise uses default Info level.
     */
    constructor(dbName: String, migration: DatabaseMigrations, debug: Boolean) : this(dbName, migration) {
        sqliteNowLogger = if (originalSqliteNowLogger == sqliteNowLogger) {
            SqliteNowLogger(if (debug) Severity.Debug else Severity.Info)
        } else {
            sqliteNowLogger
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

        val realConn = BundledSQLiteDriver().open(
            fileName = dbName,
            flags = SQLITE_OPEN_CREATE or SQLITE_OPEN_READWRITE
        )
        conn = SafeSQLiteConnection(realConn)
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
            statement.getInt(0)
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
