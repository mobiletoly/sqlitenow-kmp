package dev.goquick.sqlitenow.core

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.sqlite.driver.bundled.SQLITE_OPEN_CREATE
import androidx.sqlite.driver.bundled.SQLITE_OPEN_READWRITE
import androidx.sqlite.execSQL
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.withContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.merge

/**
 * Base class for generated database classes.
 */
open class SqliteNowDatabase {
    private val dbName: String
    private val migration: DatabaseMigrations
    private lateinit var conn: SQLiteConnection

    // Table change notification system
    private val tableChangeFlows = mutableMapOf<String, MutableSharedFlow<Unit>>()

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
     * @return The database connection
     * @throws IllegalStateException if the database is not open
     */
    fun connection(): SQLiteConnection {
        if (!::conn.isInitialized) {
            throw IllegalStateException("Database connection not initialized. Call open() first.")
        }
        return conn
    }

    /**
     * Opens a database connection and applies migrations.
     */
    suspend fun open() {
        if (::conn.isInitialized) {
            throw IllegalStateException("Database connection already initialized")
        }

        withContext(Dispatchers.IO) {
            val dbFileExists = validateFileExists(dbName)

            conn = BundledSQLiteDriver().open(
                fileName = dbName,
                flags = SQLITE_OPEN_CREATE or SQLITE_OPEN_READWRITE
            )
            transaction {
                conn.execSQL("""PRAGMA foreign_keys = ON;""")
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
    }

    /**
     * Gets the current user_version from the database.
     */
    internal fun getUserVersion(): Int {
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
    internal fun setUserVersion(version: Int) {
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
    suspend fun <T> transaction(block: suspend () -> T): T = withContext(Dispatchers.IO) {
        if (!::conn.isInitialized) {
            throw IllegalStateException("Database connection not initialized. Call open() first.")
        }

        conn.execSQL("BEGIN TRANSACTION;")

        try {
            val result = block()
            conn.execSQL("COMMIT;")
            result
        } catch (e: Exception) {
            // Roll back the transaction in case of an exception
            try {
                conn.execSQL("ROLLBACK;")
            } catch (rollbackException: Exception) {
                e.addSuppressed(rollbackException)
            }
            throw e
        }
    }

    /**
     * Closes the database connection.
     */
    fun close() {
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
        affectedTables.forEach { tableName ->
            tableChangeFlows[tableName]?.tryEmit(Unit)
        }
    }

    /**
     * Creates a Flow that emits when any of the specified tables change.
     * This is used internally by generated listen() functions.
     *
     * @param affectedTables Set of table names to listen for changes
     * @return Flow that emits Unit when any of the tables change
     */
    protected fun createTableChangeFlow(affectedTables: Set<String>): Flow<Unit> {
        if (affectedTables.isEmpty()) {
            return flow { } // Empty flow for queries with no affected tables
        }

        // Get or create SharedFlows for each table
        val flows = affectedTables.map { tableName ->
            tableChangeFlows.getOrPut(tableName) {
                MutableSharedFlow<Unit>(replay = 0, extraBufferCapacity = 1)
            }.asSharedFlow()
        }

        // Merge all table change flows into one
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
    fun applyMigration(conn: SQLiteConnection, currentVersion: Int): Int
}
