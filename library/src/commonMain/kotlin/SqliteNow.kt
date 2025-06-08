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

open class SqliteNowDatabase {
    private val dbName: String
    private val migration: DatabaseMigrations
    private lateinit var conn: SQLiteConnection

    // Table change notification system
    private val tableChangeFlows = mutableMapOf<String, MutableSharedFlow<Unit>>()

    constructor(dbName: String, migration: DatabaseMigrations) {
        this.dbName = dbName
        this.migration = migration
    }

    fun connection(): SQLiteConnection {
        if (!::conn.isInitialized) {
            throw IllegalStateException("Database connection not initialized. Call open() first.")
        }
        return conn
    }

    /**
     * Opens a database connection and applies migrations on a background dispatcher.
     */
    suspend fun open() {
        withContext(Dispatchers.Default) {
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
     * Gets the current user_version from the database using a background dispatcher.
     *
     * @return The current user_version as an Int
     */
    suspend fun getUserVersion(): Int = withContext(Dispatchers.IO) {
        if (!::conn.isInitialized) {
            throw IllegalStateException("Database connection not initialized. Call open() first.")
        }

        val statement = conn.prepare("PRAGMA user_version;")
        try {
            statement.step()
            statement.getInt(0)
        } finally {
            statement.close()
        }
    }

    /**
     * Sets the user_version in the database.
     *
     * @param version The version number to set
     */
    fun setUserVersion(version: Int) {
        if (!::conn.isInitialized) {
            throw IllegalStateException("Database connection not initialized. Call open() first.")
        }

        conn.execSQL("PRAGMA user_version = $version;")
    }

    /**
     * Sets the user_version in the database using a background dispatcher.
     *
     * @param version The version number to set
     */
    suspend fun setUserVersionSuspended(version: Int) = withContext(Dispatchers.IO) {
        setUserVersion(version)
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

        // Begin transaction
        conn.execSQL("BEGIN TRANSACTION;")

        try {
            // Execute the block
            val result = block()

            // Commit the transaction
            conn.execSQL("COMMIT;")

            // Return the result
            result
        } catch (e: Exception) {
            // Roll back the transaction in case of an exception
            try {
                conn.execSQL("ROLLBACK;")
            } catch (rollbackException: Exception) {
                // If rollback fails, add it as a suppressed exception
                e.addSuppressed(rollbackException)
            }

            // Re-throw the original exception
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
