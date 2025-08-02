package dev.goquick.sqlitenow.core

import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.sqlite.driver.bundled.SQLITE_OPEN_CREATE
import androidx.sqlite.driver.bundled.SQLITE_OPEN_READWRITE
import androidx.sqlite.execSQL
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext

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
    fun connection(): SafeSQLiteConnection {
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

        val dbFileExists = validateFileExists(dbName)

        // Create driver with session extension
        val driver = BundledSQLiteDriver().apply {
            addExtension("libnowsession_ext")
        }

        val realConn = driver.open(
            fileName = dbName,
            flags = SQLITE_OPEN_CREATE or SQLITE_OPEN_READWRITE
        )
        println("-------------- step 0005")
        conn = SafeSQLiteConnection(realConn)
        conn.ref.execSQL("""PRAGMA foreign_keys = ON;""")

        // Load session extension
        loadSessionExtension()

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
    internal fun getUserVersion(): Int {
        if (!::conn.isInitialized) {
            throw IllegalStateException("Database connection not initialized. Call open() first.")
        }

        val statement = conn.ref.prepare("PRAGMA user_version;")
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

        withContext(conn.dispatcher) {
            conn.ref.execSQL("PRAGMA user_version = $version;")
        }
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
    suspend fun <T> transaction(block: suspend () -> T): T {
        if (!::conn.isInitialized) {
            throw IllegalStateException("Database connection not initialized. Call open() first.")
        }

        // Use single-threaded dispatcher to avoid deadlock
        return withContext(conn.dispatcher) {
            conn.ref.execSQL("BEGIN TRANSACTION;")

            try {
                val result = block()
                conn.ref.execSQL("COMMIT;")
                result
            } catch (e: Exception) {
                // Roll back the transaction in case of an exception
                try {
                    conn.ref.execSQL("ROLLBACK;")
                } catch (rollbackException: Exception) {
                    e.addSuppressed(rollbackException)
                }
                throw e
            }
        }
    }

    /**
     * Closes the database connection.
     */
    suspend fun close() {
        if (::conn.isInitialized) {
            withContext(conn.dispatcher) {
                conn.ref.close()
            }
        }
    }

    /**
     * Loads the SQLite session extension using the official addExtension method.
     * This uses the new addExtension API from androidx.sqlite bundled 2.6.0-beta01.
     * On iOS, session extension loading may not be available due to platform restrictions.
     */
    private suspend fun loadSessionExtension() {
        try {
            withContext(conn.dispatcher) {
                inspectSessionFunctions()

                println("🔄 Testing session extension with sqlite3_create_function...")

                // First, try to call the test function to see if it was registered successfully
                try {
                    val testStatement = conn.ref.prepare("SELECT nowsession_ping(1234)")
                    try {
                        testStatement.step()
                        val result = testStatement.getInt(0)
                        if (result == 1234) {
                            println("✅ sqlite3_create_function worked! Test function returned: $result")
                        } else {
                            println("⚠️ Test function returned unexpected result: $result")
                        }
                    } finally {
                        testStatement.close()
                    }
                } catch (e: Exception) {
                    println("❌ Test function call failed: ${e.message}")
                    println("   This means sqlite3_create_function likely crashed during extension init")
                }

                println("✅ Session extension testing completed!")
            }
        } catch (e: Exception) {
            println("⚠️ Session extension not available: ${e.message}")
            println("   Database will work normally but session functionality will not be available")
        }
    }

    /**
     * Inspects the database for session-related functions and tables.
     */
    private fun inspectSessionFunctions() {
        try {
            println("🧪 Testing direct session function calls...")

            try {
                val sessionFunctionsStatement = conn.ref.prepare("""
                    SELECT name, builtin, type, enc, narg, flags
                    FROM pragma_function_list()
                    WHERE name LIKE '%session%'
                    ORDER BY name
                """)

                var sessionFunctionCount = 0
                try {
                    while (sessionFunctionsStatement.step()) {
                        val name = sessionFunctionsStatement.getText(0)
                        val builtin = sessionFunctionsStatement.getLong(1)
                        val type = sessionFunctionsStatement.getText(2)
                        val enc = sessionFunctionsStatement.getText(3)
                        val narg = sessionFunctionsStatement.getLong(4)
                        val flags = sessionFunctionsStatement.getLong(5)

                        sessionFunctionCount++
                        println("✅ Session function found: '$name'")
                        println("   - builtin: $builtin, type: $type, enc: $enc, narg: $narg, flags: $flags")
                    }
                } finally {
                    sessionFunctionsStatement.close()
                }

                if (sessionFunctionCount == 0) {
                    println("❌ No functions containing 'session' found in pragma_function_list")
                } else {
                    println("📊 Total session-related functions found: $sessionFunctionCount")
                }

            } catch (e: Exception) {
                println("❌ Error querying pragma_function_list for session functions: ${e.message}")
            }

            // Create a test table
            conn.ref.execSQL("""
                CREATE TEMP TABLE IF NOT EXISTS direct_session_test (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    value INTEGER
                )
            """)

            // Insert some data
            conn.ref.execSQL("INSERT INTO direct_session_test (name, value) VALUES ('test1', 100)")
            conn.ref.execSQL("INSERT INTO direct_session_test (name, value) VALUES ('test2', 200)")

            // Update data
            conn.ref.execSQL("UPDATE direct_session_test SET value = 150 WHERE name = 'test1'")

            // Delete data
            conn.ref.execSQL("DELETE FROM direct_session_test WHERE name = 'test2'")

            // Check final state
            val countStatement = conn.ref.prepare("SELECT COUNT(*) FROM direct_session_test")
            try {
                countStatement.step()
                val count = countStatement.getLong(0)
                println("✅ Direct session test operations completed. Final row count: $count")
            } finally {
                countStatement.close()
            }

            // Clean up
            conn.ref.execSQL("DROP TABLE direct_session_test")

            println("✅ Direct session function testing completed!")

        } catch (e: Exception) {
            println("⚠️ Direct session function testing failed: ${e.message}")
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
    protected suspend fun createTableChangeFlow(affectedTables: Set<String>): Flow<Unit> {
        if (affectedTables.isEmpty()) {
            return flow { } // Empty flow for queries with no affected tables
        }

        // Get or create SharedFlows for each table
        val flows = tableChangesFlowMutex.withLock {
            affectedTables.map { tableName ->
                tableChangeFlows.getOrPut(tableName) {
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
