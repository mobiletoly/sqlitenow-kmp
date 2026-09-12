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

import androidx.sqlite.async.step
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
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.transform
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.concurrent.Volatile

/**
 * Base class for generated database classes.
 *
 * The generated types build on this implementation across every supported Kotlin target:
 * Android/JVM, iOS/Native, macOS/Native, Linux/Native, JavaScript, and Kotlin/Wasm.
 * Browser targets use SQLiteNow's packaged direct-OPFS worker by default, JS Node uses a
 * transient in-memory worker, and non-web targets retain the bundled SQLite driver.
 */
@OptIn(SqliteNowInternalApi::class)
open class SqliteNowDatabase private constructor(
    private val dbName: String,
    private val migration: DatabaseMigrations,
    private val debug: Boolean,
    private val connectionProvider: SqliteConnectionProvider,
    private val adjustLogger: Boolean,
    private val syncManaged: Boolean,
) {
    @Volatile
    private var _conn: SafeSQLiteConnection? = null
    private val conn: SafeSQLiteConnection
        get() = _conn?.takeUnless { it.isPublicAccessRevoked }
            ?: error("Database connection not initialized or already closed. Call open() first.")

    private val openCloseMutex = Mutex()

    @Volatile
    private var unpublishedSession: SqliteNowUnpublishedSession? = null

    @Volatile
    private var unpublishedToken: SqliteNowUnpublishedSessionToken? = null

    @Volatile
    private var unpublishedConnection: SafeSQLiteConnection? = null

    @Volatile
    private var unpublishedProviderSession: SqliteNowCoordinatedProviderSession? = null

    @Volatile
    private var lifecycleLease: SqliteNowLifecycleLease? = null

    @Volatile
    private var pendingPublishedCloseHandler: (suspend () -> Unit)? = null

    @Volatile
    private var publishedCloseHandler: (suspend () -> Unit)? = null

    @Volatile
    private var publishedCloseToken: SqliteNowUnpublishedSessionToken? = null

    var connectionConfig: SqliteConnectionConfig = SqliteConnectionConfig(
        persistence = sqliteDefaultPersistence(dbName),
    )
        set(value) {
            if (_conn != null || unpublishedSession != null || openCloseMutex.isLocked) {
                throw IllegalStateException("Cannot update connectionConfig after open() has started")
            }
            field = value
        }

    // Table change notification system
    private val tableChanges = MutableSharedFlow<Set<String>>(
        replay = 0,
        extraBufferCapacity = 1,
    )
    @Volatile
    private var tableChangeScope: CoroutineScope? = null

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
        syncManaged = false,
    )

    constructor(
        dbName: String,
        migration: DatabaseMigrations,
        connectionProvider: SqliteConnectionProvider = BundledSqliteConnectionProvider,
        syncManaged: Boolean,
    ) : this(
        dbName = dbName,
        migration = migration,
        debug = false,
        connectionProvider = connectionProvider,
        adjustLogger = false,
        syncManaged = syncManaged,
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
        syncManaged = false,
    )

    constructor(
        dbName: String,
        migration: DatabaseMigrations,
        debug: Boolean,
        connectionProvider: SqliteConnectionProvider = BundledSqliteConnectionProvider,
        syncManaged: Boolean,
    ) : this(
        dbName = dbName,
        migration = migration,
        debug = debug,
        connectionProvider = connectionProvider,
        adjustLogger = true,
        syncManaged = syncManaged,
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
            if (unpublishedSession != null) {
                throw IllegalStateException("Database is reserved by a coordinated opening session")
            }
            if (_conn != null) {
                throw IllegalStateException("Database connection already initialized")
            }
            if (lifecycleLease != null) {
                throw IllegalStateException("Database cleanup is still pending")
            }
            if (syncManaged) {
                throw SqliteNowSyncOpenRequiredException(
                    "This sync-managed database requires openForSync(); ordinary open() is disabled",
                )
            }

            val c = connectionProvider.openConnection(dbName, debug, connectionConfig)
            try {
                requireOrdinaryOpenAllowed(c)
                preInit(c)
                ensureBootstrapUserVersion(c)

                c.transaction {
                    val currentVersion = c.readUserVersion()
                    val newVersion = migration.applyMigration(c, currentVersion)
                    if (newVersion != currentVersion) {
                        c.execSQL("PRAGMA user_version = $newVersion;")
                    }
                }

                c.setTableInvalidationListener(::notifyTablesChanged)
                tableChangeScope = createNotificationScope()

                // Publish only after successful initialization/migrations
                _conn = c
            } catch (t: Throwable) {
                c.close()
                throw t
            }
        }
    }

    /**
     * Reserves this database object for coordinated opening without opening or publishing SQLite.
     *
     * This is a cross-module runtime integration surface. Applications should use the generated
     * Oversqlite opening facade rather than driving the unpublished session directly.
     */
    @SqliteNowInternalApi
    suspend fun reserveUnpublishedSession(): SqliteNowUnpublishedSession = openCloseMutex.withLock {
        check(_conn == null) { "Database connection already initialized" }
        check(unpublishedSession == null) { "Database is already reserved by a coordinated opening session" }
        check(lifecycleLease == null) { "Database cleanup is still pending" }

        val token = SqliteNowUnpublishedSessionToken()
        SqliteNowUnpublishedSession(
            database = this,
            token = token,
            migrationTargetVersion = migration.targetVersion,
        ).also { session ->
            unpublishedToken = token
            unpublishedSession = session
        }
    }

    /**
     * Gets the current user_version from the database.
     */
    internal suspend fun getUserVersion(): Int {
        val connection = conn
        return connection.withExclusiveAccess {
            val statement = connection.prepare("PRAGMA user_version;")
            try {
                statement.step()
                statement.getLong(0).toInt()
            } finally {
                statement.close()
            }
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
        conn // Reject new observer admission after owner revocation.
        this.enableTableChangeNotifications = true
    }

    /**
     * Closes the database connection.
     */
    suspend fun close() {
        requireSqliteNowCoordinatedCloseAllowed()
        beginApplicationClose()
        val (reservedSession, coordinatedClose) = openCloseMutex.withLock {
            unpublishedSession to (publishedCloseHandler ?: pendingPublishedCloseHandler)
        }
        if (coordinatedClose != null) {
            coordinatedClose()
            return
        }
        if (reservedSession != null) {
            reservedSession.close()
            return
        }

        openCloseMutex.withLock {
            closeConnectionAndLease()
        }
    }

    private suspend fun beginApplicationClose() {
        (_conn ?: unpublishedConnection)?.beginApplicationClose()
    }

    /**
     * Check if database is open.
     */
    fun isOpen(): Boolean = _conn?.isPublicAccessRevoked == false

    /**
     * Requests that an external persistence layer store the current database snapshot.
     *
     * The JS/Wasm direct worker performs no whole-database snapshot exports, so this method is a
     * documented no-op there. File-backed targets persist ordinary writes in place. The method is
     * retained for source compatibility and for any non-web persistence implementation that
     * supports an explicit flush.
     */
    suspend fun persistSnapshotNow() {
        conn.persistSnapshotNow()
    }

    /**
     * Reports out-of-band table changes so reactive queries can re-run.
     *
     * This is intended for external writers that mutate tables outside generated SQLiteNow execute
     * helpers but still want SQLiteNow's reactive flows to observe those changes.
     *
     * @throws IllegalStateException if the database is not open
     */
    fun reportExternalTableChanges(affectedTables: Set<String>) {
        conn.reportExternalTableChanges(affectedTables)
    }

    private suspend fun ensureBootstrapUserVersion(connection: SafeSQLiteConnection) {
        val currentVersion = connection.readUserVersion()
        if (currentVersion != 0) return
        if (connection.hasUserTables() || connection.restoredFromSnapshot) return
        connection.execSQL("PRAGMA user_version = -1;")
    }

    private suspend fun requireOrdinaryOpenAllowed(connection: SafeSQLiteConnection) {
        if (syncManaged || connection.hasCoordinatedOpeningIdentity()) {
            throw SqliteNowSyncOpenRequiredException(
                if (syncManaged) {
                    "This sync-managed database requires openForSync(); ordinary open() is disabled"
                } else {
                    "This database contains SQLiteNow sync identity and requires openForSync()"
                },
            )
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

        tableChangeScope?.launch {
            tableChanges.emit(affectedTables.mapTo(linkedSetOf()) { it.lowercase() })
        }
    }

    /**
     * Creates a Flow that emits when any of the specified tables change.
     * This is used internally by generated listen() functions.
     *
     * @param affectedTables Set of table names to listen for changes
     * @return Flow that emits Unit when any of the tables change
     */
    protected suspend fun createTableChangeFlow(affectedTables: Set<String>): Flow<Unit> = flow {
        conn // A captured lazy flow must recheck readiness when collection begins.
        if (affectedTables.isEmpty()) return@flow
        val normalizedTables = affectedTables.mapTo(linkedSetOf()) { it.lowercase() }
        tableChanges
            .filter { changedTables -> changedTables.any(normalizedTables::contains) }
            .collect { emit(Unit) }
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

    private fun createNotificationScope(): CoroutineScope =
        CoroutineScope(SupervisorJob() + Dispatchers.Default)

    internal suspend fun acquireUnpublishedConnection(token: SqliteNowUnpublishedSessionToken) {
        openCloseMutex.withLock {
            requireUnpublishedToken(token)
            check(unpublishedConnection == null) { "Unpublished connection already acquired" }
            check(unpublishedProviderSession == null) { "Unpublished provider session already acquired" }
            check(lifecycleLease == null) { "Lifecycle lease already acquired" }
            val lease = acquireSqliteNowLifecycleLease(dbName)
            try {
                val providerSession = connectionProvider.openCoordinatedSession(
                    dbName = dbName,
                    debug = debug,
                    config = connectionConfig,
                )
                unpublishedProviderSession = providerSession
                unpublishedConnection = providerSession.inspectionConnection
                lifecycleLease = lease
            } catch (t: Throwable) {
                try {
                    lease.close()
                } catch (closeFailure: Throwable) {
                    t.addSuppressed(closeFailure)
                }
                throw t
            }
        }
    }

    internal fun unpublishedPreparationRequired(token: SqliteNowUnpublishedSessionToken): Boolean {
        requireUnpublishedToken(token)
        return unpublishedProviderSession?.preparationRequired
            ?: error("Unpublished provider session has not been acquired")
    }

    internal fun unpublishedInspectionAvailable(token: SqliteNowUnpublishedSessionToken): Boolean {
        requireUnpublishedToken(token)
        return unpublishedConnection != null
    }

    internal suspend fun prepareUnpublishedConnection(token: SqliteNowUnpublishedSessionToken) {
        openCloseMutex.withLock {
            requireUnpublishedToken(token)
            val providerSession = unpublishedProviderSession
                ?: error("Unpublished provider session has not been acquired")
            unpublishedConnection = providerSession.prepareConnection()
        }
    }

    internal suspend fun <T> withUnpublishedConnection(
        token: SqliteNowUnpublishedSessionToken,
        block: suspend (SafeSQLiteConnection) -> T,
    ): T {
        requireUnpublishedToken(token)
        val connection = unpublishedConnection
            ?: error("Unpublished connection has not been acquired")
        return block(connection)
    }

    internal suspend fun migrateUnpublishedConnection(
        token: SqliteNowUnpublishedSessionToken,
        fresh: Boolean,
        migrationSqlPolicy: SqliteNowCoordinatedMigrationSqlPolicy?,
        preInit: suspend (SafeSQLiteConnection) -> Unit,
        transactionSetup: suspend (SafeSQLiteConnection) -> Unit,
        validateBeforeCommit: suspend (SafeSQLiteConnection) -> Unit,
    ) {
        requireUnpublishedToken(token)
        val connection = unpublishedConnection
            ?: error("Unpublished connection has not been acquired")

        preInit(connection)
        connection.transaction {
            transactionSetup(connection)
            val storedVersion = connection.readUserVersion()
            val migrationVersion = if (
                fresh || (storedVersion == 0 &&
                !connection.hasUserTables() &&
                !connection.restoredFromSnapshot)
            ) {
                -1
            } else {
                storedVersion
            }
            val newVersion = if (migrationVersion == migration.targetVersion) storedVersion else {
                connection.withMigrationSqlValidation(migrationSqlPolicy) {
                    migration.applyMigration(connection, migrationVersion)
                }
            }
            if (newVersion != storedVersion) {
                connection.execSQL("PRAGMA user_version = $newVersion;")
            }
            validateBeforeCommit(connection)
        }
    }

    internal suspend fun publishUnpublishedConnection(token: SqliteNowUnpublishedSessionToken) {
        openCloseMutex.withLock {
            requireUnpublishedToken(token)
            check(_conn == null) { "Database connection already initialized" }
            val connection = unpublishedConnection
                ?: error("Unpublished connection has not been acquired")
            val closeHandler = pendingPublishedCloseHandler
                ?: suspend { closePublishedConnection(token) }

            unpublishedProviderSession?.publish(connection)
            connection.setTableInvalidationListener(::notifyTablesChanged)
            tableChangeScope = createNotificationScope()
            _conn = connection
            publishedCloseHandler = closeHandler
            publishedCloseToken = token
            pendingPublishedCloseHandler = null
            unpublishedProviderSession = null
            unpublishedConnection = null
            unpublishedToken = null
            unpublishedSession = null
        }
    }

    internal suspend fun bindPublishedCloseHandler(
        token: SqliteNowUnpublishedSessionToken,
        closeHandler: suspend () -> Unit,
    ) {
        openCloseMutex.withLock {
            requireUnpublishedToken(token)
            check(pendingPublishedCloseHandler == null) {
                "Coordinated publication close handler is already installed"
            }
            pendingPublishedCloseHandler = closeHandler
        }
    }

    internal suspend fun closePublishedConnection(token: SqliteNowUnpublishedSessionToken) {
        revokeConnectionAccess()
        openCloseMutex.withLock {
            check(publishedCloseToken === token && publishedCloseHandler != null) {
                "Coordinated published connection is no longer owned by this session"
            }
            closeConnectionAndLease()
        }
    }

    internal suspend fun closeUnpublishedConnection(token: SqliteNowUnpublishedSessionToken) {
        revokeConnectionAccess()
        openCloseMutex.withLock {
            requireUnpublishedToken(token)
            val connection = unpublishedConnection
            val providerSession = unpublishedProviderSession
            val lease = lifecycleLease
            var failure: Throwable? = null
            try {
                if (providerSession != null) providerSession.close() else connection?.close()
            } catch (t: Throwable) {
                failure = t
            }
            if (failure == null) {
                try {
                    lease?.close()
                } catch (t: Throwable) {
                    failure = t
                }
            }
            if (failure != null) throw failure

            unpublishedConnection = null
            unpublishedProviderSession = null
            unpublishedToken = null
            unpublishedSession = null
            pendingPublishedCloseHandler = null
            lifecycleLease = null
        }
    }

    private suspend fun closeConnectionAndLease() {
        val connection = _conn
        val lease = lifecycleLease
        var failure: Throwable? = null
        if (connection != null) {
            connection.setTableInvalidationListener(null)
            try {
                connection.close()
            } catch (t: Throwable) {
                failure = t
            }
            if (failure == null) {
                _conn = null
            }
        }
        if (failure == null) {
            try {
                lease?.close()
            } catch (t: Throwable) {
                failure = t
            }
            if (failure == null) {
                lifecycleLease = null
            }
        }
        if (failure == null && connection == null && lease == null) {
            tableChangeScope?.cancel()
            tableChangeScope = null
            publishedCloseHandler = null
            publishedCloseToken = null
        } else if (failure == null && _conn == null && lifecycleLease == null) {
            tableChangeScope?.cancel()
            tableChangeScope = null
            publishedCloseHandler = null
            publishedCloseToken = null
        }
        failure?.let { throw it }
    }

    @OptIn(SqliteNowInternalApi::class)
    internal fun revokeConnectionAccess() {
        _conn?.revokePublicAccess()
        unpublishedConnection?.revokePublicAccess()
    }

    internal fun revokeSessionAccess(token: SqliteNowUnpublishedSessionToken) {
        if (unpublishedToken === token || publishedCloseToken === token) revokeConnectionAccess()
    }

    private fun requireUnpublishedToken(token: SqliteNowUnpublishedSessionToken) {
        check(unpublishedToken === token && unpublishedSession != null) {
            "Coordinated opening session is no longer active"
        }
    }
}

private fun combineCloseFailures(primary: Throwable?, later: Throwable): Throwable {
    if (primary == null) return later
    primary.addSuppressed(later)
    return primary
}

interface DatabaseMigrations {
    /** Immutable generated schema target, or null for a legacy handwritten migration runner. */
    val targetVersion: Int?
        get() = null

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
        // Android's bundled SQLite driver may create `android_metadata` automatically on first open.
        // Treat it as an internal table so bootstrap/migrations still run for a fresh DB.
        "SELECT name FROM sqlite_master " +
            "WHERE type='table' " +
            "AND name NOT LIKE 'sqlite_%' " +
            "AND name != 'android_metadata' " +
            "LIMIT 1",
    )
    statement.use {
        return it.step()
    }
}

private suspend fun SafeSQLiteConnection.hasCoordinatedOpeningIdentity(): Boolean {
    val statement = prepare(
        "SELECT 1 FROM sqlite_schema " +
            "WHERE lower(name) IN (" +
            "'_sync_opening_state','_sync_contract_identity','_sync_admitted_contracts') LIMIT 1",
    )
    statement.use {
        return it.step()
    }
}
