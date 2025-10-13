package dev.goquick.sqlitenow.core

/**
 * Configuration applied when opening a database connection.
 *
 * Currently only used by the SQL.js (JS) implementation to enable client-side persistence,
 * but the contract lives in common code so callers can configure it from shared modules.
 */
data class SqliteConnectionConfig(
    val persistence: SqlitePersistence? = null,
    val autoFlushPersistence: Boolean = true,
)

/**
 * Provides a storage mechanism for persisting a SQLite database.
 *
 * Implementations are expected to load the database bytes (if they exist) before the connection
 * is opened and to persist updated bytes whenever requested by the runtime.
 */
interface SqlitePersistence {
    /**
     * Reads the previously persisted database bytes for [dbName], or null if none exist.
     */
    suspend fun load(dbName: String): ByteArray?

    /**
     * Persists the provided [bytes] for [dbName], overwriting any existing snapshot.
     */
    suspend fun persist(dbName: String, bytes: ByteArray)

    /**
     * Removes any stored database snapshot for [dbName].
     *
     * Optional to implement; default implementation is a no-op.
     */
    suspend fun clear(dbName: String) {}
}
