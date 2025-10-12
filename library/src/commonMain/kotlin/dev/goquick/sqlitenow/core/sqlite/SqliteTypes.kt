package dev.goquick.sqlitenow.core.sqlite

/**
 * Minimal cross-platform SQLite connection interface used by the SQLiteNow runtime
 * and generated code. Platform source sets provide actual implementations backed by
 * the corresponding native driver (androidx, SQL.js, etc).
 */
expect class SqliteConnection {
    fun execSQL(sql: String)
    fun prepare(sql: String): SqliteStatement
    fun inTransaction(): Boolean
    fun close()
}

/**
 * Minimal SQLite statement contract consumed by generated bindings.
 */
expect class SqliteStatement {
    fun bindBlob(index: Int, value: ByteArray)
    fun bindDouble(index: Int, value: Double)
    fun bindLong(index: Int, value: Long)
    fun bindText(index: Int, value: String)
    fun bindNull(index: Int)
    fun bindInt(index: Int, value: Int)

    fun getBlob(index: Int): ByteArray
    fun getDouble(index: Int): Double
    fun getLong(index: Int): Long
    fun getText(index: Int): String
    fun isNull(index: Int): Boolean
    fun getInt(index: Int): Int

    fun getColumnCount(): Int
    fun getColumnName(index: Int): String

    fun step(): Boolean
    fun reset()
    fun clearBindings()
    fun close()
}

/**
 * Convenience helper mirroring the androidx API.
 */
fun SqliteStatement.getColumnNames(): List<String> {
    val count = getColumnCount()
    if (count <= 0) return emptyList()
    return (0 until count).map { getColumnName(it) }
}


class SqliteException(message: String? = null, cause: Throwable? = null) : RuntimeException(message, cause)

inline fun <T> SqliteStatement.use(block: (SqliteStatement) -> T): T {
    var exception: Throwable? = null
    try {
        return block(this)
    } catch (t: Throwable) {
        exception = t
        throw t
    } finally {
        try {
            close()
        } catch (closeError: Throwable) {
            if (exception == null) throw closeError
        }
    }
}
