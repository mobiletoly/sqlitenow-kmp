package dev.goquick.sqlitenow.core.sqlite

import androidx.sqlite.SQLITE_DATA_BLOB
import androidx.sqlite.SQLITE_DATA_FLOAT
import androidx.sqlite.SQLITE_DATA_INTEGER
import androidx.sqlite.SQLITE_DATA_NULL
import androidx.sqlite.SQLITE_DATA_TEXT
import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import kotlin.js.jsTypeOf
import kotlin.js.unsafeCast
import org.khronos.webgl.Uint8Array

internal class SqlJsSQLiteConnection(
    private val database: SqlJsDatabase,
) : SQLiteConnection {
    private val transactionState = SqliteTransactionState()
    private var exportedBytesCache: ByteArray? = null

    override suspend fun prepare(sql: String): SQLiteStatement = wrapSqlite {
        prepareSqlJsStatement(
            prepare = { database.prepare(sql) },
            normalize = SqlJsStatement::getNormalizedSQL,
            closeOnFailure = SqlJsStatement::free,
        ) { statement, normalizedSql ->
            SqlJsSQLiteStatement(this, normalizedSql, statement)
        }
    }

    internal fun execSQL(sql: String) {
        invalidateExportCache()
        wrapSqlite {
            database.executeBatch(sql, transactionState::observeSuccessfulStatement)
        }
    }

    override fun inTransaction(): Boolean = transactionState.inTransaction()

    override fun close() {
        wrapSqlite { database.close() }
        transactionState.reset()
        exportedBytesCache = null
    }

    internal fun exportToByteArray(): ByteArray {
        val cached = exportedBytesCache
        if (cached != null) return cached
        val bytes = wrapSqlite {
            val dyn = database.asDynamic()
            val exported = when {
                dyn.export != undefined -> dyn.export()
                else -> throw SqliteException("SQL.js database does not support export()")
            }
            (exported as Uint8Array).toByteArray()
        }
        exportedBytesCache = bytes
        return bytes
    }

    internal fun invalidateExportCache() {
        exportedBytesCache = null
    }

    internal fun observeStatementExecution(normalizedSql: String) {
        transactionState.observeSuccessfulStatement(normalizedSql)
    }
}

private class SqlJsSQLiteStatement(
    private val parent: SqlJsSQLiteConnection,
    private val normalizedSql: String,
    private val statement: SqlJsStatement,
) : SQLiteStatement {
    private companion object {
        private const val JS_SAFE_INTEGER_MAX = 9_007_199_254_740_991L
        private const val JS_SAFE_INTEGER_MIN = -9_007_199_254_740_991L
        private val useBigIntConfig = js("({ useBigInt: true })")
    }

    private val boundValues = mutableMapOf<Int, dynamic>()
    private var bindingsDirty = false
    private var executionObserved = false
    private var currentRow: Array<dynamic>? = null

    private fun setBinding(index: Int, value: dynamic) {
        boundValues[index] = value
        bindingsDirty = true
        currentRow = null
        parent.invalidateExportCache()
    }

    private fun applyBindingsIfNecessary() {
        if (!bindingsDirty) return
        val maxIndex = boundValues.keys.maxOrNull() ?: 0
        val values = arrayOfNulls<Any?>(maxIndex)
        for (i in 0 until maxIndex) {
            values[i] = boundValues[i + 1]
        }
        wrapSqlite { statement.bind(values.unsafeCast<Array<dynamic>>()) }
        bindingsDirty = false
        executionObserved = false
        parent.invalidateExportCache()
    }

    override fun bindBlob(index: Int, value: ByteArray) = setBinding(index, value.toUint8Array())

    override fun bindDouble(index: Int, value: Double) = setBinding(index, value)

    override fun bindLong(index: Int, value: Long) {
        when {
            value in Int.MIN_VALUE..Int.MAX_VALUE -> setBinding(index, value.toInt())
            value in JS_SAFE_INTEGER_MIN..JS_SAFE_INTEGER_MAX -> setBinding(index, value.toDouble())
            else -> setBinding(index, value.toString())
        }
    }

    override fun bindText(index: Int, value: String) = setBinding(index, value)

    override fun bindNull(index: Int) = setBinding(index, null)

    private fun fetchValue(index: Int): dynamic {
        val row = currentRow ?: throw SqliteException("No active row. Call step() before reading columns.")
        if (index < 0 || index >= row.size) {
            throw SqliteException("Column $index out of bounds (size=${row.size})")
        }
        return row[index]
    }

    override fun getBlob(index: Int): ByteArray {
        val value = fetchValue(index)
        if (value == null) throw SqliteException("Column $index is NULL")
        return when (value) {
            is Uint8Array -> value.unsafeCast<Uint8Array>().toByteArray()
            else -> throw SqliteException("Column $index is not a blob")
        }
    }

    override fun getDouble(index: Int): Double {
        val value = fetchValue(index) ?: throw SqliteException("Column $index is NULL")
        return when {
            jsTypeOf(value) == "bigint" -> value.toString().toDouble()
            value is Double -> value
            value is Int -> value.toDouble()
            value is Number -> value.toDouble()
            value is String -> value.toDouble()
            else -> throw SqliteException("Column $index cannot convert to Double (type=${jsTypeOf(value)})")
        }
    }

    override fun getLong(index: Int): Long {
        val value = fetchValue(index) ?: throw SqliteException("Column $index is NULL")
        return when {
            jsTypeOf(value) == "bigint" -> value.toString().toLong()
            value is Double -> value.toLong()
            value is Int -> value.toLong()
            value is Number -> value.toDouble().toLong()
            value is String -> value.toLong()
            else -> throw SqliteException("Column $index cannot convert to Long (type=${jsTypeOf(value)})")
        }
    }

    override fun getText(index: Int): String {
        return when (val value = fetchValue(index)) {
            null -> throw SqliteException("Column $index is NULL")
            is String -> value
            else -> value.toString()
        }
    }

    override fun isNull(index: Int): Boolean = fetchValue(index) == null

    override fun getColumnCount(): Int = wrapSqlite { statement.columnCount() }

    override fun getColumnName(index: Int): String = wrapSqlite { statement.columnName(index) }

    override fun getColumnType(index: Int): Int {
        val value = fetchValue(index)
        return when {
            value == null -> SQLITE_DATA_NULL
            value is Uint8Array -> SQLITE_DATA_BLOB
            jsTypeOf(value) == "bigint" -> SQLITE_DATA_INTEGER
            jsTypeOf(value) == "number" -> SQLITE_DATA_FLOAT
            jsTypeOf(value) == "string" -> SQLITE_DATA_TEXT
            else -> throw SqliteException(
                "Column $index has unsupported SQL.js type ${jsTypeOf(value)}",
            )
        }
    }

    override suspend fun step(): Boolean {
        applyBindingsIfNecessary()
        return wrapSqlite {
            currentRow = null
            val hasRow = statement.step()
            parent.invalidateExportCache()
            if (!executionObserved) {
                parent.observeStatementExecution(normalizedSql)
                executionObserved = true
            }
            if (hasRow) {
                currentRow = statement.get(null, useBigIntConfig).unsafeCast<Array<dynamic>>()
            }
            hasRow
        }
    }

    override fun reset() {
        wrapSqlite { statement.reset() }
        bindingsDirty = true
        executionObserved = false
        currentRow = null
        parent.invalidateExportCache()
    }

    override fun clearBindings() {
        boundValues.clear()
        bindingsDirty = true
        currentRow = null
        wrapSqlite { statement.bind(emptyArray<Any?>().unsafeCast<Array<dynamic>>()) }
        executionObserved = false
        parent.invalidateExportCache()
    }

    override fun close() {
        wrapSqlite { statement.free() }
        currentRow = null
        parent.invalidateExportCache()
    }
}

private fun SqlJsDatabase.executeBatch(
    sql: String,
    observeSuccessfulStatement: (String) -> Unit,
) {
    val iterator = iterateStatements(sql)
    var failure: Throwable? = null

    try {
        while (true) {
            val next = iterator.next()
            if (next.done) break

            val statement = next.value ?: throw SqliteException("SQL.js statement iterator returned no statement")
            val normalizedSql = statement.getNormalizedSQL()
            while (statement.step()) {
                // execSQL intentionally discards result rows.
            }
            observeSuccessfulStatement(normalizedSql)
        }
    } catch (t: Throwable) {
        failure = t
    }

    failure = appendCleanupFailure(failure) {
        while (!iterator.next().done) {
            // Advancing frees the active statement and eventually releases the iterator SQL buffer.
        }
    }
    failure?.let { throw it }
}

private inline fun appendCleanupFailure(
    primary: Throwable?,
    cleanup: () -> Unit,
): Throwable? {
    return try {
        cleanup()
        primary
    } catch (additional: Throwable) {
        if (primary == null) {
            additional
        } else {
            if (additional !== primary && primary.suppressedExceptions.none { it === additional }) {
                primary.addSuppressed(additional)
            }
            primary
        }
    }
}

private inline fun <T> wrapSqlite(block: () -> T): T =
    try {
        block()
    } catch (t: Throwable) {
        if (t is SqliteException) throw t
        console.error("SQL.js error", t)
        throw t.toSqliteExceptionPreservingSuppressed()
    }

private fun SqlJsStatement.columnCount(): Int {
    val dyn = asDynamic()
    return when {
        dyn.columnCount != undefined -> (dyn.columnCount() as Number).toInt()
        dyn.getColumnCount != undefined -> (dyn.getColumnCount() as Number).toInt()
        dyn.getColumnNames != undefined -> {
            val names = dyn.getColumnNames() as Array<*>
            names.size
        }
        else -> throw SqliteException("Unable to determine column count")
    }
}

private fun SqlJsStatement.columnName(index: Int): String {
    val dyn = asDynamic()
    return when {
        dyn.columnName != undefined -> dyn.columnName(index) as String
        dyn.getColumnName != undefined -> dyn.getColumnName(index) as String
        dyn.getColumnNames != undefined -> {
            val names = dyn.getColumnNames() as Array<String>
            names[index]
        }
        else -> throw SqliteException("Unable to determine column name for index $index")
    }
}
