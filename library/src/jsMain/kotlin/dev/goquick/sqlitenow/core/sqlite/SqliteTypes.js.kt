package dev.goquick.sqlitenow.core.sqlite

import kotlin.js.jsTypeOf
import kotlin.js.unsafeCast
import org.khronos.webgl.Uint8Array

actual class SqliteConnection internal constructor(
    private val database: SqlJsDatabase,
) {
    private var transactionDepth = 0

    actual fun execSQL(sql: String) {
        wrapSqlite {
            val normalized = sql.trim()
            when {
                normalized.startsWith("BEGIN", ignoreCase = true) -> transactionDepth++
                normalized.startsWith("COMMIT", ignoreCase = true) -> if (transactionDepth > 0) transactionDepth--
                normalized.startsWith("ROLLBACK", ignoreCase = true) -> if (transactionDepth > 0) transactionDepth--
            }
            database.exec(sql)
        }
    }

    actual fun prepare(sql: String): SqliteStatement = wrapSqlite {
        SqliteStatement(database.prepare(sql))
    }

    actual fun inTransaction(): Boolean = transactionDepth > 0

    actual fun close() {
        wrapSqlite { database.close() }
        transactionDepth = 0
    }
}

actual class SqliteStatement internal constructor(
    private val statement: SqlJsStatement,
) {
    private companion object {
        private const val JS_SAFE_INTEGER_MAX = 9_007_199_254_740_991L
        private const val JS_SAFE_INTEGER_MIN = -9_007_199_254_740_991L
        private val useBigIntConfig = js("({ useBigInt: true })")
    }

    private val boundValues = mutableMapOf<Int, dynamic>()
    private var bindingsDirty = false
    private var currentRow: Array<dynamic>? = null

    private fun setBinding(index: Int, value: dynamic) {
        boundValues[index] = value
        bindingsDirty = true
        currentRow = null
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
    }

    actual fun bindBlob(index: Int, value: ByteArray) = setBinding(index, value.toUint8Array())

    actual fun bindDouble(index: Int, value: Double) = setBinding(index, value)

    actual fun bindLong(index: Int, value: Long) {
        when {
            value in Int.MIN_VALUE..Int.MAX_VALUE -> setBinding(index, value.toInt())
            value in JS_SAFE_INTEGER_MIN..JS_SAFE_INTEGER_MAX -> setBinding(index, value.toDouble())
            else -> setBinding(index, value.toString())
        }
    }

    actual fun bindText(index: Int, value: String) = setBinding(index, value)

    actual fun bindNull(index: Int) = setBinding(index, null)

    actual fun bindInt(index: Int, value: Int) = setBinding(index, value)

    private fun fetchValue(index: Int): dynamic {
        val row = currentRow ?: throw SqliteException("No active row. Call step() before reading columns.")
        if (index < 0 || index >= row.size) {
            throw SqliteException("Column $index out of bounds (size=${row.size})")
        }
        return row[index]
    }

    actual fun getBlob(index: Int): ByteArray {
        val value = fetchValue(index)
        if (value == null) throw SqliteException("Column $index is NULL")
        return when (value) {
            is Uint8Array -> value.unsafeCast<Uint8Array>().toByteArray()
            else -> throw SqliteException("Column $index is not a blob")
        }
    }

    actual fun getDouble(index: Int): Double {
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

    actual fun getLong(index: Int): Long {
        val value = fetchValue(index) ?: throw SqliteException("Column $index is NULL")
        return when {
            jsTypeOf(value) == "bigint" -> value.toString().toLong()
            value is Double -> value.toLong()
            value is Int -> value.toLong()
            value is Number -> value.toDouble().toLong()
            value is String -> value.toDouble().toLong()
            else -> throw SqliteException("Column $index cannot convert to Long (type=${jsTypeOf(value)})")
        }
    }

    actual fun getText(index: Int): String {
        return when (val value = fetchValue(index)) {
            null -> throw SqliteException("Column $index is NULL")
            is String -> value
            else -> value.toString()
        }
    }

    actual fun isNull(index: Int): Boolean = fetchValue(index) == null

    actual fun getInt(index: Int): Int = getLong(index).toInt()

    actual fun getColumnCount(): Int = wrapSqlite { statement.columnCount() }

    actual fun getColumnName(index: Int): String = wrapSqlite { statement.columnName(index) }

    actual fun step(): Boolean {
        applyBindingsIfNecessary()
        return wrapSqlite {
            currentRow = null
            val hasRow = statement.step()
            if (hasRow) {
                currentRow = statement.get(null, useBigIntConfig).unsafeCast<Array<dynamic>>()
            }
            hasRow
        }
    }

    actual fun reset() {
        wrapSqlite { statement.reset() }
        bindingsDirty = true
        currentRow = null
    }

    actual fun clearBindings() {
        boundValues.clear()
        bindingsDirty = true
        currentRow = null
        wrapSqlite { statement.bind(emptyArray<Any?>().unsafeCast<Array<dynamic>>()) }
    }

    actual fun close() {
        wrapSqlite { statement.free() }
        currentRow = null
    }
}

private inline fun <T> wrapSqlite(block: () -> T): T =
    try {
        block()
    } catch (t: Throwable) {
        if (t is SqliteException) throw t
        console.error("SQL.js error", t)
        throw SqliteException(t.message, t)
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

private fun ByteArray.toUint8Array(): Uint8Array {
    val array = Uint8Array(size)
    val dyn = array.asDynamic()
    for (i in indices) {
        val value = this[i].toInt() and 0xFF
        dyn[i] = value
    }
    return array
}

private fun Uint8Array.toByteArray(): ByteArray {
    val result = ByteArray(length)
    val dyn = this.asDynamic()
    for (i in 0 until length) {
        val value = (dyn[i] as Number).toInt()
        result[i] = (value and 0xFF).toByte()
    }
    return result
}
