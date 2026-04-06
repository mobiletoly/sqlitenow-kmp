package dev.goquick.sqlitenow.core.sqlite

import dev.goquick.sqlitenow.core.sqlite.dbClose
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.sqlite.dbExec
import dev.goquick.sqlitenow.core.sqlite.dbExport
import dev.goquick.sqlitenow.core.sqlite.dbOpen
import dev.goquick.sqlitenow.core.sqlite.dbCreate
import dev.goquick.sqlitenow.core.sqlite.stmtBind
import dev.goquick.sqlitenow.core.sqlite.stmtClearBindings
import dev.goquick.sqlitenow.core.sqlite.stmtFinalize
import dev.goquick.sqlitenow.core.sqlite.stmtGetColumnCount
import dev.goquick.sqlitenow.core.sqlite.stmtGetColumnName
import dev.goquick.sqlitenow.core.sqlite.stmtGetRow
import dev.goquick.sqlitenow.core.sqlite.stmtPrepare
import dev.goquick.sqlitenow.core.sqlite.stmtReset
import dev.goquick.sqlitenow.core.sqlite.stmtStep
import dev.goquick.sqlitenow.core.sqlite.loadSqlJs
import dev.goquick.sqlitenow.core.sqlite.jsArrayOfSize
import kotlin.js.JsAny
import kotlin.js.JsArray
import kotlin.js.toJsNumber
import kotlin.js.toJsString

private const val JS_SAFE_INTEGER_MAX = 9_007_199_254_740_991L
private const val JS_SAFE_INTEGER_MIN = -9_007_199_254_740_991L

actual class SqliteConnection internal constructor(
    private val handle: SqlJsDatabaseHandle,
) {
    private var transactionDepth = 0
    private var exportedBytesCache: ByteArray? = null

    actual fun execSQL(sql: String) {
        wrapSqlite {
            val normalized = sql.trim()
            when {
                normalized.startsWith("BEGIN", ignoreCase = true) -> transactionDepth++
                normalized.startsWith("COMMIT", ignoreCase = true) -> if (transactionDepth > 0) transactionDepth--
                normalized.startsWith("ROLLBACK", ignoreCase = true) -> if (transactionDepth > 0) transactionDepth--
            }
            dbExec(handle.value, sql)
            invalidateExportCache()
        }
    }

    actual fun prepare(sql: String): SqliteStatement = wrapSqlite {
        val stmtHandle = stmtPrepare(handle.value, sql)
        SqliteStatement(this, SqlJsStatementHandle(stmtHandle))
    }

    actual fun inTransaction(): Boolean = transactionDepth > 0

    actual fun close() {
        wrapSqlite { dbClose(handle.value) }
        transactionDepth = 0
        exportedBytesCache = null
    }

    internal fun exportToByteArray(): ByteArray {
        val cached = exportedBytesCache
        if (cached != null) return cached
        val bytes = wrapSqlite { dbExport(handle.value).asByteArray() }
        exportedBytesCache = bytes
        return bytes
    }

    internal fun invalidateExportCache() {
        exportedBytesCache = null
    }
}

private sealed interface BindingValue {
    data class Blob(val bytes: ByteArray) : BindingValue
    data class DoubleVal(val value: Double) : BindingValue
    data class LongVal(val value: Long) : BindingValue
    data class Text(val value: String) : BindingValue
    data class IntVal(val value: Int) : BindingValue
    object Null : BindingValue
}

actual class SqliteStatement internal constructor(
    private val parent: SqliteConnection,
    private val handle: SqlJsStatementHandle,
) {
    private val boundValues = mutableMapOf<Int, BindingValue>()
    private var bindingsDirty = false
    private var currentRow: JsArray<JsAny?>? = null

    private fun setBinding(index: Int, value: BindingValue) {
        boundValues[index] = value
        bindingsDirty = true
        currentRow = null
        parent.invalidateExportCache()
    }

    private fun applyBindingsIfNecessary() {
        if (!bindingsDirty) return
        val maxIndex = boundValues.keys.maxOrNull() ?: 0
        val params = jsArrayOfSize(maxIndex)
        if (maxIndex > 0) {
            for (i in 0 until maxIndex) {
                val binding = boundValues[i + 1]
                if (binding == null) {
                    jsArraySetNull(params, i)
                } else {
                    jsArraySetValue(params, i, binding.toJsValue())
                }
            }
        }
        wrapSqlite { stmtBind(handle.value, params) }
        bindingsDirty = false
        currentRow = null
        parent.invalidateExportCache()
    }

    actual fun bindBlob(index: Int, value: ByteArray) = setBinding(index, BindingValue.Blob(value))

    actual fun bindDouble(index: Int, value: Double) = setBinding(index, BindingValue.DoubleVal(value))

    actual fun bindLong(index: Int, value: Long) {
        when {
            value in Int.MIN_VALUE..Int.MAX_VALUE -> setBinding(index, BindingValue.IntVal(value.toInt()))
            value in JS_SAFE_INTEGER_MIN..JS_SAFE_INTEGER_MAX -> setBinding(index, BindingValue.DoubleVal(value.toDouble()))
            else -> setBinding(index, BindingValue.LongVal(value))
        }
    }

    actual fun bindText(index: Int, value: String) = setBinding(index, BindingValue.Text(value))

    actual fun bindNull(index: Int) = setBinding(index, BindingValue.Null)

    actual fun bindInt(index: Int, value: Int) = setBinding(index, BindingValue.IntVal(value))

    private fun fetchValue(index: Int): JsAny? {
        val row = currentRow ?: throw SqliteException("No active row. Call step() before reading columns.")
        if (index < 0 || index >= row.length) {
            throw SqliteException("Column $index out of bounds (size=${row.length})")
        }
        return row[index]
    }

    actual fun getBlob(index: Int): ByteArray {
        val value = fetchValue(index)
        if (value == null || isNull(value)) {
            throw SqliteException("Column $index is NULL")
        }
        if (!isJsArray(value)) {
            throw SqliteException("Column $index is not a blob (type=${jsTypeOf(value)})")
        }
        return asJsArray(value).asByteArray()
    }

    actual fun getDouble(index: Int): Double {
        val value = fetchValue(index)
        if (value == null || isNull(value)) {
            throw SqliteException("Column $index is NULL")
        }
        return when (jsTypeOf(value)) {
            "number" -> toNumber(value)
            "string" -> toNumber(value)
            else -> throw SqliteException("Column $index cannot convert to Double (type=${jsTypeOf(value)})")
        }
    }

    actual fun getLong(index: Int): Long {
        val value = fetchValue(index)
        if (value == null || isNull(value)) {
            throw SqliteException("Column $index is NULL")
        }
        return when (jsTypeOf(value)) {
            "number" -> toNumber(value).toLong()
            "string" -> toNumber(value).toLong()
            else -> throw SqliteException("Column $index cannot convert to Long (type=${jsTypeOf(value)})")
        }
    }

    actual fun getText(index: Int): String {
        val value = fetchValue(index)
        if (value == null || isNull(value)) {
            throw SqliteException("Column $index is NULL")
        }
        return toStringValue(value)
    }

    actual fun isNull(index: Int): Boolean = isNull(fetchValue(index))

    actual fun getInt(index: Int): Int = getLong(index).toInt()

    actual fun getColumnCount(): Int = wrapSqlite { stmtGetColumnCount(handle.value) }

    actual fun getColumnName(index: Int): String = wrapSqlite { stmtGetColumnName(handle.value, index) }

    actual fun step(): Boolean {
        applyBindingsIfNecessary()
        return wrapSqlite {
            currentRow = null
            val hasRow = stmtStep(handle.value)
            if (hasRow) {
                val row = stmtGetRow(handle.value)
                if (row == null) {
                    throw SqliteException("Statement reported a row but returned null data")
                }
                currentRow = row
            }
            parent.invalidateExportCache()
            hasRow
        }
    }

    actual fun reset() {
        wrapSqlite { stmtReset(handle.value) }
        bindingsDirty = true
        currentRow = null
        parent.invalidateExportCache()
    }

    actual fun clearBindings() {
        boundValues.clear()
        bindingsDirty = false
        currentRow = null
        wrapSqlite { stmtClearBindings(handle.value) }
        parent.invalidateExportCache()
    }

    actual fun close() {
        wrapSqlite { stmtFinalize(handle.value) }
        currentRow = null
        parent.invalidateExportCache()
    }

    private fun BindingValue.toJsValue(): JsAny? = when (this) {
        is BindingValue.Blob -> bytes.toSqlJsArray()
        is BindingValue.DoubleVal -> value.toJsNumber()
        is BindingValue.IntVal -> value.toJsNumber()
        is BindingValue.Text -> value.toJsString()
        is BindingValue.LongVal -> when {
            value in JS_SAFE_INTEGER_MIN..JS_SAFE_INTEGER_MAX -> value.toDouble().toJsNumber()
            else -> value.toString().toJsString()
        }
        BindingValue.Null -> null
    }
}

internal inline fun <T> wrapSqlite(block: () -> T): T =
    try {
        block()
    } catch (t: Throwable) {
        if (t is SqliteException) throw t
        sqliteNowLogger.e { "[SqlJs][Wasm] sqlite error: ${t.message}" }
        throw SqliteException(t.message, t)
    }
