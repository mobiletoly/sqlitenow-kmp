@file:OptIn(kotlin.js.ExperimentalWasmJsInterop::class)

package dev.goquick.sqlitenow.core.sqlite

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.sqlite.dbClose
import dev.goquick.sqlitenow.core.sqlite.dbExec
import dev.goquick.sqlitenow.core.sqlite.dbExport
import dev.goquick.sqlitenow.core.sqlite.stmtBind
import dev.goquick.sqlitenow.core.sqlite.stmtClearBindings
import dev.goquick.sqlitenow.core.sqlite.stmtFinalize
import dev.goquick.sqlitenow.core.sqlite.stmtGetColumnCount
import dev.goquick.sqlitenow.core.sqlite.stmtGetColumnName
import dev.goquick.sqlitenow.core.sqlite.stmtGetColumnType
import dev.goquick.sqlitenow.core.sqlite.stmtGetNormalizedSql
import dev.goquick.sqlitenow.core.sqlite.stmtGetRow
import dev.goquick.sqlitenow.core.sqlite.stmtPrepare
import dev.goquick.sqlitenow.core.sqlite.stmtReset
import dev.goquick.sqlitenow.core.sqlite.stmtStep
import dev.goquick.sqlitenow.core.sqlite.jsArrayOfSize
import kotlin.js.JsAny
import kotlin.js.JsArray
import kotlin.js.JsException
import kotlin.js.thrownValue
import kotlin.js.toJsNumber
import kotlin.js.toJsString

private const val JS_SAFE_INTEGER_MAX = 9_007_199_254_740_991L
private const val JS_SAFE_INTEGER_MIN = -9_007_199_254_740_991L

internal class SqlJsSQLiteConnection(
    private val handle: SqlJsDatabaseHandle,
) : SQLiteConnection {
    private val transactionState = SqliteTransactionState()
    private var exportedBytesCache: ByteArray? = null

    override suspend fun prepare(sql: String): SQLiteStatement = wrapSqlite {
        prepareSqlJsStatement(
            prepare = { stmtPrepare(handle.value, sql) },
            normalize = ::stmtGetNormalizedSql,
            closeOnFailure = ::stmtFinalize,
        ) { stmtHandle, normalizedSql ->
            SqlJsSQLiteStatement(this, normalizedSql, SqlJsStatementHandle(stmtHandle))
        }
    }

    internal fun execSQL(sql: String) {
        invalidateExportCache()
        wrapSqlite {
            dbExec(handle.value, sql, transactionState::observeSuccessfulStatement)
        }
    }

    override fun inTransaction(): Boolean = transactionState.inTransaction()

    override fun close() {
        wrapSqlite { dbClose(handle.value) }
        transactionState.reset()
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

    internal fun observeStatementExecution(normalizedSql: String) {
        transactionState.observeSuccessfulStatement(normalizedSql)
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

private class SqlJsSQLiteStatement(
    private val parent: SqlJsSQLiteConnection,
    private val normalizedSql: String,
    private val handle: SqlJsStatementHandle,
) : SQLiteStatement {
    private val boundValues = mutableMapOf<Int, BindingValue>()
    private var bindingsDirty = false
    private var executionObserved = false
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
        executionObserved = false
        currentRow = null
        parent.invalidateExportCache()
    }

    override fun bindBlob(index: Int, value: ByteArray) = setBinding(index, BindingValue.Blob(value))

    override fun bindDouble(index: Int, value: Double) = setBinding(index, BindingValue.DoubleVal(value))

    override fun bindLong(index: Int, value: Long) {
        when {
            value in Int.MIN_VALUE..Int.MAX_VALUE -> setBinding(index, BindingValue.IntVal(value.toInt()))
            value in JS_SAFE_INTEGER_MIN..JS_SAFE_INTEGER_MAX -> setBinding(index, BindingValue.DoubleVal(value.toDouble()))
            else -> setBinding(index, BindingValue.LongVal(value))
        }
    }

    override fun bindText(index: Int, value: String) = setBinding(index, BindingValue.Text(value))

    override fun bindNull(index: Int) = setBinding(index, BindingValue.Null)

    private fun fetchValue(index: Int): JsAny? {
        val row = currentRow ?: throw SqliteException("No active row. Call step() before reading columns.")
        if (index < 0 || index >= row.length) {
            throw SqliteException("Column $index out of bounds (size=${row.length})")
        }
        return row[index]
    }

    override fun getBlob(index: Int): ByteArray {
        val value = fetchValue(index)
        if (value == null || isNull(value)) {
            throw SqliteException("Column $index is NULL")
        }
        if (!isJsArray(value)) {
            throw SqliteException("Column $index is not a blob (type=${jsTypeOf(value)})")
        }
        return asJsArray(value).asByteArray()
    }

    override fun getDouble(index: Int): Double {
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

    override fun getLong(index: Int): Long {
        val value = fetchValue(index)
        if (value == null || isNull(value)) {
            throw SqliteException("Column $index is NULL")
        }
        return when (jsTypeOf(value)) {
            "number" -> toNumber(value).toLong()
            "string" -> toStringValue(value).toLong()
            else -> throw SqliteException("Column $index cannot convert to Long (type=${jsTypeOf(value)})")
        }
    }

    override fun getText(index: Int): String {
        val value = fetchValue(index)
        if (value == null || isNull(value)) {
            throw SqliteException("Column $index is NULL")
        }
        return toStringValue(value)
    }

    override fun isNull(index: Int): Boolean = isNull(fetchValue(index))

    override fun getColumnCount(): Int = wrapSqlite { stmtGetColumnCount(handle.value) }

    override fun getColumnName(index: Int): String = wrapSqlite { stmtGetColumnName(handle.value, index) }

    override fun getColumnType(index: Int): Int {
        fetchValue(index)
        return wrapSqlite { stmtGetColumnType(handle.value, index) }
    }

    override suspend fun step(): Boolean {
        applyBindingsIfNecessary()
        return wrapSqlite {
            currentRow = null
            val hasRow = stmtStep(handle.value)
            parent.invalidateExportCache()
            if (!executionObserved) {
                parent.observeStatementExecution(normalizedSql)
                executionObserved = true
            }
            if (hasRow) {
                val row = stmtGetRow(handle.value)
                if (row == null) {
                    throw SqliteException("Statement reported a row but returned null data")
                }
                currentRow = row
            }
            hasRow
        }
    }

    override fun reset() {
        wrapSqlite { stmtReset(handle.value) }
        bindingsDirty = true
        executionObserved = false
        currentRow = null
        parent.invalidateExportCache()
    }

    override fun clearBindings() {
        boundValues.clear()
        bindingsDirty = false
        currentRow = null
        wrapSqlite { stmtClearBindings(handle.value) }
        executionObserved = false
        parent.invalidateExportCache()
    }

    override fun close() {
        wrapSqlite { stmtFinalize(handle.value) }
        currentRow = null
        parent.invalidateExportCache()
    }

    private fun BindingValue.toJsValue(): JsAny? = when (this) {
        is BindingValue.Blob -> bytes.toJsArray()
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
        t.materializeSqliteNowSuppressed()
        sqliteNowLogger.e { "[SqlJs][Wasm] sqlite error: ${t.message}" }
        throw t.toSqliteExceptionPreservingSuppressed()
    }

private fun Throwable.materializeSqliteNowSuppressed() {
    val thrownValue = (this as? JsException)?.thrownValue ?: return
    val suppressedValues = takeSqliteNowSuppressed(thrownValue)
    for (index in 0 until suppressedValues.length) {
        val additional = try {
            throwSqliteNowFailure(suppressedValues[index])
            null
        } catch (t: Throwable) {
            t
        }
        additional?.let(::addSuppressedIfAbsent)
    }
}
