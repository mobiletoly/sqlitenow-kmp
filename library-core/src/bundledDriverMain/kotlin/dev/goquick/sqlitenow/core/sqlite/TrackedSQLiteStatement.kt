/*
 * Copyright 2026 Toly Pochkin
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
package dev.goquick.sqlitenow.core.sqlite

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import androidx.sqlite.async.executeSQL

internal actual suspend fun executeSqliteNowSql(connection: SQLiteConnection, sql: String) {
    connection.executeSQL(sql)
}

internal actual fun trackSQLiteStatement(
    statement: SQLiteStatement,
    cleanupFailureObserver: (Throwable) -> Unit,
    beforeCloseObserver: () -> Unit,
    closeSuccessObserver: () -> Unit,
    diagnostics: dev.goquick.sqlitenow.core.SqliteNowStatementDiagnostics?,
): SQLiteStatement = TrackedSQLiteStatement(
    delegate = statement,
    cleanupFailureObserver = cleanupFailureObserver,
    beforeCloseObserver = beforeCloseObserver,
    closeSuccessObserver = closeSuccessObserver,
    diagnostics = diagnostics,
)

internal actual fun clearTrackedSQLiteStatementObservers(statement: SQLiteStatement) {
    (statement as? TrackedSQLiteStatement)?.clearObservers()
}

private class TrackedSQLiteStatement(
    private val delegate: SQLiteStatement,
    private var cleanupFailureObserver: ((Throwable) -> Unit)?,
    private var beforeCloseObserver: (() -> Unit)?,
    private var closeSuccessObserver: (() -> Unit)?,
    private var diagnostics: dev.goquick.sqlitenow.core.SqliteNowStatementDiagnostics?,
) : SQLiteStatement, OwnedStatementClose, dev.goquick.sqlitenow.core.DiagnosticStatementBinding {
    override val closeProgress = StatementCloseProgress()
    override fun bindBlob(index: Int, value: ByteArray) =
        wrapSqliteCall { delegate.bindBlob(index, value) }

    override fun bindDouble(index: Int, value: Double) =
        wrapSqliteCall { delegate.bindDouble(index, value) }

    override fun bindLong(index: Int, value: Long) =
        wrapSqliteCall { delegate.bindLong(index, value) }

    override fun bindText(index: Int, value: String) =
        wrapSqliteCall { delegate.bindText(index, value) }

    override fun bindNull(index: Int) =
        wrapSqliteCall { delegate.bindNull(index) }

    override fun getBlob(index: Int): ByteArray =
        wrapSqliteCall { delegate.getBlob(index) }

    override fun getDouble(index: Int): Double =
        wrapSqliteCall { delegate.getDouble(index) }

    override fun getLong(index: Int): Long =
        wrapSqliteCall { delegate.getLong(index) }

    override fun getText(index: Int): String =
        wrapSqliteCall { delegate.getText(index) }

    override fun isNull(index: Int): Boolean =
        wrapSqliteCall { delegate.isNull(index) }

    override fun getColumnCount(): Int =
        wrapSqliteCall { delegate.getColumnCount() }

    override fun getColumnName(index: Int): String =
        wrapSqliteCall { delegate.getColumnName(index) }

    override fun getColumnNames(): List<String> =
        wrapSqliteCall { delegate.getColumnNames() }

    override fun getColumnType(index: Int): Int =
        wrapSqliteCall { delegate.getColumnType(index) }

    override fun step(): Boolean =
        observe("step") { wrapSqliteCall { delegate.step() } }

    override fun reset() = observeCleanup {
        observe("reset") { wrapSqliteCall { delegate.reset() } }
    }

    override fun clearBindings() = observeCleanup {
        observe("clear_bindings") { wrapSqliteCall { delegate.clearBindings() } }
    }

    override fun close() = closeProgress.close(::closeDelegate)

    override suspend fun closeFromOwner() = closeProgress.closeFromOwner(::closeDelegate)

    private fun closeDelegate() {
        beforeCloseObserver?.invoke()
        try {
            observe("close") { wrapAndroidxSqliteCall { delegate.close() } }
        } catch (t: Throwable) {
            cleanupFailureObserver?.invoke(t)
            throw t
        }
        closeSuccessObserver?.invoke()
        clearObservers()
    }

    private inline fun <T> observe(action: String, block: () -> T): T {
        val observer = diagnostics ?: return block()
        val span = observer.span(action)
        if (span == null) {
            try { return block().also { observer.disabledBoundary(action, action == "step" && it == true) } }
            catch (failure: Throwable) { observer.finish(); throw failure }
        }
        val began = span.nowNanos()
        var failed = false
        var row = false
        try { return block().also { row = action == "step" && it == true } }
        catch (failure: Throwable) { failed = true; throw failure }
        finally { observer.complete(span, action, began, row, failed) }
    }

    override fun bindDiagnosticScope(span: dev.goquick.sqlitenow.core.SqliteNowDiagnosticSpan?, familyLabel: String?, hit: Boolean) {
        diagnostics?.bindCacheScope(span, familyLabel, hit)
    }

    fun clearObservers() {
        diagnostics?.finish()
        diagnostics = null
        cleanupFailureObserver = null
        beforeCloseObserver = null
        closeSuccessObserver = null
    }

    private inline fun observeCleanup(block: () -> Unit) {
        try {
            block()
        } catch (t: Throwable) {
            cleanupFailureObserver?.invoke(t)
            throw t
        }
    }
}
