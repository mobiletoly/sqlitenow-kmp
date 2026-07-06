@file:OptIn(kotlinx.cinterop.BetaInteropApi::class, kotlinx.cinterop.ExperimentalForeignApi::class)

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
package dev.goquick.sqlitenow.swift.runtime.core

import dev.goquick.sqlitenow.core.DatabaseMigrations
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowDatabase
import dev.goquick.sqlitenow.core.TransactionMode
import dev.goquick.sqlitenow.core.sqlite.SqliteStatement
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.convert
import kotlinx.cinterop.usePinned
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import platform.Foundation.NSData
import platform.Foundation.create
import platform.posix.memcpy
import kotlin.coroutines.cancellation.CancellationException

class SQLiteNowCoreRuntimeBindValue private constructor(
    val kind: String,
    val int64Value: Long,
    val doubleValue: Double,
    val textValue: String?,
    val boolValue: Boolean,
    val dataValue: NSData?,
) {
    constructor() : this(
        kind = KIND_NULL,
        int64Value = 0L,
        doubleValue = 0.0,
        textValue = null,
        boolValue = false,
        dataValue = null,
    )

    constructor(int64Value: Long) : this(
        kind = KIND_INT64,
        int64Value = int64Value,
        doubleValue = 0.0,
        textValue = null,
        boolValue = false,
        dataValue = null,
    )

    constructor(doubleValue: Double) : this(
        kind = KIND_DOUBLE,
        int64Value = 0L,
        doubleValue = doubleValue,
        textValue = null,
        boolValue = false,
        dataValue = null,
    )

    constructor(textValue: String) : this(
        kind = KIND_TEXT,
        int64Value = 0L,
        doubleValue = 0.0,
        textValue = textValue,
        boolValue = false,
        dataValue = null,
    )

    constructor(boolValue: Boolean) : this(
        kind = KIND_BOOL,
        int64Value = 0L,
        doubleValue = 0.0,
        textValue = null,
        boolValue = boolValue,
        dataValue = null,
    )

    constructor(dataValue: NSData) : this(
        kind = KIND_BLOB,
        int64Value = 0L,
        doubleValue = 0.0,
        textValue = null,
        boolValue = false,
        dataValue = dataValue,
    )

    internal fun bind(statement: SqliteStatement, index: Int) {
        when (kind) {
            KIND_NULL -> statement.bindNull(index)
            KIND_INT64 -> statement.bindLong(index, int64Value)
            KIND_DOUBLE -> statement.bindDouble(index, doubleValue)
            KIND_TEXT -> statement.bindText(index, requireNotNull(textValue))
            KIND_BOOL -> statement.bindLong(index, if (boolValue) 1L else 0L)
            KIND_BLOB -> statement.bindBlob(index, requireNotNull(dataValue).toByteArray())
            else -> error("Unknown SQLiteNow runtime bind kind: $kind")
        }
    }

    private companion object {
        const val KIND_NULL = "null"
        const val KIND_INT64 = "int64"
        const val KIND_DOUBLE = "double"
        const val KIND_TEXT = "text"
        const val KIND_BOOL = "bool"
        const val KIND_BLOB = "blob"
    }
}

class SQLiteNowCoreRuntimeCell(
    val kind: String,
    val int64Value: Long,
    val doubleValue: Double,
    val textValue: String?,
    val boolValue: Boolean,
    val dataValue: NSData?,
) {
    val isNull: Boolean
        get() = kind == KIND_NULL

    companion object {
        const val KIND_NULL = "null"
        const val KIND_INT64 = "int64"
        const val KIND_DOUBLE = "double"
        const val KIND_TEXT = "text"
        const val KIND_BOOL = "bool"
        const val KIND_BLOB = "blob"
    }
}

class SQLiteNowCoreRuntimeRow internal constructor(
    private val cells: List<SQLiteNowCoreRuntimeCell>,
) {
    val count: Int
        get() = cells.size

    fun cellAt(index: Int): SQLiteNowCoreRuntimeCell = cells[index]
}

class SQLiteNowCoreRuntimeRowSet internal constructor(
    private val rows: List<SQLiteNowCoreRuntimeRow>,
) {
    val count: Int
        get() = rows.size

    fun rowAt(index: Int): SQLiteNowCoreRuntimeRow = rows[index]
}

class SQLiteNowCoreRuntimeMigrationStep(
    val version: Long,
    val sql: List<String>,
)

class SQLiteNowCoreRuntimeMigrationPlan(
    val latestVersion: Long,
    val schemaSql: List<String>,
    val initSql: List<String>,
    val migrationSteps: List<SQLiteNowCoreRuntimeMigrationStep>,
)

class SQLiteNowCoreRuntimeMutation(
    val sql: String,
    val bindValues: List<SQLiteNowCoreRuntimeBindValue>,
    val affectedTables: List<String>,
)

class SQLiteNowCoreRuntimeMutationBatch {
    internal val mutations = mutableListOf<SQLiteNowCoreRuntimeMutation>()

    fun add(
        sql: String,
        bindValues: List<SQLiteNowCoreRuntimeBindValue>,
        affectedTables: List<String>,
    ) {
        mutations += SQLiteNowCoreRuntimeMutation(
            sql = sql,
            bindValues = bindValues,
            affectedTables = affectedTables,
        )
    }
}

interface SQLiteNowCoreRuntimeTableObserver {
    fun onChanged()
    fun onError(payload: SQLiteNowCoreRuntimeErrorPayload)
}

class SQLiteNowCoreRuntimeCancelHandle internal constructor(
    private val cancelBlock: () -> Unit,
) {
    fun cancel() {
        cancelBlock()
    }
}

class SQLiteNowCoreRuntimeErrorPayload(
    val category: String,
    val code: String,
    val message: String,
)

class SQLiteNowCoreRuntimeException(
    val payload: SQLiteNowCoreRuntimeErrorPayload,
) : RuntimeException(payload.message)

class SQLiteNowCoreRuntimeDatabase(
    path: String,
    migrationPlan: SQLiteNowCoreRuntimeMigrationPlan,
    debug: Boolean,
) {
    private val database = RuntimeSqliteNowDatabase(
        dbName = path,
        migrationPlan = migrationPlan,
        debug = debug,
    )
    private var observerScope = newObserverScope()

    @Throws(SQLiteNowCoreRuntimeException::class, CancellationException::class)
    suspend fun open() = mapRuntimeErrors {
        database.open()
    }

    @Throws(SQLiteNowCoreRuntimeException::class, CancellationException::class)
    suspend fun close() = mapRuntimeErrors {
        observerScope.cancel()
        observerScope = newObserverScope()
        database.close()
    }

    fun isOpen(): Boolean = database.isOpen()

    fun connectionForSQLiteNowSyncRuntime(): SafeSQLiteConnection = database.connection()

    @Throws(SQLiteNowCoreRuntimeException::class, CancellationException::class)
    suspend fun execute(
        sql: String,
        bindValues: List<SQLiteNowCoreRuntimeBindValue>,
        affectedTables: List<String>,
    ) = mapRuntimeErrors {
        database.connection().withExclusiveAccess {
            database.connection().prepare(sql).use { statement ->
                bindValues.bindAll(statement)
                statement.step()
            }
        }
        database.reportExternalTableChanges(affectedTables.toSet())
    }

    @Throws(SQLiteNowCoreRuntimeException::class, CancellationException::class)
    suspend fun query(
        sql: String,
        bindValues: List<SQLiteNowCoreRuntimeBindValue>,
        columnTypes: List<String>,
    ): SQLiteNowCoreRuntimeRowSet = mapRuntimeErrors {
        queryInternal(sql, bindValues, columnTypes)
    }

    @Throws(SQLiteNowCoreRuntimeException::class, CancellationException::class)
    suspend fun executeReturning(
        sql: String,
        bindValues: List<SQLiteNowCoreRuntimeBindValue>,
        columnTypes: List<String>,
        affectedTables: List<String>,
    ): SQLiteNowCoreRuntimeRowSet = mapRuntimeErrors {
        val rows = queryInternal(sql, bindValues, columnTypes)
        database.reportExternalTableChanges(affectedTables.toSet())
        rows
    }

    @Throws(SQLiteNowCoreRuntimeException::class, CancellationException::class)
    suspend fun transaction(batch: SQLiteNowCoreRuntimeMutationBatch) = mapRuntimeErrors {
        val affectedTables = linkedSetOf<String>()
        database.transaction(TransactionMode.DEFERRED) {
            val connection = database.connection()
            connection.withExclusiveAccess {
                batch.mutations.forEach { mutation ->
                    connection.prepare(mutation.sql).use { statement ->
                        mutation.bindValues.bindAll(statement)
                        statement.step()
                    }
                    affectedTables += mutation.affectedTables
                }
            }
        }
        database.reportExternalTableChanges(affectedTables)
    }

    fun observeTables(
        tableNames: List<String>,
        observer: SQLiteNowCoreRuntimeTableObserver,
    ): SQLiteNowCoreRuntimeCancelHandle {
        database.enableTableChangeNotifications()
        val scope = observerScope
        val job = scope.launch {
            try {
                database.tableChangeFlow(tableNames.toSet()).collect {
                    observer.onChanged()
                }
            } catch (cancellation: CancellationException) {
                return@launch
            } catch (t: Throwable) {
                observer.onError(errorPayload(t))
            }
        }
        return SQLiteNowCoreRuntimeCancelHandle {
            job.cancel()
        }
    }

    private suspend fun queryInternal(
        sql: String,
        bindValues: List<SQLiteNowCoreRuntimeBindValue>,
        columnTypes: List<String>,
    ): SQLiteNowCoreRuntimeRowSet {
        return database.connection().withExclusiveAccess {
            database.connection().prepare(sql).use { statement ->
                bindValues.bindAll(statement)
                val rows = mutableListOf<SQLiteNowCoreRuntimeRow>()
                while (statement.step()) {
                    rows += readRow(statement, columnTypes)
                }
                SQLiteNowCoreRuntimeRowSet(rows)
            }
        }
    }
}

private fun newObserverScope(): CoroutineScope =
    CoroutineScope(SupervisorJob() + Dispatchers.Default)

private class RuntimeSqliteNowDatabase(
    dbName: String,
    migrationPlan: SQLiteNowCoreRuntimeMigrationPlan,
    debug: Boolean,
) : SqliteNowDatabase(
    dbName = dbName,
    migration = RuntimeMigrationPlan(migrationPlan),
    debug = debug,
) {
    suspend fun tableChangeFlow(tableNames: Set<String>) = createTableChangeFlow(tableNames)
}

private class RuntimeMigrationPlan(
    private val plan: SQLiteNowCoreRuntimeMigrationPlan,
) : DatabaseMigrations {
    override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
        return conn.withExclusiveAccess {
            val latestVersion = plan.latestVersion.toInt()
            if (currentVersion == -1) {
                plan.schemaSql.forEach { conn.execSQL(it) }
                plan.initSql.forEach { conn.execSQL(it) }
                return@withExclusiveAccess latestVersion
            }

            plan.migrationSteps
                .sortedBy { it.version }
                .filter { currentVersion < it.version }
                .forEach { step ->
                    step.sql.forEach { conn.execSQL(it) }
                }

            maxOf(currentVersion, latestVersion)
        }
    }
}

private fun List<SQLiteNowCoreRuntimeBindValue>.bindAll(statement: SqliteStatement) {
    forEachIndexed { index, value ->
        value.bind(statement, index + 1)
    }
}

private fun readRow(
    statement: SqliteStatement,
    columnTypes: List<String>,
): SQLiteNowCoreRuntimeRow {
    val cells = columnTypes.mapIndexed { index, type ->
        if (statement.isNull(index)) {
            SQLiteNowCoreRuntimeCell(
                kind = SQLiteNowCoreRuntimeCell.KIND_NULL,
                int64Value = 0L,
                doubleValue = 0.0,
                textValue = null,
                boolValue = false,
                dataValue = null,
            )
        } else {
            when (type) {
                SQLiteNowCoreRuntimeCell.KIND_INT64 -> SQLiteNowCoreRuntimeCell(
                    kind = SQLiteNowCoreRuntimeCell.KIND_INT64,
                    int64Value = statement.getLong(index),
                    doubleValue = 0.0,
                    textValue = null,
                    boolValue = false,
                    dataValue = null,
                )
                SQLiteNowCoreRuntimeCell.KIND_DOUBLE -> SQLiteNowCoreRuntimeCell(
                    kind = SQLiteNowCoreRuntimeCell.KIND_DOUBLE,
                    int64Value = 0L,
                    doubleValue = statement.getDouble(index),
                    textValue = null,
                    boolValue = false,
                    dataValue = null,
                )
                SQLiteNowCoreRuntimeCell.KIND_TEXT -> SQLiteNowCoreRuntimeCell(
                    kind = SQLiteNowCoreRuntimeCell.KIND_TEXT,
                    int64Value = 0L,
                    doubleValue = 0.0,
                    textValue = statement.getText(index),
                    boolValue = false,
                    dataValue = null,
                )
                SQLiteNowCoreRuntimeCell.KIND_BOOL -> {
                    val boolValue = statement.getLong(index) != 0L
                    SQLiteNowCoreRuntimeCell(
                        kind = SQLiteNowCoreRuntimeCell.KIND_BOOL,
                        int64Value = if (boolValue) 1L else 0L,
                        doubleValue = 0.0,
                        textValue = null,
                        boolValue = boolValue,
                        dataValue = null,
                    )
                }
                SQLiteNowCoreRuntimeCell.KIND_BLOB -> SQLiteNowCoreRuntimeCell(
                    kind = SQLiteNowCoreRuntimeCell.KIND_BLOB,
                    int64Value = 0L,
                    doubleValue = 0.0,
                    textValue = null,
                    boolValue = false,
                    dataValue = statement.getBlob(index).toNSData(),
                )
                else -> error("Unknown SQLiteNow runtime column type: $type")
            }
        }
    }
    return SQLiteNowCoreRuntimeRow(cells)
}

private inline fun <T> mapRuntimeErrors(block: () -> T): T {
    try {
        return block()
    } catch (t: SQLiteNowCoreRuntimeException) {
        throw t
    } catch (t: CancellationException) {
        throw t
    } catch (t: Throwable) {
        throw SQLiteNowCoreRuntimeException(errorPayload(t))
    }
}

private fun errorPayload(t: Throwable): SQLiteNowCoreRuntimeErrorPayload {
    val message = t.message ?: t.toString()
    val lower = message.lowercase()
    val category = when {
        lower.contains("migration") -> "migration"
        lower.contains("sqlite") || lower.contains("constraint") -> "sqlite"
        lower.contains("cancel") -> "cancelled"
        lower.contains("illegalstate") || lower.contains("open") -> "misuse"
        else -> "unknown"
    }
    return SQLiteNowCoreRuntimeErrorPayload(
        category = category,
        code = t::class.simpleName ?: category,
        message = message,
    )
}

private fun NSData.toByteArray(): ByteArray {
    val byteCount = length.toInt()
    if (byteCount == 0) return ByteArray(0)
    val source = requireNotNull(bytes) { "NSData.bytes was null for non-empty data" }
    val result = ByteArray(byteCount)
    result.usePinned { pinned ->
        memcpy(pinned.addressOf(0), source, length)
    }
    return result
}

private fun ByteArray.toNSData(): NSData {
    if (isEmpty()) return NSData()
    return usePinned { pinned ->
        NSData.create(bytes = pinned.addressOf(0), length = size.convert())
    }
}
