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
package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use

internal class SyncRuntimeInitializer(
    private val config: OversqliteConfig,
    private val tableInfoCache: TableInfoCache,
    private val reusableStatementCleanup: ReusableStatementCleanup = DefaultReusableStatementCleanup,
) {
    suspend fun prepareLocalRuntime(
        db: SafeSQLiteConnection,
        managedTableStore: OversqliteManagedTableStore,
    ): ValidatedConfig {
        require(config.schema.isNotBlank()) { "config.schema must be provided" }
        val validated = validateConfig(db)
        initializeOversqliteControlTables(db)
        managedTableStore.registerManagedTables(validated)
        installOversqliteTriggers(db, validated, tableInfoCache)
        return validated
    }

    suspend fun capturePreexistingAnonymousRows(
        db: SafeSQLiteConnection,
        validated: ValidatedConfig,
    ) {
        ensureInitialBindStateClear(db)
        adoptExistingManagedRows(db, validated)
    }

    private suspend fun validateConfig(db: SafeSQLiteConnection): ValidatedConfig {
        val schema = config.schema.trim()
        val pkByTable = linkedMapOf<String, String>()
        val keyByTable = linkedMapOf<String, List<String>>()
        val tableInfoByName = linkedMapOf<String, TableInfo>()
        val validatedTables = mutableListOf<ValidatedSyncTable>()
        val managedTables = linkedSetOf<String>()

        for (syncTable in config.syncTables) {
            val tableName = syncTable.tableName.trim().lowercase()
            require(tableName.isNotBlank()) { "sync table name must be provided" }
            require(!tableName.contains(".")) {
                "table ${syncTable.tableName} must not include a schema qualifier; oversqlite supports exactly one config.schema per local database"
            }
            require(tableName !in managedTables) { "duplicate sync table registration for ${syncTable.tableName}" }
            managedTables += tableName

            val keyColumns = normalizedSyncKeyColumns(syncTable)
            require(keyColumns.size == 1) {
                "table ${syncTable.tableName} must declare exactly one sync key column in the current client runtime"
            }

			val tableInfo = tableInfoCache.get(db, tableName)
            require(hiddenSyncScopeColumnName.lowercase() !in tableInfo.columnNamesLower) {
                "table ${syncTable.tableName} must not declare reserved server column $hiddenSyncScopeColumnName in local oversqlite schema"
            }
            tableInfoByName[tableName] = tableInfo
            val syncKeyColumn = configuredPrimaryKeyColumn(tableInfo, syncTable, keyColumns.single())
            pkByTable[tableName] = syncKeyColumn
            keyByTable[tableName] = listOf(syncKeyColumn)
            validatedTables += ValidatedSyncTable(tableName = tableName, syncKeyColumnName = syncKeyColumn)
        }

        validateManagedForeignKeyClosure(tableInfoByName, managedTables)
        val tableOrder = computeManagedTableOrder(tableInfoByName, validatedTables)
        return ValidatedConfig(
            schema = schema,
            tables = validatedTables,
            pkByTable = pkByTable,
            keyByTable = keyByTable,
            tableOrder = tableOrder,
            tableInfoByName = tableInfoByName,
        )
    }

    private fun normalizedSyncKeyColumns(syncTable: SyncTable): List<String> {
        val explicit = syncTable.syncKeyColumns
            .map { it.trim() }
            .filter { it.isNotEmpty() }
            .distinctBy { it.lowercase() }
        if (explicit.isNotEmpty()) {
            return explicit
        }

        val single = syncTable.syncKeyColumnName?.trim().orEmpty()
        require(single.isNotEmpty()) {
            "table ${syncTable.tableName} must declare syncKeyColumnName or syncKeyColumns explicitly"
        }
        return listOf(single)
    }

    private fun configuredPrimaryKeyColumn(
        tableInfo: TableInfo,
        syncTable: SyncTable,
        configuredKeyColumn: String,
    ): String {
        val primaryKeyColumns = tableInfo.columns.filter { it.isPrimaryKey }
        require(primaryKeyColumns.size == 1) {
            "table ${syncTable.tableName} must declare exactly one local SQLite PRIMARY KEY column for oversqlite sync"
        }

        val primaryKeyColumn = primaryKeyColumns.single()
        for (column in tableInfo.columns) {
            if (!column.name.equals(configuredKeyColumn, ignoreCase = true)) {
                continue
            }
            require(column.isPrimaryKey) {
                "configured primary key column ${column.name} for table ${syncTable.tableName} is not declared as PRIMARY KEY"
            }
            require(primaryKeyColumn.name.equals(column.name, ignoreCase = true)) {
                "table ${syncTable.tableName} must use its only local SQLite PRIMARY KEY column as the visible sync key"
            }
            require(column.kind == ColumnKind.TEXT || column.kind.isBlobKind()) {
                "configured primary key column ${column.name} for table ${syncTable.tableName} must be TEXT PRIMARY KEY or BLOB PRIMARY KEY"
            }
            require(column.notNull) {
                "configured visible sync-key column ${column.name} for table ${syncTable.tableName} must declare NOT NULL explicitly; repair or recreate the local application database before sync initialization"
            }
            return column.name
        }
        error("table ${syncTable.tableName} does not contain configured primary key column $configuredKeyColumn")
    }

    private fun validateManagedForeignKeyClosure(
        tableInfoByName: Map<String, TableInfo>,
        managedTables: Set<String>,
    ) {
        for (tableName in managedTables) {
            val tableInfo = tableInfoByName[tableName]
                ?: error("managed table $tableName is missing cached table info")
            val compositeRefs = mutableListOf<String>()
            val missingRefs = mutableListOf<String>()
            for (foreignKey in tableInfo.foreignKeys) {
                val refTable = foreignKey.refTable
                if (refTable.isEmpty()) {
                    continue
                }
                if (foreignKey.seq > 0) {
                    compositeRefs += "$tableName -> $refTable"
                    continue
                }
                if (refTable !in managedTables) {
                    missingRefs += "$tableName.${foreignKey.fromCol} -> $refTable.${foreignKey.toCol}"
                }
            }
            require(compositeRefs.isEmpty()) {
                "managed tables contain unsupported composite foreign keys: ${compositeRefs.sorted().joinToString("; ")}"
            }
            require(missingRefs.isEmpty()) {
                "managed tables are not FK-closed: ${missingRefs.sorted().joinToString("; ")}"
            }
        }
    }

    private fun computeManagedTableOrder(
        tableInfoByName: Map<String, TableInfo>,
        tables: List<ValidatedSyncTable>,
    ): Map<String, Int> {
        val originalOrder = tables.mapIndexed { index, table -> table.tableName to index }.toMap()
        val managed = tables.map { it.tableName }.toSet()
        val dependents = managed.associateWith { linkedSetOf<String>() }.toMutableMap()
        val inDegree = managed.associateWith { 0 }.toMutableMap()

        for (table in tables) {
            val tableInfo = tableInfoByName[table.tableName]
                ?: error("managed table ${table.tableName} is missing cached table info")
            for (foreignKey in tableInfo.foreignKeys) {
                val refTable = foreignKey.refTable
                if (refTable.isEmpty() || refTable == table.tableName || refTable !in managed) {
                    continue
                }
                if (dependents.getValue(refTable).add(table.tableName)) {
                    inDegree[table.tableName] = inDegree.getValue(table.tableName) + 1
                }
            }
        }

        val queue = managed.filter { inDegree.getValue(it) == 0 }.sortedBy { originalOrder.getValue(it) }.toMutableList()
        val ordered = mutableListOf<String>()
        while (queue.isNotEmpty()) {
            val current = queue.removeAt(0)
            ordered += current
            val children = dependents.getValue(current).sortedBy { originalOrder.getValue(it) }
            for (child in children) {
                val next = inDegree.getValue(child) - 1
                inDegree[child] = next
                if (next == 0) {
                    queue += child
                    queue.sortBy { originalOrder.getValue(it) }
                }
            }
        }

        for (table in tables) {
            if (table.tableName !in ordered) {
                ordered += table.tableName
            }
        }
        return ordered.mapIndexed { index, tableName -> tableName to index }.toMap()
    }

    private suspend fun ensureInitialBindStateClear(
        db: SafeSQLiteConnection,
    ) {
        val rowStateCount = db.scalarLong("SELECT COUNT(*) FROM _sync_row_state")
        val dirtyCount = db.scalarLong("SELECT COUNT(*) FROM _sync_dirty_rows")
        val outboxRowCount = db.scalarLong("SELECT COUNT(*) FROM _sync_outbox_rows")
        val incompatibilities = buildList {
            if (rowStateCount > 0) add("_sync_row_state=$rowStateCount")
            if (dirtyCount > 0) add("_sync_dirty_rows=$dirtyCount")
            if (outboxRowCount > 0) add("_sync_outbox_rows=$outboxRowCount")
        }
        require(incompatibilities.isEmpty()) {
            "oversqlite cannot treat this database as first local capture because existing sync state is already present: " +
                incompatibilities.joinToString(", ")
        }
    }

    private suspend fun adoptExistingManagedRows(
        db: SafeSQLiteConnection,
        validated: ValidatedConfig,
    ) {
        var dirtyOrdinal = 0L
        val orderedTables = validated.tables.sortedBy { validated.tableOrder[it.tableName] ?: Int.MAX_VALUE }
        for (table in orderedTables) {
            dirtyOrdinal = adoptExistingRowsForTable(
                db = db,
                validated = validated,
                table = table,
                startingDirtyOrdinal = dirtyOrdinal,
            )
        }
    }

    private suspend fun adoptExistingRowsForTable(
        db: SafeSQLiteConnection,
        validated: ValidatedConfig,
        table: ValidatedSyncTable,
        startingDirtyOrdinal: Long,
    ): Long {
        val tableInfo = validated.tableInfoByName[table.tableName]
            ?: tableInfoCache.get(db, table.tableName)
        val keyExpr = buildKeyJsonObjectExprHexAware(tableInfo, table.syncKeyColumnName, "existing_row")
        val payloadExpr = buildJsonObjectExprHexAware(tableInfo, "existing_row")
        val keyColumn = tableInfo.column(table.syncKeyColumnName)
        require(keyColumn.kind == ColumnKind.TEXT || keyColumn.kind.isBlobKind()) {
            "sync key column must use TEXT or BLOB affinity"
        }
        val quotedTable = quoteIdent(table.tableName)
        val quotedKey = quoteIdent(keyColumn.name)
        val firstRowSql = """
            SELECT existing_row.$quotedKey, $keyExpr, $payloadExpr
            FROM $quotedTable existing_row
            ORDER BY existing_row.$quotedKey
            LIMIT 1
        """.trimIndent()
        val nextRowSql = """
            SELECT existing_row.$quotedKey, $keyExpr, $payloadExpr
            FROM $quotedTable existing_row
            WHERE existing_row.$quotedKey > ?
            ORDER BY existing_row.$quotedKey
            LIMIT 1
        """.trimIndent()
        val insertSql = """
            INSERT INTO _sync_dirty_rows(
              schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at
            )
            VALUES(?, ?, ?, 'INSERT', 0, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        """.trimIndent()
        var nextDirtyOrdinal = startingDirtyOrdinal
        db.withExclusiveAccess {
            var row = db.prepare(firstRowSql).use { statement ->
                if (statement.step()) capturedExistingRow(statement, keyColumn) else null
            }
            if (row == null) return@withExclusiveAccess

            db.prepare(insertSql).use { insertStatement ->
                db.prepare(nextRowSql).use { selectStatement ->
                    var insertUsed = false
                    while (row != null) {
                        val current = requireNotNull(row)
                        if (insertUsed) {
                            reusableStatementCleanup(insertStatement)
                        } else {
                            insertUsed = true
                        }
                        nextDirtyOrdinal++
                        insertDirtyRow(
                            schemaName = validated.schema,
                            tableName = table.tableName,
                            keyJson = current.keyJson,
                            payload = current.payload,
                            dirtyOrdinal = nextDirtyOrdinal,
                            statement = insertStatement,
                        )

                        bindCapturedKey(selectStatement, current.key)
                        row = if (selectStatement.step()) {
                            capturedExistingRow(selectStatement, keyColumn)
                        } else {
                            null
                        }
                        reusableStatementCleanup(selectStatement)
                    }
                }
            }
        }
        return nextDirtyOrdinal
    }

    private suspend fun insertDirtyRow(
        schemaName: String,
        tableName: String,
        keyJson: String,
        payload: String,
        dirtyOrdinal: Long,
        statement: dev.goquick.sqlitenow.core.sqlite.SqliteStatement,
    ) {
        statement.bindText(1, schemaName)
        statement.bindText(2, tableName)
        statement.bindText(3, keyJson)
        statement.bindText(4, payload)
        statement.bindLong(5, dirtyOrdinal)
        statement.step()
    }

    private fun capturedExistingRow(
        statement: dev.goquick.sqlitenow.core.sqlite.SqliteStatement,
        keyColumn: ColumnInfo,
    ): CapturedExistingRow = CapturedExistingRow(
        key = if (keyColumn.kind.isBlobKind()) {
            CapturedSyncKey.Blob(statement.getBlob(0))
        } else {
            CapturedSyncKey.Text(statement.getText(0))
        },
        keyJson = statement.getText(1),
        payload = statement.getText(2),
    )

    private fun bindCapturedKey(
        statement: dev.goquick.sqlitenow.core.sqlite.SqliteStatement,
        key: CapturedSyncKey,
    ) {
        when (key) {
            is CapturedSyncKey.Blob -> statement.bindBlob(1, key.value)
            is CapturedSyncKey.Text -> statement.bindText(1, key.value)
        }
    }

    suspend fun clearManagedState(
        db: SafeSQLiteConnection,
        validated: ValidatedConfig,
    ) {
        for (table in validated.tables) {
            db.execSQL("DELETE FROM ${quoteIdent(table.tableName)}")
            db.withExclusiveAccess {
                db.prepare(
                    "DELETE FROM _sync_row_state WHERE schema_name = ? AND table_name = ?"
                ).use { st ->
                    st.bindText(1, validated.schema)
                    st.bindText(2, table.tableName)
                    st.step()
                }
            }
        }
        db.execSQL("DELETE FROM _sync_dirty_rows")
        db.execSQL("DELETE FROM _sync_outbox_rows")
        db.execSQL(
            """
            UPDATE _sync_outbox_bundle
            SET state = 'none',
                source_id = '',
                source_bundle_id = 0,
                initialization_id = '',
                canonical_request_hash = '',
                row_count = 0,
                remote_bundle_hash = '',
                remote_bundle_seq = 0
            WHERE singleton_key = 1
            """.trimIndent()
        )
    }

}

private data class CapturedExistingRow(
    val key: CapturedSyncKey,
    val keyJson: String,
    val payload: String,
)

private sealed interface CapturedSyncKey {
    data class Blob(val value: ByteArray) : CapturedSyncKey
    data class Text(val value: String) : CapturedSyncKey
}
