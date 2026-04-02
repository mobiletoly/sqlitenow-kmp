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
import dev.goquick.sqlitenow.core.sqlite.getColumnNames
import dev.goquick.sqlitenow.core.sqlite.use

internal data class ValidatedSyncTable(
    val tableName: String,
    val syncKeyColumnName: String,
)

internal data class ValidatedConfig(
    val schema: String,
    val tables: List<ValidatedSyncTable>,
    val pkByTable: Map<String, String>,
    val keyByTable: Map<String, List<String>>,
    val tableOrder: Map<String, Int>,
    val tableInfoByName: Map<String, TableInfo>,
)

internal class SyncRuntimeInitializer(
    private val config: OversqliteConfig,
    private val tableInfoCache: TableInfoCache,
) {
    suspend fun prepareLocalRuntime(
        db: SafeSQLiteConnection,
        managedTableStore: OversqliteManagedTableStore,
    ): ValidatedConfig {
        require(config.schema.isNotBlank()) { "config.schema must be provided" }
        initializeDatabase(db)
        managedTableStore.initializeControlTables()
        val validated = validateConfig(db)
        managedTableStore.registerManagedTables(validated)
        installTriggers(db, validated)
        return validated
    }

    suspend fun capturePreexistingAnonymousRows(
        db: SafeSQLiteConnection,
        validated: ValidatedConfig,
    ) {
        ensureInitialBindStateClear(db)
        adoptExistingManagedRows(db, validated)
    }

    internal suspend fun initializeDatabase(db: SafeSQLiteConnection) {
        db.execSQL("PRAGMA foreign_keys = ON")
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_apply_state (
              singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
              apply_mode INTEGER NOT NULL DEFAULT 0
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            INSERT INTO _sync_apply_state(singleton_key, apply_mode)
            VALUES(1, 0)
            ON CONFLICT(singleton_key) DO NOTHING
            """.trimIndent(),
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_row_state (
              schema_name TEXT NOT NULL,
              table_name TEXT NOT NULL,
              key_json TEXT NOT NULL,
              row_version INTEGER NOT NULL DEFAULT 0,
              deleted INTEGER NOT NULL DEFAULT 0,
              updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
              PRIMARY KEY (schema_name, table_name, key_json)
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_dirty_rows (
              schema_name TEXT NOT NULL,
              table_name TEXT NOT NULL,
              key_json TEXT NOT NULL,
              op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
              base_row_version INTEGER NOT NULL DEFAULT 0,
              payload TEXT,
              dirty_ordinal INTEGER NOT NULL DEFAULT 0,
              updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
              PRIMARY KEY (schema_name, table_name, key_json)
            )
            """.trimIndent()
        )
        db.execSQL("CREATE INDEX IF NOT EXISTS idx_sync_dirty_rows_dirty_ordinal ON _sync_dirty_rows(dirty_ordinal)")
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_snapshot_stage (
              snapshot_id TEXT NOT NULL,
              row_ordinal INTEGER NOT NULL,
              schema_name TEXT NOT NULL,
              table_name TEXT NOT NULL,
              key_json TEXT NOT NULL,
              row_version INTEGER NOT NULL,
              payload TEXT NOT NULL,
              PRIMARY KEY (snapshot_id, row_ordinal)
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_source_state (
              source_id TEXT NOT NULL PRIMARY KEY,
              next_source_bundle_id INTEGER NOT NULL DEFAULT 1,
              replaced_by_source_id TEXT NOT NULL DEFAULT '',
              created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_attachment_state (
              singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
              current_source_id TEXT NOT NULL DEFAULT '',
              binding_state TEXT NOT NULL DEFAULT 'anonymous' CHECK (binding_state IN ('anonymous', 'attached')),
              attached_user_id TEXT NOT NULL DEFAULT '',
              schema_name TEXT NOT NULL DEFAULT '',
              last_bundle_seq_seen INTEGER NOT NULL DEFAULT 0,
              rebuild_required INTEGER NOT NULL DEFAULT 0,
              pending_initialization_id TEXT NOT NULL DEFAULT ''
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            INSERT INTO _sync_attachment_state(
              singleton_key,
              current_source_id,
              binding_state,
              attached_user_id,
              schema_name,
              last_bundle_seq_seen,
              rebuild_required,
              pending_initialization_id
            )
            VALUES(1, '', 'anonymous', '', '', 0, 0, '')
            ON CONFLICT(singleton_key) DO NOTHING
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_operation_state (
              singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
              kind TEXT NOT NULL DEFAULT 'none' CHECK (kind IN ('none', 'remote_replace', 'source_recovery')),
              target_user_id TEXT NOT NULL DEFAULT '',
              staged_snapshot_id TEXT NOT NULL DEFAULT '',
              snapshot_bundle_seq INTEGER NOT NULL DEFAULT 0,
              snapshot_row_count INTEGER NOT NULL DEFAULT 0,
              source_recovery_reason TEXT NOT NULL DEFAULT '',
              source_recovery_source_id TEXT NOT NULL DEFAULT '',
              source_recovery_source_bundle_id INTEGER NOT NULL DEFAULT 0,
              source_recovery_intent_state TEXT NOT NULL DEFAULT ''
            )
            """.trimIndent()
        )
        ensureOperationStateSchema(db)
        db.execSQL(
            """
            INSERT INTO _sync_operation_state(
              singleton_key,
              kind,
              target_user_id,
              staged_snapshot_id,
              snapshot_bundle_seq,
              snapshot_row_count,
              source_recovery_reason,
              source_recovery_source_id,
              source_recovery_source_bundle_id,
              source_recovery_intent_state
            )
            VALUES(1, 'none', '', '', 0, 0, '', '', 0, '')
            ON CONFLICT(singleton_key) DO NOTHING
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_outbox_bundle (
              singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
              state TEXT NOT NULL DEFAULT 'none' CHECK (state IN ('none', 'prepared', 'committed_remote')),
              source_id TEXT NOT NULL DEFAULT '',
              source_bundle_id INTEGER NOT NULL DEFAULT 0,
              initialization_id TEXT NOT NULL DEFAULT '',
              canonical_request_hash TEXT NOT NULL DEFAULT '',
              row_count INTEGER NOT NULL DEFAULT 0,
              remote_bundle_hash TEXT NOT NULL DEFAULT '',
              remote_bundle_seq INTEGER NOT NULL DEFAULT 0
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            INSERT INTO _sync_outbox_bundle(
              singleton_key,
              state,
              source_id,
              source_bundle_id,
              initialization_id,
              canonical_request_hash,
              row_count,
              remote_bundle_hash,
              remote_bundle_seq
            )
            VALUES(1, 'none', '', 0, '', '', 0, '', 0)
            ON CONFLICT(singleton_key) DO NOTHING
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_outbox_rows (
              source_bundle_id INTEGER NOT NULL,
              row_ordinal INTEGER NOT NULL,
              schema_name TEXT NOT NULL,
              table_name TEXT NOT NULL,
              key_json TEXT NOT NULL,
              wire_key_json TEXT NOT NULL,
              op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
              base_row_version INTEGER NOT NULL DEFAULT 0,
              local_payload TEXT,
              wire_payload TEXT,
              PRIMARY KEY (source_bundle_id, row_ordinal)
            )
            """.trimIndent()
        )
        db.execSQL("UPDATE _sync_apply_state SET apply_mode = 0 WHERE singleton_key = 1")
    }

    private suspend fun ensureOperationStateSchema(db: SafeSQLiteConnection) {
        val (createSql, columnNames) = db.withExclusiveAccess {
            val createSql = db.prepare(
                """
                SELECT sql
                FROM sqlite_master
                WHERE type = 'table' AND name = '_sync_operation_state'
                """.trimIndent(),
            ).use { st ->
                if (!st.step() || st.isNull(0)) {
                    ""
                } else {
                    st.getText(0)
                }
            }
            val columnNames = db.prepare("PRAGMA table_info(_sync_operation_state)").use { st ->
                buildSet {
                    while (st.step()) {
                        add(st.getText(1).lowercase())
                    }
                }
            }
            createSql to columnNames
        }
        val hasCurrentSchema =
            "source_recovery" in createSql &&
                "source_recovery_reason" in columnNames &&
                "source_recovery_source_id" in columnNames &&
                "source_recovery_source_bundle_id" in columnNames &&
                "source_recovery_intent_state" in columnNames
        if (hasCurrentSchema) {
            return
        }

        val existingRow = db.withExclusiveAccess {
            db.prepare(
                """
                SELECT kind, target_user_id, staged_snapshot_id, snapshot_bundle_seq, snapshot_row_count
                FROM _sync_operation_state
                WHERE singleton_key = 1
                """.trimIndent(),
            ).use { st ->
                if (!st.step()) {
                    null
                } else {
                    OversqliteOperationState(
                        kind = st.getText(0),
                        targetUserId = st.getText(1),
                        stagedSnapshotId = st.getText(2),
                        snapshotBundleSeq = st.getLong(3),
                        snapshotRowCount = st.getLong(4),
                    )
                }
            }
        }

        db.execSQL("DROP TABLE _sync_operation_state")
        db.execSQL(
            """
            CREATE TABLE _sync_operation_state (
              singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
              kind TEXT NOT NULL DEFAULT 'none' CHECK (kind IN ('none', 'remote_replace', 'source_recovery')),
              target_user_id TEXT NOT NULL DEFAULT '',
              staged_snapshot_id TEXT NOT NULL DEFAULT '',
              snapshot_bundle_seq INTEGER NOT NULL DEFAULT 0,
              snapshot_row_count INTEGER NOT NULL DEFAULT 0,
              source_recovery_reason TEXT NOT NULL DEFAULT '',
              source_recovery_source_id TEXT NOT NULL DEFAULT '',
              source_recovery_source_bundle_id INTEGER NOT NULL DEFAULT 0,
              source_recovery_intent_state TEXT NOT NULL DEFAULT ''
            )
            """.trimIndent(),
        )
        existingRow?.let { state ->
            db.execSQL(
                """
                INSERT INTO _sync_operation_state(
                  singleton_key,
                  kind,
                  target_user_id,
                  staged_snapshot_id,
                  snapshot_bundle_seq,
                  snapshot_row_count,
                  source_recovery_reason,
                  source_recovery_source_id,
                  source_recovery_source_bundle_id,
                  source_recovery_intent_state
                )
                VALUES(
                  1,
                  ${sqlStringLiteral(state.kind)},
                  ${sqlStringLiteral(state.targetUserId)},
                  ${sqlStringLiteral(state.stagedSnapshotId)},
                  ${state.snapshotBundleSeq},
                  ${state.snapshotRowCount},
                  '',
                  '',
                  0,
                  ''
                )
                """.trimIndent(),
            )
        }
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

    private fun sqlStringLiteral(value: String): String {
        return "'" + value.replace("'", "''") + "'"
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
        val rowStateCount = scalarCount(db, "SELECT COUNT(*) FROM _sync_row_state")
        val dirtyCount = scalarCount(db, "SELECT COUNT(*) FROM _sync_dirty_rows")
        val outboxRowCount = scalarCount(db, "SELECT COUNT(*) FROM _sync_outbox_rows")
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
        val statementCache = StatementCache(db)
        try {
            var dirtyOrdinal = 0L
            val orderedTables = validated.tables.sortedBy { validated.tableOrder[it.tableName] ?: Int.MAX_VALUE }
            for (table in orderedTables) {
                dirtyOrdinal = adoptExistingRowsForTable(
                    db = db,
                    validated = validated,
                    table = table,
                    startingDirtyOrdinal = dirtyOrdinal,
                    statementCache = statementCache,
                )
            }
        } finally {
            statementCache.close()
        }
    }

    private suspend fun adoptExistingRowsForTable(
        db: SafeSQLiteConnection,
        validated: ValidatedConfig,
        table: ValidatedSyncTable,
        startingDirtyOrdinal: Long,
        statementCache: StatementCache,
    ): Long {
        val tableInfo = validated.tableInfoByName[table.tableName]
            ?: tableInfoCache.get(db, table.tableName)
        val keyExpr = buildKeyJsonObjectExprHexAware(tableInfo, table.syncKeyColumnName, "existing_row")
        val payloadExpr = buildJsonObjectExprHexAware(tableInfo, "existing_row")
        var nextDirtyOrdinal = startingDirtyOrdinal
        db.withExclusiveAccess {
            db.prepare(
                """
                SELECT $keyExpr, $payloadExpr
                FROM ${quoteIdent(table.tableName)} existing_row
                ORDER BY existing_row.${quoteIdent(table.syncKeyColumnName)}
                """.trimIndent()
            ).use { st ->
                while (st.step()) {
                    nextDirtyOrdinal++
                    insertDirtyRow(
                        db = db,
                        schemaName = validated.schema,
                        tableName = table.tableName,
                        keyJson = st.getText(0),
                        payload = st.getText(1),
                        dirtyOrdinal = nextDirtyOrdinal,
                        statementCache = statementCache,
                    )
                }
            }
        }
        return nextDirtyOrdinal
    }

    private suspend fun insertDirtyRow(
        db: SafeSQLiteConnection,
        schemaName: String,
        tableName: String,
        keyJson: String,
        payload: String,
        dirtyOrdinal: Long,
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = """
                INSERT INTO _sync_dirty_rows(
                  schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at
                )
                VALUES(?, ?, ?, 'INSERT', 0, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            st.bindText(1, schemaName)
            st.bindText(2, tableName)
            st.bindText(3, keyJson)
            st.bindText(4, payload)
            st.bindLong(5, dirtyOrdinal)
            st.step()
        }
    }

    private suspend fun scalarCount(
        db: SafeSQLiteConnection,
        sql: String,
    ): Long {
        return db.withExclusiveAccess {
            db.prepare(sql).use { st ->
                check(st.step())
                st.getLong(0)
            }
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

    private suspend fun installTriggers(
        db: SafeSQLiteConnection,
        validated: ValidatedConfig,
    ) {
        for (table in validated.tables) {
            createTriggersForTable(db, validated, table)
        }
    }

    private suspend fun createTriggersForTable(
        db: SafeSQLiteConnection,
        validated: ValidatedConfig,
        table: ValidatedSyncTable,
    ) {
        val tableInfo = validated.tableInfoByName[table.tableName]
            ?: tableInfoCache.get(db, table.tableName)
        val pkColumn = table.syncKeyColumnName
        val payloadExpr = buildJsonObjectExprHexAware(tableInfo, "NEW")
        val oldKeyExpr = buildKeyJsonObjectExprHexAware(tableInfo, pkColumn, "OLD")
        val newKeyExpr = buildKeyJsonObjectExprHexAware(tableInfo, pkColumn, "NEW")

        val data = TriggerData(
            schemaName = validated.schema,
            tableName = table.tableName,
            newRowJson = payloadExpr,
            oldKeyJson = oldKeyExpr,
            newKeyJson = newKeyExpr,
        )

        val triggers = listOf(
            "trg_${table.tableName}_bi_guard" to guardInsertTriggerSql(table.tableName),
            "trg_${table.tableName}_bu_guard" to guardUpdateTriggerSql(table.tableName),
            "trg_${table.tableName}_bd_guard" to guardDeleteTriggerSql(table.tableName),
            "trg_${table.tableName}_ai" to insertTriggerSql(data),
            "trg_${table.tableName}_au" to updateTriggerSql(data),
            "trg_${table.tableName}_ad" to deleteTriggerSql(data),
        )
        val existingSqlByName = loadExistingTriggerSqlByName(db, table.tableName)

        for ((name, sql) in triggers) {
            val existingSql = existingSqlByName[name]
            if (existingSql != null && normalizeTriggerSql(existingSql) == normalizeTriggerSql(sql)) {
                continue
            }
            if (existingSql != null) {
                db.execSQL("DROP TRIGGER IF EXISTS ${quoteIdent(name)}")
            }
            db.execSQL(sql)
        }
    }

    private suspend fun loadExistingTriggerSqlByName(
        db: SafeSQLiteConnection,
        tableName: String,
    ): Map<String, String> {
        return db.withExclusiveAccess {
            val sqlByName = linkedMapOf<String, String>()
            db.prepare(
                """
                SELECT name, sql
                FROM sqlite_master
                WHERE type = 'trigger' AND tbl_name = ?
                ORDER BY name
                """.trimIndent()
            ).use { st ->
                st.bindText(1, tableName)
                while (st.step()) {
                    if (st.isNull(1)) {
                        continue
                    }
                    sqlByName[st.getText(0)] = st.getText(1)
                }
            }
            sqlByName
        }
    }
}

private data class TriggerData(
    val schemaName: String,
    val tableName: String,
    val newRowJson: String,
    val oldKeyJson: String,
    val newKeyJson: String,
)

private val collapseWhitespaceRegex = Regex("\\s+")
private val createTriggerIfNotExistsRegex = Regex(
    pattern = "^CREATE\\s+TRIGGER\\s+IF\\s+NOT\\s+EXISTS\\s+",
    options = setOf(RegexOption.IGNORE_CASE),
)
private val createTriggerRegex = Regex(
    pattern = "^CREATE\\s+TRIGGER\\s+",
    options = setOf(RegexOption.IGNORE_CASE),
)

private fun normalizeTriggerSql(sql: String): String {
    val collapsed = collapseWhitespaceRegex.replace(sql.trim(), " ")
    return when {
        createTriggerIfNotExistsRegex.containsMatchIn(collapsed) ->
            createTriggerIfNotExistsRegex.replace(collapsed, "CREATE TRIGGER ")
        createTriggerRegex.containsMatchIn(collapsed) ->
            createTriggerRegex.replace(collapsed, "CREATE TRIGGER ")
        else -> collapsed
    }
}

private fun insertTriggerSql(data: TriggerData): String = """
CREATE TRIGGER IF NOT EXISTS trg_${data.tableName}_ai
AFTER INSERT ON ${quoteIdent(data.tableName)}
WHEN COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) = 0
BEGIN
  INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
  VALUES (
    '${data.schemaName}',
    '${data.tableName}',
    ${data.newKeyJson},
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM _sync_row_state
        WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.newKeyJson} AND deleted=0
      ) THEN 'UPDATE'
      ELSE 'INSERT'
    END,
    COALESCE(
      (SELECT base_row_version FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.newKeyJson}),
      (SELECT row_version FROM _sync_row_state WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.newKeyJson}),
      0
    ),
    ${data.newRowJson},
    COALESCE(
      (SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.newKeyJson}),
      (SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
    ),
    strftime('%Y-%m-%dT%H:%M:%fZ','now')
  )
  ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
    op=excluded.op,
    payload=excluded.payload,
    updated_at=excluded.updated_at;
END
""".trimIndent()

private fun guardInsertTriggerSql(tableName: String): String = """
CREATE TRIGGER IF NOT EXISTS trg_${tableName}_bi_guard
BEFORE INSERT ON ${quoteIdent(tableName)}
WHEN EXISTS (
  SELECT 1
  FROM _sync_operation_state
  WHERE singleton_key = 1
    AND kind != 'none'
)
  AND NOT EXISTS (
    SELECT 1
    FROM _sync_apply_state
    WHERE singleton_key = 1
      AND apply_mode = 1
  )
BEGIN
  SELECT RAISE(ABORT, 'SYNC_TRANSITION_PENDING');
END
""".trimIndent()

private fun guardUpdateTriggerSql(tableName: String): String = """
CREATE TRIGGER IF NOT EXISTS trg_${tableName}_bu_guard
BEFORE UPDATE ON ${quoteIdent(tableName)}
WHEN EXISTS (
  SELECT 1
  FROM _sync_operation_state
  WHERE singleton_key = 1
    AND kind != 'none'
)
  AND NOT EXISTS (
    SELECT 1
    FROM _sync_apply_state
    WHERE singleton_key = 1
      AND apply_mode = 1
  )
BEGIN
  SELECT RAISE(ABORT, 'SYNC_TRANSITION_PENDING');
END
""".trimIndent()

private fun guardDeleteTriggerSql(tableName: String): String = """
CREATE TRIGGER IF NOT EXISTS trg_${tableName}_bd_guard
BEFORE DELETE ON ${quoteIdent(tableName)}
WHEN EXISTS (
  SELECT 1
  FROM _sync_operation_state
  WHERE singleton_key = 1
    AND kind != 'none'
)
  AND NOT EXISTS (
    SELECT 1
    FROM _sync_apply_state
    WHERE singleton_key = 1
      AND apply_mode = 1
  )
BEGIN
  SELECT RAISE(ABORT, 'SYNC_TRANSITION_PENDING');
END
""".trimIndent()

private fun updateTriggerSql(data: TriggerData): String = """
CREATE TRIGGER IF NOT EXISTS trg_${data.tableName}_au
AFTER UPDATE ON ${quoteIdent(data.tableName)}
WHEN COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) = 0
BEGIN
  INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
  SELECT
    '${data.schemaName}',
    '${data.tableName}',
    ${data.oldKeyJson},
    'DELETE',
    COALESCE(
      (SELECT base_row_version FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.oldKeyJson}),
      (SELECT row_version FROM _sync_row_state WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.oldKeyJson}),
      0
    ),
    NULL,
    COALESCE(
      (SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.oldKeyJson}),
      (SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
    ),
    strftime('%Y-%m-%dT%H:%M:%fZ','now')
  WHERE ${data.oldKeyJson} != ${data.newKeyJson}
  ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
    op='DELETE',
    payload=NULL,
    updated_at=excluded.updated_at;

  INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
  VALUES (
    '${data.schemaName}',
    '${data.tableName}',
    ${data.newKeyJson},
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM _sync_row_state
        WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.newKeyJson} AND deleted=0
      ) THEN 'UPDATE'
      ELSE 'INSERT'
    END,
    COALESCE(
      (SELECT base_row_version FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.newKeyJson}),
      (SELECT row_version FROM _sync_row_state WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.newKeyJson}),
      0
    ),
    ${data.newRowJson},
    COALESCE(
      (SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.newKeyJson}),
      (SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
    ),
    strftime('%Y-%m-%dT%H:%M:%fZ','now')
  )
  ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
    op=excluded.op,
    payload=excluded.payload,
    updated_at=excluded.updated_at;
END
""".trimIndent()

private fun deleteTriggerSql(data: TriggerData): String = """
CREATE TRIGGER IF NOT EXISTS trg_${data.tableName}_ad
AFTER DELETE ON ${quoteIdent(data.tableName)}
WHEN COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) = 0
BEGIN
  INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
  VALUES (
    '${data.schemaName}',
    '${data.tableName}',
    ${data.oldKeyJson},
    'DELETE',
    COALESCE(
      (SELECT base_row_version FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.oldKeyJson}),
      (SELECT row_version FROM _sync_row_state WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.oldKeyJson}),
      0
    ),
    NULL,
    COALESCE(
      (SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=${data.oldKeyJson}),
      (SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
    ),
    strftime('%Y-%m-%dT%H:%M:%fZ','now')
  )
  ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
    op='DELETE',
    payload=NULL,
    updated_at=excluded.updated_at;
END
""".trimIndent()

internal fun buildJsonObjectExprHexAware(
    tableInfo: TableInfo,
    prefix: String,
): String {
    val pairs = tableInfo.columns.map { column ->
        val name = column.name.lowercase()
        val valueExpr = if (column.kind.isBlobKind()) {
            "CASE WHEN $prefix.${quoteIdent(column.name)} IS NULL THEN NULL ELSE lower(hex($prefix.${quoteIdent(column.name)})) END"
        } else {
            "$prefix.${quoteIdent(column.name)}"
        }
        "'$name', $valueExpr"
    }
    return "json_object(${pairs.joinToString(", ")})"
}

internal fun buildKeyJsonObjectExprHexAware(
    tableInfo: TableInfo,
    keyColumn: String,
    prefix: String,
): String {
    val column = tableInfo.columns.firstOrNull { it.name.equals(keyColumn, ignoreCase = true) }
        ?: error("table ${tableInfo.table} is missing sync key column $keyColumn")
    val keyName = column.name.lowercase()
    val valueExpr = if (column.kind.isBlobKind()) {
        "lower(hex($prefix.${quoteIdent(column.name)}))"
    } else {
        "$prefix.${quoteIdent(column.name)}"
    }
    return "json_object('$keyName', $valueExpr)"
}

internal fun quoteIdent(identifier: String): String = "\"${identifier.replace("\"", "\"\"")}\""
