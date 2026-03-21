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
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

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
)

internal class SyncBootstrapper(
    private val config: OversqliteConfig,
    private val tableInfoCache: TableInfoCache,
) {
    suspend fun bootstrap(
        db: SafeSQLiteConnection,
        userId: String,
        sourceId: String,
    ): Result<ValidatedConfig> = runCatching {
        require(config.schema.isNotBlank()) { "config.schema must be provided" }
        require(userId.isNotBlank()) { "userId must be provided" }
        require(sourceId.isNotBlank()) { "sourceId must be provided" }

        initializeDatabase(db)
        val validated = validateConfig(db)
        persistClientIdentity(db, validated, userId.trim(), sourceId.trim())
        installTriggers(db, validated)
        validated
    }

    private suspend fun initializeDatabase(db: SafeSQLiteConnection) {
        db.execSQL("PRAGMA foreign_keys = ON")
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_client_state (
              user_id TEXT NOT NULL PRIMARY KEY,
              source_id TEXT NOT NULL,
              schema_name TEXT NOT NULL DEFAULT '',
              next_source_bundle_id INTEGER NOT NULL DEFAULT 1,
              last_bundle_seq_seen INTEGER NOT NULL DEFAULT 0,
              apply_mode INTEGER NOT NULL DEFAULT 0,
              rebuild_required INTEGER NOT NULL DEFAULT 0
            )
            """.trimIndent()
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
            CREATE TABLE IF NOT EXISTS _sync_push_outbound (
              source_bundle_id INTEGER NOT NULL,
              row_ordinal INTEGER NOT NULL,
              schema_name TEXT NOT NULL,
              table_name TEXT NOT NULL,
              key_json TEXT NOT NULL,
              op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
              base_row_version INTEGER NOT NULL DEFAULT 0,
              payload TEXT,
              PRIMARY KEY (source_bundle_id, row_ordinal)
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_push_stage (
              bundle_seq INTEGER NOT NULL,
              row_ordinal INTEGER NOT NULL,
              schema_name TEXT NOT NULL,
              table_name TEXT NOT NULL,
              key_json TEXT NOT NULL,
              op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
              row_version INTEGER NOT NULL,
              payload TEXT,
              PRIMARY KEY (bundle_seq, row_ordinal)
            )
            """.trimIndent()
        )
        db.execSQL("UPDATE _sync_client_state SET apply_mode = 0 WHERE apply_mode = 1")
    }

    private suspend fun validateConfig(db: SafeSQLiteConnection): ValidatedConfig {
        val schema = config.schema.trim()
        val pkByTable = linkedMapOf<String, String>()
        val keyByTable = linkedMapOf<String, List<String>>()
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
            val syncKeyColumn = configuredPrimaryKeyColumn(tableInfo, syncTable, keyColumns.single())
            pkByTable[tableName] = syncKeyColumn
            keyByTable[tableName] = listOf(syncKeyColumn)
            validatedTables += ValidatedSyncTable(tableName = tableName, syncKeyColumnName = syncKeyColumn)
        }

        validateManagedForeignKeyClosure(db, managedTables)
        val tableOrder = computeManagedTableOrder(db, validatedTables)
        return ValidatedConfig(
            schema = schema,
            tables = validatedTables,
            pkByTable = pkByTable,
            keyByTable = keyByTable,
            tableOrder = tableOrder,
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
        for (column in tableInfo.columns) {
            if (!column.name.equals(configuredKeyColumn, ignoreCase = true)) {
                continue
            }
            require(column.isPrimaryKey) {
                "configured primary key column ${column.name} for table ${syncTable.tableName} is not declared as PRIMARY KEY"
            }
            return column.name
        }
        error("table ${syncTable.tableName} does not contain configured primary key column $configuredKeyColumn")
    }

    private suspend fun validateManagedForeignKeyClosure(
        db: SafeSQLiteConnection,
        managedTables: Set<String>,
    ) {
        for (tableName in managedTables) {
            val compositeRefs = mutableListOf<String>()
            val missingRefs = mutableListOf<String>()
            db.prepare("PRAGMA foreign_key_list(${quoteIdent(tableName)})").use { st ->
                val indexes = foreignKeyIndexMap(st)
                while (st.step()) {
                    val seq = st.getLong(indexes["seq"] ?: 1).toInt()
                    val refTable = st.getText(indexes["table"] ?: 2).trim().lowercase()
                    val fromCol = st.getText(indexes["from"] ?: 3)
                    val toCol = st.getText(indexes["to"] ?: 4)
                    if (refTable.isEmpty()) {
                        continue
                    }
                    if (seq > 0) {
                        compositeRefs += "$tableName -> $refTable"
                        continue
                    }
                    if (refTable !in managedTables) {
                        missingRefs += "$tableName.$fromCol -> $refTable.$toCol"
                    }
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

    private suspend fun computeManagedTableOrder(
        db: SafeSQLiteConnection,
        tables: List<ValidatedSyncTable>,
    ): Map<String, Int> {
        val originalOrder = tables.mapIndexed { index, table -> table.tableName to index }.toMap()
        val managed = tables.map { it.tableName }.toSet()
        val dependents = managed.associateWith { linkedSetOf<String>() }.toMutableMap()
        val inDegree = managed.associateWith { 0 }.toMutableMap()

        for (table in tables) {
            db.prepare("PRAGMA foreign_key_list(${quoteIdent(table.tableName)})").use { st ->
                val indexes = foreignKeyIndexMap(st)
                while (st.step()) {
                    val refTable = st.getText(indexes["table"] ?: 2).trim().lowercase()
                    if (refTable.isEmpty() || refTable == table.tableName || refTable !in managed) {
                        continue
                    }
                    if (dependents.getValue(refTable).add(table.tableName)) {
                        inDegree[table.tableName] = inDegree.getValue(table.tableName) + 1
                    }
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

    private fun foreignKeyIndexMap(statement: dev.goquick.sqlitenow.core.sqlite.SqliteStatement): Map<String, Int> {
        return statement.getColumnNames()
            .mapIndexed { index, name -> name.lowercase() to index }
            .toMap()
    }

    private suspend fun persistClientIdentity(
        db: SafeSQLiteConnection,
        validated: ValidatedConfig,
        userId: String,
        sourceId: String,
    ) {
        val identities = mutableListOf<PersistedClientIdentity>()
        db.prepare(
            """
            SELECT user_id, source_id, schema_name
            FROM _sync_client_state
            ORDER BY user_id
            """.trimIndent()
        ).use { st ->
            while (st.step()) {
                identities += PersistedClientIdentity(
                    userId = st.getText(0),
                    sourceId = st.getText(1),
                    schemaName = st.getText(2),
                )
            }
        }

        if (identities.isEmpty()) {
            insertClientIdentity(db, userId, sourceId, config.schema.trim(), rebuildRequired = false)
            return
        }

        if (identities.size == 1 && identities[0].userId == userId) {
            val persisted = identities[0]
            val persistedSchema = persisted.schemaName.trim()
            val configuredSchema = config.schema.trim()
            require(persistedSchema.isEmpty() || persistedSchema == configuredSchema) {
                "client state for user $userId is bound to schema ${persisted.schemaName}, not $configuredSchema"
            }
            if (persisted.sourceId == sourceId) {
                if (persistedSchema.isEmpty()) {
                    db.prepare(
                        """
                        UPDATE _sync_client_state
                        SET schema_name = ?, apply_mode = 0
                        WHERE user_id = ?
                        """.trimIndent()
                    ).use { st ->
                        st.bindText(1, configuredSchema)
                        st.bindText(2, userId)
                        st.step()
                    }
                }
                return
            }
        }

        db.transaction {
            db.execSQL("UPDATE _sync_client_state SET apply_mode = 1")
            clearManagedState(db, validated)
            db.execSQL("DELETE FROM _sync_snapshot_stage")
            db.execSQL("DELETE FROM _sync_client_state")
            insertClientIdentity(db, userId, sourceId, config.schema.trim(), rebuildRequired = true)
        }
    }

    private suspend fun clearManagedState(
        db: SafeSQLiteConnection,
        validated: ValidatedConfig,
    ) {
        for (table in validated.tables) {
            db.execSQL("DELETE FROM ${quoteIdent(table.tableName)}")
            db.prepare(
                "DELETE FROM _sync_row_state WHERE schema_name = ? AND table_name = ?"
            ).use { st ->
                st.bindText(1, validated.schema)
                st.bindText(2, table.tableName)
                st.step()
            }
        }
        db.execSQL("DELETE FROM _sync_dirty_rows")
        db.execSQL("DELETE FROM _sync_push_outbound")
        db.execSQL("DELETE FROM _sync_push_stage")
    }

    private suspend fun insertClientIdentity(
        db: SafeSQLiteConnection,
        userId: String,
        sourceId: String,
        schemaName: String,
        rebuildRequired: Boolean,
    ) {
        db.prepare(
            """
            INSERT INTO _sync_client_state(
              user_id, source_id, schema_name, next_source_bundle_id, last_bundle_seq_seen, apply_mode, rebuild_required
            )
            VALUES(?, ?, ?, 1, 0, 0, ?)
            """.trimIndent()
        ).use { st ->
            st.bindText(1, userId)
            st.bindText(2, sourceId)
            st.bindText(3, schemaName)
            st.bindLong(4, if (rebuildRequired) 1 else 0)
            st.step()
        }
    }

    private suspend fun installTriggers(
        db: SafeSQLiteConnection,
        validated: ValidatedConfig,
    ) {
        for (table in validated.tables) {
            createTriggersForTable(db, validated.schema, table)
        }
    }

    private suspend fun createTriggersForTable(
        db: SafeSQLiteConnection,
        schemaName: String,
        table: ValidatedSyncTable,
    ) {
        val tableInfo = tableInfoCache.get(db, table.tableName)
        val pkColumn = table.syncKeyColumnName
        val payloadExpr = buildJsonObjectExprHexAware(tableInfo, "NEW")
        val oldKeyExpr = buildKeyJsonObjectExprHexAware(tableInfo, pkColumn, "OLD")
        val newKeyExpr = buildKeyJsonObjectExprHexAware(tableInfo, pkColumn, "NEW")

        val data = TriggerData(
            schemaName = schemaName,
            tableName = table.tableName,
            newRowJson = payloadExpr,
            oldKeyJson = oldKeyExpr,
            newKeyJson = newKeyExpr,
        )

        val triggers = listOf(
            "trg_${table.tableName}_ai" to insertTriggerSql(data),
            "trg_${table.tableName}_au" to updateTriggerSql(data),
            "trg_${table.tableName}_ad" to deleteTriggerSql(data),
        )

        for ((name, sql) in triggers) {
            db.execSQL("DROP TRIGGER IF EXISTS ${quoteIdent(name)}")
            db.execSQL(sql)
        }
    }
}

private data class PersistedClientIdentity(
    val userId: String,
    val sourceId: String,
    val schemaName: String,
)

private data class TriggerData(
    val schemaName: String,
    val tableName: String,
    val newRowJson: String,
    val oldKeyJson: String,
    val newKeyJson: String,
)

private fun insertTriggerSql(data: TriggerData): String = """
CREATE TRIGGER IF NOT EXISTS trg_${data.tableName}_ai
AFTER INSERT ON ${quoteIdent(data.tableName)}
WHEN COALESCE((SELECT apply_mode FROM _sync_client_state LIMIT 1), 0) = 0
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

private fun updateTriggerSql(data: TriggerData): String = """
CREATE TRIGGER IF NOT EXISTS trg_${data.tableName}_au
AFTER UPDATE ON ${quoteIdent(data.tableName)}
WHEN COALESCE((SELECT apply_mode FROM _sync_client_state LIMIT 1), 0) = 0
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
WHEN COALESCE((SELECT apply_mode FROM _sync_client_state LIMIT 1), 0) = 0
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

private fun buildJsonObjectExprHexAware(
    tableInfo: TableInfo,
    prefix: String,
): String {
    val pairs = tableInfo.columns.map { column ->
        val name = column.name.lowercase()
        val valueExpr = if (column.declaredType.lowercase().contains("blob")) {
            "lower(hex($prefix.${quoteIdent(column.name)}))"
        } else {
            "$prefix.${quoteIdent(column.name)}"
        }
        "'$name', $valueExpr"
    }
    return "json_object(${pairs.joinToString(", ")})"
}

private fun buildKeyJsonObjectExprHexAware(
    tableInfo: TableInfo,
    keyColumn: String,
    prefix: String,
): String {
    val column = tableInfo.columns.firstOrNull { it.name.equals(keyColumn, ignoreCase = true) }
        ?: error("table ${tableInfo.table} is missing sync key column $keyColumn")
    val keyName = column.name.lowercase()
    val valueExpr = if (column.declaredType.lowercase().contains("blob")) {
        "lower(hex($prefix.${quoteIdent(column.name)}))"
    } else {
        "$prefix.${quoteIdent(column.name)}"
    }
    return "json_object('$keyName', $valueExpr)"
}

internal fun quoteIdent(identifier: String): String = "\"${identifier.replace("\"", "\"\"")}\""

@OptIn(ExperimentalUuidApi::class)
internal fun randomSourceId(): String = Uuid.random().toString()
