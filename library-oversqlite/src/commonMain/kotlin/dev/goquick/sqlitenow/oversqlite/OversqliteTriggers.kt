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

internal suspend fun installOversqliteTriggers(
    db: SafeSQLiteConnection,
    validated: ValidatedConfig,
    tableInfoCache: TableInfoCache,
) {
    for (table in validated.tables) {
        createTriggersForTable(db, validated, table, tableInfoCache)
    }
}

private suspend fun createTriggersForTable(
    db: SafeSQLiteConnection,
    validated: ValidatedConfig,
    table: ValidatedSyncTable,
    tableInfoCache: TableInfoCache,
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

private data class TriggerData(
    val schemaName: String,
    val tableName: String,
    val newRowJson: String,
    val oldKeyJson: String,
    val newKeyJson: String,
)

private val collapseWhitespaceRegex = Regex("\\s+")
private val createTriggerPrefixRegex = Regex(
    pattern = "^CREATE\\s+TRIGGER\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?",
    options = setOf(RegexOption.IGNORE_CASE),
)

private fun normalizeTriggerSql(sql: String): String {
    val collapsed = collapseWhitespaceRegex.replace(sql.trim(), " ")
    return createTriggerPrefixRegex.replace(collapsed, "CREATE TRIGGER ")
}

private fun insertTriggerSql(data: TriggerData): String = """
CREATE TRIGGER IF NOT EXISTS trg_${data.tableName}_ai
AFTER INSERT ON ${quoteIdent(data.tableName)}
WHEN COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) = 0
BEGIN
${dirtyRowCurrentPayloadUpsertSql(data, keyJson = data.newKeyJson, payloadJson = data.newRowJson)}
END
""".trimIndent()

private fun guardInsertTriggerSql(tableName: String): String =
    guardTriggerSql(tableName, suffix = "bi", operation = "INSERT")

private fun guardUpdateTriggerSql(tableName: String): String =
    guardTriggerSql(tableName, suffix = "bu", operation = "UPDATE")

private fun guardDeleteTriggerSql(tableName: String): String =
    guardTriggerSql(tableName, suffix = "bd", operation = "DELETE")

private fun guardTriggerSql(
    tableName: String,
    suffix: String,
    operation: String,
): String = """
CREATE TRIGGER IF NOT EXISTS trg_${tableName}_${suffix}_guard
BEFORE $operation ON ${quoteIdent(tableName)}
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
${dirtyRowDeleteMarkerSelectSql(
    data,
    keyJson = data.oldKeyJson,
    whereClause = "${data.oldKeyJson} != ${data.newKeyJson}",
)}

${dirtyRowCurrentPayloadUpsertSql(data, keyJson = data.newKeyJson, payloadJson = data.newRowJson)}
END
""".trimIndent()

private fun deleteTriggerSql(data: TriggerData): String = """
CREATE TRIGGER IF NOT EXISTS trg_${data.tableName}_ad
AFTER DELETE ON ${quoteIdent(data.tableName)}
WHEN COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) = 0
BEGIN
${dirtyRowDeleteMarkerValuesSql(data, keyJson = data.oldKeyJson)}
END
""".trimIndent()

private fun dirtyRowCurrentPayloadUpsertSql(
    data: TriggerData,
    keyJson: String,
    payloadJson: String,
): String = """
  INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
  VALUES (
    '${data.schemaName}',
    '${data.tableName}',
    $keyJson,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM _sync_row_state
        WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson AND deleted=0
      ) THEN 'UPDATE'
      ELSE 'INSERT'
    END,
    COALESCE(
      (SELECT base_row_version FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson),
      (SELECT row_version FROM _sync_row_state WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson),
      0
    ),
    $payloadJson,
    COALESCE(
      (SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson),
      (SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
    ),
    strftime('%Y-%m-%dT%H:%M:%fZ','now')
  )
  ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
    op=excluded.op,
    payload=excluded.payload,
    updated_at=excluded.updated_at;
""".trim('\n')

private fun dirtyRowDeleteMarkerSelectSql(
    data: TriggerData,
    keyJson: String,
    whereClause: String,
): String = """
  INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
  SELECT
    '${data.schemaName}',
    '${data.tableName}',
    $keyJson,
    'DELETE',
    COALESCE(
      (SELECT base_row_version FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson),
      (SELECT row_version FROM _sync_row_state WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson),
      0
    ),
    NULL,
    COALESCE(
      (SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson),
      (SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
    ),
    strftime('%Y-%m-%dT%H:%M:%fZ','now')
  WHERE $whereClause
  ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
    op='DELETE',
    payload=NULL,
    updated_at=excluded.updated_at;
""".trim('\n')

private fun dirtyRowDeleteMarkerValuesSql(
    data: TriggerData,
    keyJson: String,
): String = """
  INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
  VALUES (
    '${data.schemaName}',
    '${data.tableName}',
    $keyJson,
    'DELETE',
    COALESCE(
      (SELECT base_row_version FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson),
      (SELECT row_version FROM _sync_row_state WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson),
      0
    ),
    NULL,
    COALESCE(
      (SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson),
      (SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
    ),
    strftime('%Y-%m-%dT%H:%M:%fZ','now')
  )
  ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
    op='DELETE',
    payload=NULL,
    updated_at=excluded.updated_at;
""".trim('\n')
