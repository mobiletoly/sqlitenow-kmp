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

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use

internal fun oversqliteManagedTriggerNames(tableNames: Set<String>): Set<String> = buildSet {
    for (tableName in tableNames) {
        add("trg_${tableName}_bi_guard")
        add("trg_${tableName}_bu_guard")
        add("trg_${tableName}_bd_guard")
        add("trg_${tableName}_ai")
        add("trg_${tableName}_au")
        add("trg_${tableName}_ad")
    }
}

@OptIn(dev.goquick.sqlitenow.core.SqliteNowInternalApi::class)
internal fun coordinatedMigrationSqlPolicy(
    managedTables: Set<String>,
    preservedTables: Set<String> = emptySet(),
): dev.goquick.sqlitenow.core.SqliteNowCoordinatedMigrationSqlPolicy =
    dev.goquick.sqlitenow.core.SqliteNowCoordinatedMigrationSqlPolicy(
        oversqliteRetainedTables + oversqliteOpeningTable + oversqliteManagedIndexes +
            oversqliteStaticTriggers + oversqliteManagedTriggerNames(managedTables) +
            preservedLocalTableGuardNames(preservedTables),
    )

internal suspend fun installOversqliteTriggers(
    db: SafeSQLiteConnection,
    validated: ValidatedConfig,
    tableInfoCache: TableInfoCache,
) {
    for (table in validated.tables) {
        createTriggersForTable(db, validated, table, tableInfoCache)
    }
}

internal suspend fun installOversqliteOpeningGuards(
    db: SafeSQLiteConnection,
    tableNames: Set<String>,
) {
    for (tableName in tableNames.sorted()) {
        val existingSqlByName = loadExistingTriggerSqlByName(db, tableName)
        val guards = listOf(
            "trg_${tableName}_bi_guard" to guardInsertTriggerSql(tableName),
            "trg_${tableName}_bu_guard" to guardUpdateTriggerSql(tableName),
            "trg_${tableName}_bd_guard" to guardDeleteTriggerSql(tableName),
        )
        for ((name, sql) in guards) {
            val existing = existingSqlByName[name]
            if (existing != null && normalizeTriggerSql(existing) == normalizeTriggerSql(sql)) continue
            if (existing != null) db.execSQL("DROP TRIGGER IF EXISTS ${quoteIdent(name)}")
            db.execSQL(sql)
        }
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
    val triggers = expectedCaptureTriggers(validated.schemaForTable(table.tableName),table,tableInfo)
    val existingSqlByName = loadExistingTriggerSqlByName(db, table.tableName)

    for ((name, sql) in triggers) {
        val existingSql = existingSqlByName[name]
        if (existingSql != null && normalizeTriggerSql(existingSql) == normalizeTriggerSql(sql)) {
            continue
        }
        if (existingSql != null) {
            error("incompatible capture trigger $name; recreate the development database")
        }
        db.execSQL(sql)
    }
}

internal fun expectedCaptureTriggers(schemaName: String,table: ValidatedSyncTable,tableInfo: TableInfo): List<Pair<String,String>> {
    val pkColumn = table.syncKeyColumnName
    val payloadExpr = buildJsonObjectExprHexAware(tableInfo, "NEW")
    val oldPayloadExpr = if (tableInfo.foreignKeys.isEmpty()) "NULL" else buildJsonObjectExprHexAware(tableInfo, "OLD")
    val oldKeyExpr = buildKeyJsonObjectExprHexAware(tableInfo, pkColumn, "OLD")
    val newKeyExpr = buildKeyJsonObjectExprHexAware(tableInfo, pkColumn, "NEW")

    val data = TriggerData(
        schemaName = schemaName,
        tableName = table.tableName,
        newRowJson = payloadExpr,
        oldRowJson = oldPayloadExpr,
        insertRowJson = payloadExpr,
        updateRowJson = payloadExpr,
        deleteRowJson = "NULL",
        oldKeyJson = oldKeyExpr,
        newKeyJson = newKeyExpr,
    )

    return listOf(
        "trg_${table.tableName}_bi_guard" to guardInsertTriggerSql(table.tableName),
        "trg_${table.tableName}_bu_guard" to guardUpdateTriggerSql(table.tableName),
        "trg_${table.tableName}_bd_guard" to guardDeleteTriggerSql(table.tableName),
        "trg_${table.tableName}_ai" to insertTriggerSql(data),
        "trg_${table.tableName}_au" to updateTriggerSql(data),
        "trg_${table.tableName}_ad" to deleteTriggerSql(data),
    )
}

internal suspend fun validateRetainedCaptureTriggers(db: SafeSQLiteConnection) {
    val tables = db.queryList("SELECT schema_name,table_name FROM _sync_managed_tables") { it.getText(0) to it.getText(1) }
    val cache=TableInfoCache()
    val expected=linkedMapOf<String,String>()
    for((schema,name) in tables) {
        val info=cache.get(db,name)
        val key=info.columns.single { it.isPrimaryKey }.name
        expected.putAll(expectedCaptureTriggers(schema,ValidatedSyncTable(name,key),info))
    }
    val existing=db.queryList("SELECT name,tbl_name,sql FROM sqlite_schema WHERE type='trigger'") { Triple(it.getText(0),it.getText(1),it.getText(2)) }
    for((name,sql) in expected) {
        require(existing.singleOrNull { it.first==name }?.third?.let(::retainedSqlSignature)==retainedSqlSignature(sql)) { "incompatible retained capture trigger $name" }
    }
    for((name,table,sql) in existing) {
        require(name in expected || name in oversqliteStaticTriggers || (!triggerReferencesSyncTable(sql) && table !in oversqliteRetainedTables)) { "unexpected retained trigger $name" }
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
    val oldRowJson: String,
    val insertRowJson: String,
    val updateRowJson: String,
    val deleteRowJson: String,
    val oldKeyJson: String,
    val newKeyJson: String,
)

private fun baselineLookup(data: TriggerData, key: String): String =
 "FROM _sync_row_state WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$key"

private fun ensureNewBaselineSql(data: TriggerData): String = """
 INSERT INTO _sync_row_state(schema_name,table_name,key_json,row_version,server_kind,server_payload,server_provenance,relationships_known)
 SELECT '${data.schemaName}','${data.tableName}',${data.newKeyJson},0,'unpublished',NULL,${baselineProvenanceSql("'never_published'","0","NULL")},1 WHERE true
 ON CONFLICT(schema_name,table_name,key_json) DO NOTHING;
""".trimIndent()

private fun requireOldBaselineSql(data: TriggerData): String =
 "SELECT CASE WHEN NOT EXISTS(SELECT 1 ${baselineLookup(data,data.oldKeyJson)}) THEN RAISE(ABORT,'SYNC_BASELINE_MISSING') END;"

private fun capturedBaseSql(data: TriggerData, key: String): String = """
 (SELECT CASE server_kind WHEN 'live' THEN 'present' WHEN 'deleted' THEN 'deleted' WHEN 'unknown' THEN 'unavailable' ELSE 'absent' END ${baselineLookup(data,key)}),
 (SELECT server_payload ${baselineLookup(data,key)}),
 (SELECT server_provenance ${baselineLookup(data,key)}),
""".trimIndent()

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
${ensureNewBaselineSql(data)}
${dirtyRowCurrentPayloadUpsertSql(data, keyJson = data.newKeyJson, payloadJson = data.insertRowJson)}
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
WHEN (
  EXISTS (
    SELECT 1
    FROM _sync_operation_state
    WHERE singleton_key = 1
      AND kind != 'none'
  )
  OR NOT EXISTS (
    SELECT 1
    FROM _sync_opening_state
    WHERE singleton_key = 1
      AND phase = 'ready'
      AND outcome = 'ready'
      AND writes_available = 1
  )
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
${requireOldBaselineSql(data)}
${ensureNewBaselineSql(data)}
${dirtyRowDeleteMarkerSelectSql(
    data,
    keyJson = data.oldKeyJson,
    whereClause = "${data.oldKeyJson} != ${data.newKeyJson}",
)}

${dirtyRowCurrentPayloadUpsertSql(data, keyJson = data.newKeyJson, payloadJson = data.updateRowJson)}
END
""".trimIndent()

private fun deleteTriggerSql(data: TriggerData): String = """
CREATE TRIGGER IF NOT EXISTS trg_${data.tableName}_ad
AFTER DELETE ON ${quoteIdent(data.tableName)}
WHEN COALESCE((SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1), 0) = 0
BEGIN
${requireOldBaselineSql(data)}
${dirtyRowDeleteMarkerValuesSql(data, keyJson = data.oldKeyJson)}
END
""".trimIndent()

private fun dirtyRowCurrentPayloadUpsertSql(
    data: TriggerData,
    keyJson: String,
    payloadJson: String,
): String = """
  INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, base_kind, base_payload, base_provenance, dirty_ordinal, updated_at)
  VALUES (
    '${data.schemaName}',
    '${data.tableName}',
    $keyJson,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM _sync_row_state
        WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson AND server_kind='live'
      ) THEN 'UPDATE'
      ELSE 'INSERT'
    END,
    COALESCE(
      (SELECT row_version FROM _sync_row_state WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson),
      0
    ),
    $payloadJson,
    ${capturedBaseSql(data,keyJson)}
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
  INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, base_kind, base_payload, base_provenance, dirty_ordinal, updated_at)
  SELECT
    '${data.schemaName}',
    '${data.tableName}',
    $keyJson,
    'DELETE',
    COALESCE(
      (SELECT row_version FROM _sync_row_state WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson),
      0
    ),
    NULL,
    ${capturedBaseSql(data,keyJson)}
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
  INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, base_kind, base_payload, base_provenance, dirty_ordinal, updated_at)
  VALUES (
    '${data.schemaName}',
    '${data.tableName}',
    $keyJson,
    'DELETE',
    COALESCE(
      (SELECT row_version FROM _sync_row_state WHERE schema_name='${data.schemaName}' AND table_name='${data.tableName}' AND key_json=$keyJson),
      0
    ),
    NULL,
    ${capturedBaseSql(data,keyJson)}
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
