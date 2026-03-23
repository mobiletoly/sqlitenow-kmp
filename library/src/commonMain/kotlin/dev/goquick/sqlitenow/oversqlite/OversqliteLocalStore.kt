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
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonPrimitive

internal class OversqliteLocalStore(
    private val db: SafeSQLiteConnection,
    private val tableInfoCache: TableInfoCache,
    private val json: Json,
    private val runtimeState: () -> RuntimeState,
) {
    suspend fun decodeDirtyKeyForPush(
        state: RuntimeState,
        tableName: String,
        keyJson: String,
    ): Pair<String, SyncKey> {
        val keyColumn = state.validated.keyByTable[tableName]?.singleOrNull()
            ?: error("table $tableName is not configured for sync")
        val raw = json.parseToJsonElement(keyJson) as? JsonObject
            ?: error("dirty key for $tableName must be a JSON object")
        val localValue = raw[keyColumn.lowercase()]?.jsonPrimitive?.content
            ?: raw[keyColumn]?.jsonPrimitive?.content
            ?: error("dirty key for $tableName is missing $keyColumn")
        val wireValue = normalizePkForServer(tableName, localValue)
        return localValue to mapOf(keyColumn.lowercase() to wireValue)
    }

    suspend fun deleteLocalRow(
        state: RuntimeState,
        tableName: String,
        localPk: String,
        statementCache: StatementCache? = null,
    ) {
        val pkColumn = state.validated.pkByTable[tableName]
            ?: error("table $tableName is not configured for sync")
        db.withPreparedStatement(
            sql = "DELETE FROM ${quoteIdent(tableName)} WHERE ${quoteIdent(pkColumn)} = ?",
            statementCache = statementCache,
        ) { st ->
            bindPrimaryKey(st, 1, tableName, localPk)
            st.step()
        }
    }

    suspend fun bundleRowKeyToLocalKey(
        state: RuntimeState,
        tableName: String,
        key: SyncKey,
    ): Pair<String, String> {
        val keyColumn = state.validated.keyByTable[tableName]?.singleOrNull()
            ?: error("table $tableName is not configured for sync")
        val wireValue = key[keyColumn.lowercase()] ?: key[keyColumn]
            ?: error("bundle row key for $tableName is missing $keyColumn")
        val localPk = if (isPrimaryKeyBlob(tableName)) {
            bytesToHexLower(decodeWireUuidBytes(wireValue))
        } else {
            wireValue
        }
        return buildKeyJson(tableName, localPk) to localPk
    }

    suspend fun serializeExistingRow(
        tableName: String,
        localPk: String,
        statementCache: StatementCache? = null,
    ): String? {
        val tableInfo = tableInfoCache.get(db, tableName)
        if (tableInfo.columns.isEmpty()) return null
        val pkColumn = runtimeState().validated.pkByTable[tableName]
            ?: error("table $tableName is not configured for sync")
        val payloadExpr = buildJsonObjectExprHexAware(tableInfo, "live_row")
        return db.withPreparedStatement(
            sql = """
                SELECT $payloadExpr
                FROM ${quoteIdent(tableName)} live_row
                WHERE live_row.${quoteIdent(pkColumn)} = ?
            """.trimIndent(),
            statementCache = statementCache,
        ) { st ->
            bindPrimaryKey(st, 1, tableName, localPk)
            if (!st.step()) return@withPreparedStatement null
            st.getText(0)
        }
    }

    suspend fun processPayloadForUpload(
        tableName: String,
        payloadText: String,
    ): JsonElement {
        val payload = json.parseToJsonElement(payloadText) as? JsonObject
            ?: error("dirty payload for $tableName must be a JSON object")
        val tableInfo = tableInfoCache.get(db, tableName)

        return buildJsonObject {
            for (column in tableInfo.columns) {
                val key = column.name.lowercase()
                val value = payload[key] ?: payload[column.name]
                    ?: error("dirty payload for $tableName is missing column ${column.name}")
                put(key, OversqliteValueCodec.encodeWirePayloadValue(column, value))
            }
        }
    }

    suspend fun upsertRow(
        tableName: String,
        payload: JsonObject,
        payloadSource: PayloadSource,
        statementCache: StatementCache? = null,
    ) {
        val tableInfo = tableInfoCache.get(db, tableName)
        val pkColumn = runtimeState().validated.pkByTable[tableName]
            ?: error("table $tableName is not configured for sync")
        val normalized = payload.entries.associate { it.key.lowercase() to it.value }
        require(normalized.size == tableInfo.columns.size) {
            "payload for $tableName must contain every table column"
        }

        val columns = mutableListOf<String>()
        val values = mutableListOf<JsonElement>()
        val updates = mutableListOf<String>()
        for (column in tableInfo.columns) {
            val key = column.name.lowercase()
            val value = normalized[key] ?: error("payload for $tableName is missing column ${column.name}")
            columns += quoteIdent(column.name)
            values += value
            if (!column.name.equals(pkColumn, ignoreCase = true)) {
                updates += "${quoteIdent(column.name)} = excluded.${quoteIdent(column.name)}"
            }
        }

        val sql = buildString {
            append("INSERT INTO ${quoteIdent(tableName)} (${columns.joinToString(", ")}) VALUES (")
            append(columns.indices.joinToString(", ") { "?" })
            append(")")
            if (updates.isEmpty()) {
                append(" ON CONFLICT(${quoteIdent(pkColumn)}) DO NOTHING")
            } else {
                append(" ON CONFLICT(${quoteIdent(pkColumn)}) DO UPDATE SET ${updates.joinToString(", ")}")
            }
        }

        db.withPreparedStatement(sql = sql, statementCache = statementCache) { st ->
            tableInfo.columns.forEachIndexed { index, column ->
                bindPayloadValue(st, index + 1, column, values[index], payloadSource)
            }
            st.step()
        }
    }

    private suspend fun buildKeyJson(tableName: String, localPk: String): String {
        val keyColumn = runtimeState().validated.keyByTable[tableName]?.singleOrNull()
            ?: error("table $tableName is not configured for sync")
        return buildJsonObject {
            put(keyColumn.lowercase(), JsonPrimitive(localPk))
        }.toString()
    }

    private suspend fun bindPrimaryKey(
        statement: dev.goquick.sqlitenow.core.sqlite.SqliteStatement,
        index: Int,
        tableName: String,
        pkValue: String,
    ) {
        if (isPrimaryKeyBlob(tableName)) {
            statement.bindBlob(index, decodeLocalUuidBytes(pkValue))
        } else {
            statement.bindText(index, pkValue)
        }
    }

    private suspend fun isPrimaryKeyBlob(tableName: String): Boolean {
        return tableInfoCache.get(db, tableName).primaryKeyIsBlob
    }

    private suspend fun normalizePkForServer(tableName: String, pkValue: String): String {
        return if (isPrimaryKeyBlob(tableName)) {
            normalizeLocalUuidAsCanonicalWire(pkValue)
        } else {
            pkValue
        }
    }

    private suspend fun bindPayloadValue(
        statement: dev.goquick.sqlitenow.core.sqlite.SqliteStatement,
        index: Int,
        column: ColumnInfo,
        value: JsonElement,
        payloadSource: PayloadSource,
    ) {
        OversqliteValueCodec.bindPayloadValue(statement, index, column, value, payloadSource)
    }
}
