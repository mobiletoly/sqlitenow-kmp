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
import dev.goquick.sqlitenow.core.sqlite.SqliteStatement
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonPrimitive

internal class OversqliteLocalStore(
    private val db: SafeSQLiteConnection,
    private val json: Json,
    private val validatedConfig: () -> ValidatedConfig,
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
        return try {
            val keyColumn = state.validated.keyByTable[tableName]?.singleOrNull()
                ?: error("table is not configured for sync")
            val wireValue = exactWireKeyValue(key, keyColumn, "wire row key")
            val localPk = if (isPrimaryKeyBlob(tableName)) {
                bytesToHexLower(decodeWireUuidBytes(wireValue))
            } else {
                wireValue
            }
            buildKeyJson(tableName, localPk) to localPk
        } catch (error: IllegalArgumentException) {
            if (error is SnapshotSemanticException) throw error
            throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_ROW)
        } catch (_: IllegalStateException) {
            throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_ROW)
        }
    }

    suspend fun validateAndNormalizeSnapshotPayload(
        state: RuntimeState,
        tableName: String,
        key: SyncKey,
        payload: JsonObject,
    ): JsonObject {
        val tableInfo = snapshotSemantic(SnapshotSemanticFailure.INVALID_PAYLOAD_SHAPE) {
            configuredTableInfo(tableName)
        }
        val normalized = snapshotSemantic(SnapshotSemanticFailure.INVALID_PAYLOAD_SHAPE) {
            normalizePayloadColumns(payload, tableInfo, tableName)
        }
        val keyColumn = snapshotSemantic(SnapshotSemanticFailure.INVALID_ROW) {
            state.validated.keyByTable[tableName]?.singleOrNull()
                ?: error("table is not configured for sync")
        }
        val wireKeyValue = snapshotSemantic(SnapshotSemanticFailure.INVALID_ROW) {
            exactWireKeyValue(key, keyColumn, "snapshot row key")
        }
        val primaryKeyColumn = snapshotSemantic(SnapshotSemanticFailure.INVALID_PAYLOAD_SHAPE) {
            tableInfo.column(keyColumn)
        }

        var payloadKeyValue: TypedValue? = null
        for (column in tableInfo.columns) {
            val decoded = snapshotSemantic(SnapshotSemanticFailure.INVALID_PAYLOAD_VALUE) {
                OversqliteValueCodec.decodePayloadValue(
                    column = column,
                    value = normalized.getValue(column.name.lowercase()),
                    payloadSource = PayloadSource.AUTHORITATIVE_WIRE,
                )
            }
            if (column.name.equals(keyColumn, ignoreCase = true)) payloadKeyValue = decoded
        }

        val retainedPayloadKey = payloadKeyValue
            ?: throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_PAYLOAD_SHAPE)
        val wireKeyTypedValue = snapshotSemantic(SnapshotSemanticFailure.INVALID_PAYLOAD_VALUE) {
            OversqliteValueCodec.decodePayloadValue(
                column = primaryKeyColumn,
                value = JsonPrimitive(wireKeyValue),
                payloadSource = PayloadSource.AUTHORITATIVE_WIRE,
            )
        }
        if (!typedValuesEqual(retainedPayloadKey, wireKeyTypedValue)) {
            throw SnapshotSemanticException(SnapshotSemanticFailure.KEY_PAYLOAD_MISMATCH)
        }
        return normalized
    }

    suspend fun serializeExistingRow(
        tableName: String,
        localPk: String,
        statementCache: StatementCache? = null,
    ): String? {
        val tableInfo = configuredTableInfo(tableName)
        if (tableInfo.columns.isEmpty()) return null
        val pkColumn = validatedConfig().pkByTable[tableName]
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
        val tableInfo = configuredTableInfo(tableName)

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
        val tableInfo = configuredTableInfo(tableName)
        val pkColumn = validatedConfig().pkByTable[tableName]
            ?: error("table $tableName is not configured for sync")
        val normalized = normalizePayloadColumns(payload, tableInfo, tableName)

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

    suspend fun prepareSnapshotUpsertPlan(
        statementCache: StatementCache,
    ): SnapshotUpsertPlan {
        val config = validatedConfig()
        val preparedByTable = linkedMapOf<String, SnapshotUpsertPlan.PreparedTable>()
        for (table in config.tables) {
            val tableInfo = config.tableInfoByName[table.tableName]
                ?: error("table ${table.tableName} is not configured for sync")
            val pkColumn = config.pkByTable[table.tableName]
                ?: error("table ${table.tableName} is not configured for sync")
            val sql = buildUpsertSql(table.tableName, tableInfo, pkColumn)
            preparedByTable[table.tableName] = SnapshotUpsertPlan.PreparedTable(
                tableInfo = tableInfo,
                statement = statementCache.get(sql),
            )
        }
        return SnapshotUpsertPlan(preparedByTable)
    }

    private suspend fun buildKeyJson(tableName: String, localPk: String): String {
        val keyColumn = validatedConfig().keyByTable[tableName]?.singleOrNull()
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
        return configuredTableInfo(tableName).primaryKeyIsBlob
    }

    private fun configuredTableInfo(tableName: String): TableInfo =
        validatedConfig().tableInfoByName[tableName.trim().lowercase()]
            ?: error("table $tableName is not configured for sync")

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

internal class SnapshotUpsertPlan(
    private val preparedByTable: Map<String, PreparedTable>,
    private val reusableStatementCleanup: ReusableStatementCleanup = DefaultReusableStatementCleanup,
) {
    internal class PreparedTable(
        val tableInfo: TableInfo,
        val statement: SqliteStatement,
        var used: Boolean = false,
    )

    fun upsertAuthoritativeRow(
        tableName: String,
        payload: JsonObject,
    ) {
        val prepared = preparedByTable[tableName]
            ?: preparedByTable[tableName.lowercase()]
            ?: error("table $tableName is not configured for sync")
        val normalized = normalizePayloadColumns(payload, prepared.tableInfo, tableName)

        if (prepared.used) {
            reusableStatementCleanup(prepared.statement)
        } else {
            prepared.used = true
        }
        prepared.tableInfo.columns.forEachIndexed { index, column ->
            val value = normalized.getValue(column.name.lowercase())
            OversqliteValueCodec.bindPayloadValue(
                statement = prepared.statement,
                index = index + 1,
                column = column,
                value = value,
                payloadSource = PayloadSource.AUTHORITATIVE_WIRE,
            )
        }
        prepared.statement.step()
    }

    fun finish() {
        for (prepared in preparedByTable.values) {
            if (prepared.used) {
                reusableStatementCleanup(prepared.statement)
                prepared.used = false
            }
        }
    }
}

private fun buildUpsertSql(
    tableName: String,
    tableInfo: TableInfo,
    pkColumn: String,
): String {
    val columns = tableInfo.columns.map { quoteIdent(it.name) }
    val updates = tableInfo.columns
        .filterNot { it.name.equals(pkColumn, ignoreCase = true) }
        .map { "${quoteIdent(it.name)} = excluded.${quoteIdent(it.name)}" }
    return buildString {
        append("INSERT INTO ${quoteIdent(tableName)} (${columns.joinToString(", ")}) VALUES (")
        append(columns.indices.joinToString(", ") { "?" })
        append(")")
        if (updates.isEmpty()) {
            append(" ON CONFLICT(${quoteIdent(pkColumn)}) DO NOTHING")
        } else {
            append(" ON CONFLICT(${quoteIdent(pkColumn)}) DO UPDATE SET ${updates.joinToString(", ")}")
        }
    }
}

private fun normalizePayloadColumns(
    payload: JsonObject,
    tableInfo: TableInfo,
    tableName: String,
): JsonObject {
    if (
        payload.size == tableInfo.columns.size &&
        payload.keys.all(tableInfo.columnsByNameLower::containsKey)
    ) {
        return payload
    }

    val normalized = linkedMapOf<String, JsonElement>()
    for ((key, value) in payload) {
        val normalizedKey = key.lowercase()
        require(normalizedKey in tableInfo.columnsByNameLower) {
            "payload for $tableName contains unknown column $key"
        }
        require(normalized.put(normalizedKey, value) == null) {
            "payload for $tableName contains duplicate column $key"
        }
    }
    require(normalized.size == tableInfo.columns.size && tableInfo.columnNamesLower.all(normalized::containsKey)) {
        "payload for $tableName must contain every table column"
    }
    return JsonObject(normalized)
}

private fun exactWireKeyValue(
    key: SyncKey,
    keyColumn: String,
    context: String,
): String {
    require(key.size == 1) { "$context must contain exactly $keyColumn" }
    val entry = key.entries.single()
    require(entry.key.equals(keyColumn, ignoreCase = true)) {
        "$context is missing $keyColumn"
    }
    return entry.value
}

private fun typedValuesEqual(left: TypedValue, right: TypedValue): Boolean = when {
    left is TypedValue.Blob && right is TypedValue.Blob -> left.bytes.contentEquals(right.bytes)
    else -> left == right
}

private inline fun <T> snapshotSemantic(
    failure: SnapshotSemanticFailure,
    block: () -> T,
): T {
    return try {
        block()
    } catch (error: IllegalArgumentException) {
        if (error is SnapshotSemanticException) throw error
        throw SnapshotSemanticException(failure)
    } catch (_: IllegalStateException) {
        throw SnapshotSemanticException(failure)
    }
}
