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

import androidx.sqlite.SQLiteStatement
import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull

internal class OversqliteLocalStore(
    private val db: SafeSQLiteConnection,
    private val json: Json,
    private val validatedConfig: () -> ValidatedConfig,
) {
    suspend fun validateHistoricalBase(table: String, version: Long, base: HistoricalBase) {
        when(base.kind) {
            "present" -> {
                if(version<=0 || base.payload==null) throw ResolutionBaseUnavailableException()
                try { processPayloadForUpload(table,base.payload) }
                catch (_: IllegalArgumentException) { throw ResolutionBaseUnavailableException() }
                catch (_: IllegalStateException) { throw ResolutionBaseUnavailableException() }
            }
            "deleted" -> if(version<=0 || base.payload!=null) throw ResolutionBaseUnavailableException()
            "absent" -> if(version!=0L || base.payload!=null) throw ResolutionBaseUnavailableException()
            else -> throw ResolutionBaseUnavailableException()
        }
        val provenance = try {
            requireUniqueSnapshotJsonObjectMembers(base.provenance)
            json.parseToJsonElement(base.provenance) as? JsonObject
        } catch (_: IllegalArgumentException) { null }
        if (provenance == null || !validHistoricalObservation(provenance, base.kind, version)) {
            throw ResolutionBaseUnavailableException()
        }
        // B keeps its historical source and sequence; only its identity context
        // must match the existing owners on this connection/transaction.
        val current = db.withPreparedStatement("SELECT " + baselineProvenanceSql("'never_published'", "0", "NULL")) { st ->
            check(st.step())
            json.parseToJsonElement(st.getText(0)) as JsonObject
        }
        for (field in listOf("database_id", "service_id", "account_scope", "generation_id", "generation_fingerprint", "admission", "local_version", "physical_shape")) {
            if (provenance[field] != current[field]) throw ResolutionBaseUnavailableException()
        }
    }

    private fun validHistoricalObservation(p: JsonObject, kind: String, version: Long): Boolean {
        for (field in listOf("database_id", "service_id", "generation_fingerprint", "admission", "physical_shape", "origin_source_id", "origin")) {
            if ((p[field] as? JsonPrimitive)?.isString != true) return false
        }
        for (field in listOf("database_id", "service_id", "physical_shape")) {
            if (p.getValue(field).jsonPrimitive.content.isEmpty()) return false
        }
        for (field in listOf("generation_id", "local_version", "authoritative_version")) {
            val value = provenanceInteger(p[field]) ?: return false
            if (value < 0 || (field == "generation_id" && value == 0L)) return false
        }
        if (p["authoritative_version"] != JsonPrimitive(version)) return false
        val sequence = p["bundle_sequence"] ?: return false
        if (sequence != JsonNull && (provenanceInteger(sequence)?.let { it >= 0 } != true)) return false
        val origin = p.getValue("origin").jsonPrimitive.content
        val source = p.getValue("origin_source_id").jsonPrimitive.content
        return when (origin) {
            "never_published" -> kind == "absent" && sequence == JsonNull
            "explicit_conflict" -> sequence == JsonNull && source.isNotEmpty()
            "snapshot" -> sequence != JsonNull && source.isNotEmpty()
            "pull", "settlement" -> source.isNotEmpty() && kind != "absent"
            else -> false
        }
    }

    private fun provenanceInteger(value: JsonElement?): Long? =
        (value as? JsonPrimitive)?.takeUnless { it.isString }?.longOrNull

    suspend fun validateStoredServerProjection(schema: String, table: String, key: String, statementCache: StatementCache? = null) {
        val exists = db.withPreparedStatement("SELECT 1 FROM _sync_row_state WHERE schema_name=? AND table_name=? AND key_json=?", statementCache) { st ->
            st.bindText(1,schema); st.bindText(2,table); st.bindText(3,key)
            st.step()
        }
        if (exists) validateBaselineProjection(schema,table,key,"server",null,statementCache)
    }

    // Check only the bounded projection owned by this affected key, never business rows.
    suspend fun validateBaselineProjection(schema: String,table: String,key: String,view: String,desired: String?, statementCache: StatementCache? = null) {
        val info=configuredTableInfo(table)
        var payload=desired
        var revision=if (view == "desired") db.scalarLong("SELECT dirty_revision FROM _sync_conflict_state WHERE singleton_key=1") else 0L
        if(view=="server") db.withPreparedStatement("SELECT server_kind,server_payload,relationships_known,server_revision FROM _sync_row_state WHERE schema_name=? AND table_name=? AND key_json=?", statementCache) { st ->
            st.bindText(1,schema);st.bindText(2,table);st.bindText(3,key)
            check(st.step()) { "authoritative baseline missing" }
            check(st.getLong(2)==1L && st.getText(0)!="unknown") { "authoritative relationship evidence unavailable" }
            payload=if(st.isNull(1)) null else st.getText(1);revision=st.getLong(3)
        }
        val expected=linkedMapOf<String,List<String>>()
        payload?.let { text ->
            processPayloadForUpload(table,text)
            val fields=json.parseToJsonElement(text) as JsonObject
            for(fk in info.foreignKeys) {
                val captured=fields.getValue(fk.fromCol.lowercase()) as JsonObject
                if(captured.getValue("storage").jsonPrimitive.content!="null") {
                    val parentKey=buildJsonObject { put(fk.toCol.lowercase(),captured.getValue("value")) }.toString()
                    expected[fk.fromCol.lowercase()]=listOf(validatedConfig().schemaForTable(fk.refTable),fk.refTable.lowercase(),parentKey)
                }
            }
        }
        val sql = "SELECT fk_column,parent_schema,parent_table,parent_key,owner_revision FROM _sync_relationships WHERE view=? AND child_schema=? AND child_table=? AND child_key=? LIMIT ${info.foreignKeys.size+1}"
        val bind: (SQLiteStatement) -> Unit = { st ->
            st.bindText(1,view);st.bindText(2,schema);st.bindText(3,table);st.bindText(4,key)
        }
        val read: (SQLiteStatement) -> Pair<String, List<String>> = { st ->
            check(st.getLong(4)>0 && (if (view=="server") st.getLong(4)==revision else st.getLong(4)<=revision)) { "relationship owner revision mismatch" }
            st.getText(0) to listOf(st.getText(1),st.getText(2),st.getText(3))
        }
        val actual = (if (statementCache == null) db.queryList(sql, bind, read)
        else db.withPreparedStatement(sql, statementCache) { st ->
            bind(st)
            buildList { while (st.step()) add(read(st)) }
        }).toMap()
        check(actual==expected) { "relationship projection differs from $view owner" }
    }

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
            val localPk = decodeWireKeyToLocalValue(tableName, keyColumn, wireValue)
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

    // Convert validated wire values directly; business triggers cannot alter this evidence.
    suspend fun captureAuthoritativePayload(tableName: String, payload: JsonObject, statementCache: StatementCache? = null): String {
        val config = validatedConfig()
        val info = configuredTableInfo(config, tableName)
        val normalized = normalizePayloadColumns(payload,info,tableName)
        require(normalized.keys==info.columns.map { it.name.lowercase() }.toSet()) { "authoritative payload column set mismatch" }
        return db.withPreparedStatement(config.replaySql.table(tableName.trim().lowercase()).authoritativeSelect,statementCache) { st ->
            info.columns.forEachIndexed { index,column -> OversqliteValueCodec.bindPayloadValue(st,index+1,column,normalized.getValue(column.name.lowercase()),PayloadSource.AUTHORITATIVE_WIRE) }
            check(st.step()); st.getText(0)
        }
    }

    suspend fun serializeExistingRow(
        tableName: String,
        localPk: String,
        statementCache: StatementCache? = null,
    ): String? {
        val config = validatedConfig()
        val tableInfo = configuredTableInfo(config, tableName)
        if (tableInfo.columns.isEmpty()) return null
        val pkColumn = config.pkByTable[tableName]
            ?: error("table $tableName is not configured for sync")
        return db.withPreparedStatement(
            sql = config.replaySql.table(tableName).liveSelect,
            statementCache = statementCache,
        ) { st ->
            if (tableInfo.primaryKeyIsBlob) st.bindBlob(1, decodeLocalUuidBytes(localPk))
            else st.bindText(1, localPk)
            if (!st.step()) return@withPreparedStatement null
            st.getText(0)
        }
    }

    suspend fun processPayloadForUpload(
        tableName: String,
        payloadText: String,
    ): JsonElement {
        val currentPayload = payloadText
        requireUniqueSnapshotJsonObjectMembers(currentPayload)
        val payload = json.parseToJsonElement(currentPayload) as? JsonObject
            ?: error("dirty payload for $tableName must be a JSON object")
        val tableInfo = configuredTableInfo(tableName)
        require(payload.keys == tableInfo.columns.map { it.name }.toSet()) { "dirty payload column set mismatch for $tableName" }

        return buildJsonObject {
            for (column in tableInfo.columns) {
                val key = column.name
                val value = payload[key]
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
        val config = validatedConfig()
        val tableInfo = configuredTableInfo(config, tableName)
        val pkColumn = config.pkByTable[tableName]
            ?: error("table $tableName is not configured for sync")
        val normalized = normalizePayloadColumns(payload, tableInfo, tableName)
        val values = tableInfo.columns.map { column ->
            normalized[column.name.lowercase()] ?: error("payload for $tableName is missing column ${column.name}")
        }
        val sql = config.replaySql.table(tableName).upsert

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
            val sql = config.replaySql.table(table.tableName).upsert
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
        statement: SQLiteStatement,
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

    private fun configuredTableInfo(tableName: String): TableInfo = configuredTableInfo(validatedConfig(), tableName)

    private fun configuredTableInfo(config: ValidatedConfig, tableName: String): TableInfo =
        config.tableInfoByName[tableName.trim().lowercase()]
            ?: error("table $tableName is not configured for sync")

    private suspend fun normalizePkForServer(tableName: String, pkValue: String): String {
        return if (isPrimaryKeyBlob(tableName)) {
            normalizeLocalUuidAsCanonicalWire(pkValue)
        } else {
            val key = checkNotNull(configuredTableInfo(tableName).primaryKey)
            if (key.wireType == "uuid") decodeWireUuidBytes(pkValue)
            pkValue
        }
    }

    private suspend fun decodeWireKeyToLocalValue(
        tableName: String,
        keyColumn: String,
        wireValue: String,
    ): String {
        val column = configuredTableInfo(tableName).column(keyColumn)
        return when (val typed = OversqliteValueCodec.decodePayloadValue(
            column = column,
            value = JsonPrimitive(wireValue),
            payloadSource = PayloadSource.AUTHORITATIVE_WIRE,
        )) {
            TypedValue.Null -> error("sync key must not be null")
            is TypedValue.Text -> typed.value
            is TypedValue.Integer -> typed.canonicalText
            is TypedValue.Real -> typed.canonicalText
            is TypedValue.Blob -> bytesToHexLower(typed.bytes)
        }
    }

    private suspend fun bindPayloadValue(
        statement: SQLiteStatement,
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
        val statement: SQLiteStatement,
        var used: Boolean = false,
    )

    suspend fun upsertAuthoritativeRow(
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

private fun normalizePayloadColumns(
    payload: JsonObject,
    tableInfo: TableInfo,
    tableName: String,
): JsonObject {
    require(payload.size == tableInfo.columns.size && payload.keys.all(tableInfo.columnsByNameLower::containsKey)) {
        "payload for $tableName must contain every table column with its exact name"
    }
    return payload
}

private fun exactWireKeyValue(
    key: SyncKey,
    keyColumn: String,
    context: String,
): String {
    require(key.size == 1) { "$context must contain exactly $keyColumn" }
    val entry = key.entries.single()
    require(entry.key == keyColumn) {
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
