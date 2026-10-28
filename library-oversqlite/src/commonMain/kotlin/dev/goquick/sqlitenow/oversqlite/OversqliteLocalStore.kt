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
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull

internal data class AuthoritativeRowProjection(val version: Long, val kind: String, val payload: String?,
    val relationshipsKnown: Boolean, val revision: Long, val contextPresent: Boolean, val provenanceMatches: Boolean)

internal class OversqliteLocalStore(
    private val db: SafeSQLiteConnection,
    private val json: Json,
    private val validatedConfig: () -> ValidatedConfig,
) {
    internal var uniqueReplay: (suspend (String,String,StatementCache?) -> Boolean)? = null

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
        val context = provenanceInteger(provenance["context_id"]) ?: throw ResolutionBaseUnavailableException()
        OversqliteProvenanceContextStore(db).validateHistorical(context,
            provenance.getValue("origin").jsonPrimitive.content != "never_published")
    }

    private fun validHistoricalObservation(p: JsonObject, kind: String, version: Long): Boolean {
        if (p.keys != setOf("context_id", "origin", "bundle_sequence")) return false
        if ((provenanceInteger(p["context_id"]) ?: 0) <= 0) return false
        val origin = (p["origin"] as? JsonPrimitive)?.takeIf { it.isString }?.content ?: return false
        val sequence = p["bundle_sequence"] ?: return false
        if (sequence != JsonNull && (provenanceInteger(sequence)?.let { it >= 0 } != true)) return false
        return when (origin) {
            "never_published" -> kind == "absent" && version == 0L && sequence == JsonNull
            "explicit_conflict" -> sequence == JsonNull
            "snapshot" -> sequence != JsonNull
            "pull", "settlement" -> kind != "absent"
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
        if (exists) {
            db.withPreparedStatement("SELECT c.context_id FROM _sync_row_state r LEFT JOIN _sync_provenance_contexts c ON c.context_id=r.server_context_id WHERE r.schema_name=? AND r.table_name=? AND r.key_json=?", statementCache) { st ->
                st.bindText(1, schema); st.bindText(2, table); st.bindText(3, key)
                check(st.step() && !st.isNull(0)) { "missing authoritative provenance context" }
            }
            validateBaselineProjection(schema,table,key,"server",null,statementCache)
        }
    }

    internal suspend fun validateStoredServerProjection(schema: String, table: String, key: String,
        loaded: AuthoritativeRowProjection, retainPayload: Boolean, cache: StatementCache?): SnapshotTypedValues? {
        check(loaded.contextPresent) { "missing authoritative provenance context" }
        return validateBaselineProjectionImpl(schema, table, key, "server", null, cache, loaded, retainPayload)
    }

    // Check only the bounded projection owned by this affected key, never business rows.
    suspend fun validateBaselineProjection(schema: String,table: String,key: String,view: String,desired: String?, statementCache: StatementCache? = null) {
        validateBaselineProjectionImpl(schema, table, key, view, desired, statementCache, null, false)
    }

    private suspend fun validateBaselineProjectionImpl(schema: String,table: String,key: String,view: String,desired: String?,
        statementCache: StatementCache?, loaded: AuthoritativeRowProjection?, retainPayload: Boolean): SnapshotTypedValues? {
        val info=configuredTableInfo(table)
        var payload=desired
        var revision=if (view == "desired") db.scalarLong("SELECT dirty_revision FROM _sync_conflict_state WHERE singleton_key=1") else 0L
        if (view == "server" && loaded != null) {
            check(loaded.relationshipsKnown && loaded.kind != "unknown") { "authoritative relationship evidence unavailable" }
            payload = loaded.payload; revision = loaded.revision
        } else {
            if(view=="server") db.withPreparedStatement("SELECT server_kind,server_payload,relationships_known,server_revision FROM _sync_row_state WHERE schema_name=? AND table_name=? AND key_json=?", statementCache) { st ->
                st.bindText(1,schema);st.bindText(2,table);st.bindText(3,key)
                check(st.step()) { "authoritative baseline missing" }
                check(st.getLong(2)==1L && st.getText(0)!="unknown") { "authoritative relationship evidence unavailable" }
                payload=if(st.isNull(1)) null else st.getText(1);revision=st.getLong(3)
            }
        }
        var retained: SnapshotTypedValues? = null
        try {
        val expected=linkedMapOf<String,List<String>>()
        payload?.let { text ->
            snapshotRowWork("row_processing.authoritative_decode")
            val fields = parseStoredPayload(table, text)
            val values = if (retainPayload) mutableListOf<TypedValue>() else null
            val span = dev.goquick.sqlitenow.core.SqliteNowDiagnosticContext.current(currentCoroutineContext())?.span
            for (column in info.columns) {
                val value = decodeSnapshotScalar(span) { OversqliteValueCodec.decodePayloadValue(column, fields.getValue(column.name), PayloadSource.LOCAL_STATE) }
                values?.add(value)
            }
            if (values != null) retained = SnapshotTypedValues(db, info, PayloadSource.LOCAL_STATE, values, fields, span)
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
        else syncDiagnosticSql("snapshot.relationship_projection") { db.withPreparedStatement(sql, statementCache) { st ->
            bind(st)
            buildList { while (st.step()) add(read(st)) }
        } }).toMap()
        check(actual==expected) { "relationship projection differs from $view owner" }
        return retained
        } catch (failure: Throwable) {
            retained?.close()
            throw failure
        }
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
        skipUniqueReplay: Boolean = false,
    ) {
        if(!skipUniqueReplay && uniqueReplay?.invoke(tableName,localPk,statementCache)==true)return
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
        decodedValues: MutableList<TypedValue>? = null,
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
        val diagnosticSpan = dev.goquick.sqlitenow.core.SqliteNowDiagnosticContext.current(currentCoroutineContext())?.span
        for (column in tableInfo.columns) {
            val decoded = decodeSnapshotScalar(diagnosticSpan) { snapshotSemantic(SnapshotSemanticFailure.INVALID_PAYLOAD_VALUE) {
                OversqliteValueCodec.decodePayloadValue(
                    column = column,
                    value = normalized.getValue(column.name.lowercase()),
                    payloadSource = PayloadSource.AUTHORITATIVE_WIRE,
                )
            }
            }
            decodedValues?.add(decoded)
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
    suspend fun captureAuthoritativePayload(tableName: String, payload: JsonObject, statementCache: StatementCache? = null, typedValues: SnapshotTypedValues? = null): String {
        snapshotRowWork("row_processing.expected_projection_text")
        return readAuthoritativeProjection(tableName, payload, statementCache, typedValues, false) { _, st -> st.getText(0) }
    }

    /** Caller admits both typed holders before this allocation; statement cleanup precedes ownership transfer. */
    suspend fun captureAuthoritativeTypedProjection(tableName: String, payload: JsonObject, statementCache: StatementCache?, typedValues: SnapshotTypedValues?): SnapshotTypedValues {
        val values = readAuthoritativeProjection(tableName, payload, statementCache, typedValues, true) { info, st ->
            SnapshotTypedValues.readProjection(info, st)
        }
        return SnapshotTypedValues(db, configuredTableInfo(tableName), PayloadSource.LOCAL_STATE, values, null,
            dev.goquick.sqlitenow.core.SqliteNowDiagnosticContext.current(currentCoroutineContext())?.span)
    }

    private suspend fun <T> readAuthoritativeProjection(tableName: String, payload: JsonObject, cache: StatementCache?, typedValues: SnapshotTypedValues?, typed: Boolean,
        read: (TableInfo, SQLiteStatement) -> T): T {
        val config = validatedConfig()
        val info = configuredTableInfo(config, tableName)
        val normalized = normalizePayloadColumns(payload, info, tableName)
        typedValues?.requireOwner(db, info, payload, PayloadSource.AUTHORITATIVE_WIRE)
        require(normalized.keys == info.columnNamesLowerSet) { "authoritative payload column set mismatch" }
        val sql = config.replaySql.table(tableName.trim().lowercase())
        return syncDiagnosticSql(if (typed) "snapshot.expected_typed_projection" else "row.authoritative_capture") {
            db.withPreparedStatement(if (typed) sql.authoritativeTypedSelect else sql.authoritativeSelect, cache) { st ->
                if (typedValues != null) typedValues.bind(st)
                else info.columns.forEachIndexed { index, column ->
                    OversqliteValueCodec.bindPayloadValue(st, index + 1, column, normalized.getValue(column.name.lowercase()), PayloadSource.AUTHORITATIVE_WIRE)
                }
                check(st.step())
                read(info, st)
            }
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

    suspend fun processPayloadForUpload(tableName: String, payloadText: String): JsonElement = payloadForUpload(tableName, payloadText).second

    private suspend fun payloadForUpload(tableName: String, payloadText: String): Pair<JsonObject, JsonObject> {
        snapshotRowWork("row_processing.authoritative_decode")
        val payload = parseStoredPayload(tableName, payloadText)
        val tableInfo = configuredTableInfo(tableName)

        val wire = buildJsonObject {
            for (column in tableInfo.columns) {
                val key = column.name
                val value = payload[key]
                    ?: error("dirty payload for $tableName is missing column ${column.name}")
                put(key, OversqliteValueCodec.encodeWirePayloadValue(column, value))
            }
        }
        return payload to wire
    }

    private fun parseStoredPayload(tableName: String, payloadText: String): JsonObject {
        requireUniqueSnapshotJsonObjectMembers(payloadText)
        val payload = json.parseToJsonElement(payloadText) as? JsonObject
            ?: error("dirty payload for $tableName must be a JSON object")
        require(payload.keys == configuredTableInfo(tableName).columnNameSet) { "dirty payload column set mismatch for $tableName" }
        return payload
    }

    suspend fun upsertRow(
        tableName: String,
        payload: JsonObject,
        payloadSource: PayloadSource,
        statementCache: StatementCache? = null,
        skipUniqueReplay: Boolean = false,
        preservedRowId: Long? = null,
        typedValues: SnapshotTypedValues? = null,
    ) {
        val config = validatedConfig()
        val tableInfo = configuredTableInfo(config, tableName)
        val pkColumn = config.pkByTable[tableName]
            ?: error("table $tableName is not configured for sync")
        typedValues?.requireOwner(db, tableInfo, payload, payloadSource)
        val normalized = normalizePayloadColumns(payload, tableInfo, tableName)
        val values = tableInfo.columns.map { column ->
            normalized[column.name.lowercase()] ?: error("payload for $tableName is missing column ${column.name}")
        }
        if(!skipUniqueReplay) {
            val rawKey=values[tableInfo.columns.indexOfFirst{it.name.equals(pkColumn,true)}]
            val typed=typedValues?.value(pkColumn) ?: OversqliteValueCodec.decodePayloadValue(checkNotNull(tableInfo.primaryKey),rawKey,payloadSource)
            val key=when(typed){is TypedValue.Text->typed.value;is TypedValue.Blob->typed.bytes.joinToString(""){it.toUByte().toString(16).padStart(2,'0')};else->error("invalid sync key")}
            if(uniqueReplay?.invoke(tableName,key,statementCache)==true)return
        }
        var sql = config.replaySql.table(tableName).upsert
        if (preservedRowId != null) {
            val alias = checkNotNull(observableRowIdColumn(tableName, statementCache))
            // Extend the shared INSERT only; existing UPDATE assignments retain
            // the live row's identity. Replay SQL owns these fixed delimiters.
            sql = sql.replaceFirst(") VALUES (", ", ${quoteIdent(alias)}) VALUES (")
                .replaceFirst(") ON CONFLICT(", ", ?) ON CONFLICT(")
        }

        db.withPreparedStatement(sql = sql, statementCache = statementCache) { st ->
            if (typedValues != null) typedValues.bind(st)
            else tableInfo.columns.forEachIndexed { index, column ->
                bindPayloadValue(st, index + 1, column, values[index], payloadSource)
            }
            preservedRowId?.let { st.bindLong(tableInfo.columns.size + 1, it) }
            st.step()
        }
    }

    internal suspend fun observableRowIdColumn(table: String, cache: StatementCache? = null): String? {
        val without = db.withPreparedStatement("SELECT wr FROM pragma_table_list WHERE schema='main' AND name=?", cache) { st ->
            st.bindText(1, table); check(st.step()); st.getLong(0) != 0L
        }
        if (without) return null
        val names = configuredTableInfo(table).columns.map { it.name.lowercase() }
        return listOf("rowid", "_rowid_", "oid").firstOrNull { it !in names }
    }

    internal suspend fun captureRowId(table: String, pk: String, cache: StatementCache? = null): Long? {
        val alias = observableRowIdColumn(table, cache) ?: return null
        val key = checkNotNull(configuredTableInfo(table).primaryKey).name
        return db.withPreparedStatement("SELECT ${quoteIdent(alias)} FROM ${quoteIdent(table)} WHERE ${quoteIdent(key)}=?", cache) { st ->
            bindPrimaryKey(st, 1, table, pk); if (st.step()) st.getLong(0) else null
        }
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

internal fun typedValuesEqual(left: TypedValue, right: TypedValue): Boolean = when {
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
