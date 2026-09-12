/*
 * Copyright 2026 Toly Pochkin
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.doubleOrNull
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull

internal object OversqliteCompactCodec {
    internal data class SemanticRow(
        val schema: String,
        val table: String,
        val key: Map<String, JsonElement>,
        val op: String,
        val version: Long,
        val payload: JsonObject?,
    )

    fun encodePushRows(generation: SyncGeneration, rows: List<PushRequestRow>): JsonArray {
        require(rows.isNotEmpty()) { "compact push rows must not be empty" }
        val groups = mutableListOf<JsonElement>()
        var activeTableIndex = -1
        var activeStart = 0
        var activeRows = mutableListOf<JsonElement>()
        rows.forEachIndexed { ordinal, row ->
            val tableIndex = generation.tableIndex(row.schema, row.table)
            val table = generation.tables[tableIndex]
            validateLogicalRow(table, row.key, row.op, row.baseRowVersion, row.payload, snapshot = false)
            if (tableIndex != activeTableIndex) {
                if (activeTableIndex >= 0) groups.add(group(activeTableIndex, activeStart, activeRows))
                activeTableIndex = tableIndex
                activeStart = ordinal
                activeRows = mutableListOf()
            }
            activeRows.add(tuple(table, row.key, row.op, row.baseRowVersion, row.payload))
        }
        groups.add(group(activeTableIndex, activeStart, activeRows))
        return JsonArray(groups)
    }

    fun encodeBundleRows(
        generation: SyncGeneration,
        rows: List<BundleRow>,
        firstOrdinal: Long = 0,
        snapshot: Boolean = false,
    ): JsonArray {
        if (rows.isEmpty()) return JsonArray(emptyList())
        require(firstOrdinal >= 0 && firstOrdinal <= Int.MAX_VALUE) { "compact first ordinal is invalid" }
        val groups = mutableListOf<JsonElement>()
        var activeTableIndex = -1
        var activeStart = firstOrdinal.toInt()
        var activeRows = mutableListOf<JsonElement>()
        rows.forEachIndexed { index, row ->
            val tableIndex = generation.tableIndex(row.schema, row.table)
            val table = generation.tables[tableIndex]
            validateLogicalRow(table, row.key, row.op, row.rowVersion, row.payload, snapshot = snapshot)
            if (tableIndex != activeTableIndex) {
                if (activeTableIndex >= 0) groups.add(group(activeTableIndex, activeStart, activeRows))
                activeTableIndex = tableIndex
                activeStart = firstOrdinal.toInt() + index
                activeRows = mutableListOf()
            }
            activeRows.add(tuple(table, row.key, row.op, row.rowVersion, row.payload))
        }
        groups.add(group(activeTableIndex, activeStart, activeRows))
        return JsonArray(groups)
    }

    fun decodeBundleRows(
        generation: SyncGeneration,
        encoded: JsonElement,
        expectedFirstOrdinal: Long = 0,
        allowEmpty: Boolean = false,
        snapshot: Boolean = false,
    ): List<BundleRow> = decodeSemanticRows(
        generation = generation,
        encoded = encoded,
        expectedFirstOrdinal = expectedFirstOrdinal,
        allowEmpty = allowEmpty,
        snapshot = snapshot,
    ).map { row ->
        BundleRow(
            schema = row.schema,
            table = row.table,
            key = row.key.mapValues { (name, value) -> exactString(value, "compact sync key $name") },
            op = row.op,
            rowVersion = row.version,
            payload = row.payload,
        )
    }

    internal fun decodeSemanticRows(
        generation: SyncGeneration,
        encoded: JsonElement,
        expectedFirstOrdinal: Long = 0,
        allowEmpty: Boolean = false,
        snapshot: Boolean = false,
    ): List<SemanticRow> {
        val groups = encoded as? JsonArray ?: error("compact rows must be an array")
        if (groups.isEmpty()) {
            require(allowEmpty) { "compact rows must not be empty" }
            return emptyList()
        }
        var nextOrdinal = expectedFirstOrdinal
        var previousTableIndex = -1
        val seen = mutableSetOf<String>()
        val result = mutableListOf<SemanticRow>()
        for (groupElement in groups) {
            val group = groupElement as? JsonArray ?: error("compact row group must be an array")
            require(group.size == 3) { "compact row group must have exactly three fields" }
            val tableIndex = exactNonNegativeInt(group[0], "compact table index")
            require(tableIndex in generation.tables.indices) { "unknown compact table index $tableIndex" }
            require(tableIndex != previousTableIndex) { "adjacent compact groups repeat table index $tableIndex" }
            val firstOrdinal = exactNonNegativeLong(group[1], "compact first row ordinal")
            require(firstOrdinal == nextOrdinal) { "compact row ordinal gap or overlap at $firstOrdinal" }
            val tuples = group[2] as? JsonArray ?: error("compact group rows must be an array")
            require(tuples.isNotEmpty()) { "compact row group must not be empty" }
            val table = generation.tables[tableIndex]
            for (tupleElement in tuples) {
                val tuple = tupleElement as? JsonArray ?: error("compact row tuple must be an array")
                require(tuple.size == 3 || tuple.size == 4) { "compact row tuple has invalid arity" }
                val opCode = exactString(tuple[0], "compact row operation")
                val logicalOp = when (opCode) {
                    "i" -> "INSERT"
                    "u" -> "UPDATE"
                    "d" -> "DELETE"
                    else -> error("compact row tuple operation is invalid")
                }
                require((logicalOp == "DELETE") == (tuple.size == 3)) {
                    "compact row tuple arity does not match operation"
                }
                require(!snapshot || logicalOp == "INSERT") { "snapshot compact tuple must use insert operation" }
                val keyValues = tuple[1] as? JsonArray ?: error("compact row key values must be an array")
                require(keyValues.size == table.syncKeyColumns.size) { "compact row key arity is invalid" }
                val key = linkedMapOf<String, JsonElement>()
                table.syncKeyColumns.forEachIndexed { index, column ->
                    val value = keyValues[index]
                    validateWireValue(table.column(column), value, allowNull = false)
                    key[column] = value
                }
                val version = exactNonNegativeLong(tuple[2], "compact row version")
                val payload = if (tuple.size == 4) tuple[3] as? JsonObject
                    ?: error("compact row payload must be an object") else null
                validateSemanticRow(table, key, logicalOp, version, payload, snapshot)
                val identity = "${table.schema}.${table.table}:${canonicalizeJsonElement(semanticKeyJson(key))}"
                require(seen.add(identity)) { "duplicate compact logical target $identity" }
                result += SemanticRow(
                    schema = table.schema,
                    table = table.table,
                    key = key,
                    op = logicalOp,
                    version = version,
                    payload = payload,
                )
                nextOrdinal++
            }
            previousTableIndex = tableIndex
        }
        return result
    }

    fun expandedRowDecodedBytes(row: PushRequestRow): Long {
        val keyBytes = canonicalizeJsonElement(syncKeyJson(row.key)).encodeToByteArray().size.toLong()
        if (row.op == "DELETE") return keyBytes
        val payload = row.payload as? JsonObject ?: error("non-delete row payload must be an object")
        return checkedAdd(keyBytes, canonicalizeJsonElement(payload).encodeToByteArray().size.toLong())
    }

    fun snapshotLogicalByteCount(generation: SyncGeneration, rows: List<BundleRow>): Long {
        var total = 0L
        for (row in rows) {
            require(row.op == "INSERT") { "snapshot row must use insert operation" }
            val tableIndex = generation.tableIndex(row.schema, row.table)
            val table = generation.tables[tableIndex]
            val values = JsonArray(table.syncKeyColumns.map { JsonPrimitive(row.key.getValue(it)) })
            val value = JsonArray(listOf(
                JsonPrimitive("i"), JsonPrimitive(tableIndex), values,
                JsonPrimitive(row.rowVersion), row.payload ?: error("snapshot payload is required"),
            ))
            total = checkedAdd(total, canonicalizeJsonElement(value).encodeToByteArray().size.toLong())
        }
        return total
    }

    private fun tuple(
        table: GenerationTable,
        key: SyncKey,
        op: String,
        version: Long,
        payload: JsonElement?,
    ): JsonArray = buildJsonArray {
        add(JsonPrimitive(when (op) { "INSERT" -> "i"; "UPDATE" -> "u"; "DELETE" -> "d"; else -> error("invalid row operation") }))
        add(JsonArray(table.syncKeyColumns.map { JsonPrimitive(key.getValue(it)) }))
        add(JsonPrimitive(version))
        if (op != "DELETE") add(payload ?: error("non-delete row payload is required"))
    }

    private fun group(tableIndex: Int, firstOrdinal: Int, rows: List<JsonElement>): JsonArray =
        JsonArray(listOf(JsonPrimitive(tableIndex), JsonPrimitive(firstOrdinal), JsonArray(rows)))

    private fun validateSemanticRow(
        table: GenerationTable,
        key: Map<String, JsonElement>,
        op: String,
        version: Long,
        payload: JsonElement?,
        snapshot: Boolean,
    ) {
        require(op in setOf("INSERT", "UPDATE", "DELETE")) { "invalid logical operation" }
        require(version >= 0) { "row version must be non-negative" }
        require(key.keys == table.syncKeyColumns.toSet()) { "compact row key columns are invalid" }
        require(key.keys.none(hiddenWireColumnNames::contains)) { "compact row key contains hidden scope column" }
        if (op == "DELETE") {
            require(payload == null || payload is JsonNull) { "delete row must not contain payload" }
            return
        }
        val objectPayload = payload as? JsonObject ?: error("insert/update payload must be an object")
        val columns = table.columns.associateBy { it.name }
        for ((name, value) in objectPayload) {
            require(name !in hiddenWireColumnNames) { "compact payload contains hidden scope column" }
            val column = columns[name] ?: error("compact payload contains unknown column $name")
            validateWireValue(column, value, allowNull = column.nullable)
        }
        if (snapshot) {
            require(objectPayload.keys == columns.keys) { "snapshot payload must be a full after-image" }
            for (keyColumn in table.syncKeyColumns) {
                require(objectPayload[keyColumn] == key.getValue(keyColumn)) {
                    "snapshot payload key does not match tuple key"
                }
            }
        }
    }

    private fun validateLogicalRow(
        table: GenerationTable,
        key: SyncKey,
        op: String,
        version: Long,
        payload: JsonElement?,
        snapshot: Boolean,
    ) {
        require(op in setOf("INSERT", "UPDATE", "DELETE")) { "invalid logical operation" }
        require(version >= 0) { "row version must be non-negative" }
        require(key.keys == table.syncKeyColumns.toSet()) { "compact row key columns are invalid" }
        require(key.keys.none(hiddenWireColumnNames::contains)) { "compact row key contains hidden scope column" }
        if (op == "DELETE") {
            require(payload == null || payload is JsonNull) { "delete row must not contain payload" }
            return
        }
        val objectPayload = payload as? JsonObject ?: error("insert/update payload must be an object")
        val columns = table.columns.associateBy { it.name }
        for ((name, value) in objectPayload) {
            require(name !in hiddenWireColumnNames) { "compact payload contains hidden scope column" }
            val column = columns[name] ?: error("compact payload contains unknown column $name")
            validateWireValue(column, value, allowNull = column.nullable)
        }
        if (snapshot) {
            require(objectPayload.keys == columns.keys) { "snapshot payload must be a full after-image" }
            for (keyColumn in table.syncKeyColumns) {
                require(objectPayload[keyColumn] == JsonPrimitive(key.getValue(keyColumn))) {
                    "snapshot payload key does not match tuple key"
                }
            }
        }
    }

    private fun validateWireValue(column: GenerationColumn, value: JsonElement, allowNull: Boolean) {
        if (value is JsonNull) {
            require(allowNull) { "non-null column ${column.name} is null" }
            return
        }
        val primitive = value as? JsonPrimitive
        when (column.wireType) {
            "boolean" -> require(primitive?.isString == false && primitive.booleanOrNull != null) {
                "boolean wire value required for ${column.name}"
            }
            "int64" -> require(primitive?.isString == true && primitive.longOrNull != null) {
                "canonical int64 JSON string required for ${column.name}; got ${primitive?.content}"
            }
            "float64" -> require(primitive?.isString == true && primitive.doubleOrNull != null) {
                "canonical float64 JSON string required for ${column.name}; got ${primitive?.content}"
            }
            "decimal", "text", "bytes", "uuid" -> require(primitive?.isString == true)
            "json" -> Unit
            else -> error("unsupported generation wire type ${column.wireType}")
        }
    }

    private fun SyncGeneration.tableIndex(schema: String, table: String): Int =
        tables.indexOfFirst { it.schema == schema && it.table == table }
            .also { require(it >= 0) { "row table $schema.$table is not in generation" } }

    private fun GenerationTable.column(name: String): GenerationColumn =
        columns.singleOrNull { it.name == name } ?: error("generation key column $name is missing")

    private fun syncKeyJson(key: SyncKey): JsonObject = JsonObject(
        key.entries.sortedBy { it.key }.associate { it.key to JsonPrimitive(it.value) },
    )

    private fun semanticKeyJson(key: Map<String, JsonElement>): JsonObject = JsonObject(
        key.entries.sortedBy { it.key }.associate { it.key to it.value },
    )

    private fun exactString(element: JsonElement, field: String): String {
        val primitive = element as? JsonPrimitive ?: error("$field must be a string")
        require(primitive.isString) { "$field must be a string" }
        return primitive.content
    }

    private fun exactNonNegativeLong(element: JsonElement, field: String): Long {
        val primitive = element as? JsonPrimitive ?: error("$field must be an integer")
        require(!primitive.isString) { "$field must be an integer" }
        return primitive.longOrNull?.also { require(it >= 0) { "$field must be non-negative" } }
            ?: error("$field must be an integer")
    }

    private fun exactNonNegativeInt(element: JsonElement, field: String): Int {
        val value = exactNonNegativeLong(element, field)
        require(value <= Int.MAX_VALUE) { "$field is too large" }
        return value.toInt()
    }

    private fun checkedAdd(left: Long, right: Long): Long {
        require(right >= 0 && left <= Long.MAX_VALUE - right) { "decoded byte count overflow" }
        return left + right
    }
}

private val hiddenWireColumnNames = setOf(hiddenSyncScopeColumnName, "__oversync_owner")
