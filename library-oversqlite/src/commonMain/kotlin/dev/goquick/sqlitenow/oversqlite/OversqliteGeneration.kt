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

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonPrimitive

// Server-owned metadata. Applications select an ID; they do not construct these records.
@Serializable
internal data class GenerationDiscovery(
    @SerialName("protocol_version") val protocolVersion: String,
    @SerialName("schema_generation") val generation: SyncGeneration,
    val limits: OversqliteWireLimits,
    @SerialName("watch_enabled") val watchEnabled: Boolean,
)

@Serializable
internal data class SyncGeneration(
    val id: String,
    val fingerprint: String,
    val tables: List<GenerationTable>,
)

@Serializable
internal data class GenerationTable(
    val schema: String,
    val table: String,
    @SerialName("sync_key_columns") val syncKeyColumns: List<String>,
    val columns: List<GenerationColumn>,
    @SerialName("foreign_keys") val foreignKeys: List<GenerationForeignKey>,
)

@Serializable
internal data class GenerationColumn(
    val name: String,
    @SerialName("wire_type") val wireType: String,
    val nullable: Boolean,
)

@Serializable
internal data class GenerationForeignKey(
    val columns: List<String>,
    @SerialName("referenced_schema") val referencedSchema: String,
    @SerialName("referenced_table") val referencedTable: String,
    @SerialName("referenced_columns") val referencedColumns: List<String>,
    @SerialName("on_delete") val onDelete: String,
    @SerialName("on_update") val onUpdate: String,
)

private val generationIdentifier = Regex("[a-z0-9_]{1,63}")
private val generationFingerprint = Regex("[0-9a-f]{64}")
private val generationWireTypes = setOf("boolean", "int64", "decimal", "float64", "text", "bytes", "uuid", "json")
private val generationForeignKeyActions = setOf("no_action", "restrict", "cascade", "set_null")
internal const val generationControlBodyLimit = 65_536L

internal fun decodeGenerationDiscovery(raw: String, selectedId: Long): AdmittedOversqliteContract {
    require(selectedId in 1L..Int.MAX_VALUE.toLong()) { "generationId must be in 1..2147483647" }
    require(raw.encodeToByteArray().size <= generationControlBodyLimit) { "generation descriptor exceeds control body limit" }
    // Reuse the bounded control-body duplicate/depth guard before serialization builds maps.
    requireUniqueSnapshotJsonObjectMembers(raw)
    val root = Json.parseToJsonElement(raw)
    val rootObject = root.jsonObject
    require(rootObject.keys == setOf("protocol_version", "schema_generation", "limits", "watch_enabled")) {
        "invalid generation discovery fields"
    }
    require(!rootObject.getValue("watch_enabled").jsonPrimitive.isString) { "watch_enabled must be a JSON Boolean" }
    val limitsObject = rootObject.getValue("limits").jsonObject
    require(limitsObject.keys == setOf(
        "max_push_rows", "max_push_decoded_bytes", "max_pull_bundles", "max_pull_decoded_bytes",
        "max_snapshot_rows_per_chunk", "max_snapshot_decoded_bytes_per_chunk", "max_snapshot_rows",
        "max_snapshot_logical_bytes", "max_row_decoded_bytes",
    )) { "invalid generation discovery limits" }
    val response = Json.decodeFromJsonElement<GenerationDiscovery>(root)
    // kotlinx.serialization accepts quoted Boolean values even in its strict mode.
    for (table in root.jsonObject.getValue("schema_generation").jsonObject.getValue("tables").jsonArray) {
        for (column in table.jsonObject.getValue("columns").jsonArray) {
            require(!column.jsonObject.getValue("nullable").jsonPrimitive.isString) { "nullable must be a JSON Boolean" }
        }
    }
    if (response.protocolVersion != "v1") throw ProtocolVersionMismatchException(actual = response.protocolVersion)
    response.generation.validate(selectedId)
    validateAdmittedWireLimits(response.limits)
    return AdmittedOversqliteContract(
        protocolVersion = response.protocolVersion,
        generation = response.generation,
        limits = response.limits,
        watchEnabled = response.watchEnabled,
    )
}

internal fun decodeStoredGeneration(raw: String, selectedId: Long): SyncGeneration {
    require(raw.encodeToByteArray().size <= generationControlBodyLimit) { "generation descriptor exceeds control body limit" }
    requireUniqueSnapshotJsonObjectMembers(raw)
    val generation = Json.decodeFromString<SyncGeneration>(raw)
    generation.validate(selectedId)
    return generation
}

private fun SyncGeneration.validate(selectedId: Long) {
    require(id == selectedId.toString()) { "descriptor does not match selected generation" }
    require(generationFingerprint.matches(fingerprint)) { "invalid generation fingerprint" }
    require(tables.isNotEmpty()) { "generation requires managed tables" }
    require(tables.distinctBy { it.schema to it.table }.size == tables.size) { "duplicate generation table" }
    for (table in tables) table.validate()
    for (table in tables) {
        for (foreignKey in table.foreignKeys) validateForeignKey(table, foreignKey)
    }
    require(computeFingerprint() == fingerprint) { "generation fingerprint does not match descriptor" }
}

private fun GenerationTable.validate() {
    require(generationIdentifier.matches(schema) && generationIdentifier.matches(table)) { "invalid generation table identifier" }
    require(columns.isNotEmpty() && columns.distinctBy { it.name }.size == columns.size) { "missing or duplicate generation columns" }
    for (column in columns) {
        require(generationIdentifier.matches(column.name) && column.name != hiddenSyncScopeColumnName) { "invalid generation column identifier" }
        require(column.wireType in generationWireTypes) { "unsupported generation wire type" }
    }
    require(syncKeyColumns.size == 1) { "generation requires exactly one sync key" }
    val key = columns.singleOrNull { it.name == syncKeyColumns.single() }
    require(key != null && !key.nullable && key.wireType in setOf("text", "uuid")) { "invalid generation sync key" }
    require(foreignKeys.distinct().size == foreignKeys.size) { "duplicate generation foreign key" }
}

private fun SyncGeneration.validateForeignKey(table: GenerationTable, key: GenerationForeignKey) {
    require(key.columns.size == 1 && key.referencedColumns.size == 1) { "generation foreign key requires one column" }
    require(key.referencedSchema != hiddenSyncScopeColumnName && key.referencedTable != hiddenSyncScopeColumnName) { "invalid foreign key target" }
    require(key.onDelete in generationForeignKeyActions && key.onUpdate in generationForeignKeyActions && key.onUpdate != "set_null") {
        "unsupported generation foreign key action"
    }
    val parent = tables.singleOrNull { it.schema == key.referencedSchema && it.table == key.referencedTable }
    require(parent != null && key.referencedColumns == parent.syncKeyColumns) { "foreign key target must be a visible sync key" }
    val childColumn = table.columns.singleOrNull { it.name == key.columns.single() }
    val parentColumn = parent.columns.single { it.name == parent.syncKeyColumns.single() }
    require(childColumn != null && childColumn.wireType == parentColumn.wireType) { "foreign key columns must have matching wire types" }
    require(key.onDelete != "set_null" || childColumn.nullable) { "SET NULL requires a nullable foreign key" }
}

// generation fingerprinting ordering and array framing, using the engine's existing JCS and SHA-256.
internal fun SyncGeneration.computeFingerprint(): String {
    val orderedTables = tables.sortedWith(compareBy({ it.schema }, { it.table }))
    val tableValues = orderedTables.map { table ->
        val columns = table.columns.sortedBy { it.name }.map { column ->
            JsonArray(listOf(JsonPrimitive(column.name), JsonPrimitive(column.wireType), JsonPrimitive(column.nullable)))
        }
        val foreignKeys = table.foreignKeys.sortedWith(compareBy(
            { it.columns.single() }, { it.referencedSchema }, { it.referencedTable },
            { it.referencedColumns.single() }, { it.onDelete }, { it.onUpdate },
        )).map { key ->
            JsonArray(listOf(
                stringArray(key.columns), JsonPrimitive(key.referencedSchema), JsonPrimitive(key.referencedTable),
                stringArray(key.referencedColumns), JsonPrimitive(key.onDelete), JsonPrimitive(key.onUpdate),
            ))
        }
        JsonArray(listOf(
            JsonPrimitive(table.schema), JsonPrimitive(table.table), stringArray(table.syncKeyColumns),
            JsonArray(columns), JsonArray(foreignKeys),
        ))
    }
    val value = JsonArray(listOf(JsonPrimitive("oversync.schema-generation.v1"), JsonPrimitive(id), JsonArray(tableValues)))
    return sha256Hex(canonicalizeJsonElement(value).encodeToByteArray())
}

private fun stringArray(values: List<String>): JsonElement = JsonArray(values.map(::JsonPrimitive))

internal fun decodeGenerationDiscoveryError(status: Int, raw: String, selectedId: Long): Throwable? =
    decodeRemoteContractErrorOrNull(
        status = status,
        raw = raw,
        operation = "generation discovery",
        expectedGenerationId = selectedId,
    )
