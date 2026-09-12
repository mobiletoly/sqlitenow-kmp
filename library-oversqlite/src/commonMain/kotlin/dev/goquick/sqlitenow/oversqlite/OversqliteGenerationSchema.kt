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

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive

// SQLite's documented affinity precedence. No CREATE-statement parsing is involved.
internal fun sqliteAffinity(declaredType: String): String {
    val type = declaredType.uppercase()
    return when {
        "INT" in type -> "INTEGER"
        listOf("CHAR", "CLOB", "TEXT").any { it in type } -> "TEXT"
        type.isEmpty() || "BLOB" in type -> "BLOB"
        listOf("REAL", "FLOA", "DOUB").any { it in type } -> "REAL"
        else -> "NUMERIC"
    }
}

internal fun GenerationColumn.requireCompatibleStorage(local: ColumnInfo) {
    val allowed = when (wireType) {
        "boolean", "int64" -> setOf("INTEGER")
        "float64" -> setOf("REAL")
        "decimal", "text", "json" -> setOf("TEXT")
        "bytes" -> setOf("BLOB")
        "uuid" -> setOf("TEXT", "BLOB")
        else -> error("unsupported server wire type $wireType")
    }
    require(sqliteAffinity(local.declaredType) in allowed) { "column ${local.name}: SQLite ${local.declaredType} cannot represent $wireType" }
}

internal fun SyncGeneration.requireMatchingLocalSchema(localTables: Map<String, TableInfo>, keys: Map<String, List<String>>) {
    require(tables.map { it.table }.distinct().size == tables.size) { "server table names collide in local SQLite" }
    require(tables.map { it.table }.toSet() == localTables.keys && localTables.keys == keys.keys) {
        "selected generation must match the complete managed local table set"
    }
    val generationTables = tables.associateBy { it.table }
    for (table in tables) {
        val local = localTables.getValue(table.table)
        require(local.table == table.table && local.tableKind == "table") { "managed ${table.table} must be an ordinary table with its exact name" }
        require(keys.getValue(table.table) == table.syncKeyColumns) { "sync key mismatch for ${table.table}" }
        require(local.columns.map { it.name }.toSet() == table.columns.map { it.name }.toSet()) { "column set mismatch for ${table.table}" }
        for (column in table.columns) {
            val physical = local.columns.single { it.name == column.name }
            require(physical.hidden == 0) { "generated or hidden managed column ${table.table}.${column.name}" }
            require(column.nullable != (physical.notNull || physical.primaryKeyOrdinal > 0)) { "nullability mismatch for ${table.table}.${column.name}" }
            val expectedKeyOrdinal = if (column.name == table.syncKeyColumns.single()) 1 else 0
            require(physical.primaryKeyOrdinal == expectedKeyOrdinal) { "primary key mismatch for ${table.table}.${column.name}" }
            column.requireCompatibleStorage(physical)
        }
        table.requireMatchingForeignKeys(local, localTables, generationTables)
    }
}

private fun GenerationTable.requireMatchingForeignKeys(
    local: TableInfo,
    localTables: Map<String, TableInfo>,
    generationTables: Map<String, GenerationTable>,
) {
    val remaining = local.foreignKeys.toMutableList()
    require(remaining.size == foreignKeys.size && remaining.all { it.seq == 0 }) { "foreign key shape mismatch for $table" }
    for (key in foreignKeys) {
        val index = remaining.indexOfFirst {
            it.fromCol == key.columns.single() && it.refTable == key.referencedTable && it.toCol == key.referencedColumns.single() &&
                it.onDelete.lowercase().replace(' ', '_') == key.onDelete && it.onUpdate.lowercase().replace(' ', '_') == key.onUpdate
        }
        require(index >= 0) { "foreign key contract mismatch for $table" }
        val childColumn = columns.single { it.name == key.columns.single() }
        if (childColumn.wireType == "uuid") {
            val parentGeneration = generationTables.getValue(key.referencedTable)
            val parentColumn = parentGeneration.columns.single { it.name == key.referencedColumns.single() }
            require(parentColumn.wireType == "uuid") { "UUID foreign key endpoints must use matching wire types" }
            val childAffinity = sqliteAffinity(local.column(childColumn.name).declaredType)
            val parentAffinity = sqliteAffinity(
                localTables.getValue(key.referencedTable).column(parentColumn.name).declaredType,
            )
            require(childAffinity == parentAffinity) {
                "UUID foreign key $table.${childColumn.name} must use the same TEXT/BLOB representation as ${key.referencedTable}.${parentColumn.name}"
            }
        }
        remaining.removeAt(index)
    }
}

// Local integrity identity is independent of mutable operation journals and server proof.
internal fun managedPhysicalShape(tables: Map<String, TableInfo>, keys: Map<String, List<String>>): String {
    val rows = tables.entries.sortedBy { it.key }.map { (name, table) ->
        require(table.tableKind == "table" && name == table.table) { "managed $name must be an ordinary table with its exact name" }
        val columns = table.columns.sortedBy { it.name }.map {
            JsonArray(listOf(JsonPrimitive(it.name), JsonPrimitive(it.declaredType), JsonPrimitive(it.notNull),
                JsonPrimitive(it.primaryKeyOrdinal), JsonPrimitive(it.hidden)))
        }
        val foreignKeys = table.foreignKeys.sortedWith(compareBy({ it.fromCol }, { it.refTable }, { it.toCol }, { it.seq })).map {
            JsonArray(listOf(JsonPrimitive(it.fromCol), JsonPrimitive(it.toCol), JsonPrimitive(it.refTable),
                JsonPrimitive(it.onUpdate), JsonPrimitive(it.onDelete), JsonPrimitive(it.match), JsonPrimitive(it.seq)))
        }
        JsonArray(listOf(JsonPrimitive(name), JsonArray(keys.getValue(name).map(::JsonPrimitive)),
            JsonPrimitive(table.withoutRowId), JsonPrimitive(table.strict), JsonArray(columns), JsonArray(foreignKeys)))
    }
    return canonicalizeJsonElement(JsonArray(rows))
}
