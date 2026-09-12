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

/** Internal schema-evolution compatibility profile; it is neither a plan field nor durable state. */
internal fun requireSchemaEvolutionCompatibility(
    old: SyncGeneration,
    target: SyncGeneration,
) {
    val oldTables = old.tables.associateBy { it.schema to it.table }
    val targetTables = target.tables.associateBy { it.schema to it.table }
    requireProfile(oldTables.keys.all(targetTables::containsKey), "table_omission")

    oldTables.keys.sortedBy { "${it.first}.${it.second}" }.forEach { identity ->
        val oldTable = checkNotNull(oldTables[identity])
        val targetTable = checkNotNull(targetTables[identity])
        requireProfile(oldTable.syncKeyColumns == targetTable.syncKeyColumns, "sync_key")

        val oldColumns = oldTable.columns.associateBy(GenerationColumn::name)
        val targetColumns = targetTable.columns.associateBy(GenerationColumn::name)
        (oldColumns.keys intersect targetColumns.keys).sorted().forEach { name ->
            val oldColumn = checkNotNull(oldColumns[name])
            val targetColumn = checkNotNull(targetColumns[name])
            requireProfile(oldColumn.wireType == targetColumn.wireType, "shared_wire_type")
            requireProfile(oldColumn.nullable == targetColumn.nullable, "shared_nullability")
        }

        val targetForeignKeys = targetTable.foreignKeys.toSet()
        oldTable.foreignKeys.sortedWith(generationForeignKeyOrder).forEach { key ->
            requireProfile(key in targetForeignKeys, "old_foreign_key")
        }
        (targetForeignKeys - oldTable.foreignKeys.toSet())
            .sortedWith(generationForeignKeyOrder)
            .forEach { key ->
                requireAdditiveForeignKey(
                    oldChild = oldTable,
                    targetChild = targetTable,
                    targetTables = targetTables,
                    key = key,
                )
            }

        (targetColumns.keys - oldColumns.keys).sorted().forEach { name ->
            requireProfile(name !in targetTable.syncKeyColumns, "target_only_key")
        }
        (oldColumns.keys - targetColumns.keys).sorted().forEach { name ->
            requireProfile(name !in oldTable.syncKeyColumns, "old_only_key")
            requireProfile(!old.hasForeignKeyEndpoint(identity, name), "old_only_foreign_key")
        }
    }

    (targetTables.keys - oldTables.keys).sortedBy { "${it.first}.${it.second}" }.forEach { identity ->
        val targetTable = checkNotNull(targetTables[identity])
        targetTable.foreignKeys.sortedWith(generationForeignKeyOrder).forEach { key ->
            requireAdditiveForeignKey(
                oldChild = null,
                targetChild = targetTable,
                targetTables = targetTables,
                key = key,
            )
        }
    }
}

internal suspend fun requireNoAddedSynchronizedTableNamespaceCollisions(
    connection: SafeSQLiteConnection,
    old: SyncGeneration,
    target: SyncGeneration,
) {
    val oldIdentities = old.tables.map { it.schema to it.table }.toSet()
    target.tables
        .filter { (it.schema to it.table) !in oldIdentities }
        .sortedWith(compareBy({ it.schema }, { it.table }))
        .forEach { table ->
            val collisions = connection.queryList(
                """
                SELECT type,name FROM sqlite_schema
                WHERE lower(name)=? AND type IN ('table','view','index','trigger')
                ORDER BY type COLLATE BINARY,name COLLATE BINARY
                """.trimIndent(),
                bind = { it.bindText(1, table.table.lowercase()) },
            ) { row -> "${row.getText(0)}:${row.getText(1)}" }
            requireProfile(collisions.isEmpty(), "new_table_namespace")
        }
}

private fun requireAdditiveForeignKey(
    oldChild: GenerationTable?,
    targetChild: GenerationTable,
    targetTables: Map<Pair<String, String>, GenerationTable>,
    key: GenerationForeignKey,
) {
    requireProfile(key.columns.size == 1 && key.referencedColumns.size == 1, "new_foreign_key_shape")
    val childName = key.columns.single()
    val parentIdentity = key.referencedSchema to key.referencedTable
    val parent = targetTables[parentIdentity]
    requireProfile(parent != null && key.referencedColumns == parent.syncKeyColumns, "new_foreign_key_endpoint")
    val childColumn = targetChild.columns.singleOrNull { it.name == childName }
    val parentColumn = parent?.columns?.singleOrNull { it.name == key.referencedColumns.single() }
    requireProfile(childColumn != null && parentColumn != null, "new_foreign_key_endpoint")
    requireProfile(oldChild?.columns?.none { it.name == childName } != false, "new_foreign_key_old_child")
    requireProfile(childColumn!!.nullable, "new_foreign_key_nullability")
    requireProfile(childColumn.wireType == parentColumn!!.wireType, "new_foreign_key_wire_type")
    requireProfile(
        key.onDelete in setOf("no_action", "restrict", "cascade", "set_null") &&
            key.onUpdate in setOf("no_action", "restrict", "cascade") &&
            (key.onDelete != "set_null" || childColumn.nullable),
        "new_foreign_key_action",
    )
}

private fun SyncGeneration.hasForeignKeyEndpoint(
    identity: Pair<String, String>,
    column: String,
): Boolean = tables.any { table ->
    table.foreignKeys.any { key ->
        ((table.schema to table.table) == identity && column in key.columns) ||
            ((key.referencedSchema to key.referencedTable) == identity && column in key.referencedColumns)
    }
}

private val generationForeignKeyOrder = compareBy<GenerationForeignKey>(
    { it.columns.joinToString("\u0000") },
    GenerationForeignKey::referencedSchema,
    GenerationForeignKey::referencedTable,
    { it.referencedColumns.joinToString("\u0000") },
    GenerationForeignKey::onDelete,
    GenerationForeignKey::onUpdate,
)

private fun requireProfile(condition: Boolean, reason: String) {
    if (!condition) throw OversqliteUpgradePlanMismatchException("schema_evolution_compatibility_$reason")
}
