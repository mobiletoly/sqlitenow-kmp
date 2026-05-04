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
package dev.goquick.sqlitenow.gradle.oversqlite

import dev.goquick.sqlitenow.gradle.model.AnnotatedCreateTableStatement
import dev.goquick.sqlitenow.gradle.sqlinspect.CreateTableStatement

internal data class ResolvedOversqliteSyncTable(
    val table: AnnotatedCreateTableStatement,
    val syncKeyColumnName: String,
)

internal fun resolveOversqliteSyncTables(
    databaseClassName: String,
    createTableStatements: List<AnnotatedCreateTableStatement>,
    oversqlite: Boolean,
): List<ResolvedOversqliteSyncTable> {
    val syncTables = createTableStatements
        .filter { it.annotations.enableSync }
        .distinct()
        .map { table ->
            ResolvedOversqliteSyncTable(
                table = table,
                syncKeyColumnName = resolveOversqliteSyncKeyColumn(table),
            )
        }

    if (oversqlite) {
        require(syncTables.isNotEmpty()) {
            "Database '$databaseClassName' sets oversqlite=true, but no tables are annotated with enableSync=true."
        }
    }

    return syncTables
}

private fun resolveOversqliteSyncKeyColumn(table: AnnotatedCreateTableStatement): String {
    require(table.findColumnByName(HIDDEN_SYNC_SCOPE_COLUMN_NAME) == null) {
        "table ${table.name} annotated with enableSync=true must not declare reserved server column " +
            HIDDEN_SYNC_SCOPE_COLUMN_NAME
    }
    val primaryKeyColumns = table.columns.filter { it.src.primaryKey }
    require(primaryKeyColumns.size == 1) {
        "table ${table.name} annotated with enableSync=true must declare exactly one PRIMARY KEY column"
    }

    val primaryKeyColumn = primaryKeyColumns.single()
    val resolvedSyncKey = table.annotations.syncKeyColumnName ?: primaryKeyColumn.src.name
    val configuredColumn = table.findColumnByName(resolvedSyncKey)
        ?: error("table ${table.name} annotated with enableSync=true does not contain sync key column $resolvedSyncKey")

    require(configuredColumn.src.primaryKey) {
        "table ${table.name} annotated with enableSync=true must use its PRIMARY KEY column as the visible sync key"
    }
    require(primaryKeyColumn.src.name.equals(configuredColumn.src.name, ignoreCase = true)) {
        "table ${table.name} annotated with enableSync=true must use its only PRIMARY KEY column as the visible sync key"
    }
    require(isSupportedOversqliteSyncKeyType(configuredColumn.src)) {
        "table ${table.name} annotated with enableSync=true must use TEXT PRIMARY KEY or BLOB PRIMARY KEY for its " +
            "visible sync key"
    }
    return configuredColumn.src.name
}

private fun isSupportedOversqliteSyncKeyType(column: CreateTableStatement.Column): Boolean {
    val type = column.dataType.trim().lowercase()
    return type.contains("text") || type.contains("blob")
}

private const val HIDDEN_SYNC_SCOPE_COLUMN_NAME = "_sync_scope_id"
