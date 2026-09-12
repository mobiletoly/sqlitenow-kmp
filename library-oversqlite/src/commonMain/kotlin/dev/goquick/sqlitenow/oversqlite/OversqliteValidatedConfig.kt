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

internal data class ValidatedSyncTable(
    val tableName: String,
    val syncKeyColumnName: String,
)

internal data class ValidatedConfig(
    val generation: SyncGeneration?,
    val tables: List<ValidatedSyncTable>,
    val pkByTable: Map<String, String>,
    val keyByTable: Map<String, List<String>>,
    val tableOrder: Map<String, Int>,
    val tableInfoByName: Map<String, TableInfo>,
) {
    val replaySql = ReplaySchemaSql(tables, tableInfoByName, pkByTable)

    fun schemaForTable(table: String): String {
        require(tables.any { it.tableName == table }) { "table $table is not managed" }
        return generation?.tables?.single { it.table == table }?.schema ?: unconfirmedLocalSchema
    }

    fun withGeneration(accepted: SyncGeneration?): ValidatedConfig {
        if (accepted == null) return this
        accepted.requireMatchingLocalSchema(tableInfoByName, keyByTable)
        val mapped = accepted.tables.associate { table ->
            val physical = tableInfoByName.getValue(table.table)
            table.table to physical.copy(columns = physical.columns.map { column ->
                column.copy(wireType = table.columns.single { it.name == column.name }.wireType)
            })
        }
        return copy(generation = accepted, tableInfoByName = mapped)
    }

    suspend fun contractIdentity(config: OversqliteConfig, db: SafeSQLiteConnection): LocalContractIdentity {
        val localVersion = db.scalarLong("PRAGMA user_version")
        require(localVersion >= 0L) { "PRAGMA user_version must be nonnegative" }
        return LocalContractIdentity(
            config.databaseId, config.serviceId, config.generationId, localVersion,
            managedPhysicalShape(tableInfoByName, keyByTable),
        )
    }
}

internal const val unconfirmedLocalSchema = "local"
