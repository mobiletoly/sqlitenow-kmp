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

private data class SyncTableContractKey(
    val schema: String,
    val table: String,
) {
    val qualifiedTable: String = "$schema.$table"
}

internal fun CapabilitiesResponse.requireMatchingSyncTableContract(
    validated: ValidatedConfig,
): CapabilitiesResponse {
    val server = linkedMapOf<SyncTableContractKey, List<String>>()
    registeredTableSpecs.forEach { spec ->
        if (
            spec.schema.isBlank() ||
            spec.table.isBlank() ||
            spec.syncKeyColumns.size != 1 ||
            spec.syncKeyColumns.any(String::isBlank) ||
            spec.syncKeyColumns.distinct().size != spec.syncKeyColumns.size
        ) {
            throw RemoteResponseSemanticException("capabilities request")
        }
        val key = SyncTableContractKey(spec.schema, spec.table)
        if (server.put(key, spec.syncKeyColumns.toList()) != null) {
            throw RemoteResponseSemanticException("capabilities request")
        }
    }

    val client = validated.tables.associate { table ->
        SyncTableContractKey(validated.schema, table.tableName.lowercase()) to
            listOf(table.syncKeyColumnName.lowercase())
    }
    val serverKeys = server.keys
    val clientKeys = client.keys
    val serverOnlyTables = (serverKeys - clientKeys).map(SyncTableContractKey::qualifiedTable).sorted()
    val clientOnlyTables = (clientKeys - serverKeys).map(SyncTableContractKey::qualifiedTable).sorted()
    val syncKeyMismatches = (serverKeys intersect clientKeys)
        .mapNotNull { key ->
            val clientColumns = checkNotNull(client[key])
            val serverColumns = checkNotNull(server[key])
            if (clientColumns == serverColumns) {
                null
            } else {
                SyncKeyContractMismatch(
                    qualifiedTable = key.qualifiedTable,
                    clientSyncKeyColumns = clientColumns,
                    serverSyncKeyColumns = serverColumns,
                )
            }
        }
        .sortedBy(SyncKeyContractMismatch::qualifiedTable)

    if (serverOnlyTables.isNotEmpty() || clientOnlyTables.isNotEmpty() || syncKeyMismatches.isNotEmpty()) {
        throw SyncTableContractMismatchException(
            serverOnlyTables = serverOnlyTables,
            clientOnlyTables = clientOnlyTables,
            syncKeyMismatches = syncKeyMismatches,
        )
    }
    return this
}
