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

import kotlinx.serialization.json.JsonObject

internal class OversqliteBundleApplier(
    private val localStore: OversqliteLocalStore,
    private val syncStateStore: OversqliteSyncStateStore,
    val survivors: OversqliteApplySurvivors,
) {
    suspend fun applyAuthoritativeRow(
        state: RuntimeState,
        row: BundleRow,
        localPk: String,
        keyJson: String,
        statementCache: StatementCache? = null,
        origin: String = "explicit_conflict",
        bundleSeq: Long? = null,
    ) {
        when (row.op) {
            "INSERT", "UPDATE" -> {
                val payload = row.payload as? JsonObject
                    ?: error("bundle row payload must be a JSON object for ${row.schema}.${row.table}")
                localStore.upsertRow(row.table, payload, PayloadSource.AUTHORITATIVE_WIRE, statementCache)
            }
            "DELETE" -> localStore.deleteLocalRow(state, row.table, localPk, statementCache)
            else -> error("unsupported bundle row op ${row.op}")
        }

        syncStateStore.installAuthoritativeBaseline(
            schemaName = state.validated.schemaForTable(row.table),
            tableName = row.table,
            keyJson = keyJson,
            rowVersion = row.rowVersion,
            kind = if(row.op=="DELETE") "deleted" else "live",
            payload = (row.payload as? JsonObject)?.let { localStore.captureAuthoritativePayload(row.table,it,statementCache) },
            origin = origin, bundleSeq = bundleSeq,
            statementCache = statementCache,
        )
    }
}
