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

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import kotlinx.serialization.json.Json

internal class OversqliteContractGateway(
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
    private val json: Json,
    private val tableInfoCache: TableInfoCache,
    private val runtimeInitializer: SyncRuntimeInitializer,
    private val remoteApi: OversqliteRemoteApi,
    private val sessionState: OversqliteClientSessionState,
    private val operationController: OversqliteClientOperationController,
) {
    internal suspend fun ensureAdmittedContract(): Boolean {
        if (sessionState.admittedContract != null) return false
        val current = sessionState.requireValidatedConfig()
        val sourceId = sessionState.requireCurrentSourceId()
        val contract = operationController.withTransientRetry("generation discovery") {
            remoteApi.fetchGeneration(sourceId, config.generationId)
        }
        val discovered = contract.generation
        val existing = current.generation
        if (existing != null && (existing.id != discovered.id || existing.fingerprint != discovered.fingerprint)) {
            throw GenerationContractMismatchException()
        }
        val candidate = current.withGeneration(discovered)
        if (existing == null) {
            db.transaction(TransactionMode.IMMEDIATE) {
                tableInfoCache.clear()
                val physical = runtimeInitializer.validateConfig(db)
                val identity = physical.contractIdentity(config, db)
                val store = OversqliteContractStore(db)
                require(store.read(identity) == null) { "generation was admitted concurrently" }
                discovered.requireMatchingLocalSchema(physical.tableInfoByName, physical.keyByTable)
                validatePendingAdmission(candidate, sourceId, contract)
                store.install(identity, discovered)
                for (table in candidate.tables) {
                    for (metadata in listOf("_sync_dirty_rows", "_sync_row_state", "_sync_managed_tables")) {
                        db.withPreparedStatement(
                            "UPDATE $metadata SET schema_name=? WHERE table_name=? AND schema_name=?",
                        ) { statement ->
                            statement.bindText(1, candidate.schemaForTable(table.tableName))
                            statement.bindText(2, table.tableName)
                            statement.bindText(3, unconfirmedLocalSchema)
                            statement.step()
                        }
                    }
                }
                bindLocalBaselineContext(db)
                for (table in candidate.tables) {
                    for(suffix in listOf("ai","au","ad")) db.execSQL("DROP TRIGGER ${quoteIdent("trg_${table.tableName}_$suffix")}")
                }
                installOversqliteTriggers(db, candidate, tableInfoCache)
            }
            tableInfoCache.install(candidate.tableInfoByName)
            sessionState.validatedConfig = candidate
        }
        sessionState.admittedContract = contract
        return true
    }

    internal suspend fun verifyConnectLifecycleSupported() {
        ensureAdmittedContract()
    }

    internal suspend fun requireAdmittedRuntime(operation: String) {
        sessionState.requireConnectedRuntimeState(operation)
        sessionState.requireAdmittedContract()
    }

    private suspend fun validatePendingAdmission(
        candidate: ValidatedConfig,
        sourceId: String,
        contract: AdmittedOversqliteContract,
    ) {
        val maxBytes = contract.limits.maxRowDecodedBytes
        require(
            db.scalarLong(
                "SELECT count(*) FROM _sync_dirty_rows " +
                    "WHERE length(CAST(CASE WHEN json_type(payload)='array' THEN json_extract(payload,'$[0]') ELSE payload END AS BLOB))>$maxBytes " +
                    "OR length(CAST(key_json AS BLOB))>$maxBytes",
            ) == 0L,
        ) {
            "pending local row exceeds the server payload bound"
        }
        val validator = OversqliteLocalStore(db, json) { candidate }
        val state = RuntimeState(candidate, contract, "", sourceId, "")
        db.withPreparedStatement(
            "SELECT table_name,key_json,payload FROM _sync_dirty_rows ORDER BY rowid",
        ) { statement ->
            while (statement.step()) {
                val table = statement.getText(0)
                validator.decodeDirtyKeyForPush(state, table, statement.getText(1))
                if (!statement.isNull(2)) {
                    validator.processPayloadForUpload(table, statement.getText(2))
                }
            }
        }
    }
}
