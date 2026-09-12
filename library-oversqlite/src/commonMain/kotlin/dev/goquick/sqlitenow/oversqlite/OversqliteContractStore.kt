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
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.encodeToJsonElement

internal data class LocalContractIdentity(
    val databaseId: String,
    val serviceId: String,
    val generationId: Long,
    val localVersion: Long,
    val physicalShape: String,
)

// Opening and admission call these operations inside their existing writer transaction.
internal class OversqliteContractStore(private val db: SafeSQLiteConnection) {
    suspend fun open(identity: LocalContractIdentity): SyncGeneration? {
        require(identity.databaseId.isNotBlank() && identity.serviceId.isNotBlank()) { "database and service identities are required" }
        require(identity.generationId in 1L..Int.MAX_VALUE.toLong() && identity.localVersion >= 0) { "invalid generation or local version" }
        val exists = db.scalarLong(
            "SELECT count(*) FROM sqlite_schema WHERE lower(type)='table' AND lower(name)='_sync_contract_identity'",
        ) != 0L
        if (!exists) {
            val retained = firstRetainedSyncObject(db)
            val openingOnly = retained == "table $oversqliteOpeningTable"
            require(retained == null || openingOnly) {
                "existing sync state has no contract identity; recreate incompatible development databases explicitly"
            }
            if (openingOnly) {
                inspectOversqliteOpeningExtension(db, tableExists = true)
            }
            initializeOversqliteControlTables(db)
            createIdentity(identity)
        }
        return read(identity)
    }

    private suspend fun createIdentity(identity: LocalContractIdentity) {
        db.execSQL(oversqliteRetainedTableDdl.getValue("_sync_contract_identity"))
        db.execSQL(oversqliteRetainedTableDdl.getValue("_sync_admitted_contracts"))
        db.withPreparedStatement("INSERT INTO _sync_contract_identity(singleton_key,database_id,service_id,generation_id,local_version,physical_shape) VALUES(1,?,?,?,?,?)") { st ->
            st.bindText(1, identity.databaseId)
            st.bindText(2, identity.serviceId)
            st.bindLong(3, identity.generationId)
            st.bindLong(4, identity.localVersion)
            st.bindText(5, identity.physicalShape)
            st.step()
        }
    }

    suspend fun read(identity: LocalContractIdentity): SyncGeneration? {
        val fingerprint = db.withPreparedStatement("""
            SELECT fingerprint FROM _sync_contract_identity
            WHERE singleton_key=1 AND database_id=? AND service_id=? AND generation_id=?
                AND local_version=? AND physical_shape=? AND length(fingerprint) IN (0,64)
        """.trimIndent()) { st ->
            st.bindText(1, identity.databaseId)
            st.bindText(2, identity.serviceId)
            st.bindLong(3, identity.generationId)
            st.bindLong(4, identity.localVersion)
            st.bindText(5, identity.physicalShape)
            require(st.step()) { "retained contract identity is missing, changed or corrupt" }
            st.getText(0)
        }
        if (fingerprint.isEmpty()) {
            require(db.scalarLong("SELECT count(*) FROM _sync_admitted_contracts") == 0L) { "unconfirmed identity has admitted contract records" }
            require(db.scalarLong("SELECT count(*) FROM _sync_attachment_state WHERE singleton_key=1 AND binding_state='anonymous' AND attached_user_id=''") == 1L) {
                "attached state requires an admitted contract"
            }
            require(db.scalarLong("SELECT count(*) FROM _sync_outbox_bundle WHERE singleton_key=1 AND state='none'") == 1L &&
                db.scalarLong("SELECT count(*) FROM _sync_outbox_rows") == 0L) { "unconfirmed contract cannot own a wire request" }
            require(db.scalarLong("SELECT count(*) FROM _sync_operation_state WHERE singleton_key=1 AND kind='none'") == 1L) { "unconfirmed contract cannot own a remote lifecycle operation" }
            return null
        }
        return readGeneration(identity.generationId).also {
            require(it.fingerprint == fingerprint) { "installed generation fingerprint mismatch" }
        }
    }

    suspend fun readGeneration(generationId: Long): SyncGeneration = db.withPreparedStatement("""
            SELECT descriptor, descriptor_hash, fingerprint FROM _sync_admitted_contracts
            WHERE generation_id=? AND length(CAST(descriptor AS BLOB))<=65536 AND length(descriptor_hash)=64
        """.trimIndent()) { st ->
            st.bindLong(1, generationId)
            require(st.step()) { "admitted generation record is missing or corrupt" }
            val descriptor = st.getText(0)
            val fingerprint = st.getText(2)
            require(sha256Hex(descriptor.encodeToByteArray()) == st.getText(1)) {
                "admitted generation integrity mismatch"
            }
            decodeStoredGeneration(descriptor, generationId).also {
                require(it.fingerprint == fingerprint) { "admitted generation fingerprint mismatch" }
            }
        }

    suspend fun install(identity: LocalContractIdentity, generation: SyncGeneration) {
        require(read(identity) == null) { "generation is already admitted" }
        val descriptor = canonicalizeJsonElement(Json.encodeToJsonElement(generation))
        decodeStoredGeneration(descriptor, identity.generationId)
        db.withPreparedStatement("INSERT INTO _sync_admitted_contracts(generation_id,fingerprint,descriptor,descriptor_hash) VALUES(?,?,?,?)") { st ->
            st.bindLong(1, identity.generationId)
            st.bindText(2, generation.fingerprint)
            st.bindText(3, descriptor)
            st.bindText(4, sha256Hex(descriptor.encodeToByteArray()))
            st.step()
        }
        db.withPreparedStatement("UPDATE _sync_contract_identity SET fingerprint=? WHERE singleton_key=1 AND fingerprint=''") { st ->
            st.bindText(1, generation.fingerprint)
            st.step()
        }
        require(db.scalarLong("SELECT changes()") == 1L) { "contract identity changed during admission" }
        val opening = if (db.scalarLong(
                "SELECT count(*) FROM sqlite_schema WHERE lower(type)='table' AND lower(name)='$oversqliteOpeningTable'",
            ) == 1L && db.scalarLong("SELECT count(*) FROM $oversqliteOpeningTable") == 1L
        ) {
            OversqliteOpeningStore(db).readOrNull()
        } else {
            null
        }
        if (opening?.outcome == "ready" && opening.toGenerationId == identity.generationId &&
            opening.toLocalVersion == identity.localVersion && opening.targetFingerprint.isEmpty()
        ) {
            val confirmed = opening.copy(
                revision = opening.revision + 1L,
                targetFingerprint = generation.fingerprint,
                contractConfirmed = true,
            )
            OversqliteOpeningStore(db).update(confirmed, opening)
        }
    }

    suspend fun installAdditional(generation: SyncGeneration) {
        val descriptor = canonicalizeJsonElement(Json.encodeToJsonElement(generation))
        decodeStoredGeneration(descriptor, generation.id.toLong())
        val existing = db.scalarLong(
            "SELECT count(*) FROM _sync_admitted_contracts WHERE generation_id=${generation.id.toLong()}",
        )
        if (existing != 0L) {
            val retained = readGeneration(generation.id.toLong())
            require(retained.fingerprint == generation.fingerprint) {
                "target generation differs from retained admission"
            }
            return
        }
        db.withPreparedStatement(
            "INSERT INTO _sync_admitted_contracts(generation_id,fingerprint,descriptor,descriptor_hash) VALUES(?,?,?,?)",
        ) { st ->
            st.bindLong(1, generation.id.toLong())
            st.bindText(2, generation.fingerprint)
            st.bindText(3, descriptor)
            st.bindText(4, sha256Hex(descriptor.encodeToByteArray()))
            st.step()
        }
    }
}
