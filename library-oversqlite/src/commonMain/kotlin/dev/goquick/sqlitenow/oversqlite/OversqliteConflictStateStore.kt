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
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject

internal data class StoredPendingConflict(
    val pending: PendingConflict,
    val details: PushConflictDetails,
    val localPayload: JsonElement?,
)

internal class OversqliteConflictStateStore(
    private val db: SafeSQLiteConnection,
    private val json: Json,
) {
    suspend fun load(): StoredPendingConflict? = db.withExclusiveAccess {
        db.prepare(
            """
            SELECT dirty_revision, decision_state, source_id, source_bundle_id,
                   canonical_request_hash, generation_id, generation_fingerprint,
                   conflict_json, local_payload_json, baseline_revision
            FROM _sync_conflict_state WHERE singleton_key = 1
            """.trimIndent(),
        ).use { st ->
            check(st.step()) { "_sync_conflict_state singleton row is missing" }
            if (st.getText(1) == "none") return@use null
            check(st.getText(1) == "pending") { "invalid durable conflict decision state" }
            val storedJson = json.parseToJsonElement(st.getText(7)) as? JsonObject
                ?: error("durable conflict details must be a JSON object")
            require(storedJson.keys.containsAll(requiredConflictDetailFields)) {
                "durable conflict details are missing required fields"
            }
            val details = json.decodeFromString<PushConflictDetails>(storedJson.toString())
            validatePushConflictDetails(details)
            val localPayload = if (st.isNull(8)) null else json.parseToJsonElement(st.getText(8))
            StoredPendingConflict(
                pending = PendingConflict(
                    revision = st.getLong(0),
                    baselineRevision = st.getLong(9),
                    sourceId = requireValidOversqliteSourceId(st.getText(2)),
                    sourceBundleId = st.getLong(3),
                    canonicalRequestHash = st.getText(4),
                    generationId = st.getLong(5),
                    generationFingerprint = st.getText(6),
                    conflict = details.toContext(localPayload),
                ),
                details = details,
                localPayload = localPayload,
            )
        }
    }

    suspend fun preserve(
        state: RuntimeState,
        snapshot: PushOutboundSnapshot,
        details: PushConflictDetails,
        localPayload: JsonElement?,
    ): StoredPendingConflict {
        val generation = checkNotNull(state.validated.generation)
        db.withPreparedStatement(
            """
            UPDATE _sync_conflict_state
            SET decision_state = 'pending', source_id = ?, source_bundle_id = ?,
                canonical_request_hash = ?, generation_id = ?, generation_fingerprint = ?,
                conflict_json = ?, local_payload_json = ?
            WHERE singleton_key = 1
            """.trimIndent(),
        ) { st ->
            st.bindText(1, state.sourceId)
            st.bindLong(2, snapshot.sourceBundleId)
            st.bindText(3, snapshot.canonicalRequestHash)
            st.bindLong(4, generation.id.toLong())
            st.bindText(5, generation.fingerprint)
            st.bindText(6, details.toStoredJson().toString())
            if (localPayload == null) st.bindNull(7) else st.bindText(7, localPayload.toString())
            st.step()
        }
        return checkNotNull(load())
    }

    // Inspect the durable decision identity, including its key fields, before
    // admitting payload copies. Stream only key scalars; full context equality
    // remains checked after admission and inside the final guarded writer.
    suspend fun requireCurrentIdentity(selected: PendingConflict) {
        val current = db.withPreparedStatement(
            """SELECT key_field.key, key_field.value, key_field.type
                FROM _sync_conflict_state,
                     json_each(_sync_conflict_state.conflict_json, '$.key') AS key_field
                WHERE singleton_key=1 AND decision_state='pending'
                AND dirty_revision=? AND baseline_revision=? AND source_id=? AND source_bundle_id=?
                AND canonical_request_hash=? AND generation_id=? AND generation_fingerprint=?
                AND json_extract(conflict_json,'$.schema')=? AND json_extract(conflict_json,'$.table')=?
                AND json_extract(conflict_json,'$.op')=? AND json_extract(conflict_json,'$.base_row_version')=?
                AND json_extract(conflict_json,'$.server_row_version')=?
                AND json_extract(conflict_json,'$.server_row_deleted')=?""",
        ) { st ->
            st.bindLong(1, selected.revision)
            st.bindLong(2, selected.baselineRevision)
            st.bindText(3, selected.sourceId)
            st.bindLong(4, selected.sourceBundleId)
            st.bindText(5, selected.canonicalRequestHash)
            st.bindLong(6, selected.generationId)
            st.bindText(7, selected.generationFingerprint)
            st.bindText(8, selected.conflict.schema)
            st.bindText(9, selected.conflict.table)
            st.bindText(10, selected.conflict.localOp)
            st.bindLong(11, selected.conflict.baseRowVersion)
            st.bindLong(12, selected.conflict.serverRowVersion)
            st.bindLong(13, if (selected.conflict.serverRowDeleted) 1 else 0)
            var matchedKeyFields = 0
            while (st.step()) {
                if (st.getText(2) != "text" || selected.conflict.key[st.getText(0)] != st.getText(1)) {
                    return@withPreparedStatement false
                }
                matchedKeyFields++
            }
            matchedKeyFields > 0 && matchedKeyFields == selected.conflict.key.size
        }
        if (!current) throw StaleConflictResolutionException()
    }

    suspend fun requireCurrent(selected: PendingConflict): StoredPendingConflict {
        val stored = load() ?: throw StaleConflictResolutionException()
        if (stored.pending != selected) throw StaleConflictResolutionException()
        return stored
    }

    suspend fun clear() {
        db.execSQL(
            """
            UPDATE _sync_conflict_state
            SET decision_state = 'none', source_id = '', source_bundle_id = 0,
                canonical_request_hash = '', generation_id = 0, generation_fingerprint = '',
                conflict_json = '', local_payload_json = NULL
            WHERE singleton_key = 1
            """.trimIndent()
        )
    }
}

private val requiredConflictDetailFields = setOf(
    "schema",
    "table",
    "key",
    "op",
    "base_row_version",
    "server_row_version",
    "server_row_deleted",
    "server_row",
)

private fun PushConflictDetails.toStoredJson(): JsonObject = buildJsonObject {
    put("schema", JsonPrimitive(schema))
    put("table", JsonPrimitive(table))
    put("key", JsonObject(key.mapValues { JsonPrimitive(it.value) }))
    put("op", JsonPrimitive(op))
    put("base_row_version", JsonPrimitive(baseRowVersion))
    put("server_row_version", JsonPrimitive(serverRowVersion))
    put("server_row_deleted", JsonPrimitive(serverRowDeleted))
    put("server_row", serverRow ?: JsonNull)
}

private fun PushConflictDetails.toContext(localPayload: JsonElement?): ConflictContext = ConflictContext(
    schema = schema,
    table = table,
    key = key,
    localOp = op,
    localPayload = localPayload,
    baseRowVersion = baseRowVersion,
    serverRowVersion = serverRowVersion,
    serverRowDeleted = serverRowDeleted,
    serverRow = serverRow,
)
