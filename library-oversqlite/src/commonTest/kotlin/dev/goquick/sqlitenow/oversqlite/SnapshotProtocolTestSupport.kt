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

import kotlinx.serialization.json.*

internal fun testWireLimits(): OversqliteWireLimits = OversqliteWireLimits(
    maxPushRows = 100_000,
    maxPushDecodedBytes = 64L * 1024L * 1024L,
    maxPullBundles = 1_000,
    maxPullDecodedBytes = 128L * 1024L * 1024L,
    maxSnapshotRowsPerChunk = 10_000,
    maxSnapshotDecodedBytesPerChunk = 16L * 1024L * 1024L,
    maxSnapshotRows = 10_000_000L,
    maxSnapshotLogicalBytes = 2L * 1024L * 1024L * 1024L,
    maxRowDecodedBytes = 4L * 1024L * 1024L,
)

internal fun testRegisteredTableSpecs(vararg tables: String): List<RegisteredTableSpec> =
    tables.map { table ->
        RegisteredTableSpec(
            schema = "main",
            table = table,
            syncKeyColumns = listOf("id"),
        )
    }

internal fun snapshotRowWireBytes(json: Json, row: SnapshotRow): Long =
    json.encodeToString(SnapshotRow.serializer(), row).encodeToByteArray().size.toLong()

internal fun snapshotRowsWireBytes(json: Json, rows: List<SnapshotRow>): Long =
    rows.fold(0L) { total, row -> total + snapshotRowWireBytes(json, row) }

/** The ordinary users/posts fixture's server-owned generation. Tests may supply other table/key sets. */
internal fun testGeneration(
    specs: List<RegisteredTableSpec> = testRegisteredTableSpecs("users"),
    schema: String? = null,
): SyncGeneration {
    val tables = specs.map { spec ->
        val key = spec.syncKeyColumns.firstOrNull() ?: "id"
        val columns = when (spec.table) {
            "users" -> listOf(GenerationColumn(key, "text", false), GenerationColumn("name", "text", false))
            "posts" -> listOf(GenerationColumn(key, "text", false), GenerationColumn("user_id", "text", false), GenerationColumn("title", "text", false))
            "blob_docs" -> listOf(GenerationColumn(key, "uuid", false), GenerationColumn("name", "text", false), GenerationColumn("payload", "bytes", false))
            "typed_rows" -> listOf(
                GenerationColumn(key, "text", false), GenerationColumn("name", "text", false),
                GenerationColumn("note", "text", true), GenerationColumn("count_value", "int64", true),
                GenerationColumn("enabled_flag", "boolean", false), GenerationColumn("rating", "float64", true),
                GenerationColumn("data", "bytes", true), GenerationColumn("created_at", "text", true),
            )
            "employees" -> listOf(GenerationColumn(key, "text", false), GenerationColumn("manager_id", "text", true), GenerationColumn("name", "text", false))
            "authors" -> listOf(GenerationColumn(key, "text", false), GenerationColumn("profile_id", "text", false), GenerationColumn("name", "text", false))
            "profiles" -> listOf(GenerationColumn(key, "text", false), GenerationColumn("author_id", "text", false), GenerationColumn("bio", "text", false))
            "files" -> listOf(GenerationColumn(key, "uuid", false), GenerationColumn("name", "text", false), GenerationColumn("data", "bytes", false))
            "file_reviews" -> listOf(GenerationColumn(key, "uuid", false), GenerationColumn("file_id", "uuid", false), GenerationColumn("review", "text", false))
            "activity" -> listOf(GenerationColumn(key, "text", false), GenerationColumn("title", "text", false))
            "program_item" -> listOf(GenerationColumn(key, "text", false), GenerationColumn("activity_id", "text", false), GenerationColumn("title", "text", false))
            "daily_log" -> listOf(GenerationColumn(key, "text", false), GenerationColumn("activity_id", "text", false), GenerationColumn("program_item_id", "text", false), GenerationColumn("notes", "text", false))
            else -> listOf(GenerationColumn(key, "text", false))
        }
        val foreignKeys = if (spec.table == "posts" && specs.any { it.table == "users" }) {
            listOf(GenerationForeignKey(listOf("user_id"), schema ?: spec.schema, "users", listOf("id"), "no_action", "no_action"))
        } else when (spec.table) {
            "employees" -> listOf(GenerationForeignKey(listOf("manager_id"), schema ?: spec.schema, "employees", listOf("id"), "no_action", "no_action"))
            "authors" -> listOf(GenerationForeignKey(listOf("profile_id"), schema ?: spec.schema, "profiles", listOf("id"), "no_action", "no_action"))
            "profiles" -> listOf(GenerationForeignKey(listOf("author_id"), schema ?: spec.schema, "authors", listOf("id"), "no_action", "no_action"))
            "file_reviews" -> listOf(GenerationForeignKey(listOf("file_id"), schema ?: spec.schema, "files", listOf("id"), "no_action", "no_action"))
            "program_item" -> listOf(GenerationForeignKey(listOf("activity_id"), schema ?: spec.schema, "activity", listOf("id"), "no_action", "no_action"))
            "daily_log" -> listOf(
                GenerationForeignKey(listOf("activity_id"), schema ?: spec.schema, "activity", listOf("id"), "no_action", "no_action"),
                GenerationForeignKey(listOf("program_item_id"), schema ?: spec.schema, "program_item", listOf("id"), "no_action", "no_action"),
            )
            else -> emptyList()
        }
        GenerationTable(schema ?: spec.schema, spec.table, spec.syncKeyColumns, columns, foreignKeys)
    }
    val generation = SyncGeneration("1", "", tables)
    return generation.copy(fingerprint = generation.computeFingerprint())
}

internal fun testCommittedByteCount(rows: List<BundleRow>): Long = canonicalizeJsonElement(
    JsonArray(rows.mapIndexed { index, row ->
        buildJsonObject {
            put("row_ordinal", JsonPrimitive(index.toString()))
            put("schema", JsonPrimitive(row.schema))
            put("table", JsonPrimitive(row.table))
            put("key", JsonObject(row.key.mapValues { JsonPrimitive(it.value) }))
            put("op", JsonPrimitive(row.op))
            put("row_version", JsonPrimitive(row.rowVersion.toString()))
            put("payload", row.payload ?: JsonNull)
        }
    }),
).encodeToByteArray().size.toLong()
