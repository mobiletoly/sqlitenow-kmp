package dev.goquick.sqlitenow.oversqlite.platform

import kotlinx.serialization.json.*

internal val testJson = Json { ignoreUnknownKeys = true; encodeDefaults = true }

internal data class TestRegisteredTableSpec(
    val schema: String,
    val table: String,
    val syncKeyColumns: List<String>,
)

internal data class TestWireLimits(
    val maxPushRows: Int = 100_000,
    val maxPushDecodedBytes: Long = 64L * 1024L * 1024L,
    val maxPullBundles: Int = 1000,
    val maxPullDecodedBytes: Long = 64L * 1024L * 1024L,
    val maxSnapshotRowsPerChunk: Int = 10_000,
    val maxSnapshotDecodedBytesPerChunk: Long = 16L * 1024L * 1024L,
    val maxSnapshotRows: Long = 10_000_000L,
    val maxSnapshotLogicalBytes: Long = 2L * 1024L * 1024L * 1024L,
    val maxRowDecodedBytes: Long = 4L * 1024L * 1024L,
)

internal fun testWireLimits(): TestWireLimits = TestWireLimits()

internal fun testRegisteredTableSpecs(vararg tables: String): List<TestRegisteredTableSpec> =
    tables.map { table ->
        TestRegisteredTableSpec(schema = "main", table = table, syncKeyColumns = listOf("id"))
    }

// Server-owned fixtures for the SQL layouts exercised by the platform suite.
internal fun testGenerationResponse(specs: List<TestRegisteredTableSpec>, usersUpdatedAt: Boolean = false): JsonObject {
    val fixture = if (specs.size == 8) "production" else specs.joinToString(",") { it.table } + if (usersUpdatedAt) "+updated_at" else ""
    val descriptor = when (fixture) {
        "users" -> """{"id":"1","fingerprint":"8cd94e6ebb91e4022dd8bb64f7d60048b8bf380d5af4982313d23d2d7999b625","tables":[{"schema":"main","table":"users","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"text","nullable":false},{"name":"name","wire_type":"text","nullable":false}],"foreign_keys":[]}]}"""
        "users,posts" -> """{"id":"1","fingerprint":"24eb072b43c47e99f1b4e1bef42de3e7e1cf9f275811a5f6442437f22a9bc48e","tables":[{"schema":"main","table":"posts","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"text","nullable":false},{"name":"user_id","wire_type":"text","nullable":false},{"name":"title","wire_type":"text","nullable":false}],"foreign_keys":[{"columns":["user_id"],"referenced_schema":"main","referenced_table":"users","referenced_columns":["id"],"on_delete":"no_action","on_update":"no_action"}]},{"schema":"main","table":"users","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"text","nullable":false},{"name":"name","wire_type":"text","nullable":false}],"foreign_keys":[]}]}"""
        "users,posts+updated_at" -> """{"id":"1","fingerprint":"bb26c7b3b81ccb2626b9dc81cc1ef56dc089c5d542ffb1cdb3801ccdbcf4862e","tables":[{"schema":"main","table":"posts","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"text","nullable":false},{"name":"user_id","wire_type":"text","nullable":false},{"name":"title","wire_type":"text","nullable":false}],"foreign_keys":[{"columns":["user_id"],"referenced_schema":"main","referenced_table":"users","referenced_columns":["id"],"on_delete":"no_action","on_update":"no_action"}]},{"schema":"main","table":"users","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"text","nullable":false},{"name":"name","wire_type":"text","nullable":false},{"name":"updated_at","wire_type":"text","nullable":false}],"foreign_keys":[]}]}"""
        "files" -> """{"id":"1","fingerprint":"9046cec4c1d716e38af72cd55222f0fd3a624bcc9875948e077ccffaba64d6f0","tables":[{"schema":"main","table":"files","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"text","nullable":false},{"name":"name","wire_type":"text","nullable":false},{"name":"data","wire_type":"bytes","nullable":false}],"foreign_keys":[]}]}"""
        "typed_rows" -> """{"id":"1","fingerprint":"3a0eeecc4773dcec544bf551ee68b0934cceb5ea2b3d7b768909e2dfb402c47b","tables":[{"schema":"main","table":"typed_rows","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"text","nullable":false},{"name":"signed_value","wire_type":"int64","nullable":false},{"name":"real_value","wire_type":"float64","nullable":false},{"name":"decimal_text","wire_type":"decimal","nullable":false},{"name":"nullable_value","wire_type":"int64","nullable":true},{"name":"enabled_flag","wire_type":"boolean","nullable":false}],"foreign_keys":[]}]}"""
        "production" -> """{"id":"1","fingerprint":"bdb0200a304258e64b6d4cfc61ae01b94233f06aa6f530928d2605878a59b1f5","tables":[{"schema":"main","table":"categories","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"uuid","nullable":false},{"name":"name","wire_type":"text","nullable":false},{"name":"parent_id","wire_type":"uuid","nullable":true}],"foreign_keys":[{"columns":["parent_id"],"referenced_schema":"main","referenced_table":"categories","referenced_columns":["id"],"on_delete":"cascade","on_update":"no_action"}]},{"schema":"main","table":"file_reviews","sync_key_columns":["id"],"columns":[{"name":"file_id","wire_type":"uuid","nullable":false},{"name":"id","wire_type":"uuid","nullable":false},{"name":"review","wire_type":"text","nullable":false}],"foreign_keys":[{"columns":["file_id"],"referenced_schema":"main","referenced_table":"files","referenced_columns":["id"],"on_delete":"cascade","on_update":"no_action"}]},{"schema":"main","table":"files","sync_key_columns":["id"],"columns":[{"name":"data","wire_type":"bytes","nullable":false},{"name":"id","wire_type":"uuid","nullable":false},{"name":"name","wire_type":"text","nullable":false}],"foreign_keys":[]},{"schema":"main","table":"posts","sync_key_columns":["id"],"columns":[{"name":"author_id","wire_type":"uuid","nullable":true},{"name":"content","wire_type":"text","nullable":false},{"name":"created_at","wire_type":"text","nullable":false},{"name":"id","wire_type":"uuid","nullable":false},{"name":"title","wire_type":"text","nullable":false},{"name":"updated_at","wire_type":"text","nullable":false}],"foreign_keys":[{"columns":["author_id"],"referenced_schema":"main","referenced_table":"users","referenced_columns":["id"],"on_delete":"no_action","on_update":"no_action"}]},{"schema":"main","table":"team_members","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"uuid","nullable":false},{"name":"name","wire_type":"text","nullable":false},{"name":"team_id","wire_type":"uuid","nullable":false}],"foreign_keys":[{"columns":["team_id"],"referenced_schema":"main","referenced_table":"teams","referenced_columns":["id"],"on_delete":"cascade","on_update":"no_action"}]},{"schema":"main","table":"teams","sync_key_columns":["id"],"columns":[{"name":"captain_member_id","wire_type":"uuid","nullable":true},{"name":"id","wire_type":"uuid","nullable":false},{"name":"name","wire_type":"text","nullable":false}],"foreign_keys":[{"columns":["captain_member_id"],"referenced_schema":"main","referenced_table":"team_members","referenced_columns":["id"],"on_delete":"no_action","on_update":"no_action"}]},{"schema":"main","table":"typed_rows","sync_key_columns":["id"],"columns":[{"name":"count_value","wire_type":"int64","nullable":true},{"name":"created_at","wire_type":"text","nullable":true},{"name":"data","wire_type":"bytes","nullable":true},{"name":"enabled_flag","wire_type":"boolean","nullable":false},{"name":"exact_amount","wire_type":"decimal","nullable":true},{"name":"float4_value","wire_type":"float64","nullable":true},{"name":"id","wire_type":"uuid","nullable":false},{"name":"medium_count","wire_type":"int64","nullable":true},{"name":"name","wire_type":"text","nullable":false},{"name":"note","wire_type":"text","nullable":true},{"name":"rating","wire_type":"float64","nullable":true},{"name":"small_count","wire_type":"int64","nullable":true}],"foreign_keys":[]},{"schema":"main","table":"users","sync_key_columns":["id"],"columns":[{"name":"created_at","wire_type":"text","nullable":false},{"name":"email","wire_type":"text","nullable":false},{"name":"id","wire_type":"uuid","nullable":false},{"name":"name","wire_type":"text","nullable":false},{"name":"updated_at","wire_type":"text","nullable":false}],"foreign_keys":[]}]}"""
        else -> error("Missing generation fixture for $fixture")
    }
    return buildJsonObject {
        put("protocol_version", "v1")
        put("schema_generation", testJson.parseToJsonElement(descriptor))
        put("limits", testWireLimitsJson(testWireLimits()))
        put("watch_enabled", false)
    }
}

internal fun schemaevolutionGenerationResponse(generationId: Long): JsonObject {
    val descriptor = when (generationId) {
        1L -> """{"id":"1","fingerprint":"8ca15c3175effaadd0337dfe608aa388f6dfe9855778cb64800b4cfaf6d3889c","tables":[{"schema":"main","table":"notes","sync_key_columns":["id"],"columns":[{"name":"body","wire_type":"text","nullable":false},{"name":"id","wire_type":"uuid","nullable":false}],"foreign_keys":[]},{"schema":"main","table":"people","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"uuid","nullable":false},{"name":"name","wire_type":"text","nullable":false}],"foreign_keys":[]}]}"""
        2L -> """{"id":"2","fingerprint":"6ccaf15c128dfffd6ba05ed01ae26ff8d27c9dd54b0b8d942ba3e5200964d623","tables":[{"schema":"main","table":"attachments","sync_key_columns":["id"],"columns":[{"name":"filename","wire_type":"text","nullable":false},{"name":"id","wire_type":"uuid","nullable":false},{"name":"person_id","wire_type":"uuid","nullable":true}],"foreign_keys":[{"columns":["person_id"],"referenced_schema":"main","referenced_table":"people","referenced_columns":["id"],"on_delete":"cascade","on_update":"no_action"}]},{"schema":"main","table":"notes","sync_key_columns":["id"],"columns":[{"name":"body","wire_type":"text","nullable":false},{"name":"id","wire_type":"uuid","nullable":false}],"foreign_keys":[]},{"schema":"main","table":"people","sync_key_columns":["id"],"columns":[{"name":"details","wire_type":"text","nullable":true},{"name":"id","wire_type":"uuid","nullable":false},{"name":"name","wire_type":"text","nullable":false},{"name":"project_id","wire_type":"uuid","nullable":true}],"foreign_keys":[{"columns":["project_id"],"referenced_schema":"main","referenced_table":"projects","referenced_columns":["id"],"on_delete":"set_null","on_update":"no_action"}]},{"schema":"main","table":"projects","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"uuid","nullable":false},{"name":"title","wire_type":"text","nullable":false}],"foreign_keys":[]}]}"""
        3L -> """{"id":"3","fingerprint":"29c900117308ea441fc459b5274a3e81fae1b2214da6297b3196c191a18935e1","tables":[{"schema":"main","table":"attachments","sync_key_columns":["id"],"columns":[{"name":"filename","wire_type":"text","nullable":false},{"name":"id","wire_type":"uuid","nullable":false},{"name":"person_id","wire_type":"uuid","nullable":true}],"foreign_keys":[{"columns":["person_id"],"referenced_schema":"main","referenced_table":"people","referenced_columns":["id"],"on_delete":"cascade","on_update":"no_action"}]},{"schema":"main","table":"notes","sync_key_columns":["id"],"columns":[{"name":"body","wire_type":"text","nullable":false},{"name":"id","wire_type":"uuid","nullable":false}],"foreign_keys":[]},{"schema":"main","table":"people","sync_key_columns":["id"],"columns":[{"name":"details","wire_type":"text","nullable":true},{"name":"id","wire_type":"uuid","nullable":false},{"name":"name","wire_type":"text","nullable":false},{"name":"project_id","wire_type":"uuid","nullable":true}],"foreign_keys":[{"columns":["project_id"],"referenced_schema":"main","referenced_table":"projects","referenced_columns":["id"],"on_delete":"set_null","on_update":"no_action"}]},{"schema":"main","table":"projects","sync_key_columns":["id"],"columns":[{"name":"id","wire_type":"uuid","nullable":false},{"name":"title","wire_type":"text","nullable":false}],"foreign_keys":[]}]}"""
        else -> error("Missing schemaevolution generation fixture for $generationId")
    }
    return buildJsonObject {
        put("protocol_version", "v1")
        put("schema_generation", testJson.parseToJsonElement(descriptor))
        put("limits", testWireLimitsJson(testWireLimits()))
        put("watch_enabled", false)
    }
}

internal fun testDiscoveryResponse(
    generation: JsonObject,
    limits: TestWireLimits = testWireLimits(),
    watchEnabled: Boolean = false,
): JsonObject = JsonObject(generation + mapOf(
    "limits" to testWireLimitsJson(limits),
    "watch_enabled" to JsonPrimitive(watchEnabled),
))

private fun testWireLimitsJson(limits: TestWireLimits): JsonObject = buildJsonObject {
    put("max_push_rows", limits.maxPushRows)
    put("max_push_decoded_bytes", limits.maxPushDecodedBytes)
    put("max_pull_bundles", limits.maxPullBundles)
    put("max_pull_decoded_bytes", limits.maxPullDecodedBytes)
    put("max_snapshot_rows_per_chunk", limits.maxSnapshotRowsPerChunk)
    put("max_snapshot_decoded_bytes_per_chunk", limits.maxSnapshotDecodedBytesPerChunk)
    put("max_snapshot_rows", limits.maxSnapshotRows)
    put("max_snapshot_logical_bytes", limits.maxSnapshotLogicalBytes)
    put("max_row_decoded_bytes", limits.maxRowDecodedBytes)
}
