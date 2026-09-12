package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.serialization.json.*
import kotlin.test.assertEquals
import kotlin.test.assertTrue

// Capture every column with its SQLite storage class. Integers are strings so JS
// cannot round int64 values; blobs are exact hex. Sorting is independent of owners.
internal data class BaselineQualificationVector(val tables: Map<String, List<JsonObject>>) {
    fun rows(table: String): List<JsonObject> = tables.getValue(table)
    fun row(table: String, key: String): JsonObject = rows(table).single { it.text("key_json") == "{\"id\":\"$key\"}" }
}

internal fun JsonObject.text(column: String): String? =
    getValue(column).jsonObject.getValue("value").let { if (it is JsonNull) null else it.jsonPrimitive.content }

internal fun JsonObject.payload(column: String): JsonElement? = text(column)?.let(Json::parseToJsonElement)

internal suspend fun baselineQualificationVector(db: SafeSQLiteConnection, business: List<String> = listOf("users", "posts")): BaselineQualificationVector {
    val names = business + listOf("_sync_row_state", "_sync_dirty_rows", "_sync_relationships", "_sync_outbox_rows", "_sync_outbox_bundle", "_sync_source_state", "_sync_attachment_state", "_sync_operation_state", "_sync_conflict_state")
    val result = linkedMapOf<String, List<JsonObject>>()
    for (name in names) {
        val columns = db.queryList("PRAGMA table_info(\"$name\")") { it.getText(1) }
        assertTrue(columns.isNotEmpty(), "complete vector requires $name")
        val projections = columns.joinToString(",") { c ->
            "'$c',json_object('storage',typeof(\"$c\"),'value',CASE WHEN typeof(\"$c\")='blob' THEN lower(hex(\"$c\")) WHEN \"$c\" IS NULL THEN NULL ELSE CAST(\"$c\" AS TEXT) END)"
        }
        val rows = db.queryList("SELECT json_object($projections) FROM \"$name\" ORDER BY ${columns.joinToString(",") { "\"$it\"" }}") { Json.parseToJsonElement(it.getText(0)).jsonObject }
        result[name] = rows
    }
    return BaselineQualificationVector(result)
}

internal fun assertTextPayload(actual: JsonElement?, expected: Map<String, String?>) {
    val encoded = JsonObject(expected.mapValues { (_,value) -> buildJsonObject {
        put("storage", if (value == null) "null" else "text")
        put("value", value?.let(::JsonPrimitive) ?: JsonNull)
    } })
    assertEquals(encoded, actual, "complete captured payload, including nulls")
}

internal fun assertBaselineProvenance(row: JsonObject, column: String, version: Long, account: String = "user-1") {
    val p = row.payload(column)!!.jsonObject
    assertEquals("fixture",p.getValue("database_id").jsonPrimitive.content)
    assertEquals("main",p.getValue("service_id").jsonPrimitive.content)
    assertEquals("1",p.getValue("generation_id").jsonPrimitive.content)
    assertEquals(version.toString(),p.getValue("authoritative_version").jsonPrimitive.content)
    assertEquals(account,p.getValue("account_scope").jsonObject.getValue("account_id").jsonPrimitive.content)
    assertEquals("attached",p.getValue("account_scope").jsonObject.getValue("binding").jsonPrimitive.content)
    assertTrue(p.getValue("generation_fingerprint").jsonPrimitive.content.isNotEmpty())
    assertTrue(p.getValue("physical_shape").jsonPrimitive.content.isNotEmpty())
    assertTrue(p.getValue("origin").jsonPrimitive.content in setOf("pull","settlement","snapshot","explicit_conflict","never_published","local_adoption"))
}

internal fun assertBaselineCounters(vector: BaselineQualificationVector, source: String, next: Long, checkpoint: Long) {
    assertEquals(source,vector.rows("_sync_attachment_state").single().text("current_source_id"))
    assertEquals(checkpoint.toString(),vector.rows("_sync_attachment_state").single().text("last_bundle_seq_seen"))
    assertEquals(next.toString(),vector.rows("_sync_source_state").single { it.text("source_id")==source }.text("next_source_bundle_id"))
}

internal fun assertBaselineEdges(vector: BaselineQualificationVector, view: String, child: String, parent: String?) {
    val edges=vector.rows("_sync_relationships").filter { it.text("view")==view && it.text("child_key")=="{\"id\":\"$child\"}" }
    if (parent==null) assertEquals(emptyList(),edges) else {
        assertEquals(1,edges.size)
        assertEquals("{\"id\":\"$parent\"}",edges.single().text("parent_key"))
        assertEquals("user_id",edges.single().text("fk_column"))
        assertEquals("users",edges.single().text("parent_table"))
        assertEquals("posts",edges.single().text("child_table"))
        assertEquals("main",edges.single().text("parent_schema"))
        assertEquals("main",edges.single().text("child_schema"))
        assertTrue(edges.single().text("owner_revision")!!.toLong()>0)
    }
}
