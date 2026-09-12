package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SharedPushFixtureTest {
    private val root = Json.parseToJsonElement(
        oversqliteContractFixture("push/basic-insert.json").readText(),
    ).jsonObject
    private val generation = Json.parseToJsonElement(
        oversqliteContractFixture("wire/compact-rows.json").readText(),
    ).jsonObject.getValue("generation").jsonObject

    @Test
    fun directPushCasesUseOneCompleteRequestAndAuthoritativeResponse() {
        assertEquals(2, root.getValue("formatVersion").jsonPrimitive.int)
        root.getValue("cases").jsonArray.map { it.jsonObject }.forEach { case ->
            val request = case.getValue("pushRequest").jsonObject
            assertTrue(request.keys == setOf("source_bundle_id", "canonical_request_hash", "rows") ||
                request.keys == setOf("source_bundle_id", "canonical_request_hash", "initialization_id", "rows"))
            assertTrue(request.getValue("source_bundle_id").jsonPrimitive.long > 0)
            assertTrue(request.getValue("canonical_request_hash").jsonPrimitive.content.matches(Regex("[0-9a-f]{64}")))
            request["initialization_id"]?.let {
                assertTrue(it.jsonPrimitive.content.matches(Regex("[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}")))
            }
            validateNonEmptyCompactRows(request.getValue("rows").jsonArray)
            val requestRows = expandRows(request.getValue("rows").jsonArray, "base_row_version")
            val requestEnvelope = buildJsonObject {
                put("generation_fingerprint", generation.getValue("fingerprint"))
                put("generation_id", generation.getValue("id"))
                put("rows", JsonArray(requestRows))
                put("type", JsonPrimitive("oversync.generation-pinned-push-request.v1"))
            }
            assertEquals(
                request.getValue("canonical_request_hash").jsonPrimitive.content,
                sha256Hex(canonicalizeJsonElement(requestEnvelope).encodeToByteArray()),
                case.getValue("name").jsonPrimitive.content,
            )

            val response = case.getValue("pushResponse").jsonObject
            assertEquals(setOf("bundle_seq", "bundle_hash", "rows"), response.keys)
            assertTrue(response.getValue("bundle_seq").jsonPrimitive.long > 0)
            assertTrue(response.getValue("bundle_hash").jsonPrimitive.content.matches(Regex("[0-9a-f]{64}")))
            validateNonEmptyCompactRows(response.getValue("rows").jsonArray)
            val responseRows = expandRows(response.getValue("rows").jsonArray, "row_version")
            assertEquals(
                response.getValue("bundle_hash").jsonPrimitive.content,
                sha256Hex(canonicalizeJsonElement(JsonArray(responseRows)).encodeToByteArray()),
                case.getValue("name").jsonPrimitive.content,
            )
        }
    }

    private fun expandRows(groups: JsonArray, versionField: String): List<JsonObject> {
        val tables = generation.getValue("tables").jsonArray.map { it.jsonObject }
        val result = mutableListOf<JsonObject>()
        groups.forEach { groupElement ->
            val group = groupElement.jsonArray
            val table = tables[group[0].jsonPrimitive.int]
            val keyColumns = table.getValue("sync_key_columns").jsonArray.map { it.jsonPrimitive.content }
            var ordinal = group[1].jsonPrimitive.long
            group[2].jsonArray.forEach { tupleElement ->
                val tuple = tupleElement.jsonArray
                val keyValues = tuple[1].jsonArray
                val op = when (tuple[0].jsonPrimitive.content) {
                    "i" -> "INSERT"
                    "u" -> "UPDATE"
                    "d" -> "DELETE"
                    else -> error("unknown compact operation")
                }
                result += buildJsonObject {
                    put("row_ordinal", JsonPrimitive(ordinal.toString()))
                    put("schema", table.getValue("schema"))
                    put("table", table.getValue("table"))
                    put("key", keyObject(keyColumns, keyValues))
                    put("op", JsonPrimitive(op))
                    put(versionField, JsonPrimitive(tuple[2].jsonPrimitive.long.toString()))
                    put("payload", tuple.getOrNull(3) ?: JsonNull)
                }
                ordinal++
            }
        }
        return result
    }

    private fun keyObject(columns: List<String>, values: JsonArray): JsonObject = buildJsonObject {
        assertEquals(columns.size, values.size)
        columns.zip(values).forEach { (column, value) -> put(column, value) }
    }

    private fun validateNonEmptyCompactRows(groups: JsonArray) {
        assertTrue(groups.isNotEmpty())
        var nextOrdinal = 0L
        groups.forEach { element ->
            val group = element.jsonArray
            assertEquals(3, group.size)
            assertTrue(group[0].jsonPrimitive.int >= 0)
            assertEquals(nextOrdinal, group[1].jsonPrimitive.long)
            val rows = group[2].jsonArray
            assertTrue(rows.isNotEmpty())
            rows.forEach { tupleElement ->
                val tuple = tupleElement.jsonArray
                val op = tuple[0].jsonPrimitive.content
                assertTrue(op in setOf("i", "u", "d"))
                assertEquals(if (op == "d") 3 else 4, tuple.size)
                assertTrue(tuple[1] is JsonArray)
                nextOrdinal++
            }
        }
    }
}
