package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.*
import kotlin.io.path.readText
import kotlin.test.*

/** Executes the existing compact corpus against the production semantic codec. */
class OversqliteProtocolCodecTest {
    private fun fixture() = Json.parseToJsonElement(
        oversqliteContractFixture("wire/compact-rows.json").readText(),
    ).jsonObject

    private fun generation(f: JsonObject): SyncGeneration {
        val descriptor = f.getValue("generation").jsonObject
        // The shared codec corpus has untyped generic columns, as does its Go
        // capabilities projection. This tests tuples, not admission of composite keys.
        return SyncGeneration(descriptor.getValue("id").jsonPrimitive.content,
            descriptor.getValue("fingerprint").jsonPrimitive.content,
            descriptor.getValue("tables").jsonArray.map { value ->
                val t = value.jsonObject
                GenerationTable(t.getValue("schema").jsonPrimitive.content, t.getValue("table").jsonPrimitive.content,
                    t.getValue("sync_key_columns").jsonArray.map { it.jsonPrimitive.content },
                    t.getValue("columns").jsonArray.map { GenerationColumn(it.jsonPrimitive.content, "json", false) }, emptyList())
            })
    }

    @Test
    fun compactEquivalenceVectorsExecuteAgainstCodec() {
        val f = fixture(); assertEquals(2, f.getValue("formatVersion").jsonPrimitive.int)
        val generation = generation(f)
        val cases = f.getValue("equivalenceCases").jsonArray
        assertEquals(3, cases.size)
        for (value in cases) {
            val c = value.jsonObject
            val snapshot = c["snapshot"]?.jsonPrimitive?.boolean ?: false
            val rows = OversqliteCompactCodec.decodeSemanticRows(generation, c.getValue("compactRows"), snapshot = snapshot)
            c["expectedExpandedRows"]?.let { expected ->
                val actual = JsonArray(rows.map { row -> buildJsonObject {
                    put("schema", row.schema); put("table", row.table); put("key", JsonObject(row.key))
                    put("op", row.op); put("version", row.version); put("payload", row.payload ?: JsonNull)
                } })
                assertEquals(expected, actual, c.getValue("name").jsonPrimitive.content)
            }
            c["expectedExpandedOperations"]?.let {
                assertEquals(it.jsonArray.map { v -> v.jsonPrimitive.content }, rows.map { row -> row.op })
                assertEquals(c.getValue("expectedOrdinals").jsonArray.map { v -> v.jsonPrimitive.int }, rows.indices.toList())
            }
            c["expectedSnapshotLogicalRows"]?.let {
                assertEquals(it, JsonArray(rows.map { row -> JsonArray(listOf(JsonPrimitive("i"),
                    JsonPrimitive(generation.tables.indexOfFirst { table -> table.table == row.table && table.schema == row.schema }),
                    JsonArray(generation.tables.first { table -> table.table == row.table }.syncKeyColumns.map { key -> row.key.getValue(key) }),
                    JsonPrimitive(row.version), row.payload ?: JsonNull)) }))
            }
            c["hashDomain"]?.let { assertEquals("oversync.generation-pinned-push-request.v1", it.jsonPrimitive.content) }
            c["snapshotLogicalHashDomain"]?.let { assertEquals("oversync.snapshot-logical-row.v1", it.jsonPrimitive.content) }
        }
    }

    @Test
    fun malformedVectorsRejectBeforeReturningRows() {
        val f = fixture(); val generation = generation(f)
        val cases = f.getValue("invalidCases").jsonArray
        assertEquals(14, cases.size)
        assertEquals(cases.size, cases.map { it.jsonObject.getValue("name") }.toSet().size)
        for (value in cases) {
            val c = value.jsonObject
            assertFails(c.getValue("name").jsonPrimitive.content) {
                OversqliteCompactCodec.decodeSemanticRows(generation, c.getValue("rows"),
                    snapshot = c["snapshot"]?.jsonPrimitive?.boolean ?: false)
            }
        }
    }
}
