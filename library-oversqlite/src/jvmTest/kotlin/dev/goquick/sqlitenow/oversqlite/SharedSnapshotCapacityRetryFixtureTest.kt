package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals

class SharedSnapshotCapacityRetryFixtureTest {
    @Test
    fun kmpConsumesCapacityRetryContract() {
        val fixture = Json.parseToJsonElement(
            oversqliteContractFixture("snapshot-capacity/retry.json").readText(),
        ).jsonObject
        assertEquals("snapshot-capacity-retry-v1", fixture.getValue("contract").jsonPrimitive.content)
        val defaults = fixture.getValue("defaults").jsonObject
        assertEquals(30_000L, defaults.getValue("max_wait_millis").jsonPrimitive.content.toLong())
        assertEquals(1_000L, defaults.getValue("fallback_delay_millis").jsonPrimitive.content.toLong())
        fixture.getValue("retry_after_cases").jsonArray.forEach { element ->
            val case = element.jsonObject
            val wire = case.getValue("wire").jsonPrimitive.content
            val expected = case.getValue("expected_millis").jsonPrimitive.content
                .takeUnless { it == "null" }
                ?.toLong()
            assertEquals(expected, parseSnapshotRetryAfterMillis(wire), wire)
        }
        assertEquals(
            setOf("snapshot_build_capacity", "snapshot_chunk_capacity"),
            fixture.getValue("capacity_error_codes").jsonArray
                .map { it.jsonPrimitive.content }
                .toSet(),
        )
    }
}
