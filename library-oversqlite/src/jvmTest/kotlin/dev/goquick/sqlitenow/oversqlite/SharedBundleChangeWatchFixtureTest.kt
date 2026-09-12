package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertTrue

class SharedBundleChangeWatchFixtureTest {
    private val root = Json.parseToJsonElement(
        oversqliteContractFixture("watch/basic.json").readText(),
    ).jsonObject

    @Test
    fun discoveryAndSseCasesUseTheReducedWatchContract() {
        assertEquals(2, root.getValue("formatVersion").jsonPrimitive.int)
        root.getValue("discoveryCases").jsonArray.map { it.jsonObject }.forEach { case ->
            val response = case.getValue("response").jsonObject
            assertEquals(200, response.getValue("status").jsonPrimitive.int)
            val body = response.getValue("body").jsonObject
            assertEquals(case.getValue("expectedWatchEnabled").jsonPrimitive.boolean, body.getValue("watch_enabled").jsonPrimitive.boolean)
            assertEquals(setOf("protocol_version", "schema_generation", "limits", "watch_enabled"), body.keys)
        }

        root.getValue("sseCases").jsonArray.map { it.jsonObject }.forEach { case ->
            val result = runCatching { parseBundleEvents(case.getValue("lines").jsonArray.map { it.jsonPrimitive.content }) }
            if (case["expectError"]?.jsonPrimitive?.boolean == true) {
                assertFails { result.getOrThrow() }
            } else {
                val expected = case.getValue("expectedEvents").jsonArray.map {
                    it.jsonObject.getValue("bundle_seq").jsonPrimitive.int
                }
                assertEquals(expected, result.getOrThrow(), case.getValue("name").jsonPrimitive.content)
            }
        }
    }

    @Test
    fun burstCasesBoundWatchPullConflation() {
        root.getValue("conflationCases").jsonArray.map { it.jsonObject }.forEach { case ->
            val events = case.getValue("events").jsonPrimitive.int
            val initialCheckpoint = case["checkpoint"]?.jsonPrimitive?.long ?: 0L
            val highWater = case["highWater"]?.jsonPrimitive?.long ?: events.toLong()
            var activePulls = 0
            var trailingPulls = 0
            var checkpoint = initialCheckpoint
            repeat(events) { index ->
                val signaled = index + 1L
                if (activePulls == 0) {
                    activePulls = 1
                } else {
                    trailingPulls = 1
                }
                checkpoint = maxOf(checkpoint, signaled)
            }
            if (events > 0) {
                checkpoint = maxOf(checkpoint, highWater)
            }
            assertEquals(case.getValue("expectedActivePulls").jsonPrimitive.int, activePulls)
            assertTrue(trailingPulls <= case.getValue("expectedTrailingPullsAtMost").jsonPrimitive.int)
            case["expectedCheckpoint"]?.let { assertEquals(it.jsonPrimitive.long, checkpoint) }
            if (events == 0) assertEquals(highWater, checkpoint)
        }
    }

    private fun parseBundleEvents(lines: List<String>): List<Int> {
        val events = mutableListOf<Int>()
        var event = ""
        var data = ""
        fun flush() {
            if (event == "bundle") {
                val body = Json.parseToJsonElement(data).jsonObject
                assertEquals(setOf("bundle_seq"), body.keys)
                val sequence = body.getValue("bundle_seq").jsonPrimitive.int
                require(sequence > 0) { "bundle_seq must be positive" }
                events += sequence
            }
            event = ""
            data = ""
        }
        lines.forEach { line ->
            when {
                line.isEmpty() -> flush()
                line.startsWith(":") -> Unit
                line.startsWith("event: ") -> event = line.removePrefix("event: ")
                line.startsWith("data: ") -> data = line.removePrefix("data: ")
            }
        }
        flush()
        return events
    }
}
