package dev.goquick.sqlitenow.oversqlite

import java.lang.management.ManagementFactory
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement

/**
 * Canonicalizing a REAL runs on every push, pull, hash and restore row. Its allocation must stay a small
 * constant per value rather than the exact decimal expansions of three doubles.
 */
class CanonicalBinary64RenderingAllocationJvmTest {
    @Test
    fun renderingAllocatesAFewKilobytesPerValue() {
        val threads = ManagementFactory.getThreadMXBean() as com.sun.management.ThreadMXBean
        val random = Random(20261112)
        // Typical payload magnitudes with full 17-digit precision, parsed before measuring.
        val values: List<JsonElement> = List(2_000) {
            Json.parseToJsonElement((random.nextDouble() * 1_000_000.0).toString())
        }
        repeat(2) { values.forEach { canonicalizeJsonElement(it) } } // warm up
        val thread = Thread.currentThread().id
        val before = threads.getThreadAllocatedBytes(thread)
        values.forEach { canonicalizeJsonElement(it) }
        val perValue = (threads.getThreadAllocatedBytes(thread) - before) / values.size
        println("canonical REAL rendering: $perValue bytes allocated per value")
        assertTrue(perValue < 16_384, "canonical REAL rendering allocated $perValue bytes per value")
    }
}
