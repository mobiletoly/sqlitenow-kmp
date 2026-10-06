package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlin.test.*

internal class SnapshotNativeAllocationObservationTest {
    @Test fun metricUnitsAndVersionArePreserved() {
        val metrics = decodeNativeAllocationMetrics(longArrayOf(10, 20, 4, 8, 2, 3, 1, 5, 3_050_001))
        assertEquals(4L, metrics.getValue("pageCacheOverflowBytes").jsonPrimitive.long)
        assertEquals(2L, metrics.getValue("configuredPageCacheUsedPages").jsonPrimitive.long)
        assertEquals(3_050_001L, metrics.getValue("versionNumber").jsonPrimitive.long)
    }
    @Test fun invalidOrInconsistentMetricsCannotQualifyObservation() {
        for (values in listOf(longArrayOf(), longArrayOf(-1, 20, 4, 8, 2, 3, 1, 5, 3_050_001),
            longArrayOf(21, 20, 4, 8, 2, 3, 1, 5, 3_050_001), longArrayOf(10, 20, 4, 8, 2, 3, 1, 5, 0)))
            assertFailsWith<IllegalArgumentException> { decodeNativeAllocationMetrics(values) }
    }
    @Test fun closedEngineMayRetainHighwaterWithoutLiveAllocations() {
        val metrics = decodeNativeAllocationMetrics(longArrayOf(0, 20, 0, 8, 0, 3, 0, 5, 3_050_001))
        assertEquals(0L, metrics.getValue("currentBytes").jsonPrimitive.long)
        assertEquals(20L, metrics.getValue("highwaterBytes").jsonPrimitive.long)
    }
}
