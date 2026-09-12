package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class GenerationDiscoveryValidationTest {
    @Test
    fun admittedWireLimitsRequirePositiveValuesAndEnvelopeRelationships() {
        val valid = testWireLimits()
        validateAdmittedWireLimits(valid)
        assertEquals(100_000, valid.maxPushRows)

        val invalid = listOf(
            valid.copy(maxPushRows = 0),
            valid.copy(maxPushDecodedBytes = 0),
            valid.copy(maxPullBundles = 0),
            valid.copy(maxPullDecodedBytes = 0),
            valid.copy(maxSnapshotRowsPerChunk = 0),
            valid.copy(maxSnapshotDecodedBytesPerChunk = 0),
            valid.copy(maxSnapshotRows = 0),
            valid.copy(maxSnapshotLogicalBytes = 0),
            valid.copy(maxRowDecodedBytes = 0),
            valid.copy(maxPushDecodedBytes = valid.maxRowDecodedBytes - 1),
            valid.copy(maxSnapshotDecodedBytesPerChunk = valid.maxRowDecodedBytes - 1),
        )
        invalid.forEach { limits ->
            assertFailsWith<IllegalArgumentException> {
                validateAdmittedWireLimits(limits)
            }
        }
    }
}
