package dev.goquick.sqlitenow.oversqlite

import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

internal class ApplyStatementVolumeTest : ApplyStatementTestSupport() {
    @Test fun pullHundredRows() = volume("pull", 100)
    @Test fun pullThousandRows() = volume("pull", 1000)
    @Test fun pullTenThousandRows() = volume("pull", 10000)
    @Test fun conflictHundredRows() = volume("conflict", 100)
    @Test fun conflictThousandRows() = volume("conflict", 1000)
    @Test fun conflictTenThousandRows() = volume("conflict", 10000)

    private fun volume(operation: String, rows: Int) = runOwnedWebDatabaseTest(timeout = 180.seconds) {
        withApplyFixture(rows) { f ->
            repeat(4) { sample ->
                val observation = measure(f, operation, rows, sample)
                assertEquals(rows.toLong(), observation.steps.filterKeys { it.startsWith("INSERT OR IGNORE INTO _sync_apply_edges") }.values.sum())
            }
        }
    }
}
