package dev.goquick.sqlitenow.core.sqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame

class SqlJsFailureHandlingTest {

    @Test
    fun successfulPreparationTransfersOwnershipWithoutClosing() {
        val statement = Any()
        var closeCalls = 0

        val result = prepareSqlJsStatement(
            prepare = { statement },
            normalize = { "SELECT 1" },
            closeOnFailure = { closeCalls++ },
        ) { wrapped, normalizedSql ->
            assertSame(statement, wrapped)
            normalizedSql
        }

        assertEquals("SELECT 1", result)
        assertEquals(0, closeCalls)
    }

    @Test
    fun failedPreparationClosesOnceAndPreservesFailureOrdering() {
        val scenarios = listOf(
            PreparationFailureScenario(
                name = "normalization-failure",
                cleanupFailure = null,
            ),
            PreparationFailureScenario(
                name = "normalization-and-cleanup-failure",
                cleanupFailure = IllegalStateException("CLEANUP_SENTINEL"),
            ),
        )

        scenarios.forEach { scenario ->
            val primary = IllegalStateException("NORMALIZATION_SENTINEL")
            var closeCalls = 0

            val thrown = assertFailsWithType<IllegalStateException> {
                prepareSqlJsStatement(
                    prepare = { Any() },
                    normalize = { throw primary },
                    closeOnFailure = {
                        closeCalls++
                        scenario.cleanupFailure?.let { throw it }
                    },
                ) { _, _ ->
                    error("The wrapper must not be created after normalization fails")
                }
            }

            assertSame(primary, thrown, scenario.name)
            assertEquals(1, closeCalls, scenario.name)
            assertEquals(listOfNotNull(scenario.cleanupFailure), thrown.suppressedExceptions, scenario.name)
        }
    }

    @Test
    fun sqliteExceptionNormalizationExposesSuppressedFailuresInOrder() {
        val primary = IllegalStateException("PRIMARY_SENTINEL")
        val firstCleanup = IllegalStateException("FIRST_CLEANUP_SENTINEL")
        val secondCleanup = IllegalStateException("SECOND_CLEANUP_SENTINEL")
        primary.addSuppressed(firstCleanup)
        primary.addSuppressed(secondCleanup)

        val normalized = primary.toSqliteExceptionPreservingSuppressed()

        assertSame(primary, normalized.cause)
        assertEquals(listOf(firstCleanup, secondCleanup), normalized.suppressedExceptions)
    }

    private data class PreparationFailureScenario(
        val name: String,
        val cleanupFailure: Throwable?,
    )
}

private inline fun <reified T : Throwable> assertFailsWithType(block: () -> Unit): T {
    try {
        block()
    } catch (t: Throwable) {
        if (t is T) return t
        throw AssertionError("Expected ${T::class.simpleName}, but caught ${t::class.simpleName}: ${t.message}")
    }
    throw AssertionError("Expected ${T::class.simpleName} to be thrown")
}
