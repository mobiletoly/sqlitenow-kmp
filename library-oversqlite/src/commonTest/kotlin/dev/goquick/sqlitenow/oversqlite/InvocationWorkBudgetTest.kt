package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

internal class InvocationWorkBudgetTest : SurvivorTestSupport() {
    @Test fun boundedStepCompletesBeforeYield() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, http, _ ->
            val operations = controller(db, http, 17)
            operations.reserveSyncWork("step", 16)
            operations.reserveSyncWork("step", 16)
            assertEquals(32L, operations.usedSyncWork())
            val pending = assertFailsWith<SyncWorkLimitException> { operations.reserveSyncWork("step", 1) }
            assertEquals(17L, pending.limit)
            assertEquals(32L, pending.used)
        }
    }

    @Test fun firstReadyUnitRunsAfterEarlierWorkAndLaterUnitYields() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, http, _ ->
            val operations = controller(db, http, 17)
            operations.reserveSyncWork("prepare", 32)
            operations.reserveSyncWork("ready", 1_000_000, readyAtomic = true)
            assertEquals(1_000_032L, operations.usedSyncWork())
            val pending = assertFailsWith<SyncWorkLimitException> {
                operations.reserveSyncWork("later", 1, readyAtomic = true)
            }
            assertEquals(1_000_032L, pending.used)
        }
    }

    @Test fun saturatedReadyEstimateCannotWrapOrStarveTheFirstUnit() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, http, _ ->
            val operations = controller(db, http, 1)
            operations.reserveSyncWork("prepare", 16)
            operations.reserveSyncWork("ready", Long.MAX_VALUE, readyAtomic = true)
            assertEquals(Long.MAX_VALUE, operations.usedSyncWork())
            assertFailsWith<SyncWorkLimitException> { operations.reserveSyncWork("later", 16) }
        }
    }

    @Test fun availabilityCheckDoesNotChargeOrConsumeFirstReadyOpportunity() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, http, _ ->
            val operations = controller(db, http, 1)
            operations.ensureSyncWorkAvailable("stage")
            assertEquals(0L, operations.usedSyncWork())
            operations.reserveSyncWork("stage", 8)
            assertFailsWith<SyncWorkLimitException> { operations.ensureSyncWorkAvailable("next") }
            assertEquals(8L, operations.usedSyncWork())
            operations.reserveSyncWork("ready", 32, readyAtomic = true)
            assertEquals(40L, operations.usedSyncWork())
        }
    }

    private fun controller(
        db: dev.goquick.sqlitenow.core.SafeSQLiteConnection,
        http: io.ktor.client.HttpClient,
        target: Long,
    ): OversqliteClientOperationController = OversqliteClientOperationController(
        OversqliteConfig(1, nodeTables, "fixture", "main", maxSyncWorkUnitsPerInvocation = target),
        OversqliteClientResourceOwner(db, http),
        SnapshotDiagnosticsState(),
    ) {}
}
