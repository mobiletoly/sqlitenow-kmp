package dev.goquick.sqlitenow.oversqlite.e2e

import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class RealServerThreeDeviceConvergenceTest {
    @Test
    fun threeDevices_takeTurnsAuthoringAndConverge() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val deviceA = randomSourceId("three-a")
        val deviceB = randomSourceId("three-b")
        val deviceC = randomSourceId("three-c")

        val tokenA = issueDummySigninToken(config.baseUrl, userId, deviceA)
        val tokenB = issueDummySigninToken(config.baseUrl, userId, deviceB)
        val tokenC = issueDummySigninToken(config.baseUrl, userId, deviceC)
        val httpA = newAuthenticatedHttpClient(config.baseUrl, tokenA)
        val httpB = newAuthenticatedHttpClient(config.baseUrl, tokenB)
        val httpC = newAuthenticatedHttpClient(config.baseUrl, tokenC)
        val dbA = newInMemoryDb()
        val dbB = newInMemoryDb()
        val dbC = newInMemoryDb()
        try {
            createBusinessRichSchemaTables(dbA)
            createBusinessRichSchemaTables(dbB)
            createBusinessRichSchemaTables(dbC)

            val clientA = newRealServerClient(
                db = dbA,
                config = config,
                http = httpA,
                syncTables = richSchemaSyncTables,
                uploadLimit = 8,
                downloadLimit = 2,
            )
            val clientB = newRealServerClient(
                db = dbB,
                config = config,
                http = httpB,
                syncTables = richSchemaSyncTables,
                uploadLimit = 8,
                downloadLimit = 2,
            )
            val clientC = newRealServerClient(
                db = dbC,
                config = config,
                http = httpC,
                syncTables = richSchemaSyncTables,
                uploadLimit = 8,
                downloadLimit = 2,
            )

            clientA.openAndAttach(userId).getOrThrow()
            clientB.openAndAttach(userId).getOrThrow()
            clientC.openAndAttach(userId).getOrThrow()
            clientA.rebuild().getOrThrow()
            clientB.rebuild().getOrThrow()
            clientC.rebuild().getOrThrow()

            val hotGraph = insertHotGraph(dbA)
            insertRichSchemaBatch(dbA, "three-a-0")
            clientA.pushPending().getOrThrow()

            clientB.pullToStable().getOrThrow()
            clientC.pullToStable().getOrThrow()

            mutateHotGraph(dbB, hotGraph, 1)
            insertRichSchemaBatch(dbB, "three-b-1")
            clientB.pushPending().getOrThrow()

            clientC.pullToStable().getOrThrow()
            mutateHotGraph(dbC, hotGraph, 2)
            insertRichSchemaBatch(dbC, "three-c-2")
            clientC.pushPending().getOrThrow()

            clientA.pullToStable().getOrThrow()
            mutateHotGraph(dbA, hotGraph, 3)
            insertRichSchemaBatch(dbA, "three-a-3")
            clientA.pushPending().getOrThrow()

            clientA.pullToStable().getOrThrow()
            clientB.pullToStable().getOrThrow()
            clientC.pullToStable().getOrThrow()

            assertEquals(4L, clientA.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(4L, clientB.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(4L, clientC.syncStatus().getOrThrow().lastBundleSeqSeen)

            assertHotGraphDrivenCounts(dbA, rounds = 3, extraRichBatches = 1)
            assertHotGraphDrivenCounts(dbB, rounds = 3, extraRichBatches = 1)
            assertHotGraphDrivenCounts(dbC, rounds = 3, extraRichBatches = 1)

            assertHotGraphState(dbA, hotGraph, finalRound = 3)
            assertHotGraphState(dbB, hotGraph, finalRound = 3)
            assertHotGraphState(dbC, hotGraph, finalRound = 3)

            assertRoundPresence(dbA, "three-a-0")
            assertRoundPresence(dbA, "three-b-1")
            assertRoundPresence(dbA, "three-c-2")
            assertRoundPresence(dbA, "three-a-3")
            assertRoundPresence(dbB, "three-a-0")
            assertRoundPresence(dbB, "three-b-1")
            assertRoundPresence(dbB, "three-c-2")
            assertRoundPresence(dbB, "three-a-3")
            assertRoundPresence(dbC, "three-a-0")
            assertRoundPresence(dbC, "three-b-1")
            assertRoundPresence(dbC, "three-c-2")
            assertRoundPresence(dbC, "three-a-3")

            assertForeignKeyIntegrity(dbA)
            assertForeignKeyIntegrity(dbB)
            assertForeignKeyIntegrity(dbC)
            assertEquals(0L, scalarLong(dbA, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbC, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            httpA.close()
            httpB.close()
            httpC.close()
            dbA.close()
            dbB.close()
            dbC.close()
        }
    }
}
