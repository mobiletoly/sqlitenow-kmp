package dev.goquick.sqlitenow.oversqlite.e2e

import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class RealServerStressTest {
    @Test
    fun largeVolume_twoDevicesConvergeAcrossMultipleRoundtrips() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val deviceA = randomSourceId("stress-a")
        val deviceB = randomSourceId("stress-b")

        val tokenA = issueDummySigninToken(config.baseUrl, userId, deviceA)
        val tokenB = issueDummySigninToken(config.baseUrl, userId, deviceB)
        val httpA = newAuthenticatedHttpClient(config.baseUrl, tokenA)
        val httpB = newAuthenticatedHttpClient(config.baseUrl, tokenB)
        val dbA = newInMemoryDb()
        val dbB = newInMemoryDb()
        try {
            createBusinessSubsetTables(dbA)
            createBusinessSubsetTables(dbB)

            val clientA = newRealServerClient(
                db = dbA,
                config = config,
                http = httpA,
                uploadLimit = 16,
                downloadLimit = 2,
            )
            val clientB = newRealServerClient(
                db = dbB,
                config = config,
                http = httpB,
                uploadLimit = 16,
                downloadLimit = 2,
            )

            clientA.openAndAttach(userId, deviceA).getOrThrow()
            clientB.openAndAttach(userId, deviceB).getOrThrow()

            repeat(6) { batch ->
                insertUserAndPostBatch(
                    db = dbA,
                    batchPrefix = "seed-a-$batch",
                    count = 5,
                    titlePrefix = "A title",
                    contentPrefix = "A payload",
                )
                clientA.pushPending().getOrThrow()
            }

            clientB.pullToStable().getOrThrow()

            assertEquals(30L, scalarLong(dbB, "SELECT COUNT(*) FROM users"))
            assertEquals(30L, scalarLong(dbB, "SELECT COUNT(*) FROM posts"))
            assertEquals(
                1L,
                scalarLong(dbB, "SELECT COUNT(*) FROM posts WHERE title = 'A title seed-a-5-4' AND content = 'A payload seed-a-5-4'")
            )
            assertEquals(6L, clientB.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))

            repeat(4) { batch ->
                insertUserAndPostBatch(
                    db = dbB,
                    batchPrefix = "seed-b-$batch",
                    count = 5,
                    titlePrefix = "B title",
                    contentPrefix = "B payload",
                )
                clientB.pushPending().getOrThrow()
            }

            clientA.pullToStable().getOrThrow()

            assertEquals(50L, scalarLong(dbA, "SELECT COUNT(*) FROM users"))
            assertEquals(50L, scalarLong(dbA, "SELECT COUNT(*) FROM posts"))
            assertEquals(50L, scalarLong(dbB, "SELECT COUNT(*) FROM users"))
            assertEquals(50L, scalarLong(dbB, "SELECT COUNT(*) FROM posts"))
            assertEquals(
                1L,
                scalarLong(dbA, "SELECT COUNT(*) FROM posts WHERE title = 'B title seed-b-3-4' AND content = 'B payload seed-b-3-4'")
            )
            assertEquals(
                1L,
                scalarLong(dbB, "SELECT COUNT(*) FROM posts WHERE title = 'B title seed-b-3-4' AND content = 'B payload seed-b-3-4'")
            )
            assertEquals(10L, clientA.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(10L, clientB.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(0L, scalarLong(dbA, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            httpA.close()
            httpB.close()
            dbA.close()
            dbB.close()
        }
    }
}
