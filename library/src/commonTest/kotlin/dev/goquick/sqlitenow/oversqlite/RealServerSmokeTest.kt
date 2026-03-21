package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals

internal class RealServerSmokeTest : RealServerSmokeSupport() {
    @Test
    fun bootstrapHydratePushPullAndFreshHydrate_workAgainstRealServer() = runTest {
        val config = requireRealServerSmokeConfig() ?: return@runTest
        resetRealServerState(config.baseUrl)

        val userId = randomSmokeId("smoke-user")
        val deviceA = randomSmokeId("smoke-a")
        val deviceB = randomSmokeId("smoke-b")
        val deviceC = randomSmokeId("smoke-c")

        val tokenA = issueDummySigninToken(config.baseUrl, userId, deviceA)
        val tokenB = issueDummySigninToken(config.baseUrl, userId, deviceB)
        val tokenC = issueDummySigninToken(config.baseUrl, userId, deviceC)
        val httpA = newRealServerHttpClient(config.baseUrl, tokenA)
        val httpB = newRealServerHttpClient(config.baseUrl, tokenB)
        val httpC = newRealServerHttpClient(config.baseUrl, tokenC)
        val dbA = newDb()
        val dbB = newDb()
        val dbC = newDb()
        try {
            createBusinessSubsetTables(dbA)
            createBusinessSubsetTables(dbB)
            createBusinessSubsetTables(dbC)

            val clientA = newRealServerClient(dbA, httpA)
            val clientB = newRealServerClient(dbB, httpB)
            val clientC = newRealServerClient(dbC, httpC)

            clientA.bootstrap(userId, deviceA).getOrThrow()
            clientB.bootstrap(userId, deviceB).getOrThrow()
            clientC.bootstrap(userId, deviceC).getOrThrow()

            clientA.hydrate().getOrThrow()
            clientB.hydrate().getOrThrow()

            val rowUserId = "11111111-1111-1111-1111-111111111111"
            val rowPostId = "22222222-2222-2222-2222-222222222222"
            insertBusinessUserAndPost(dbA, rowUserId, rowPostId, "real-server-smoke")

            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()
            clientC.hydrate().getOrThrow()

            assertEquals(1L, clientA.lastBundleSeqSeen().getOrThrow())
            assertEquals(1L, clientB.lastBundleSeqSeen().getOrThrow())
            assertEquals(1L, clientC.lastBundleSeqSeen().getOrThrow())
            assertEquals("User real-server-smoke", scalarText(dbB, "SELECT name FROM users WHERE id = '$rowUserId'"))
            assertEquals("Title real-server-smoke", scalarText(dbB, "SELECT title FROM posts WHERE id = '$rowPostId'"))
            assertEquals("User real-server-smoke", scalarText(dbC, "SELECT name FROM users WHERE id = '$rowUserId'"))
            assertEquals("Payload real-server-smoke", scalarText(dbC, "SELECT content FROM posts WHERE id = '$rowPostId'"))
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
