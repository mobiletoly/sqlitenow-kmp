package dev.goquick.sqlitenow.oversqlite.e2e

import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class RealServerLongHorizonStaleFollowerStressTest {
    @Test
    fun longHorizon_staleFollowerEventuallyConvergesAcrossRichSchema() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val leaderDevice = randomDeviceId("leader")
        val followerDevice = randomDeviceId("follower")
        val uploadLimit = 8

        val leaderToken = issueDummySigninToken(config.baseUrl, userId, leaderDevice)
        val followerToken = issueDummySigninToken(config.baseUrl, userId, followerDevice)
        val leaderHttp = newAuthenticatedHttpClient(config.baseUrl, leaderToken)
        val followerHttp = newAuthenticatedHttpClient(config.baseUrl, followerToken)
        val leaderDb = newInMemoryDb()
        val followerDb = newInMemoryDb()
        try {
            createBusinessRichSchemaTables(leaderDb)
            createBusinessRichSchemaTables(followerDb)

            val leader = newRealServerClient(
                db = leaderDb,
                config = config,
                http = leaderHttp,
                syncTables = richSchemaSyncTables,
                uploadLimit = uploadLimit,
                downloadLimit = 2,
            )
            val follower = newRealServerClient(
                db = followerDb,
                config = config,
                http = followerHttp,
                syncTables = richSchemaSyncTables,
                uploadLimit = uploadLimit,
                downloadLimit = 2,
            )

            leader.bootstrap(userId, leaderDevice).getOrThrow()
            follower.bootstrap(userId, followerDevice).getOrThrow()
            leader.hydrate().getOrThrow()
            follower.hydrate().getOrThrow()

            val hotGraph = insertHotGraph(leaderDb)
            leader.pushPending().getOrThrow()

            val rounds = 12
            val followerCheckpoints = setOf(4, 8, 12)
            for (round in 1..rounds) {
                mutateHotGraph(leaderDb, hotGraph, round)
                insertRichSchemaBatch(leaderDb, "long-horizon-$round")
                val pendingDirty = scalarLong(leaderDb, "SELECT COUNT(*) FROM _sync_dirty_rows")
                assertTrue(
                    "round $round should exceed one upload chunk; pendingDirty=$pendingDirty uploadLimit=$uploadLimit",
                    pendingDirty > uploadLimit.toLong()
                )
                leader.pushPending().getOrThrow()

                if (round in followerCheckpoints) {
                    follower.pullToStable().getOrThrow()
                    assertEquals((round + 1).toLong(), follower.lastBundleSeqSeen().getOrThrow())
                    assertEquals(0L, scalarLong(followerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
                }
            }

            assertEquals(13L, leader.lastBundleSeqSeen().getOrThrow())
            assertEquals(13L, follower.lastBundleSeqSeen().getOrThrow())

            assertHotGraphDrivenCounts(leaderDb, rounds)
            assertHotGraphDrivenCounts(followerDb, rounds)
            assertHotGraphState(leaderDb, hotGraph, rounds)
            assertHotGraphState(followerDb, hotGraph, rounds)
            assertRoundPresence(leaderDb, "long-horizon-12")
            assertRoundPresence(followerDb, "long-horizon-12")
            assertForeignKeyIntegrity(leaderDb)
            assertForeignKeyIntegrity(followerDb)
            assertEquals(0L, scalarLong(leaderDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(followerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            leaderHttp.close()
            followerHttp.close()
            leaderDb.close()
            followerDb.close()
        }
    }
}
