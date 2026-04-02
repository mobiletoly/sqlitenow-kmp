package dev.goquick.sqlitenow.oversqlite.realserver

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
        val leaderDevice = randomSourceId("leader")
        val followerDevice = randomSourceId("follower")
        val uploadLimit = 8

        val leaderToken = issueDummySigninToken(config.baseUrl, userId, leaderDevice)
        val followerToken = issueDummySigninToken(config.baseUrl, userId, followerDevice)
        val leaderHttp = newAuthenticatedHttpClient(config.baseUrl, leaderToken)
        val followerHttp = newAuthenticatedHttpClient(config.baseUrl, followerToken)
        val leaderDb = newFileBackedDb()
        val followerDb = newFileBackedDb()
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

            leader.openAndAttach(userId).getOrThrow()
            follower.openAndAttach(userId).getOrThrow()
            leader.rebuild().getOrThrow()
            follower.rebuild().getOrThrow()

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
                    assertEquals((round + 1).toLong(), follower.syncStatus().getOrThrow().lastBundleSeqSeen)
                    assertEquals(0L, scalarLong(followerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
                }
            }

            assertEquals(13L, leader.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(13L, follower.syncStatus().getOrThrow().lastBundleSeqSeen)

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
