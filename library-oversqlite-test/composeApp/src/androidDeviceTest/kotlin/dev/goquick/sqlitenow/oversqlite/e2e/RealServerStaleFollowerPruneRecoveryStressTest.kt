package dev.goquick.sqlitenow.oversqlite.e2e

import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class RealServerStaleFollowerPruneRecoveryStressTest {
    @Test
    fun staleFollower_historyPrunedPullRebuildsThroughSnapshotAndConverges() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val leaderDevice = randomDeviceId("prune-leader")
        val followerDevice = randomDeviceId("prune-follower")

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
                uploadLimit = 8,
                downloadLimit = 2,
            )
            val follower = newRealServerClient(
                db = followerDb,
                config = config,
                http = followerHttp,
                syncTables = richSchemaSyncTables,
                uploadLimit = 8,
                downloadLimit = 2,
            )

            leader.bootstrap(userId, leaderDevice).getOrThrow()
            follower.bootstrap(userId, followerDevice).getOrThrow()
            leader.hydrate().getOrThrow()
            follower.hydrate().getOrThrow()

            val hotGraph = insertHotGraph(leaderDb)
            leader.pushPending().getOrThrow()
            follower.pullToStable().getOrThrow()

            val rounds = 8
            for (round in 1..rounds) {
                mutateHotGraph(leaderDb, hotGraph, round)
                insertRichSchemaBatch(leaderDb, "prune-recovery-$round")
                leader.pushPending().getOrThrow()

                if (round == 3) {
                    follower.pullToStable().getOrThrow()
                    assertEquals(4L, follower.lastBundleSeqSeen().getOrThrow())
                }
            }

            val followerSourceIdBefore = scalarText(followerDb, "SELECT source_id FROM _sync_client_state WHERE user_id = '$userId'")
            val leaderBundleSeq = leader.lastBundleSeqSeen().getOrThrow()
            assertEquals(9L, leaderBundleSeq)

            setRetainedBundleFloor(config.baseUrl, userId, leaderBundleSeq)

            follower.pullToStable().getOrThrow()

            assertEquals(leaderBundleSeq, follower.lastBundleSeqSeen().getOrThrow())
            assertEquals(followerSourceIdBefore, scalarText(followerDb, "SELECT source_id FROM _sync_client_state WHERE user_id = '$userId'"))
            assertEquals(0L, scalarLong(followerDb, "SELECT rebuild_required FROM _sync_client_state WHERE user_id = '$userId'"))
            assertEquals(0L, scalarLong(followerDb, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            assertEquals(0L, scalarLong(followerDb, "SELECT COUNT(*) FROM _sync_push_outbound"))
            assertEquals(0L, scalarLong(followerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))

            assertHotGraphDrivenCounts(leaderDb, rounds)
            assertHotGraphDrivenCounts(followerDb, rounds)
            assertHotGraphState(leaderDb, hotGraph, rounds)
            assertHotGraphState(followerDb, hotGraph, rounds)
            assertRoundPresence(leaderDb, "prune-recovery-8")
            assertRoundPresence(followerDb, "prune-recovery-8")
            assertForeignKeyIntegrity(leaderDb)
            assertForeignKeyIntegrity(followerDb)
        } finally {
            leaderHttp.close()
            followerHttp.close()
            leaderDb.close()
            followerDb.close()
        }
    }
}
