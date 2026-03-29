package dev.goquick.sqlitenow.oversqlite.e2e

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.oversqlite.RebuildMode
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
        val leaderDevice = randomSourceId("prune-leader")
        val followerDevice = randomSourceId("prune-follower")

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

            leader.openAndAttach(userId, leaderDevice).getOrThrow()
            follower.openAndAttach(userId, followerDevice).getOrThrow()
            leader.rebuild(RebuildMode.KEEP_SOURCE).getOrThrow()
            follower.rebuild(RebuildMode.KEEP_SOURCE).getOrThrow()

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
                    assertEquals(4L, follower.syncStatus().getOrThrow().lastBundleSeqSeen)
                }
            }

            val followerSourceIdBefore = scalarText(followerDb, "SELECT current_source_id FROM _sync_attachment_state")
            val leaderBundleSeq = leader.syncStatus().getOrThrow().lastBundleSeqSeen
            assertEquals(9L, leaderBundleSeq)

            setRetainedBundleFloor(config.baseUrl, userId, leaderBundleSeq)

            follower.pullToStable().getOrThrow()

            assertEquals(leaderBundleSeq, follower.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(followerSourceIdBefore, scalarText(followerDb, "SELECT current_source_id FROM _sync_attachment_state"))
            assertEquals(0L, scalarLong(followerDb, "SELECT rebuild_required FROM _sync_attachment_state"))
            assertEquals(0L, scalarLong(followerDb, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            assertEquals(0L, scalarLong(followerDb, "SELECT COUNT(*) FROM _sync_outbox_rows"))
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
