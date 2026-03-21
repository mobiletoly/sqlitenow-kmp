package dev.goquick.sqlitenow.oversqlite.e2e

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.oversqlite.PendingPushReplayException
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class RealServerLongHorizonDivergentWriterStressTest {
    @Test
    fun longHorizon_divergentWriterFailsClosedThenRecoversThroughRebuild() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val leaderDevice = randomDeviceId("divergent-leader")
        val writerDevice = randomDeviceId("divergent-writer")
        val observerDevice = randomDeviceId("divergent-observer")

        val leaderToken = issueDummySigninToken(config.baseUrl, userId, leaderDevice)
        val writerToken = issueDummySigninToken(config.baseUrl, userId, writerDevice)
        val observerToken = issueDummySigninToken(config.baseUrl, userId, observerDevice)
        val leaderHttp = newAuthenticatedHttpClient(config.baseUrl, leaderToken)
        var writerHttp = newAuthenticatedHttpClient(config.baseUrl, writerToken)
        val observerHttp = newAuthenticatedHttpClient(config.baseUrl, observerToken)
        val leaderDb = newInMemoryDb()
        val writerDb = newInMemoryDb()
        val observerDb = newInMemoryDb()
        try {
            createBusinessRichSchemaTables(leaderDb)
            createBusinessRichSchemaTables(writerDb)
            createBusinessRichSchemaTables(observerDb)

            val leader = newRealServerClient(
                db = leaderDb,
                config = config,
                http = leaderHttp,
                syncTables = richSchemaSyncTables,
                uploadLimit = 8,
                downloadLimit = 2,
            )
            val writer = newRealServerClient(
                db = writerDb,
                config = config,
                http = writerHttp,
                syncTables = richSchemaSyncTables,
                uploadLimit = 8,
                downloadLimit = 2,
            )
            val observer = newRealServerClient(
                db = observerDb,
                config = config,
                http = observerHttp,
                syncTables = richSchemaSyncTables,
                uploadLimit = 8,
                downloadLimit = 2,
            )

            leader.bootstrap(userId, leaderDevice).getOrThrow()
            writer.bootstrap(userId, writerDevice).getOrThrow()
            observer.bootstrap(userId, observerDevice).getOrThrow()
            leader.hydrate().getOrThrow()
            writer.hydrate().getOrThrow()
            observer.hydrate().getOrThrow()

            val hotGraph = insertHotGraph(leaderDb)
            leader.pushPending().getOrThrow()
            writer.pullToStable().getOrThrow()
            observer.pullToStable().getOrThrow()

            val rounds = 6
            val writerConflictRounds = setOf(2, 4, 6)
            for (round in 1..rounds) {
                mutateHotGraph(leaderDb, hotGraph, round)
                insertRichSchemaBatch(leaderDb, "divergent-leader-$round")
                leader.pushPending().getOrThrow()

                if (round in writerConflictRounds) {
                    mutateHotGraph(writerDb, hotGraph, 100 + round)
                }
            }

            val conflict = writer.pushPending().exceptionOrNull()
            assertTrue(conflict != null)
            assertTrue(conflict?.message?.contains("409") == true)
            assertEquals(0L, scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertTrue(scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_push_outbound") > 0L)
            assertEquals("Hot User 106", scalarText(writerDb, "SELECT name FROM users WHERE id = '${hotGraph.userId}'"))
            assertEquals("Hot Review 106", scalarText(writerDb, "SELECT review FROM file_reviews WHERE id = '${hotGraph.reviewId}'"))

            val pullBlocked = writer.pullToStable().exceptionOrNull()
            assertTrue(pullBlocked is PendingPushReplayException)

            observer.pullToStable().getOrThrow()
            assertEquals(7L, observer.lastBundleSeqSeen().getOrThrow())
            assertHotGraphDrivenCounts(observerDb, rounds)
            assertHotGraphState(observerDb, hotGraph, rounds)
            assertRoundPresence(observerDb, "divergent-leader-6")
            assertForeignKeyIntegrity(observerDb)
            assertEquals(0L, scalarLong(observerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))

            val resetSourceId = randomDeviceId("divergent-writer-reset")
            writerHttp.close()
            val resetToken = issueDummySigninToken(config.baseUrl, userId, resetSourceId)
            writerHttp = newAuthenticatedHttpClient(config.baseUrl, resetToken)
            val rebuiltWriter = newRealServerClient(
                db = writerDb,
                config = config,
                http = writerHttp,
                syncTables = richSchemaSyncTables,
                uploadLimit = 8,
                downloadLimit = 2,
            )
            rebuiltWriter.bootstrap(userId, resetSourceId).getOrThrow()
            rebuiltWriter.hydrate().getOrThrow()

            assertEquals(7L, rebuiltWriter.lastBundleSeqSeen().getOrThrow())
            assertEquals(resetSourceId, scalarText(writerDb, "SELECT source_id FROM _sync_client_state WHERE user_id = '$userId'"))
            assertEquals(0L, scalarLong(writerDb, "SELECT rebuild_required FROM _sync_client_state WHERE user_id = '$userId'"))
            assertEquals(0L, scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_push_outbound"))
            assertEquals(0L, scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertHotGraphDrivenCounts(writerDb, rounds)
            assertHotGraphState(writerDb, hotGraph, rounds)
            assertForeignKeyIntegrity(writerDb)

            insertRichSchemaBatch(writerDb, "divergent-writer-recovered")
            rebuiltWriter.pushPending().getOrThrow()
            leader.pullToStable().getOrThrow()
            observer.pullToStable().getOrThrow()

            assertEquals(8L, leader.lastBundleSeqSeen().getOrThrow())
            assertEquals(8L, rebuiltWriter.lastBundleSeqSeen().getOrThrow())
            assertEquals(8L, observer.lastBundleSeqSeen().getOrThrow())

            assertHotGraphDrivenCounts(leaderDb, rounds, extraRichBatches = 1)
            assertHotGraphDrivenCounts(writerDb, rounds, extraRichBatches = 1)
            assertHotGraphDrivenCounts(observerDb, rounds, extraRichBatches = 1)
            assertHotGraphState(leaderDb, hotGraph, rounds)
            assertHotGraphState(writerDb, hotGraph, rounds)
            assertHotGraphState(observerDb, hotGraph, rounds)
            assertRoundPresence(leaderDb, "divergent-writer-recovered")
            assertRoundPresence(writerDb, "divergent-writer-recovered")
            assertRoundPresence(observerDb, "divergent-writer-recovered")
            assertForeignKeyIntegrity(leaderDb)
            assertForeignKeyIntegrity(writerDb)
            assertForeignKeyIntegrity(observerDb)
            assertEquals(0L, scalarLong(leaderDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(observerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            leaderHttp.close()
            writerHttp.close()
            observerHttp.close()
            leaderDb.close()
            writerDb.close()
            observerDb.close()
        }
    }
}
