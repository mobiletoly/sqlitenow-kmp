package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

internal class CrossTargetSyncIntegrationTest : CrossTargetSyncTestSupport() {
    @Test
    fun chunkedPushPullAndHydrate_convergeAcrossClients() = runTest {
        val server = MockSyncServer()
        val dbA = newDb()
        val dbB = newDb()
        val dbC = newDb()
        val httpA = server.newHttpClient()
        val httpB = server.newHttpClient()
        val httpC = server.newHttpClient()
        try {
            createUsersAndPostsTables(dbA)
            createUsersAndPostsTables(dbB)
            createUsersAndPostsTables(dbC)

            val clientA = newClient(dbA, httpA, uploadLimit = 2, downloadLimit = 2)
            val clientB = newClient(dbB, httpB, uploadLimit = 2, downloadLimit = 2)
            val clientC = newClient(dbC, httpC, uploadLimit = 2, downloadLimit = 2)

            clientA.bootstrap(userId = "user-1", sourceId = "device-a").getOrThrow()
            clientB.bootstrap(userId = "user-1", sourceId = "device-b").getOrThrow()
            clientC.bootstrap(userId = "user-1", sourceId = "device-c").getOrThrow()

            clientA.hydrate().getOrThrow()
            clientB.hydrate().getOrThrow()
            clientC.hydrate().getOrThrow()

            insertUser(dbA, "u1", "Ada")
            insertPost(dbA, "p1", "u1", "Ada Post")
            insertUser(dbA, "u2", "Grace")
            insertPost(dbA, "p2", "u2", "Grace Post")
            insertUser(dbA, "u3", "Linus")
            insertPost(dbA, "p3", "u3", "Linus Post")

            clientA.pushPending().getOrThrow()
            assertTrue(server.uploadedChunkCount >= 3)

            clientB.pullToStable().getOrThrow()
            clientC.hydrate().getOrThrow()

            assertEquals(1L, clientA.lastBundleSeqSeen().getOrThrow())
            assertEquals(1L, clientB.lastBundleSeqSeen().getOrThrow())
            assertEquals(1L, clientC.lastBundleSeqSeen().getOrThrow())
            assertEquals(3L, scalarLong(dbB, "SELECT COUNT(*) FROM users"))
            assertEquals(3L, scalarLong(dbB, "SELECT COUNT(*) FROM posts"))
            assertEquals(3L, scalarLong(dbC, "SELECT COUNT(*) FROM users"))
            assertEquals(3L, scalarLong(dbC, "SELECT COUNT(*) FROM posts"))
            assertEquals("Grace Post", scalarText(dbB, "SELECT title FROM posts WHERE id = 'p2'"))
            assertEquals("Linus", scalarText(dbC, "SELECT name FROM users WHERE id = 'u3'"))
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

    @Test
    fun conflictingPush_failsClosedAndBlocksPull() = runTest {
        val server = MockSyncServer()
        val dbA = newDb()
        val dbB = newDb()
        val httpA = server.newHttpClient()
        val httpB = server.newHttpClient()
        try {
            createUsersAndPostsTables(dbA)
            createUsersAndPostsTables(dbB)

            val clientA = newClient(dbA, httpA)
            val clientB = newClient(dbB, httpB)

            clientA.bootstrap(userId = "user-1", sourceId = "device-a").getOrThrow()
            clientB.bootstrap(userId = "user-1", sourceId = "device-b").getOrThrow()
            clientA.hydrate().getOrThrow()
            clientB.hydrate().getOrThrow()

            insertUser(dbA, "u1", "Original")
            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()

            updateUserName(dbA, "u1", "Server Name")
            updateUserName(dbB, "u1", "Local Name")
            clientA.pushPending().getOrThrow()

            val conflict = clientB.pushPending().exceptionOrNull()
            assertTrue(conflict != null)
            assertContains(conflict!!.message ?: "", "409")
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertTrue(scalarLong(dbB, "SELECT COUNT(*) FROM _sync_push_outbound") > 0L)
            assertEquals("Local Name", scalarText(dbB, "SELECT name FROM users WHERE id = 'u1'"))

            val blocked = clientB.pullToStable().exceptionOrNull()
            assertTrue(blocked is PendingPushReplayException)
        } finally {
            httpA.close()
            httpB.close()
            dbA.close()
            dbB.close()
        }
    }

    @Test
    fun historyPruned_pullRebuildsThroughSnapshotWithoutSourceRotation() = runTest {
        val server = MockSyncServer()
        val leaderDb = newDb()
        val followerDb = newDb()
        val leaderHttp = server.newHttpClient()
        val followerHttp = server.newHttpClient()
        try {
            createUsersAndPostsTables(leaderDb)
            createUsersAndPostsTables(followerDb)

            val leader = newClient(leaderDb, leaderHttp, uploadLimit = 2, downloadLimit = 2)
            val follower = newClient(followerDb, followerHttp, uploadLimit = 2, downloadLimit = 2)

            leader.bootstrap(userId = "user-1", sourceId = "leader-device").getOrThrow()
            follower.bootstrap(userId = "user-1", sourceId = "follower-device").getOrThrow()
            leader.hydrate().getOrThrow()
            follower.hydrate().getOrThrow()

            insertUser(leaderDb, "u1", "Seed")
            leader.pushPending().getOrThrow()
            follower.pullToStable().getOrThrow()

            val sourceBefore = scalarText(
                followerDb,
                "SELECT source_id FROM _sync_client_state WHERE user_id = 'user-1'",
            )

            updateUserName(leaderDb, "u1", "Latest")
            insertPost(leaderDb, "p1", "u1", "Latest Post")
            leader.pushPending().getOrThrow()

            server.retainedBundleFloor = 2
            follower.pullToStable().getOrThrow()

            assertEquals(2L, follower.lastBundleSeqSeen().getOrThrow())
            assertEquals(sourceBefore, scalarText(followerDb, "SELECT source_id FROM _sync_client_state WHERE user_id = 'user-1'"))
            assertEquals("Latest", scalarText(followerDb, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("Latest Post", scalarText(followerDb, "SELECT title FROM posts WHERE id = 'p1'"))
            assertEquals(0L, scalarLong(followerDb, "SELECT rebuild_required FROM _sync_client_state WHERE user_id = 'user-1'"))
            assertEquals(0L, scalarLong(followerDb, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            assertEquals(0L, scalarLong(followerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            leaderHttp.close()
            followerHttp.close()
            leaderDb.close()
            followerDb.close()
        }
    }

    @Test
    fun recover_rotatesSourceAndAllowsFollowupPush() = runTest {
        val server = MockSyncServer()
        val seedDb = newDb()
        val recoverDb = newDb()
        val verifyDb = newDb()
        val seedHttp = server.newHttpClient()
        val recoverHttp = server.newHttpClient()
        val verifyHttp = server.newHttpClient()
        try {
            createUsersAndPostsTables(seedDb)
            createUsersAndPostsTables(recoverDb)
            createUsersAndPostsTables(verifyDb)

            val seedClient = newClient(seedDb, seedHttp)
            val recoverClient = newClient(recoverDb, recoverHttp)
            val verifyClient = newClient(verifyDb, verifyHttp)

            seedClient.bootstrap(userId = "user-1", sourceId = "seed-device").getOrThrow()
            recoverClient.bootstrap(userId = "user-1", sourceId = "recover-device").getOrThrow()
            verifyClient.bootstrap(userId = "user-1", sourceId = "verify-device").getOrThrow()

            insertUser(seedDb, "u1", "Recover Seed")
            insertPost(seedDb, "p1", "u1", "Recover Seed Post")
            seedClient.pushPending().getOrThrow()

            val sourceBefore = scalarText(
                recoverDb,
                "SELECT source_id FROM _sync_client_state WHERE user_id = 'user-1'",
            )
            recoverClient.recover().getOrThrow()

            val sourceAfter = scalarText(
                recoverDb,
                "SELECT source_id FROM _sync_client_state WHERE user_id = 'user-1'",
            )
            assertNotEquals(sourceBefore, sourceAfter)
            assertEquals(1L, scalarLong(recoverDb, "SELECT next_source_bundle_id FROM _sync_client_state WHERE user_id = 'user-1'"))
            assertEquals("Recover Seed", scalarText(recoverDb, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("Recover Seed Post", scalarText(recoverDb, "SELECT title FROM posts WHERE id = 'p1'"))

            insertUser(recoverDb, "u2", "Recover Writer")
            insertPost(recoverDb, "p2", "u2", "Recover Followup")
            recoverClient.pushPending().getOrThrow()
            verifyClient.pullToStable().getOrThrow()

            assertEquals(2L, verifyClient.lastBundleSeqSeen().getOrThrow())
            assertEquals(2L, scalarLong(verifyDb, "SELECT COUNT(*) FROM users"))
            assertEquals(2L, scalarLong(verifyDb, "SELECT COUNT(*) FROM posts"))
            assertEquals("Recover Followup", scalarText(verifyDb, "SELECT title FROM posts WHERE id = 'p2'"))
            assertEquals(0L, scalarLong(recoverDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(verifyDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            seedHttp.close()
            recoverHttp.close()
            verifyHttp.close()
            seedDb.close()
            recoverDb.close()
            verifyDb.close()
        }
    }
}
