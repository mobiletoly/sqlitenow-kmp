package dev.goquick.sqlitenow.oversqlite.platform

import dev.goquick.sqlitenow.oversqlite.*
import dev.goquick.sqlitenow.oversqlite.platformsupport.openAndConnect

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

internal class PlatformParityTest : PlatformCrossTargetTestSupport() {
    private fun updatedAtResolver(): Resolver = Resolver { conflict ->
        val serverUpdatedAt = conflict.serverRow
            ?.jsonObject
            ?.get("updated_at")
            ?.jsonPrimitive
            ?.contentOrNull
        val localUpdatedAt = conflict.localPayload
            ?.jsonObject
            ?.get("updated_at")
            ?.jsonPrimitive
            ?.contentOrNull
        if (serverUpdatedAt != null && localUpdatedAt != null && localUpdatedAt > serverUpdatedAt) {
            MergeResult.KeepLocal
        } else {
            MergeResult.AcceptServer
        }
    }

    @Test
    fun chunkedPushPullAndHydrate_convergeAcrossClients() = runTest {
        if (!platformSuiteEnabled()) return@runTest
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

            clientA.openAndConnect(userId = "user-1").getOrThrow()
            clientB.openAndConnect(userId = "user-1").getOrThrow()
            clientC.openAndConnect(userId = "user-1").getOrThrow()

            clientA.rebuild().getOrThrow()
            clientB.rebuild().getOrThrow()
            clientC.rebuild().getOrThrow()

            insertUser(dbA, "u1", "Ada")
            insertPost(dbA, "p1", "u1", "Ada Post")
            insertUser(dbA, "u2", "Grace")
            insertPost(dbA, "p2", "u2", "Grace Post")
            insertUser(dbA, "u3", "Linus")
            insertPost(dbA, "p3", "u3", "Linus Post")

            clientA.pushPending().getOrThrow()
            assertTrue(server.uploadedChunkCount >= 3)

            clientB.pullToStable().getOrThrow()
            clientC.rebuild().getOrThrow()

            assertEquals(1L, clientA.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(1L, clientB.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(1L, clientC.syncStatus().getOrThrow().lastBundleSeqSeen)
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
    fun pullToStable_succeedsWithoutSqliteNowInvalidationBinding() = runTest {
        if (!platformSuiteEnabled()) return@runTest
        val server = MockSyncServer()
        val leaderDb = newDb()
        val followerDb = newDb()
        val leaderHttp = server.newHttpClient()
        val followerHttp = server.newHttpClient()
        try {
            createUsersAndPostsTables(leaderDb)
            createUsersAndPostsTables(followerDb)

            val leader = newClient(leaderDb, leaderHttp)
            val follower = newClient(followerDb, followerHttp)

            leader.openAndConnect(userId = "user-1").getOrThrow()
            follower.openAndConnect(userId = "user-1").getOrThrow()
            leader.rebuild().getOrThrow()
            follower.rebuild().getOrThrow()

            insertUser(leaderDb, "u1", "Ada")
            leader.pushPending().getOrThrow()

            follower.pullToStable().getOrThrow()

            assertEquals("Ada", scalarText(followerDb, "SELECT name FROM users WHERE id = 'u1'"))
        } finally {
            leaderHttp.close()
            followerHttp.close()
            leaderDb.close()
            followerDb.close()
        }
    }

    @Test
    fun pushPending_doesNotSkipEarlierUnseenPeerBundles() = runTest {
        if (!platformSuiteEnabled()) return@runTest
        val server = MockSyncServer()
        val dbA = newDb()
        val dbB = newDb()
        try {
            createUsersAndPostsTables(dbA)
            createUsersAndPostsTables(dbB)

            val clientA = newClient(dbA, server.newHttpClient())
            val clientB = newClient(dbB, server.newHttpClient())

            clientA.openAndConnect(userId = "user-1").getOrThrow()
            clientB.openAndConnect(userId = "user-1").getOrThrow()
            clientA.rebuild().getOrThrow()
            clientB.rebuild().getOrThrow()

            insertUser(dbA, "u1", "From A")
            clientA.pushPending().getOrThrow()

            insertUser(dbB, "u2", "From B")
            clientB.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()

            assertEquals(2L, clientB.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals("From A", scalarText(dbB, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("From B", scalarText(dbB, "SELECT name FROM users WHERE id = 'u2'"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            dbA.close()
            dbB.close()
        }
    }

    @Test
    fun threeDevices_pushWithoutPull_thenAllPull_converge() = runTest {
        if (!platformSuiteEnabled()) return@runTest
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

            val clientA = newClient(dbA, httpA)
            val clientB = newClient(dbB, httpB)
            val clientC = newClient(dbC, httpC)

            clientA.openAndConnect(userId = "user-1").getOrThrow()
            clientB.openAndConnect(userId = "user-1").getOrThrow()
            clientC.openAndConnect(userId = "user-1").getOrThrow()
            clientA.rebuild().getOrThrow()
            clientB.rebuild().getOrThrow()
            clientC.rebuild().getOrThrow()

            insertUser(dbA, "u1", "From A")
            clientA.pushPending().getOrThrow()

            insertUser(dbB, "u2", "From B")
            clientB.pushPending().getOrThrow()

            insertUser(dbC, "u3", "From C")
            clientC.pushPending().getOrThrow()

            clientA.pullToStable().getOrThrow()
            clientB.pullToStable().getOrThrow()
            clientC.pullToStable().getOrThrow()

            assertEquals(3L, clientA.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(3L, clientB.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(3L, clientC.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(3L, scalarLong(dbA, "SELECT COUNT(*) FROM users"))
            assertEquals(3L, scalarLong(dbB, "SELECT COUNT(*) FROM users"))
            assertEquals(3L, scalarLong(dbC, "SELECT COUNT(*) FROM users"))
            assertEquals("From A", scalarText(dbB, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("From B", scalarText(dbC, "SELECT name FROM users WHERE id = 'u2'"))
            assertEquals("From C", scalarText(dbA, "SELECT name FROM users WHERE id = 'u3'"))
            assertEquals(0L, scalarLong(dbA, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbC, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbA, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(dbC, "SELECT COUNT(*) FROM _sync_outbox_rows"))
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
    fun restartAfterOwnPushBeforePull_stillFetchesEarlierPeerBundles() = runTest {
        if (!platformSuiteEnabled()) return@runTest
        val server = MockSyncServer()
        val dbA = newDb()
        val dbB = newDb()
        val httpA = server.newHttpClient()
        val httpB = server.newHttpClient()
        val restartedHttpB = server.newHttpClient()
        val sourceIdA = "restart-a-source"
        val sourceIdB = "restart-b-source"
        try {
            createUsersAndPostsTables(dbA)
            createUsersAndPostsTables(dbB)

            val clientA = newClient(dbA, httpA)
            val clientB = newClient(dbB, httpB)

            clientA.openAndConnect(userId = "user-1").getOrThrow()
            clientB.openAndConnect(userId = "user-1").getOrThrow()
            clientA.rebuild().getOrThrow()
            clientB.rebuild().getOrThrow()

            insertUser(dbA, "u1", "From A")
            clientA.pushPending().getOrThrow()

            insertUser(dbB, "u2", "From B")
            clientB.pushPending().getOrThrow()

            val restartedClientB = newClient(dbB, restartedHttpB)
            restartedClientB.openAndConnect(userId = "user-1").getOrThrow()
            restartedClientB.pullToStable().getOrThrow()

            assertEquals(2L, restartedClientB.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals("From A", scalarText(dbB, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("From B", scalarText(dbB, "SELECT name FROM users WHERE id = 'u2'"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            httpA.close()
            httpB.close()
            restartedHttpB.close()
            dbA.close()
            dbB.close()
        }
    }

    @Test
    fun conflictingPush_withUnseenPeerBundles_clientWinsStillConverges() = runTest {
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

            val clientA = newClient(dbA, httpA)
            val clientB = newClient(dbB, httpB, resolver = ClientWinsResolver)
            val clientC = newClient(dbC, httpC)

            clientA.openAndConnect(userId = "user-1").getOrThrow()
            clientB.openAndConnect(userId = "user-1").getOrThrow()
            clientC.openAndConnect(userId = "user-1").getOrThrow()
            clientA.rebuild().getOrThrow()
            clientB.rebuild().getOrThrow()
            clientC.rebuild().getOrThrow()

            insertUser(dbA, "u1", "Original")
            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()
            clientC.pullToStable().getOrThrow()

            updateUserName(dbA, "u1", "Server Name")
            clientA.pushPending().getOrThrow()

            insertUser(dbC, "u2", "Peer Row")
            clientC.pushPending().getOrThrow()

            updateUserName(dbB, "u1", "Client Wins")
            clientB.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()
            clientA.pullToStable().getOrThrow()
            clientC.pullToStable().getOrThrow()

            assertEquals(4L, clientA.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(4L, clientB.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(4L, clientC.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals("Client Wins", scalarText(dbA, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("Client Wins", scalarText(dbB, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("Client Wins", scalarText(dbC, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("Peer Row", scalarText(dbA, "SELECT name FROM users WHERE id = 'u2'"))
            assertEquals("Peer Row", scalarText(dbB, "SELECT name FROM users WHERE id = 'u2'"))
            assertEquals("Peer Row", scalarText(dbC, "SELECT name FROM users WHERE id = 'u2'"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
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
    fun conflictingPush_keepLocalResolverAutoRetriesAndWins() = runTest {
        val server = MockSyncServer()
        val dbA = newDb()
        val dbB = newDb()
        val observerDb = newDb()
        val httpA = server.newHttpClient()
        val httpB = server.newHttpClient()
        val observerHttp = server.newHttpClient()
        try {
            createUsersAndPostsTables(dbA)
            createUsersAndPostsTables(dbB)
            createUsersAndPostsTables(observerDb)

            val clientA = newClient(dbA, httpA)
            val clientB = newClient(dbB, httpB, resolver = Resolver { MergeResult.KeepLocal })
            val observer = newClient(observerDb, observerHttp)

            clientA.openAndConnect(userId = "user-1").getOrThrow()
            clientB.openAndConnect(userId = "user-1").getOrThrow()
            observer.openAndConnect(userId = "user-1").getOrThrow()
            clientA.rebuild().getOrThrow()
            clientB.rebuild().getOrThrow()
            observer.rebuild().getOrThrow()

            insertUser(dbA, "u1", "Original")
            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()
            observer.pullToStable().getOrThrow()

            updateUserName(dbA, "u1", "Server Name")
            updateUserName(dbB, "u1", "Local Name")
            clientA.pushPending().getOrThrow()

            clientB.pushPending().getOrThrow()

            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("Local Name", scalarText(dbB, "SELECT name FROM users WHERE id = 'u1'"))
            clientB.pullToStable().getOrThrow()

            observer.pullToStable().getOrThrow()
            assertEquals("Local Name", scalarText(observerDb, "SELECT name FROM users WHERE id = 'u1'"))
        } finally {
            httpA.close()
            httpB.close()
            observerHttp.close()
            dbA.close()
            dbB.close()
            observerDb.close()
        }
    }

    @Test
    fun conflictingPush_updatedAtResolverKeepsNewerLocalIntent() = runTest {
        val server = MockSyncServer()
        val dbA = newDb()
        val dbB = newDb()
        val observerDb = newDb()
        val httpA = server.newHttpClient()
        val httpB = server.newHttpClient()
        val observerHttp = server.newHttpClient()
        try {
            createUsersWithUpdatedAtAndPostsTables(dbA)
            createUsersWithUpdatedAtAndPostsTables(dbB)
            createUsersWithUpdatedAtAndPostsTables(observerDb)

            val clientA = newClient(dbA, httpA)
            val clientB = newClient(dbB, httpB, resolver = updatedAtResolver())
            val observer = newClient(observerDb, observerHttp)

            clientA.openAndConnect(userId = "user-1").getOrThrow()
            clientB.openAndConnect(userId = "user-1").getOrThrow()
            observer.openAndConnect(userId = "user-1").getOrThrow()
            clientA.rebuild().getOrThrow()
            clientB.rebuild().getOrThrow()
            observer.rebuild().getOrThrow()

            dbA.execSQL(
                """
                INSERT INTO users(id, name, updated_at)
                VALUES('u1', 'Original', '2026-03-24T00:00:00Z')
                """.trimIndent(),
            )
            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()
            observer.pullToStable().getOrThrow()

            dbA.execSQL("UPDATE users SET name = 'Server Older', updated_at = '2026-03-24T00:00:10Z' WHERE id = 'u1'")
            dbB.execSQL("UPDATE users SET name = 'Local Newer', updated_at = '2026-03-24T00:00:20Z' WHERE id = 'u1'")
            clientA.pushPending().getOrThrow()
            clientB.pushPending().getOrThrow()
            observer.pullToStable().getOrThrow()

            assertEquals("Local Newer", scalarText(dbB, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("2026-03-24T00:00:20Z", scalarText(dbB, "SELECT updated_at FROM users WHERE id = 'u1'"))
            assertEquals("Local Newer", scalarText(observerDb, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("2026-03-24T00:00:20Z", scalarText(observerDb, "SELECT updated_at FROM users WHERE id = 'u1'"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            httpA.close()
            httpB.close()
            observerHttp.close()
            dbA.close()
            dbB.close()
            observerDb.close()
        }
    }

    @Test
    fun conflictingPush_updatedAtResolverAcceptsNewerServerState() = runTest {
        val server = MockSyncServer()
        val dbA = newDb()
        val dbB = newDb()
        val observerDb = newDb()
        val httpA = server.newHttpClient()
        val httpB = server.newHttpClient()
        val observerHttp = server.newHttpClient()
        try {
            createUsersWithUpdatedAtAndPostsTables(dbA)
            createUsersWithUpdatedAtAndPostsTables(dbB)
            createUsersWithUpdatedAtAndPostsTables(observerDb)

            val clientA = newClient(dbA, httpA)
            val clientB = newClient(dbB, httpB, resolver = updatedAtResolver())
            val observer = newClient(observerDb, observerHttp)

            clientA.openAndConnect(userId = "user-1").getOrThrow()
            clientB.openAndConnect(userId = "user-1").getOrThrow()
            observer.openAndConnect(userId = "user-1").getOrThrow()
            clientA.rebuild().getOrThrow()
            clientB.rebuild().getOrThrow()
            observer.rebuild().getOrThrow()

            dbA.execSQL(
                """
                INSERT INTO users(id, name, updated_at)
                VALUES('u1', 'Original', '2026-03-24T00:00:00Z')
                """.trimIndent(),
            )
            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()
            observer.pullToStable().getOrThrow()

            dbA.execSQL("UPDATE users SET name = 'Server Newer', updated_at = '2026-03-24T00:00:20Z' WHERE id = 'u1'")
            dbB.execSQL("UPDATE users SET name = 'Local Older', updated_at = '2026-03-24T00:00:10Z' WHERE id = 'u1'")
            clientA.pushPending().getOrThrow()
            clientB.pushPending().getOrThrow()
            observer.pullToStable().getOrThrow()

            assertEquals("Server Newer", scalarText(dbB, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("2026-03-24T00:00:20Z", scalarText(dbB, "SELECT updated_at FROM users WHERE id = 'u1'"))
            assertEquals("Server Newer", scalarText(observerDb, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("2026-03-24T00:00:20Z", scalarText(observerDb, "SELECT updated_at FROM users WHERE id = 'u1'"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            httpA.close()
            httpB.close()
            observerHttp.close()
            dbA.close()
            dbB.close()
            observerDb.close()
        }
    }

    @Test
    fun conflictingPush_clientWinsResolverAutoRetriesAndWins() = runTest {
        val server = MockSyncServer()
        val dbA = newDb()
        val dbB = newDb()
        val observerDb = newDb()
        val httpA = server.newHttpClient()
        val httpB = server.newHttpClient()
        val observerHttp = server.newHttpClient()
        try {
            createUsersAndPostsTables(dbA)
            createUsersAndPostsTables(dbB)
            createUsersAndPostsTables(observerDb)

            val clientA = newClient(dbA, httpA)
            val clientB = newClient(dbB, httpB, resolver = ClientWinsResolver)
            val observer = newClient(observerDb, observerHttp)

            clientA.openAndConnect(userId = "user-1").getOrThrow()
            clientB.openAndConnect(userId = "user-1").getOrThrow()
            observer.openAndConnect(userId = "user-1").getOrThrow()
            clientA.rebuild().getOrThrow()
            clientB.rebuild().getOrThrow()
            observer.rebuild().getOrThrow()

            insertUser(dbA, "u1", "Original")
            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()
            observer.pullToStable().getOrThrow()

            updateUserName(dbA, "u1", "Server Name")
            updateUserName(dbB, "u1", "Client Wins")
            clientA.pushPending().getOrThrow()
            clientB.pushPending().getOrThrow()
            observer.pullToStable().getOrThrow()

            assertEquals("Client Wins", scalarText(dbB, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("Client Wins", scalarText(observerDb, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            httpA.close()
            httpB.close()
            observerHttp.close()
            dbA.close()
            dbB.close()
            observerDb.close()
        }
    }

    @Test
    fun conflictingPush_keepMergedResolverCommitsMergedPayload() = runTest {
        val server = MockSyncServer()
        val dbA = newDb()
        val dbB = newDb()
        val observerDb = newDb()
        val httpA = server.newHttpClient()
        val httpB = server.newHttpClient()
        val observerHttp = server.newHttpClient()
        try {
            createUsersAndPostsTables(dbA)
            createUsersAndPostsTables(dbB)
            createUsersAndPostsTables(observerDb)

            val clientA = newClient(dbA, httpA)
            val clientB = newClient(
                dbB,
                httpB,
                resolver = Resolver { conflict ->
                    MergeResult.KeepMerged(
                        buildJsonObject {
                            put("id", JsonPrimitive("u1"))
                            put("name", JsonPrimitive("Merged Name"))
                        },
                    )
                },
            )
            val observer = newClient(observerDb, observerHttp)

            clientA.openAndConnect(userId = "user-1").getOrThrow()
            clientB.openAndConnect(userId = "user-1").getOrThrow()
            observer.openAndConnect(userId = "user-1").getOrThrow()
            clientA.rebuild().getOrThrow()
            clientB.rebuild().getOrThrow()
            observer.rebuild().getOrThrow()

            insertUser(dbA, "u1", "Original")
            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()
            observer.pullToStable().getOrThrow()

            updateUserName(dbA, "u1", "Server Name")
            updateUserName(dbB, "u1", "Local Name")
            clientA.pushPending().getOrThrow()
            clientB.pushPending().getOrThrow()
            observer.pullToStable().getOrThrow()

            assertEquals("Merged Name", scalarText(dbB, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("Merged Name", scalarText(observerDb, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            httpA.close()
            httpB.close()
            observerHttp.close()
            dbA.close()
            dbB.close()
            observerDb.close()
        }
    }

    @Test
    fun conflictingPush_preservesSiblingRowsFromRejectedBundle() = runTest {
        val server = MockSyncServer()
        val dbA = newDb()
        val dbB = newDb()
        val observerDb = newDb()
        val httpA = server.newHttpClient()
        val httpB = server.newHttpClient()
        val observerHttp = server.newHttpClient()
        try {
            createUsersAndPostsTables(dbA)
            createUsersAndPostsTables(dbB)
            createUsersAndPostsTables(observerDb)

            val clientA = newClient(dbA, httpA)
            val clientB = newClient(dbB, httpB, resolver = ClientWinsResolver)
            val observer = newClient(observerDb, observerHttp)

            clientA.openAndConnect(userId = "user-1").getOrThrow()
            clientB.openAndConnect(userId = "user-1").getOrThrow()
            observer.openAndConnect(userId = "user-1").getOrThrow()
            clientA.rebuild().getOrThrow()
            clientB.rebuild().getOrThrow()
            observer.rebuild().getOrThrow()

            insertUser(dbA, "u1", "Original")
            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()
            observer.pullToStable().getOrThrow()

            updateUserName(dbA, "u1", "Server Name")
            updateUserName(dbB, "u1", "Client Name")
            insertPost(dbB, "p1", "u1", "Sibling Post")
            clientA.pushPending().getOrThrow()

            clientB.pushPending().getOrThrow()
            observer.pullToStable().getOrThrow()

            assertEquals("Client Name", scalarText(observerDb, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("Sibling Post", scalarText(observerDb, "SELECT title FROM posts WHERE id = 'p1'"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            httpA.close()
            httpB.close()
            observerHttp.close()
            dbA.close()
            dbB.close()
            observerDb.close()
        }
    }

    @Test
    fun conflictingPush_retryExhaustionLeavesReplayableDirtyState() = runTest {
        val server = MockSyncServer()
        val dbA = newDb()
        val dbB = newDb()
        val httpA = server.newHttpClient()
        val httpB = server.newHttpClient()
        try {
            createUsersAndPostsTables(dbA)
            createUsersAndPostsTables(dbB)

            var forcedConflicts = 0
            server.conflictOverride = { row, _, _ ->
                if (row.table == "users" && row.key["id"] == "u1" && forcedConflicts in 1..3) {
                    forcedConflicts++
                    PushConflictDetails(
                        schema = row.schema,
                        table = row.table,
                        key = row.key,
                        op = row.op,
                        baseRowVersion = row.baseRowVersion,
                        serverRowVersion = 7L + forcedConflicts,
                        serverRowDeleted = false,
                        serverRow = buildJsonObject {
                            put("id", JsonPrimitive("u1"))
                            put("name", JsonPrimitive("Server $forcedConflicts"))
                        },
                    )
                } else {
                    null
                }
            }

            val clientA = newClient(dbA, httpA)
            val clientB = newClient(dbB, httpB, resolver = ClientWinsResolver)

            clientA.openAndConnect(userId = "user-1").getOrThrow()
            clientB.openAndConnect(userId = "user-1").getOrThrow()
            clientA.rebuild().getOrThrow()
            clientB.rebuild().getOrThrow()

            insertUser(dbA, "u1", "Original")
            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()

            updateUserName(dbB, "u1", "Replay Me")
            forcedConflicts = 1

            val exhausted = clientB.pushPending().exceptionOrNull()
            assertNotNull(exhausted)
            assertTrue(exhausted is PushConflictRetryExhaustedException)
            exhausted as PushConflictRetryExhaustedException
            assertEquals(2, exhausted.retryCount)
            assertEquals(1, exhausted.remainingDirtyCount)
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(1L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals("Replay Me", scalarText(dbB, "SELECT name FROM users WHERE id = 'u1'"))

            server.conflictOverride = null
            clientB.pushPending().getOrThrow()
            clientA.pullToStable().getOrThrow()

            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("Replay Me", scalarText(dbA, "SELECT name FROM users WHERE id = 'u1'"))
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

            leader.openAndConnect(userId = "user-1").getOrThrow()
            follower.openAndConnect(userId = "user-1").getOrThrow()
            leader.rebuild().getOrThrow()
            follower.rebuild().getOrThrow()

            insertUser(leaderDb, "u1", "Seed")
            leader.pushPending().getOrThrow()
            follower.pullToStable().getOrThrow()

            val sourceBefore = scalarText(
                followerDb,
                "SELECT current_source_id FROM _sync_attachment_state",
            )

            updateUserName(leaderDb, "u1", "Latest")
            insertPost(leaderDb, "p1", "u1", "Latest Post")
            leader.pushPending().getOrThrow()

            server.retainedBundleFloor = 2
            follower.pullToStable().getOrThrow()

            assertEquals(2L, follower.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(sourceBefore, scalarText(followerDb, "SELECT current_source_id FROM _sync_attachment_state"))
            assertEquals("Latest", scalarText(followerDb, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("Latest Post", scalarText(followerDb, "SELECT title FROM posts WHERE id = 'p1'"))
            assertEquals(0L, scalarLong(followerDb, "SELECT rebuild_required FROM _sync_attachment_state"))
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
    fun detach_rotatesSourceAndAllowsFollowupPush() = runTest {
        if (!platformSuiteEnabled()) return@runTest
        val server = MockSyncServer()
        val activeDb = newDb()
        val verifyDb = newDb()
        val activeHttp = server.newHttpClient()
        val verifyHttp = server.newHttpClient()
        try {
            createUsersAndPostsTables(activeDb)
            createUsersAndPostsTables(verifyDb)

            val activeClient = newClient(activeDb, activeHttp)
            val verifyClient = newClient(verifyDb, verifyHttp)

            activeClient.openAndConnect(userId = "user-1").getOrThrow()
            verifyClient.openAndConnect(userId = "user-1").getOrThrow()

            insertUser(activeDb, "u1", "Recover Seed")
            insertPost(activeDb, "p1", "u1", "Recover Seed Post")
            activeClient.pushPending().getOrThrow()

            val sourceBefore = scalarText(
                activeDb,
                "SELECT current_source_id FROM _sync_attachment_state",
            )
            assertEquals(DetachOutcome.DETACHED, activeClient.detach().getOrThrow())

            val sourceAfter = scalarText(
                activeDb,
                "SELECT current_source_id FROM _sync_attachment_state",
            )
            assertNotEquals(sourceBefore, sourceAfter)
            activeClient.openAndConnect(userId = "user-1").getOrThrow()
            assertEquals(
                1L,
                scalarLong(
                    activeDb,
                    "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$sourceAfter'",
                ),
            )
            assertEquals("Recover Seed", scalarText(activeDb, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("Recover Seed Post", scalarText(activeDb, "SELECT title FROM posts WHERE id = 'p1'"))

            insertUser(activeDb, "u2", "Recover Writer")
            insertPost(activeDb, "p2", "u2", "Recover Followup")
            activeClient.pushPending().getOrThrow()
            verifyClient.pullToStable().getOrThrow()

            assertEquals(2L, verifyClient.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(2L, scalarLong(verifyDb, "SELECT COUNT(*) FROM users"))
            assertEquals(2L, scalarLong(verifyDb, "SELECT COUNT(*) FROM posts"))
            assertEquals("Recover Followup", scalarText(verifyDb, "SELECT title FROM posts WHERE id = 'p2'"))
            assertEquals(0L, scalarLong(activeDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(verifyDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            activeHttp.close()
            verifyHttp.close()
            activeDb.close()
            verifyDb.close()
        }
    }

    @Test
    fun sourceRetired_pushCreateRotatesSourceAndPreservesReservedReplacementAcrossRebuild() = runTest {
        if (!platformSuiteEnabled()) return@runTest
        val server = MockSyncServer()
        val activeDb = newDb()
        val verifyDb = newDb()
        val activeHttp = server.newHttpClient()
        val verifyHttp = server.newHttpClient()
        try {
            createUsersAndPostsTables(activeDb)
            createUsersAndPostsTables(verifyDb)

            val activeClient = newClient(activeDb, activeHttp)
            val verifyClient = newClient(verifyDb, verifyHttp)

            activeClient.openAndConnect(userId = "user-1").getOrThrow()
            verifyClient.openAndConnect(userId = "user-1").getOrThrow()
            activeClient.rebuild().getOrThrow()
            verifyClient.rebuild().getOrThrow()

            val sourceBefore = scalarText(activeDb, "SELECT current_source_id FROM _sync_attachment_state")
            insertUser(activeDb, "u1", "Platform Rotated")
            server.sourceRetiredOnPushCreate = SourceRetiredResponse(
                error = "source_retired",
                message = "source retired on create",
                sourceId = sourceBefore,
                replacedBySourceId = "platform-rotated-device",
            )

            val error = assertFailsWith<SourceRecoveryRequiredException> {
                activeClient.pushPending().getOrThrow()
            }
            assertEquals(SourceRecoveryReason.SOURCE_RETIRED, error.reason)
            assertEquals("platform-rotated-device", scalarText(activeDb, "SELECT replacement_source_id FROM _sync_operation_state"))
            assertEquals(sourceBefore, scalarText(activeDb, "SELECT current_source_id FROM _sync_attachment_state"))

            server.sourceRetiredOnPushCreate = null
            activeClient.rebuild().getOrThrow()

            assertEquals(
                listOf("platform-rotated-device"),
                server.requestedSnapshotSourceReplacements.map { it.newSourceId },
            )
            assertEquals("platform-rotated-device", scalarText(activeDb, "SELECT current_source_id FROM _sync_attachment_state"))

            activeClient.pushPending().getOrThrow()
            verifyClient.pullToStable().getOrThrow()
            assertEquals("Platform Rotated", scalarText(verifyDb, "SELECT name FROM users WHERE id = 'u1'"))
        } finally {
            activeHttp.close()
            verifyHttp.close()
            activeDb.close()
            verifyDb.close()
        }
    }
}
