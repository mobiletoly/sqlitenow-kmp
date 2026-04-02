package dev.goquick.sqlitenow.oversqlite.e2e

import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class RealServerBasicContractTest {
    @Test
    fun pushPending_thenPullToStable_convergesAcrossTwoDevices() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val deviceA = randomSourceId("device-a")
        val deviceB = randomSourceId("device-b")

        val tokenA = issueDummySigninToken(config.baseUrl, userId, deviceA)
        val tokenB = issueDummySigninToken(config.baseUrl, userId, deviceB)
        val httpA = newAuthenticatedHttpClient(config.baseUrl, tokenA)
        val httpB = newAuthenticatedHttpClient(config.baseUrl, tokenB)
        val dbA = newInMemoryDb()
        val dbB = newInMemoryDb()
        try {
            createBusinessSubsetTables(dbA)
            createBusinessSubsetTables(dbB)

            val clientA = newRealServerClient(dbA, config, httpA)
            val clientB = newRealServerClient(dbB, config, httpB)

            clientA.openAndAttach(userId).getOrThrow()
            clientB.openAndAttach(userId).getOrThrow()

            val authorId = randomRowId()
            val postId = randomRowId()
            dbA.execSQL(
                """
                INSERT INTO users(id, name, email)
                VALUES('$authorId', 'Ada A', 'ada-a@example.com')
                """.trimIndent()
            )
            dbA.execSQL(
                """
                INSERT INTO posts(id, title, content, author_id)
                VALUES('$postId', 'Round 1', 'from device A', '$authorId')
                """.trimIndent()
            )

            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()

            assertEquals("Ada A", scalarText(dbB, "SELECT name FROM users WHERE id = '$authorId'"))
            assertEquals("Round 1", scalarText(dbB, "SELECT title FROM posts WHERE id = '$postId'"))
            assertEquals("from device A", scalarText(dbB, "SELECT content FROM posts WHERE id = '$postId'"))
            assertEquals(0L, scalarLong(dbA, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            httpA.close()
            httpB.close()
            dbA.close()
            dbB.close()
        }
    }

    @Test
    fun pushPending_thenOwnPush_thenPullToStable_stillFetchesEarlierPeerBundle() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val deviceA = randomSourceId("device-a")
        val deviceB = randomSourceId("device-b")

        val tokenA = issueDummySigninToken(config.baseUrl, userId, deviceA)
        val tokenB = issueDummySigninToken(config.baseUrl, userId, deviceB)
        val httpA = newAuthenticatedHttpClient(config.baseUrl, tokenA)
        val httpB = newAuthenticatedHttpClient(config.baseUrl, tokenB)
        val dbA = newInMemoryDb()
        val dbB = newInMemoryDb()
        try {
            createBusinessSubsetTables(dbA)
            createBusinessSubsetTables(dbB)

            val clientA = newRealServerClient(dbA, config, httpA)
            val clientB = newRealServerClient(dbB, config, httpB)

            clientA.openAndAttach(userId).getOrThrow()
            clientB.openAndAttach(userId).getOrThrow()

            val firstAuthorId = randomRowId()
            dbA.execSQL(
                """
                INSERT INTO users(id, name, email)
                VALUES('$firstAuthorId', 'From A', 'from-a@example.com')
                """.trimIndent()
            )
            clientA.pushPending().getOrThrow()

            val secondAuthorId = randomRowId()
            dbB.execSQL(
                """
                INSERT INTO users(id, name, email)
                VALUES('$secondAuthorId', 'From B', 'from-b@example.com')
                """.trimIndent()
            )
            clientB.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()

            assertEquals(2L, clientB.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals("From A", scalarText(dbB, "SELECT name FROM users WHERE id = '$firstAuthorId'"))
            assertEquals("From B", scalarText(dbB, "SELECT name FROM users WHERE id = '$secondAuthorId'"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            httpA.close()
            httpB.close()
            dbA.close()
            dbB.close()
        }
    }

    @Test
    fun threeDevices_pushWithoutPull_thenAllPull_converge() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val deviceA = randomSourceId("device-a")
        val deviceB = randomSourceId("device-b")
        val deviceC = randomSourceId("device-c")

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
            createBusinessSubsetTables(dbA)
            createBusinessSubsetTables(dbB)
            createBusinessSubsetTables(dbC)

            val clientA = newRealServerClient(dbA, config, httpA)
            val clientB = newRealServerClient(dbB, config, httpB)
            val clientC = newRealServerClient(dbC, config, httpC)

            clientA.openAndAttach(userId).getOrThrow()
            clientB.openAndAttach(userId).getOrThrow()
            clientC.openAndAttach(userId).getOrThrow()

            val userA = randomRowId()
            dbA.execSQL(
                """
                INSERT INTO users(id, name, email)
                VALUES('$userA', 'From A', 'from-a@example.com')
                """.trimIndent()
            )
            clientA.pushPending().getOrThrow()

            val userB = randomRowId()
            dbB.execSQL(
                """
                INSERT INTO users(id, name, email)
                VALUES('$userB', 'From B', 'from-b@example.com')
                """.trimIndent()
            )
            clientB.pushPending().getOrThrow()

            val userC = randomRowId()
            dbC.execSQL(
                """
                INSERT INTO users(id, name, email)
                VALUES('$userC', 'From C', 'from-c@example.com')
                """.trimIndent()
            )
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
            assertEquals("From A", scalarText(dbB, "SELECT name FROM users WHERE id = '$userA'"))
            assertEquals("From B", scalarText(dbC, "SELECT name FROM users WHERE id = '$userB'"))
            assertEquals("From C", scalarText(dbA, "SELECT name FROM users WHERE id = '$userC'"))
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
    fun restartAfterOwnPushBeforePull_stillFetchesEarlierPeerBundle() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val deviceA = randomSourceId("device-a")
        val deviceB = randomSourceId("device-b")

        val tokenA = issueDummySigninToken(config.baseUrl, userId, deviceA)
        val tokenB = issueDummySigninToken(config.baseUrl, userId, deviceB)
        val httpA = newAuthenticatedHttpClient(config.baseUrl, tokenA)
        val httpB = newAuthenticatedHttpClient(config.baseUrl, tokenB)
        val dbA = newInMemoryDb()
        val dbB = newInMemoryDb()
        try {
            createBusinessSubsetTables(dbA)
            createBusinessSubsetTables(dbB)

            val clientA = newRealServerClient(dbA, config, httpA)
            val clientB = newRealServerClient(dbB, config, httpB)

            clientA.openAndAttach(userId).getOrThrow()
            clientB.openAndAttach(userId).getOrThrow()

            val firstAuthorId = randomRowId()
            dbA.execSQL(
                """
                INSERT INTO users(id, name, email)
                VALUES('$firstAuthorId', 'From A', 'from-a@example.com')
                """.trimIndent()
            )
            clientA.pushPending().getOrThrow()

            val secondAuthorId = randomRowId()
            dbB.execSQL(
                """
                INSERT INTO users(id, name, email)
                VALUES('$secondAuthorId', 'From B', 'from-b@example.com')
                """.trimIndent()
            )
            clientB.pushPending().getOrThrow()

            val restartedClientB = newRealServerClient(dbB, config, httpB)
            restartedClientB.openAndAttach(userId).getOrThrow()
            restartedClientB.pullToStable().getOrThrow()

            assertEquals(2L, restartedClientB.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals("From A", scalarText(dbB, "SELECT name FROM users WHERE id = '$firstAuthorId'"))
            assertEquals("From B", scalarText(dbB, "SELECT name FROM users WHERE id = '$secondAuthorId'"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            httpA.close()
            httpB.close()
            dbA.close()
            dbB.close()
        }
    }

    @Test
    fun hydrate_rebuildsFreshDeviceFromManualNethttpServer() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val seedDevice = randomSourceId("device-seed")
        val restoreDevice = randomSourceId("device-restore")

        val seedToken = issueDummySigninToken(config.baseUrl, userId, seedDevice)
        val restoreToken = issueDummySigninToken(config.baseUrl, userId, restoreDevice)
        val seedHttp = newAuthenticatedHttpClient(config.baseUrl, seedToken)
        val restoreHttp = newAuthenticatedHttpClient(config.baseUrl, restoreToken)
        val seedDb = newInMemoryDb()
        val restoreDb = newInMemoryDb()
        try {
            createBusinessSubsetTables(seedDb)
            createBusinessSubsetTables(restoreDb)

            val seedClient = newRealServerClient(seedDb, config, seedHttp)
            val restoreClient = newRealServerClient(restoreDb, config, restoreHttp)

            seedClient.openAndAttach(userId).getOrThrow()
            restoreClient.openAndAttach(userId).getOrThrow()

            repeat(8) { index ->
                val authorId = randomRowId()
                val postId = randomRowId()
                seedDb.execSQL(
                    """
                    INSERT INTO users(id, name, email)
                    VALUES('$authorId', 'User $index', 'user-$index@example.com')
                    """.trimIndent()
                )
                seedDb.execSQL(
                    """
                    INSERT INTO posts(id, title, content, author_id)
                    VALUES('$postId', 'Post $index', 'payload-$index', '$authorId')
                    """.trimIndent()
                )
            }

            seedClient.pushPending().getOrThrow()
            restoreClient.rebuild().getOrThrow()

            assertEquals(8L, scalarLong(restoreDb, "SELECT COUNT(*) FROM users"))
            assertEquals(8L, scalarLong(restoreDb, "SELECT COUNT(*) FROM posts"))
            assertEquals(0L, scalarLong(restoreDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(restoreDb, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            assertEquals(
                scalarLong(seedDb, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"),
                restoreClient.syncStatus().getOrThrow().lastBundleSeqSeen,
            )
        } finally {
            seedHttp.close()
            restoreHttp.close()
            seedDb.close()
            restoreDb.close()
        }
    }
}
