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
        val deviceA = randomDeviceId("device-a")
        val deviceB = randomDeviceId("device-b")

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

            clientA.bootstrap(userId, deviceA).getOrThrow()
            clientB.bootstrap(userId, deviceB).getOrThrow()

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
    fun hydrate_rebuildsFreshDeviceFromManualNethttpServer() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val seedDevice = randomDeviceId("device-seed")
        val restoreDevice = randomDeviceId("device-restore")

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

            seedClient.bootstrap(userId, seedDevice).getOrThrow()
            restoreClient.bootstrap(userId, restoreDevice).getOrThrow()

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
            restoreClient.hydrate().getOrThrow()

            assertEquals(8L, scalarLong(restoreDb, "SELECT COUNT(*) FROM users"))
            assertEquals(8L, scalarLong(restoreDb, "SELECT COUNT(*) FROM posts"))
            assertEquals(0L, scalarLong(restoreDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(restoreDb, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            assertEquals(
                scalarLong(seedDb, "SELECT last_bundle_seq_seen FROM _sync_client_state WHERE user_id = '$userId'"),
                restoreClient.lastBundleSeqSeen().getOrThrow(),
            )
        } finally {
            seedHttp.close()
            restoreHttp.close()
            seedDb.close()
            restoreDb.close()
        }
    }
}
