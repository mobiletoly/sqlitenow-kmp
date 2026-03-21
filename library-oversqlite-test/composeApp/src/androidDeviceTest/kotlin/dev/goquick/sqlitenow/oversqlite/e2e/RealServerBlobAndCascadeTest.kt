package dev.goquick.sqlitenow.oversqlite.e2e

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.oversqlite.SyncTable
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class RealServerBlobAndCascadeTest {
    @Test
    fun blobRows_pushPullAndHydrateConvergeAcrossDevices() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val seedDevice = randomDeviceId("blob-seed")
        val pullDevice = randomDeviceId("blob-pull")
        val hydrateDevice = randomDeviceId("blob-hydrate")
        val syncTables = listOf(
            SyncTable("files", syncKeyColumnName = "id"),
            SyncTable("file_reviews", syncKeyColumnName = "id"),
        )

        val seedToken = issueDummySigninToken(config.baseUrl, userId, seedDevice)
        val pullToken = issueDummySigninToken(config.baseUrl, userId, pullDevice)
        val hydrateToken = issueDummySigninToken(config.baseUrl, userId, hydrateDevice)
        val seedHttp = newAuthenticatedHttpClient(config.baseUrl, seedToken)
        val pullHttp = newAuthenticatedHttpClient(config.baseUrl, pullToken)
        val hydrateHttp = newAuthenticatedHttpClient(config.baseUrl, hydrateToken)
        val seedDb = newInMemoryDb()
        val pullDb = newInMemoryDb()
        val hydrateDb = newInMemoryDb()
        try {
            createBusinessRichSchemaTables(seedDb)
            createBusinessRichSchemaTables(pullDb)
            createBusinessRichSchemaTables(hydrateDb)

            val seedClient = newRealServerClient(seedDb, config, seedHttp, syncTables = syncTables)
            val pullClient = newRealServerClient(pullDb, config, pullHttp, syncTables = syncTables)
            val hydrateClient = newRealServerClient(hydrateDb, config, hydrateHttp, syncTables = syncTables)

            seedClient.bootstrap(userId, seedDevice).getOrThrow()
            pullClient.bootstrap(userId, pullDevice).getOrThrow()
            hydrateClient.bootstrap(userId, hydrateDevice).getOrThrow()

            insertBlobPair(seedDb, "blob-contract-a")
            insertBlobPair(seedDb, "blob-contract-b")

            seedClient.pushPending().getOrThrow()
            pullClient.pullToStable().getOrThrow()
            hydrateClient.hydrate().getOrThrow()

            assertBlobState(pullDb)
            assertBlobState(hydrateDb)
            assertEquals(0L, scalarLong(pullDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(hydrateDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            seedHttp.close()
            pullHttp.close()
            hydrateHttp.close()
            seedDb.close()
            pullDb.close()
            hydrateDb.close()
        }
    }

    private suspend fun assertBlobState(
        db: dev.goquick.sqlitenow.core.SafeSQLiteConnection,
    ) {
        assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM files"))
        assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM file_reviews"))
        assertEquals(
            1L,
            scalarLong(
                db,
                """
                SELECT COUNT(*)
                FROM files f
                JOIN file_reviews r ON r.file_id = f.id
                WHERE f.name = 'File blob-contract-a'
                  AND r.review = 'Review blob-contract-a'
                  AND length(f.data) > 0
                """.trimIndent(),
            ),
        )
        assertEquals(
            1L,
            scalarLong(
                db,
                """
                SELECT COUNT(*)
                FROM files f
                JOIN file_reviews r ON r.file_id = f.id
                WHERE f.name = 'File blob-contract-b'
                  AND r.review = 'Review blob-contract-b'
                  AND length(f.data) > 0
                """.trimIndent(),
            ),
        )
    }
}
