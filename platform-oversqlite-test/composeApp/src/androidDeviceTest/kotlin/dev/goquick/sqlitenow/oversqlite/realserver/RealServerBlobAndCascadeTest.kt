package dev.goquick.sqlitenow.oversqlite.realserver

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
        val seedDevice = randomSourceId("blob-seed")
        val pullDevice = randomSourceId("blob-pull")
        val hydrateDevice = randomSourceId("blob-hydrate")
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
        val seedDb = newFileBackedDb()
        val pullDb = newFileBackedDb()
        val hydrateDb = newFileBackedDb()
        try {
            createBusinessBlobKeyTables(seedDb)
            createBusinessBlobKeyTables(pullDb)
            createBusinessBlobKeyTables(hydrateDb)

            val seedClient = newRealServerClient(seedDb, config, seedHttp, syncTables = syncTables)
            val pullClient = newRealServerClient(pullDb, config, pullHttp, syncTables = syncTables)
            val hydrateClient = newRealServerClient(hydrateDb, config, hydrateHttp, syncTables = syncTables)

            seedClient.openAndAttach(userId).getOrThrow()
            pullClient.openAndAttach(userId).getOrThrow()
            hydrateClient.openAndAttach(userId).getOrThrow()

            val blobA = insertBlobPair(seedDb, "blob-contract-a")
            val blobB = insertBlobPair(seedDb, "blob-contract-b")

            seedClient.pushPending().getOrThrow()
            pullClient.pullToStable().getOrThrow()
            hydrateClient.rebuild().getOrThrow()

            assertBlobState(pullDb, blobA, blobB)
            assertBlobState(hydrateDb, blobA, blobB)
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
        vararg rows: BlobPairRow,
    ) {
        assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM files"))
        assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM file_reviews"))
        for (row in rows) {
            val fileHex = uuidTextToBlobHex(row.fileId)
            val reviewHex = uuidTextToBlobHex(row.reviewId)
            assertEquals("File ${row.label}", scalarText(db, "SELECT name FROM files WHERE lower(hex(id)) = '$fileHex'"))
            assertEquals(16L, scalarLong(db, "SELECT length(data) FROM files WHERE lower(hex(id)) = '$fileHex'"))
            assertEquals(row.dataHex.uppercase(), scalarText(db, "SELECT hex(data) FROM files WHERE lower(hex(id)) = '$fileHex'"))
            assertEquals("Review ${row.label}", scalarText(db, "SELECT review FROM file_reviews WHERE lower(hex(id)) = '$reviewHex'"))
            assertEquals(fileHex, scalarText(db, "SELECT lower(hex(file_id)) FROM file_reviews WHERE lower(hex(id)) = '$reviewHex'"))
        }
    }
}
