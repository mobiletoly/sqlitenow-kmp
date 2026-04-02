package dev.goquick.sqlitenow.oversqlite.e2e

import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class RealServerChunkedPushTransportTest {
    @Test
    fun offlineDirtySetLargerThanOneChunk_pushesAndConverges() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val deviceA = randomSourceId("chunk-a")
        val deviceB = randomSourceId("chunk-b")
        val uploadLimit = 4
        val insertedPairs = 7

        val tokenA = issueDummySigninToken(config.baseUrl, userId, deviceA)
        val tokenB = issueDummySigninToken(config.baseUrl, userId, deviceB)
        val httpA = newAuthenticatedHttpClient(config.baseUrl, tokenA)
        val httpB = newAuthenticatedHttpClient(config.baseUrl, tokenB)
        val dbA = newInMemoryDb()
        val dbB = newInMemoryDb()
        try {
            createBusinessSubsetTables(dbA)
            createBusinessSubsetTables(dbB)

            val clientA = newRealServerClient(
                db = dbA,
                config = config,
                http = httpA,
                uploadLimit = uploadLimit,
                downloadLimit = 2,
            )
            val clientB = newRealServerClient(
                db = dbB,
                config = config,
                http = httpB,
                uploadLimit = uploadLimit,
                downloadLimit = 2,
            )

            clientA.openAndAttach(userId).getOrThrow()
            clientB.openAndAttach(userId).getOrThrow()

            insertUserAndPostBatch(
                db = dbA,
                batchPrefix = "offline-large",
                count = insertedPairs,
                titlePrefix = "Chunked title",
                contentPrefix = "Chunked payload",
            )

            assertEquals(14L, scalarLong(dbA, "SELECT COUNT(*) FROM _sync_dirty_rows"))

            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()

            assertEquals(insertedPairs.toLong(), scalarLong(dbA, "SELECT COUNT(*) FROM users"))
            assertEquals(insertedPairs.toLong(), scalarLong(dbA, "SELECT COUNT(*) FROM posts"))
            assertEquals(insertedPairs.toLong(), scalarLong(dbB, "SELECT COUNT(*) FROM users"))
            assertEquals(insertedPairs.toLong(), scalarLong(dbB, "SELECT COUNT(*) FROM posts"))
            assertEquals(
                1L,
                scalarLong(
                    dbB,
                    "SELECT COUNT(*) FROM posts WHERE title = 'Chunked title offline-large-6' AND content = 'Chunked payload offline-large-6'"
                )
            )
            assertEquals(1L, clientA.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(1L, clientB.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(0L, scalarLong(dbA, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            httpA.close()
            httpB.close()
            dbA.close()
            dbB.close()
        }
    }
}
