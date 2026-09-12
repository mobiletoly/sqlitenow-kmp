package dev.goquick.sqlitenow.oversqlite.realserver

import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class RealServerDirectPushSelectionTest {
    @Test
    fun offlineDirtySetLargerThanOneSelection_pushesAndConverges() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val deviceA = randomSourceId("selection-a")
        val deviceB = randomSourceId("selection-b")
        val maxPushRows = 4
        val insertedPairs = 7

        val tokenA = issueDummySigninToken(config.baseUrl, userId, deviceA)
        val tokenB = issueDummySigninToken(config.baseUrl, userId, deviceB)
        val httpA = newAuthenticatedHttpClient(config.baseUrl, tokenA)
        val httpB = newAuthenticatedHttpClient(config.baseUrl, tokenB)
        val dbA = newFileBackedDb()
        val dbB = newFileBackedDb()
        try {
            createBusinessSubsetTables(dbA)
            createBusinessSubsetTables(dbB)

            val clientA = newRealServerClient(
                db = dbA,
                config = config,
                http = httpA,
                maxPushRows = maxPushRows,
                maxPullBundles = 2,
            )
            val clientB = newRealServerClient(
                db = dbB,
                config = config,
                http = httpB,
                maxPushRows = maxPushRows,
                maxPullBundles = 2,
            )

            clientA.openAndAttach(userId).getOrThrow()
            clientB.openAndAttach(userId).getOrThrow()

            insertUserAndPostBatch(
                db = dbA,
                batchPrefix = "offline-large",
                count = insertedPairs,
                titlePrefix = "Selected title",
                contentPrefix = "Selected payload",
            )

            assertEquals(14L, scalarLong(dbA, "SELECT COUNT(*) FROM _sync_dirty_rows"))

            repeat(4) { selection ->
                clientA.pushPending().getOrThrow()
                assertEquals((14 - (selection + 1) * maxPushRows).coerceAtLeast(0).toLong(),
                    scalarLong(dbA, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            }
            clientB.pullToStable().getOrThrow()

            assertEquals(insertedPairs.toLong(), scalarLong(dbA, "SELECT COUNT(*) FROM users"))
            assertEquals(insertedPairs.toLong(), scalarLong(dbA, "SELECT COUNT(*) FROM posts"))
            assertEquals(insertedPairs.toLong(), scalarLong(dbB, "SELECT COUNT(*) FROM users"))
            assertEquals(insertedPairs.toLong(), scalarLong(dbB, "SELECT COUNT(*) FROM posts"))
            assertEquals(
                1L,
                scalarLong(
                    dbB,
                    "SELECT COUNT(*) FROM posts WHERE title = 'Selected title offline-large-6' AND content = 'Selected payload offline-large-6'"
                )
            )
            assertEquals(4L, clientA.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(4L, clientB.syncStatus().getOrThrow().lastBundleSeqSeen)
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
