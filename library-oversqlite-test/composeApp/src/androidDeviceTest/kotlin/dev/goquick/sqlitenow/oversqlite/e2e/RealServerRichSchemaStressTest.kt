package dev.goquick.sqlitenow.oversqlite.e2e

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.oversqlite.SyncTable
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class RealServerRichSchemaStressTest {
    @Test
    fun richSchema_largeVolume_twoDevicesConvergeAcrossMultipleRoundtrips() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val deviceA = randomSourceId("rich-a")
        val deviceB = randomSourceId("rich-b")
        val syncTables = listOf(
            SyncTable("users", syncKeyColumnName = "id"),
            SyncTable("posts", syncKeyColumnName = "id"),
            SyncTable("categories", syncKeyColumnName = "id"),
            SyncTable("teams", syncKeyColumnName = "id"),
            SyncTable("team_members", syncKeyColumnName = "id"),
            SyncTable("files", syncKeyColumnName = "id"),
            SyncTable("file_reviews", syncKeyColumnName = "id"),
        )

        val tokenA = issueDummySigninToken(config.baseUrl, userId, deviceA)
        val tokenB = issueDummySigninToken(config.baseUrl, userId, deviceB)
        val httpA = newAuthenticatedHttpClient(config.baseUrl, tokenA)
        val httpB = newAuthenticatedHttpClient(config.baseUrl, tokenB)
        val dbA = newInMemoryDb()
        val dbB = newInMemoryDb()
        try {
            createBusinessRichSchemaTables(dbA)
            createBusinessRichSchemaTables(dbB)

            val clientA = newRealServerClient(
                db = dbA,
                config = config,
                http = httpA,
                syncTables = syncTables,
                uploadLimit = 20,
                downloadLimit = 2,
            )
            val clientB = newRealServerClient(
                db = dbB,
                config = config,
                http = httpB,
                syncTables = syncTables,
                uploadLimit = 20,
                downloadLimit = 2,
            )

            clientA.openAndAttach(userId, deviceA).getOrThrow()
            clientB.openAndAttach(userId, deviceB).getOrThrow()

            repeat(6) { batch ->
                insertRichSchemaBatch(dbA, "seed-a-$batch")
                clientA.pushPending().getOrThrow()
            }

            clientB.pullToStable().getOrThrow()

            assertRichSchemaCounts(dbB, expectedBundles = 6)
            assertEquals(6L, clientB.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(
                1L,
                scalarLong(
                    dbB,
                    "SELECT COUNT(*) FROM categories c JOIN categories p ON p.id = c.parent_id WHERE c.name = 'Category leaf seed-a-5' AND p.name = 'Category child seed-a-5'"
                )
            )
            assertEquals(
                1L,
                scalarLong(
                    dbB,
                    "SELECT COUNT(*) FROM teams t JOIN team_members m ON m.id = t.captain_member_id WHERE t.name = 'Team seed-a-5' AND m.name = 'Captain seed-a-5'"
                )
            )
            assertEquals(
                1L,
                scalarLong(
                    dbB,
                    "SELECT COUNT(*) FROM files f JOIN file_reviews r ON r.file_id = f.id WHERE f.name = 'File B seed-a-5' AND r.review = 'Review B seed-a-5'"
                )
            )
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))

            repeat(4) { batch ->
                insertRichSchemaBatch(dbB, "seed-b-$batch")
                clientB.pushPending().getOrThrow()
            }

            clientA.pullToStable().getOrThrow()

            assertRichSchemaCounts(dbA, expectedBundles = 10)
            assertRichSchemaCounts(dbB, expectedBundles = 10)
            assertEquals(10L, clientA.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(10L, clientB.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(
                1L,
                scalarLong(
                    dbA,
                    "SELECT COUNT(*) FROM categories c JOIN categories p ON p.id = c.parent_id WHERE c.name = 'Category leaf seed-b-3' AND p.name = 'Category child seed-b-3'"
                )
            )
            assertEquals(
                1L,
                scalarLong(
                    dbA,
                    "SELECT COUNT(*) FROM teams t JOIN team_members m ON m.id = t.captain_member_id WHERE t.name = 'Team seed-b-3' AND m.name = 'Captain seed-b-3'"
                )
            )
            assertEquals(
                1L,
                scalarLong(
                    dbA,
                    "SELECT COUNT(*) FROM file_reviews r JOIN files f ON f.id = r.file_id WHERE f.name = 'File A seed-b-3' AND r.review = 'Review A seed-b-3' AND length(f.data) = 16"
                )
            )
            assertEquals(0L, scalarLong(dbA, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            httpA.close()
            httpB.close()
            dbA.close()
            dbB.close()
        }
    }

    private suspend fun assertRichSchemaCounts(db: dev.goquick.sqlitenow.core.SafeSQLiteConnection, expectedBundles: Int) {
        val bundles = expectedBundles.toLong()
        assertEquals(2L * bundles, scalarLong(db, "SELECT COUNT(*) FROM users"))
        assertEquals(2L * bundles, scalarLong(db, "SELECT COUNT(*) FROM posts"))
        assertEquals(3L * bundles, scalarLong(db, "SELECT COUNT(*) FROM categories"))
        assertEquals(1L * bundles, scalarLong(db, "SELECT COUNT(*) FROM teams"))
        assertEquals(2L * bundles, scalarLong(db, "SELECT COUNT(*) FROM team_members"))
        assertEquals(2L * bundles, scalarLong(db, "SELECT COUNT(*) FROM files"))
        assertEquals(2L * bundles, scalarLong(db, "SELECT COUNT(*) FROM file_reviews"))
    }
}
