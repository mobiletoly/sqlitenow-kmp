package dev.goquick.sqlitenow.oversqlite.e2e

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.oversqlite.SyncTable
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class RealServerFkTopologyTest {
    @Test
    fun fkTopology_pullAndHydratePreserveSelfReferencesAndCycles() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val seedDevice = randomSourceId("fk-seed")
        val pullDevice = randomSourceId("fk-pull")
        val hydrateDevice = randomSourceId("fk-hydrate")
        val syncTables = listOf(
            SyncTable("categories", syncKeyColumnName = "id"),
            SyncTable("teams", syncKeyColumnName = "id"),
            SyncTable("team_members", syncKeyColumnName = "id"),
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

            seedClient.openAndAttach(userId).getOrThrow()
            pullClient.openAndAttach(userId).getOrThrow()
            hydrateClient.openAndAttach(userId).getOrThrow()

            insertCategoryGraph(seedDb, "fk-topology")
            insertTeamGraph(seedDb, "fk-topology")

            seedClient.pushPending().getOrThrow()
            pullClient.pullToStable().getOrThrow()
            hydrateClient.rebuild().getOrThrow()

            assertTopologyState(pullDb, "pull")
            assertTopologyState(hydrateDb, "hydrate")
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

    private suspend fun assertTopologyState(
        db: dev.goquick.sqlitenow.core.SafeSQLiteConnection,
        label: String,
    ) {
        assertEquals(3L, scalarLong(db, "SELECT COUNT(*) FROM categories"))
        assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM teams"))
        assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM team_members"))
        assertEquals(
            1L,
            scalarLong(
                db,
                """
                SELECT COUNT(*)
                FROM categories c
                JOIN categories p ON p.id = c.parent_id
                WHERE c.name = 'Category child fk-topology'
                  AND p.name = 'Category root fk-topology'
                """.trimIndent(),
            ),
        )
        assertEquals(
            1L,
            scalarLong(
                db,
                """
                SELECT COUNT(*)
                FROM categories c
                JOIN categories p ON p.id = c.parent_id
                WHERE c.name = 'Category leaf fk-topology'
                  AND p.name = 'Category child fk-topology'
                """.trimIndent(),
            ),
        )
        assertEquals(
            1L,
            scalarLong(
                db,
                """
                SELECT COUNT(*)
                FROM teams t
                JOIN team_members captain ON captain.id = t.captain_member_id
                WHERE t.name = 'Team fk-topology'
                  AND captain.name = 'Captain fk-topology'
                """.trimIndent(),
            ),
        )
        assertEquals(
            1L,
            scalarLong(
                db,
                """
                SELECT COUNT(*)
                FROM team_members m
                JOIN teams t ON t.id = m.team_id
                WHERE m.name = 'Member fk-topology'
                  AND t.name = 'Team fk-topology'
                """.trimIndent(),
            ),
        )
        assertEquals("snapshot stage not cleared on $label", 0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
    }
}
