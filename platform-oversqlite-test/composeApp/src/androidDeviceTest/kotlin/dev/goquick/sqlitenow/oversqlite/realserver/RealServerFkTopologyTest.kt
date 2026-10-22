package dev.goquick.sqlitenow.oversqlite.realserver

import androidx.test.ext.junit.runners.AndroidJUnit4
import kotlinx.serialization.json.*
import dev.goquick.sqlitenow.oversqlite.realserver.runEnabledRealServerTest as runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class RealServerFkTopologyTest {
    private suspend fun frozenKeys(db: RealServerDatabase, table: String): Set<String> {
        require(table == "_sync_dirty_rows" || table == "_sync_outbox_rows")
        val json = scalarText(db, "SELECT json_group_array(table_name || '/' || key_json) FROM $table")
        return Json.parseToJsonElement(json).jsonArray.map { it.jsonPrimitive.content }.toSet()
    }

    private fun assertCyclicMembership(batches: List<Set<String>>, expected: Set<String>, team: String, captain: String) {
        val teamKey = "teams/{\"id\":\"$team\"}"
        val captainKey = "team_members/{\"id\":\"$captain\"}"
        assertTrue(expected.containsAll(setOf(teamKey, captainKey)))
        assertEquals(expected, batches.flatten().toSet())
        assertEquals(expected.size, batches.sumOf { it.size })
        for (batch in batches) {
            assertTrue("a dispatched bundle is nonempty", batch.isNotEmpty())
            assertEquals("only the two-node cycle is indivisible; the other member is independent",
                teamKey in batch, captainKey in batch)
        }
    }

    @Test
    fun byteTargetKeepsNewForeignKeyCycleInOneFrozenBundle() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val writerHttp = newAuthenticatedHttpClient(config.baseUrl,
            issueDummySigninToken(config.baseUrl, userId, randomSourceId("fk-byte-writer")))
        val readerHttp = newAuthenticatedHttpClient(config.baseUrl,
            issueDummySigninToken(config.baseUrl, userId, randomSourceId("fk-byte-reader")))
        val writerDb = newFileBackedDb()
        val readerDb = newFileBackedDb()
        try {
            createBusinessRichSchemaTables(writerDb)
            createBusinessRichSchemaTables(readerDb)
            val writer = newRealServerClient(writerDb, config, writerHttp,
                maxPushRows = 8, maxPushDecodedBytes = 64)
            val reader = newRealServerClient(readerDb, config, readerHttp)
            writer.openAndAttach(userId).getOrThrow()
            reader.openAndAttach(userId).getOrThrow()
            insertTeamGraph(writerDb, "fk-byte-target")

            assertEquals(3L, scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            writer.pushPending().getOrThrow()
            assertEquals(1L, scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            writer.pushPending().getOrThrow()
            reader.pullToStable().getOrThrow()
            assertEquals(0L, scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(1L, scalarLong(readerDb, "SELECT COUNT(*) FROM teams"))
            assertEquals(2L, scalarLong(readerDb, "SELECT COUNT(*) FROM team_members"))
            assertForeignKeyIntegrity(readerDb)
        } finally {
            writerHttp.close()
            readerHttp.close()
            writerDb.close()
            readerDb.close()
        }
    }

    @Test
    fun cyclicDependencyAboveSoftRowTargetPushesCompleteGroup() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val token = issueDummySigninToken(config.baseUrl, userId, randomSourceId("fk-too-small"))
        val http = newAuthenticatedHttpClient(config.baseUrl, token)
        val db = newFileBackedDb()
        try {
            createBusinessRichSchemaTables(db)
            val client = newRealServerClient(db, config, http, maxPushRows = 1)
            client.openAndAttach(userId).getOrThrow()
            insertTeamGraph(db, "fk-too-small")

            repeat(3) {
                if (scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows") > 0L) {
                    client.pushPending().getOrThrow()
                }
            }
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            http.close()
            db.close()
        }
    }

    @Test
    fun cyclicDeletesStayTogetherAcrossSoftTargetsAgainstServer() = runBlocking {
        val config = requireRealServerConfig()
        for (target in listOf("rows", "bytes")) {
            resetRealServerState(config.baseUrl)
            val userId = randomUserId()
            val writerDb = newFileBackedDb()
            val readerDb = newFileBackedDb()
            val batches = mutableListOf<Set<String>>()
            val writerHttp = newAuthenticatedHttpClient(config.baseUrl,
                issueDummySigninToken(config.baseUrl, userId, randomSourceId("fk-delete-writer")), beforeSend = { path ->
                if (path == "/sync/push") batches += frozenKeys(writerDb, "_sync_outbox_rows")
            })
            val readerHttp = newAuthenticatedHttpClient(config.baseUrl,
                issueDummySigninToken(config.baseUrl, userId, randomSourceId("fk-delete-reader")))
            try {
                createBusinessRichSchemaTables(writerDb)
                createBusinessRichSchemaTables(readerDb)
                val writer = newRealServerClient(writerDb, config, writerHttp,
                    maxPushRows = if (target == "rows") 1 else 8,
                    maxPushDecodedBytes = if (target == "bytes") 64 else 1_000_000)
                val reader = newRealServerClient(readerDb, config, readerHttp)
                writer.openAndAttach(userId).getOrThrow()
                reader.openAndAttach(userId).getOrThrow()
                insertTeamGraph(writerDb, "fk-delete-$target")
                repeat(3) {
                    if (scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_dirty_rows") > 0L) {
                        writer.pushPending().getOrThrow()
                    }
                }
                assertEquals(0L, scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))

                val teamId = scalarText(writerDb, "SELECT id FROM teams")
                val captainId = scalarText(writerDb, "SELECT captain_member_id FROM teams")
                batches.clear()
                writerDb.execSQL("BEGIN")
                writerDb.execSQL("DELETE FROM teams")
                writerDb.execSQL("DELETE FROM team_members")
                writerDb.execSQL("COMMIT")
                assertEquals(3L, scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_dirty_rows WHERE op='DELETE'"))
                assertEquals(0L, scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_outbox_rows"))
                val expected = frozenKeys(writerDb, "_sync_dirty_rows")
                val sequenceBefore = scalarLong(writerDb, "SELECT next_source_bundle_id FROM _sync_source_state")
                repeat(3) {
                    if (scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_dirty_rows") > 0L) writer.pushPending().getOrThrow()
                }
                assertEquals(0L, scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
                assertEquals(0L, scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_outbox_rows"))
                assertCyclicMembership(batches, expected, teamId, captainId)
                assertEquals(sequenceBefore + batches.size, scalarLong(writerDb, "SELECT next_source_bundle_id FROM _sync_source_state"))
                reader.pullToStable().getOrThrow()
                assertEquals(0L, scalarLong(readerDb, "SELECT COUNT(*) FROM teams"))
                assertEquals(0L, scalarLong(readerDb, "SELECT COUNT(*) FROM team_members"))
                assertForeignKeyIntegrity(readerDb)
            } finally {
                writerHttp.close()
                readerHttp.close()
                writerDb.close()
                readerDb.close()
            }
        }
    }

    @Test
    fun updateThenDeleteUsesServerBaseFkAgainstServer() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)
        val userId = randomUserId()
        val writerDb = newFileBackedDb()
        val readerDb = newFileBackedDb()
        val batches = mutableListOf<Set<String>>()
        val writerHttp = newAuthenticatedHttpClient(config.baseUrl,
            issueDummySigninToken(config.baseUrl, userId, randomSourceId("fk-base-writer")), beforeSend = { path ->
                if (path == "/sync/push") batches += frozenKeys(writerDb, "_sync_outbox_rows")
            })
        val readerHttp = newAuthenticatedHttpClient(config.baseUrl,
            issueDummySigninToken(config.baseUrl, userId, randomSourceId("fk-base-reader")))
        try {
            createBusinessRichSchemaTables(writerDb)
            createBusinessRichSchemaTables(readerDb)
            val writer = newRealServerClient(writerDb, config, writerHttp, maxPushRows = 1)
            val reader = newRealServerClient(readerDb, config, readerHttp)
            writer.openAndAttach(userId).getOrThrow()
            reader.openAndAttach(userId).getOrThrow()
            insertTeamGraph(writerDb, "fk-base")
            repeat(3) {
                if (scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_dirty_rows") > 0L) writer.pushPending().getOrThrow()
            }
            reader.pullToStable().getOrThrow()
            val teamId = scalarText(writerDb, "SELECT id FROM teams LIMIT 1")
            val captainId = scalarText(writerDb, "SELECT captain_member_id FROM teams LIMIT 1")
            writerDb.execSQL("UPDATE team_members SET name='Changed' WHERE id='$captainId'")
            writerDb.execSQL("UPDATE teams SET captain_member_id=NULL WHERE id='$teamId'")
            writerDb.execSQL("DELETE FROM team_members WHERE id='$captainId'")
            writerDb.execSQL("DELETE FROM teams WHERE id='$teamId'")
            assertEquals(captainId, scalarText(writerDb,
                """SELECT json_extract(base_payload,'$.captain_member_id.value') FROM _sync_dirty_rows WHERE table_name='teams' AND key_json='{"id":"$teamId"}'"""))
            val expected = frozenKeys(writerDb, "_sync_dirty_rows")
            assertEquals("the team and both original members remain delete intents", 3, expected.size)
            batches.clear()
            val sequenceBefore = scalarLong(writerDb, "SELECT next_source_bundle_id FROM _sync_source_state")
            repeat(3) {
                if (scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_dirty_rows") > 0L) writer.pushPending().getOrThrow()
            }
            assertEquals(0L, scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertCyclicMembership(batches, expected, teamId, captainId)
            assertEquals(sequenceBefore + batches.size, scalarLong(writerDb, "SELECT next_source_bundle_id FROM _sync_source_state"))
            reader.pullToStable().getOrThrow()
            assertEquals(0L, scalarLong(readerDb, "SELECT COUNT(*) FROM teams"))
            assertEquals(0L, scalarLong(readerDb, "SELECT COUNT(*) FROM team_members"))
            assertForeignKeyIntegrity(readerDb)
        } finally {
            writerHttp.close()
            readerHttp.close()
            writerDb.close()
            readerDb.close()
        }
    }

    @Test
    fun boundedPushKeepsNewForeignKeyCycleInOneBundle() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val writerToken = issueDummySigninToken(config.baseUrl, userId, randomSourceId("fk-bounded-writer"))
        val readerToken = issueDummySigninToken(config.baseUrl, userId, randomSourceId("fk-bounded-reader"))
        val writerDb = newFileBackedDb()
        val readerDb = newFileBackedDb()
        val batches = mutableListOf<Set<String>>()
        val writerHttp = newAuthenticatedHttpClient(config.baseUrl, writerToken,
            beforeSend = { path -> if (path == "/sync/push") batches += frozenKeys(writerDb, "_sync_outbox_rows") })
        val readerHttp = newAuthenticatedHttpClient(config.baseUrl, readerToken)
        try {
            createBusinessRichSchemaTables(writerDb)
            createBusinessRichSchemaTables(readerDb)
            val writer = newRealServerClient(writerDb, config, writerHttp, maxPushRows = 8)
            val reader = newRealServerClient(readerDb, config, readerHttp, maxPushRows = 8)
            writer.openAndAttach(userId).getOrThrow()
            reader.openAndAttach(userId).getOrThrow()

            val ids = insertHotGraph(writerDb)
            assertEquals(10L, scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            val expected = frozenKeys(writerDb, "_sync_dirty_rows")
            writer.pushPending().getOrThrow()
            assertTrue("one bounded push leaves a second batch", scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_dirty_rows") in 1L..9L)
            assertTrue("independent work respects the row target", batches.single().size <= 8)
            writer.pushPending().getOrThrow()
            assertEquals(0L, scalarLong(writerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))

            assertCyclicMembership(batches, expected, ids.teamId, ids.captainId)
            assertEquals(2, batches.size)
            reader.pullToStable().getOrThrow()
            assertEquals(2L, reader.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertHotGraphDrivenCounts(readerDb, rounds = 0)
            assertForeignKeyIntegrity(readerDb)
        } finally {
            writerHttp.close()
            readerHttp.close()
            writerDb.close()
            readerDb.close()
        }
    }

    @Test
    fun fkTopology_pullAndHydratePreserveSelfReferencesAndCycles() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val seedDevice = randomSourceId("fk-seed")
        val pullDevice = randomSourceId("fk-pull")
        val hydrateDevice = randomSourceId("fk-hydrate")
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
            createBusinessRichSchemaTables(seedDb)
            createBusinessRichSchemaTables(pullDb)
            createBusinessRichSchemaTables(hydrateDb)

            val seedClient = newRealServerClient(seedDb, config, seedHttp)
            val pullClient = newRealServerClient(pullDb, config, pullHttp)
            val hydrateClient = newRealServerClient(hydrateDb, config, hydrateHttp)

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
        db: RealServerDatabase,
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
