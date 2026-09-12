package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.serialization.json.Json
import kotlinx.coroutines.CancellationException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

internal class SurvivorApplyTest : SurvivorTestSupport() {
    @Test
    fun survivorApplyRetainsDeleteFirstUniqueOrdering() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, _, client ->
            db.execSQL("CREATE UNIQUE INDEX nodes_name ON nodes(name)")
            val normalized = effects().toMutableList()
            normalized[1] = normalized[1].copy(payload = Json.parseToJsonElement(
                """{"id":"C","name":"parent","parent_id":"Q"}""",
            ) as kotlinx.serialization.json.JsonObject)
            server.addRemoteBundleForTest(normalized)
            client.pullToStable().getOrThrow()
            assertEquals("parent", scalarText(db, "SELECT name FROM nodes WHERE id='C'"))
            assertEquals("Q", scalarText(db, "SELECT parent_id FROM nodes WHERE id='C'"))
            assertEquals("grandchild", scalarText(db, "SELECT name FROM nodes WHERE id='D'"))
            assertEquals("C", scalarText(db, "SELECT parent_id FROM nodes WHERE id='D'"))
            assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        }
    }

    @Test
    fun pullPreservesCleanGrandchild() = runOwnedWebDatabaseTest { survivorCase(settlement = false) }

    @Test
    fun settlementPreservesCleanGrandchild() = runOwnedWebDatabaseTest { survivorCase(settlement = true) }

    @Test
    fun resumesDiscoveryAcrossReopen() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, http, seed ->
            db.execSQL("UPDATE _sync_apply_state SET apply_mode=1")
            repeat(250) { db.execSQL("INSERT INTO nodes VALUES('Z$it','unrelated',NULL)") }
            db.execSQL("UPDATE _sync_apply_state SET apply_mode=0")
            seed.closeFromCoordinatedOwner()
            server.addRemoteBundleForTest(effects())
            var interruptions = 0
            var completed = false
            repeat(12) {
                if (!completed) {
                    val client = newClient(db, http, syncTables = nodeTables, workUnits = 1800)
                    client.openAndConnect("user-1").getOrThrow()
                    val result = client.pullToStable()
                    if (result.isSuccess) completed = true else {
                        assertIs<SyncWorkLimitException>(result.exceptionOrNull())
                        interruptions++
                        assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                        assertEquals(3L, scalarLong(db, "SELECT count(*) FROM nodes WHERE id IN ('P','C','D')"))
                        assertEquals(0L, scalarLong(db, "SELECT apply_mode FROM _sync_apply_state"))
                        assertTrue(scalarLong(db, "SELECT count(*) FROM _sync_apply_rows") > 0)
                    }
                    client.closeFromCoordinatedOwner()
                }
            }
            assertTrue(completed)
            assertTrue(interruptions > 1)
            requireSurvivors(db)
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_apply_plan"))
        }
    }

    @Test
    fun capacityFailurePreservesState() = runOwnedWebDatabaseTest {
        for (dimension in listOf("entries", "bytes", "atomic_apply_work")) {
            withSurvivorFixture { server, db, http, seed ->
                seed.closeFromCoordinatedOwner()
                val client = newClient(db, http, syncTables = nodeTables,
                    workspaceEntries = if (dimension == "entries") 2 else 100000,
                    workspaceBytes = if (dimension == "bytes") 600 else 256L * 1024 * 1024,
                    workUnits = if (dimension == "atomic_apply_work") 100 else 1000000)
                client.openAndConnect("user-1").getOrThrow()
                server.addRemoteBundleForTest(effects())
                var capacity: SyncWorkspaceLimitException? = null
                repeat(8) {
                    if (capacity == null) {
                        val failure = client.pullToStable().exceptionOrNull()
                        if (failure !is SyncWorkLimitException) capacity = assertIs<SyncWorkspaceLimitException>(failure)
                    }
                }
                assertEquals(dimension, capacity?.dimension)
                assertEquals(4L, scalarLong(db, "SELECT count(*) FROM nodes"))
                assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                assertEquals(0L, scalarLong(db, "SELECT apply_mode FROM _sync_apply_state"))
                client.closeFromCoordinatedOwner()
            }
        }
    }

    @Test
    fun settlementPreservesDirtyGrandchild() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, _, client ->
            db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
            server.normalizePushEffects = { effects() }
            server.beforePushResponse = { db.execSQL("UPDATE nodes SET name='local grandchild' WHERE id='D'") }
            client.pushPending().getOrThrow()
            assertEquals("local grandchild", scalarText(db, "SELECT name FROM nodes WHERE id='D'"))
            assertEquals("C", scalarText(db, "SELECT parent_id FROM nodes WHERE id='D'"))
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertTrue(scalarText(db, "SELECT payload FROM _sync_dirty_rows").contains("local grandchild"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
        }
    }


    @Test
    fun revisionMismatchRestartsSavedDiscovery() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, http, seed ->
            db.execSQL("UPDATE _sync_apply_state SET apply_mode=1")
            repeat(250) { db.execSQL("INSERT INTO nodes VALUES('Z$it','unrelated',NULL)") }
            db.execSQL("UPDATE _sync_apply_state SET apply_mode=0")
            seed.closeFromCoordinatedOwner()
            val client = newClient(db, http, syncTables = nodeTables, workUnits = 1800)
            client.openAndConnect("user-1").getOrThrow()
            server.addRemoteBundleForTest(effects())
            assertIs<SyncWorkLimitException>(client.pullToStable().exceptionOrNull())
            val before = scalarText(db, "SELECT guard FROM _sync_apply_plan")
            db.execSQL("UPDATE _sync_conflict_state SET dirty_revision=dirty_revision+1")
            assertIs<SyncWorkLimitException>(client.pullToStable().exceptionOrNull())
            assertTrue(before != scalarText(db, "SELECT guard FROM _sync_apply_plan"))
            assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            client.closeFromCoordinatedOwner()
            val restarted = newClient(db, http, syncTables = nodeTables)
            restarted.openAndConnect("user-1").getOrThrow()
            restarted.pullToStable().getOrThrow()
            requireSurvivors(db)
            restarted.closeFromCoordinatedOwner()
        }
    }

    @Test
    fun entryLimitBoundary() = runOwnedWebDatabaseTest {
        for (entries in listOf(44L, 45L, 46L)) withSurvivorFixture { server, db, http, seed ->
            seed.closeFromCoordinatedOwner()
            val client = newClient(db, http, syncTables = nodeTables, workspaceEntries = entries)
            client.openAndConnect("user-1").getOrThrow()
            server.addRemoteBundleForTest(effects())
            val result = client.pullToStable()
            if (entries == 44L) {
                val limit = assertIs<SyncWorkspaceLimitException>(result.exceptionOrNull())
                assertEquals("entries", limit.dimension)
                // 32 context slots + 4 registration + 8 FK + singleton.
                assertEquals(45L, limit.used + limit.reserved)
            } else {
                result.getOrThrow()
                requireSurvivors(db)
            }
            client.closeFromCoordinatedOwner()
        }
    }

    @Test
    fun oldLayoutAndLimitsBelowRetainedUseFailBeforeMutation() = runOwnedWebDatabaseTest {
        for (oldLayout in listOf(false, true)) withSurvivorFixture { _, db, http, client ->
            client.closeFromCoordinatedOwner()
            if (oldLayout) db.execSQL("DROP TABLE _sync_apply_plan")
            else db.execSQL("INSERT INTO _sync_apply_plan(singleton_key,unit,guard,phase,root_cursor,visit_cursor,table_cursor,fk_cursor,row_cursor,entries,bytes) VALUES(1,'unit','guard','walk',0,0,0,0,NULL,2,64)")
            val reopened = newClient(db, http, syncTables = nodeTables, workspaceEntries = 1)
            assertTrue(reopened.open().isFailure)
            assertEquals(4L, scalarLong(db, "SELECT count(*) FROM nodes"))
            assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            if (oldLayout) assertEquals(0L, scalarLong(db, "SELECT count(*) FROM sqlite_schema WHERE name='_sync_apply_plan'"))
            reopened.closeFromCoordinatedOwner()
        }
    }

    @Test
    fun restoresCleanSurvivorAfterSetNull() = runOwnedWebDatabaseTest {
        withSurvivorFixture(action = "SET NULL") { server, db, _, client ->
            db.execSQL("CREATE TRIGGER delete_c AFTER DELETE ON nodes WHEN OLD.id='P' BEGIN DELETE FROM nodes WHERE id='C'; END")
            server.addRemoteBundleForTest(effects())
            client.pullToStable().getOrThrow()
            requireSurvivors(db)
        }
    }

    @Test
    fun ordinaryRequestCountsStaySmall() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, _, client ->
            suspend fun logState(label: String) {
                println("C07 $label complete local state: ${baselineQualificationVector(db, listOf("nodes")).tables}")
                println("C07 $label remote state: ${server.remoteRowsForBaselineTest()}")
                println("C07 $label frozen requests: ${server.pushBodies}")
            }
            val beforePush = server.pushRequestCount
            val beforePull = server.pullRequestCount
            val unchanged = baselineQualificationVector(db, listOf("nodes"))
            logState("before no-change")
            client.sync().getOrThrow()
            assertEquals(beforePush, server.pushRequestCount)
            assertEquals(beforePull + 1, server.pullRequestCount)
            assertEquals(unchanged, baselineQualificationVector(db, listOf("nodes")))
            println("C07 no-change public sync: GET /sync/pull count=1; POST /sync/push count=0")
            logState("after no-change")
            db.execSQL("UPDATE nodes SET name='single edit' WHERE id='D'")
            logState("before one-row")
            client.sync().getOrThrow()
            assertEquals(beforePush + 1, server.pushRequestCount)
            assertEquals(beforePull + 2, server.pullRequestCount)
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            println("C07 one-row public sync: POST /sync/push count=1; GET /sync/pull count=1")
            logState("after one-row")
        }
    }

    @Test
    fun rollbackAndCancellationKeepWholeBundlePending() = runOwnedWebDatabaseTest {
        for (cancel in listOf(false, true)) withSurvivorFixture { server, db, _, client ->
            server.addRemoteBundleForTest(effects())
            client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(beforeCommit = {
                if (cancel) throw CancellationException("survivor apply cancellation")
                error("survivor apply rollback")
            }))
            if (cancel) {
                val failure = assertFailsWith<CancellationException> { client.pullToStable() }
                assertEquals("survivor apply cancellation", failure.message)
            } else {
                val failure = assertIs<IllegalStateException>(client.pullToStable().exceptionOrNull())
                assertEquals("survivor apply rollback", failure.message)
            }
            assertEquals(4L, scalarLong(db, "SELECT count(*) FROM nodes"))
            assertEquals("P", scalarText(db, "SELECT parent_id FROM nodes WHERE id='C'"))
            assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals(0L, scalarLong(db, "SELECT apply_mode FROM _sync_apply_state"))
            client.setSnapshotApplyFaultInjectorForTest(null)
            client.pullToStable().getOrThrow()
            requireSurvivors(db)
        }
    }

    @Test
    fun restoresVersionSkippedSurvivorAfterApplicationTriggerCascade() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, _, client ->
            db.execSQL("UPDATE _sync_apply_state SET apply_mode=1")
            db.execSQL("UPDATE nodes SET name='newer grandchild' WHERE id='D'")
            db.execSQL("UPDATE _sync_row_state SET row_version=3 WHERE key_json='{" + "\"id\":\"D\"}'")
            db.execSQL("UPDATE _sync_apply_state SET apply_mode=0")
            db.execSQL("CREATE TRIGGER delete_c AFTER DELETE ON nodes WHEN OLD.id='P' BEGIN DELETE FROM nodes WHERE id='C'; END")
            server.addRemoteBundleForTest(effects() + BundleRow("main", "nodes", mapOf("id" to "D"), "UPDATE", 2,
                Json.parseToJsonElement("""{"id":"D","name":"older","parent_id":"C"}""")))
            client.pullToStable().getOrThrow()
            assertEquals("newer grandchild", scalarText(db, "SELECT name FROM nodes WHERE id='D'"))
            assertEquals("C", scalarText(db, "SELECT parent_id FROM nodes WHERE id='D'"))
            assertEquals(3L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE key_json='{" + "\"id\":\"D\"}'"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
        }
    }


    @Test
    fun triggerDeletesAlreadyReparentedSurvivor() = runOwnedWebDatabaseTest {
        for (settlement in listOf(false, true)) withSurvivorFixture { server, db, _, client ->
            db.execSQL("CREATE TRIGGER delete_c AFTER DELETE ON nodes WHEN OLD.id='P' BEGIN DELETE FROM nodes WHERE id='C'; END")
            if (settlement) {
                db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
                server.normalizePushEffects = { effects() }
                server.beforePushResponse = { db.execSQL("UPDATE nodes SET parent_id='Q' WHERE id='C'") }
                client.pushPending().getOrThrow()
                assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            } else {
                db.execSQL("UPDATE _sync_apply_state SET apply_mode=1")
                db.execSQL("UPDATE nodes SET parent_id='Q' WHERE id='C'")
                db.execSQL("UPDATE _sync_row_state SET row_version=3 WHERE json_extract(key_json,'$.id')='C'")
                db.execSQL("UPDATE _sync_apply_state SET apply_mode=0")
                server.addRemoteBundleForTest(effects())
                client.pullToStable().getOrThrow()
                assertEquals(3L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE json_extract(key_json,'$.id')='C'"))
            }
            requireSurvivors(db)
            assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        }
    }

    @Test
    fun discoveryDoesNotRescanChildrenPerParent() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, http, seed ->
            val rows = effects().map { it.copy(rowVersion = 3) }.toMutableList()
            for (i in 1 until 8) {
                db.execSQL("INSERT INTO nodes VALUES('P$i','parent',NULL),('C$i','child','P$i'),('D$i','grandchild','C$i')")
                rows += BundleRow("main", "nodes", mapOf("id" to "P$i"), "DELETE", 3, null)
                rows += BundleRow("main", "nodes", mapOf("id" to "C$i"), "UPDATE", 3,
                    Json.parseToJsonElement("""{"id":"C$i","name":"child","parent_id":"Q"}"""))
            }
            seed.pushPending().getOrThrow()
            db.execSQL("UPDATE _sync_apply_state SET apply_mode=1")
            repeat(500) { db.execSQL("INSERT INTO nodes VALUES('Z$it','unrelated',NULL)") }
            db.execSQL("UPDATE _sync_apply_state SET apply_mode=0")
            seed.closeFromCoordinatedOwner()
            val client = newClient(db, http, syncTables = nodeTables, workUnits = 50000)
            client.openAndConnect("user-1").getOrThrow()
            server.addRemoteBundleForTest(rows)
            client.pullToStable().getOrThrow()
            requireSurvivors(db)
            assertEquals(8L, scalarLong(db, "SELECT count(*) FROM nodes WHERE name='grandchild'"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_apply_edges"))
            client.closeFromCoordinatedOwner()
        }
    }

    @Test
    fun reorderedRegistrationInvalidatesTraversal() = runOwnedWebDatabaseTest {
        val names = listOf("parents", "children", "grandchildren")
        val tables = names.map { SyncTable(it, syncKeyColumnName = "id") }
        val server = MockSyncServer()
        val generation = names.mapIndexed { index, name ->
            GenerationTable("main", name, listOf("id"), listOf(
                GenerationColumn("id", "text", false), GenerationColumn("name", "text", false),
            ) + if (index == 0) emptyList() else listOf(GenerationColumn("parent_id", "text", true)),
                if (index == 0) emptyList() else listOf(GenerationForeignKey(
                    listOf("parent_id"), "main", names[index - 1], listOf("id"), "cascade", "no_action")))
        }
        server.generationOverride = SyncGeneration("1", "", generation).let { it.copy(fingerprint = it.computeFingerprint()) }
        val db = newDb()
        val http = server.newHttpClient()
        try {
            db.execSQL("CREATE TABLE parents(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)")
            for (i in 1..2) db.execSQL("CREATE TABLE ${names[i]}(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES ${names[i - 1]}(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
            val seed = newClient(db, http, syncTables = tables)
            seed.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO parents VALUES('P','parent'),('Q','other')")
            db.execSQL("INSERT INTO children VALUES('C','child','P')")
            db.execSQL("INSERT INTO grandchildren VALUES('D','grandchild','C')")
            seed.pushPending().getOrThrow()
            db.execSQL("UPDATE _sync_apply_state SET apply_mode=1")
            repeat(250) { db.execSQL("INSERT INTO children VALUES('A${it.toString().padStart(4, '0')}','unrelated','Q')") }
            db.execSQL("UPDATE _sync_apply_state SET apply_mode=0")
            seed.closeFromCoordinatedOwner()
            server.addRemoteBundleForTest(listOf(
                BundleRow("main", "parents", mapOf("id" to "P"), "DELETE", 2, null),
                BundleRow("main", "children", mapOf("id" to "C"), "UPDATE", 2,
                    Json.parseToJsonElement("""{"id":"C","name":"child","parent_id":"Q"}""")),
            ))
            val first = newClient(db, http, syncTables = tables, workUnits = 1800)
            first.openAndConnect("user-1").getOrThrow()
            assertIs<SyncWorkLimitException>(first.pullToStable().exceptionOrNull())
            val guard = scalarText(db, "SELECT guard FROM _sync_apply_plan")
            first.closeFromCoordinatedOwner()
            val reordered = listOf(tables[1], tables[0], tables[2])
            val resumed = newClient(db, http, syncTables = reordered, workUnits = 1800)
            resumed.openAndConnect("user-1").getOrThrow()
            assertIs<SyncWorkLimitException>(resumed.pullToStable().exceptionOrNull())
            assertTrue(guard != scalarText(db, "SELECT guard FROM _sync_apply_plan"))
            assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            resumed.closeFromCoordinatedOwner()
            val finish = newClient(db, http, syncTables = reordered)
            finish.openAndConnect("user-1").getOrThrow()
            finish.pullToStable().getOrThrow()
            assertEquals("grandchild", scalarText(db, "SELECT name FROM grandchildren WHERE id='D'"))
            assertEquals("C", scalarText(db, "SELECT parent_id FROM grandchildren WHERE id='D'"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
            finish.closeFromCoordinatedOwner()
        } finally { http.close(); db.close() }
    }

    @Test
    fun largeWirePayloadRefusedWithoutChangingBusinessState() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, http, seed ->
            seed.closeFromCoordinatedOwner()
            val client = newClient(db, http, syncTables = nodeTables, workspaceBytes = 8192)
            client.openAndConnect("user-1").getOrThrow()
            server.addRemoteBundleForTest(listOf(BundleRow("main", "nodes", mapOf("id" to "C"), "UPDATE", 2,
                kotlinx.serialization.json.buildJsonObject {
                    put("id", kotlinx.serialization.json.JsonPrimitive("C"))
                    put("name", kotlinx.serialization.json.JsonPrimitive("x".repeat(65536)))
                    put("parent_id", kotlinx.serialization.json.JsonPrimitive("P"))
                })))
            val error = assertIs<SyncWorkspaceLimitException>(client.pullToStable().exceptionOrNull())
            assertEquals("bytes", error.dimension)
            assertEquals("child", scalarText(db, "SELECT name FROM nodes WHERE id='C'"))
            assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            client.closeFromCoordinatedOwner()
        }
    }

    private suspend fun survivorCase(settlement: Boolean) {
        withSurvivorFixture { server, db, _, client ->
            val effects = effects()
            if (settlement) {
                db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
                server.normalizePushEffects = { effects }
                client.pushPending().getOrThrow()
            } else {
                server.addRemoteBundleForTest(effects)
                client.pullToStable().getOrThrow()
            }
            requireSurvivors(db)
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_apply_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_apply_plan"))
            assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            for ((key, version, deleted) in listOf(Triple("P", 2L, 1L), Triple("C", 2L, 0L), Triple("D", 1L, 0L), Triple("Q", 1L, 0L))) {
                val where = "FROM _sync_row_state WHERE table_name='nodes' AND json_extract(key_json,'$.id')='$key'"
                assertEquals(version, scalarLong(db, "SELECT row_version $where"))
                assertEquals(deleted, scalarLong(db, "SELECT server_kind='deleted' $where"))
            }
            assertEquals(if (settlement) 3L else 2L, scalarLong(db,
                "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id=(SELECT current_source_id FROM _sync_attachment_state)"))
            assertEquals("none", scalarText(db, "SELECT state FROM _sync_outbox_bundle"))
            assertEquals(0L, scalarLong(db, "SELECT apply_mode FROM _sync_apply_state"))
        }
    }


}
