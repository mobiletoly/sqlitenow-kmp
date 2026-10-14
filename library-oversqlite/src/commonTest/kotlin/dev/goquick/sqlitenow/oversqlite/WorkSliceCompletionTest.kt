package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.assertIs
import kotlin.test.assertFailsWith
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

internal class WorkSliceCompletionTest : SurvivorTestSupport() {
    @Test
    fun planningWorkspaceSnapshotIsChargedAndReleasedAtTheWriterBoundary() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, _, _ ->
            val config = OversqliteConfig(1, nodeTables, "fixture", "main")
            val workspace = OversqliteWorkspace(db, config)
            val push = WorkspaceCharge(8, 4096)
            val before = workspace.usage("snapshot-test", push = push)
            db.transaction(dev.goquick.sqlitenow.core.TransactionMode.IMMEDIATE) {
                kotlinx.coroutines.withContext(PushPlanningWorkspaceSnapshot(db)) {
                    assertEquals(WorkspaceCharge(before.entries + 2, before.bytes + 256), workspace.usage("snapshot-test", push = push))
                    assertEquals(WorkspaceCharge(before.entries + 5, before.bytes + 384), workspace.usage("snapshot-test", push = WorkspaceCharge(11, 4224)))
                    val snapshot = kotlinx.coroutines.currentCoroutineContext()[PushPlanningWorkspaceSnapshot]!!
                    snapshot.guard = PushPlanningGuard(Triple("synthetic",1,""),0,0)
                    OversqliteWorkspace(db,config.copy(maxSyncWorkspaceEntries = before.entries + 2)).check("snapshot-test",push = push)
                    assertTrue(snapshot.guard == null, "optional guard cache yields to useful workspace")
                    val limited = OversqliteWorkspace(db, config.copy(maxSyncWorkspaceEntries = before.entries + 1))
                    val capacity = assertFailsWith<SyncWorkspaceLimitException> { limited.check("snapshot-test", push = push) }
                    assertEquals("entries", capacity.dimension)
                }
            }
            assertEquals(before, workspace.usage("snapshot-test", push = push))
        }
    }

    @Test
    fun onlyUnpublishedDeletesCompleteWithoutAnEmptyBundle() = runOwnedWebDatabaseTest {
        withSurvivorFixture(workUnits = 1) { server, db, _, client ->
            val requests = server.pushRequestCount
            db.execSQL("INSERT INTO nodes VALUES('temporary','temporary',NULL)")
            db.execSQL("DELETE FROM nodes WHERE id='temporary'")
            client.pushPending().getOrThrow()
            assertEquals(requests, server.pushRequestCount)
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_push_plan"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
        }
    }

    @Test
    fun onePushIsOneBundleAndOneSyncDrainsTheRest() = runOwnedWebDatabaseTest {
        val db = newDb()
        val server = MockSyncServer()
        val generation = SyncGeneration("1", "", listOf(GenerationTable("main", "users", listOf("id"), listOf(
            GenerationColumn("email", "text", false), GenerationColumn("id", "text", false), GenerationColumn("name", "text", false),
        ), emptyList())))
        server.generationOverride = generation.copy(fingerprint = generation.computeFingerprint())
        val http = server.newHttpClient()
        db.execSQL("CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,email TEXT NOT NULL)")
        val client = newClient(db, http, syncTables = listOf(SyncTable("users", "id")), maxPushRows = 2, workUnits = 1)
        try {
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users VALUES('a','A','a@t'),('b','B','b@t'),('c','C','c@t'),('d','D','d@t'),('e','E','e@t')")
            client.pushPending().getOrThrow()
            assertEquals(1, server.pushRequestCount)
            assertEquals(3L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            client.sync().getOrThrow()
            assertEquals(3, server.pushRequestCount)
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            db.execSQL("INSERT INTO users VALUES('f','F','f@t'),('g','G','g@t'),('h','H','h@t')")
            assertEquals(DetachOutcome.DETACHED, client.syncThenDetach().getOrThrow().detach)
            assertEquals(5, server.pushRequestCount)
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
        } finally { client.closeFromCoordinatedOwner(); http.close(); db.close() }
    }

    @Test
    fun explicitResolutionCompletesAndKeepsTheChosenIntentAcrossTinySlices() = runOwnedWebDatabaseTest {
        for (decision in listOf(MergeResult.AcceptServer, MergeResult.KeepLocal)) {
            withSurvivorFixture { server, db, http, seed ->
                seed.closeFromCoordinatedOwner()
                val client = newClient(db, http, syncTables = nodeTables, workUnits = 1,
                    resolver = object : Resolver {
                        override fun resolve(conflict: ConflictContext) = MergeResult.DeferConflict
                    })
                try {
                    client.openAndConnect("user-1").getOrThrow()
                    db.execSQL("UPDATE nodes SET name='local' WHERE id='P'")
                    server.conflictOverride = { row, _, _ ->
                        PushConflictDetails(row.schema, row.table, row.key, row.op, row.baseRowVersion, 2, false,
                            buildJsonObject { put("id", "P"); put("name", "remote"); put("parent_id", JsonNull) })
                    }
                    assertIs<ConflictNeedsResolutionException>(client.pushPending().exceptionOrNull())
                    val handle = checkNotNull(client.pendingConflict().getOrThrow())
                    val requests = server.pushRequestCount
                    client.resolveConflict(handle, decision).getOrThrow()
                    assertTrue(client.workSliceDiagnostics().workSlices > 0)
                    assertEquals(requests, server.pushRequestCount, "local resolution retains the confirmed remote prefix")
                    assertEquals(if (decision == MergeResult.AcceptServer) "remote" else "local", scalarText(db, "SELECT name FROM nodes WHERE id='P'"))
                    assertEquals(2L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE key_json='{\"id\":\"P\"}'"))
                    assertEquals(if (decision == MergeResult.AcceptServer) 0L else 1L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_apply_plan"))
                    assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                } finally { client.closeFromCoordinatedOwner() }
            }
        }
    }

    @Test
    fun unpublishedDeletesDoNotRebuildUnchangedRoots() = runOwnedWebDatabaseTest {
        val db = newDb(); val server = MockSyncServer(); val http = server.newHttpClient()
        val generation = SyncGeneration("1", "", listOf(GenerationTable("main", "users", listOf("id"), listOf(
            GenerationColumn("id", "text", false), GenerationColumn("name", "text", false),
        ), emptyList())))
        server.generationOverride = generation.copy(fingerprint = generation.computeFingerprint())
        db.execSQL("CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)")
        val client = newClient(db,http,syncTables=listOf(SyncTable("users","id")),maxPushRows=10)
        try {
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("WITH RECURSIVE n(i) AS(SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<125) INSERT INTO users SELECT printf('live-%03d',i),'value' FROM n")
            repeat(10) { id -> db.execSQL("INSERT INTO users VALUES('noop-$id','temporary')"); db.execSQL("DELETE FROM users WHERE id='noop-$id'") }
            client.pushPending().getOrThrow()
            assertEquals(136L,client.workSliceDiagnostics().seedVisits)
            assertEquals(1,server.pushRequestCount)
            assertEquals(115L,scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(125L,scalarLong(db,"SELECT count(*) FROM users"))
        } finally {client.closeFromCoordinatedOwner();http.close();db.close()}
    }

    @Test
    fun schedulingStallAndCleanupFailureAreNotRetried() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, http, _ ->
            val owner = OversqliteClientOperationController(
                OversqliteConfig(1, nodeTables, "fixture", "main"),
                OversqliteClientResourceOwner(db, http), SnapshotDiagnosticsState(),
            ) {}
            assertFailsWith<SyncWorkNoProgressException> {
                owner.completeWorkPhase("stuck") { throw SyncWorkLimitException("unit", 1, 1) }
            }
            val cleanup = IllegalStateException("cleanup failed")
            val failure = SyncWorkLimitException("unit", 1, 1).also { it.addSuppressed(cleanup) }
            var calls = 0
            val mixed = assertFailsWith<SyncWorkLimitException> {
                owner.completeWorkPhase("mixed") { calls++; checkpointWorkStep(); commitWorkStep(); throw failure }
            }
            assertTrue(generateSequence<Throwable>(mixed) { it.cause }.any { cleanup in it.suppressedExceptions })
            assertEquals(1, calls)
            assertFailsWith<kotlinx.coroutines.CancellationException> {
                owner.completeWorkPhase("cancelled") { throw kotlinx.coroutines.CancellationException("cancelled") }
            }
        }
    }

    @Test fun publicPushSyncAndRebuildCompleteAtTargetOne() = publicCompletion(1)
    @Test fun publicPushSyncAndRebuildCompleteAtTargetEight() = publicCompletion(8)
    @Test fun publicPushSyncAndRebuildCompleteAtTargetSeventeen() = publicCompletion(17)
    @Test fun publicPushSyncAndRebuildCompleteAtDefaultTarget() = publicCompletion(1_000_000)

    private fun publicCompletion(target: Long) = runOwnedWebDatabaseTest {
            withSurvivorFixture(workUnits = target) { _, db, _, client ->
                db.execSQL("UPDATE nodes SET name='updated parent' WHERE id='P'")
                client.pushPending().getOrThrow()
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
                if (target < 1_000_000L) assertTrue(client.workSliceDiagnostics().workSlices > 0)
                assertEquals(2L, client.workSliceDiagnostics().pushMetadataMeasurements)
                println("work_slice_metadata target=$target diagnostics=${client.workSliceDiagnostics()}")
                db.execSQL("UPDATE nodes SET name='updated child' WHERE id='C'")
                client.sync().getOrThrow()
                client.rebuild().getOrThrow()
                assertEquals(4L, scalarLong(db, "SELECT count(*) FROM nodes"))
                assertEquals("updated parent", scalarText(db, "SELECT name FROM nodes WHERE id='P'"))
                assertEquals("updated child", scalarText(db, "SELECT name FROM nodes WHERE id='C'"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
                assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
            }
    }
}
