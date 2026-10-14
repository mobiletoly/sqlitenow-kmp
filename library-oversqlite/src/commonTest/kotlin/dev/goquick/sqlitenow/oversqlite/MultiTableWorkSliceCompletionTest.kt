package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.assertContains
import kotlin.test.assertNotNull

internal class MultiTableWorkSliceCompletionTest : CrossTargetSyncTestSupport() {
    @Test fun eightTable125RowPushCompletesOneFortyRowBundle() = verify(1_000_000)
    @Test fun eightTable125RowPushCompletesAcrossTinySlices() = verify(1)
    @Test fun eightTable125RowPushCompletesAtTargetEight() = verify(8)
    @Test fun eightTable125RowPushCompletesAtTargetSeventeen() = verify(17)

    @Test fun eightTable125RowPushCompletesWithoutReservationObserver() = verify(1, observeReservations = false, drain = false)

    @Test fun replayDecisionContinuationInvalidatesAfterLocalWrite() = verify(1, drain = false, changeLocal = true)

    @Test fun emptyDependencyCounterContradictionFailsBeforeBusinessMutation() = verify(1, drain = false, contradictIndex = true)

    @Test fun emptyDependencyTraversalRejectsMissingRetainedRoot() = verify(1, drain = false, contradictIndex = true, missingRoot = true)

    private fun verify(workTarget: Long, observeReservations: Boolean = true, drain: Boolean = true, changeLocal: Boolean = false, contradictIndex: Boolean = false, missingRoot: Boolean = false) = runOwnedWebDatabaseTest {
        val names = listOf("users") + (1..7).map { "aux_$it" }
        val tables = names.map { name -> GenerationTable("main",name,listOf("id"),listOf(
            GenerationColumn("id","text",false),GenerationColumn("name","text",false)),emptyList()) }
        val generation = SyncGeneration("1","",tables).let { it.copy(fingerprint = it.computeFingerprint()) }
        val db = newDb(); val server = MockSyncServer(); server.generationOverride = generation
        val http = server.newHttpClient()
        val client = newClient(db,http,maxPushRows = 40,workUnits = workTarget,syncTables = names.map { SyncTable(it,"id") })
        try {
            for (name in names) db.execSQL("CREATE TABLE $name(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)")
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("WITH RECURSIVE n(i) AS(SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<125) INSERT INTO users SELECT printf('user-%03d',i),'value' FROM n")
            var peakReservationEntries = 0L
            var peakReservationBytes = 0L
            var commitsObserved = 0
            var changedLocal = false
            var contradictedIndex = false
            var frozenIdentity: String? = null
            if (observeReservations) client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(beforeCommit = {
                peakReservationEntries = maxOf(peakReservationEntries, scalarLong(db, "SELECT reserved_entries FROM _sync_operation_state"))
                peakReservationBytes = maxOf(peakReservationBytes, scalarLong(db, "SELECT reserved_bytes FROM _sync_operation_state"))
                assertEquals(1L, scalarLong(db, "SELECT json_array_length(workspace_context) FROM _sync_operation_state"), "one outer operation owns replay buffers across slices")
                if (changeLocal && !changedLocal && client.workSliceDiagnostics().replayDecisionReuses > 0) {
                    frozenIdentity = scalarText(db, "SELECT source_id || ':' || source_bundle_id || ':' || canonical_request_hash FROM _sync_outbox_bundle")
                    assertEquals(0L, scalarLong(db, "SELECT apply_mode FROM _sync_apply_state"))
                    db.execSQL("UPDATE users SET name='changed during continuation' WHERE id='user-001'")
                    changedLocal = true
                }
                if (changedLocal && scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows") > 0) {
                    assertEquals(frozenIdentity, scalarText(db, "SELECT source_id || ':' || source_bundle_id || ':' || canonical_request_hash FROM _sync_outbox_bundle"))
                }
                if (contradictIndex && !contradictedIndex && scalarText(db, "SELECT COALESCE((SELECT phase FROM _sync_apply_plan LIMIT 1),'')") == "walk") {
                    assertEquals(0L, scalarLong(db, "SELECT edge_entries FROM _sync_apply_plan"))
                    if (missingRoot) db.execSQL("UPDATE _sync_apply_plan SET visit_cursor=999999")
                    else db.execSQL("INSERT INTO _sync_apply_edges VALUES(1,'users','user-001','users','user-002',232)")
                    contradictedIndex = true
                }
                commitsObserved++
            }))
            val pushStarted = kotlin.time.TimeSource.Monotonic.markNow()
            println("paired_phase push_start target=$workTarget")
            val push = client.pushPending()
            if (contradictIndex) {
                assertTrue(contradictedIndex)
                val failure = assertNotNull(push.exceptionOrNull())
                val root = generateSequence(failure) { it.cause }.last()
                assertContains(root.message.orEmpty(), if (missingRoot) "pending dependency root is missing" else "empty dependency index contradicts retained edges")
                assertEquals(1, server.pushRequestCount)
                assertEquals(125L, scalarLong(db, "SELECT count(*) FROM users"))
                assertEquals(40L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
                assertEquals(0L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                return@runOwnedWebDatabaseTest
            }
            push.getOrThrow()
            println("paired_phase push_complete target=$workTarget elapsed=${pushStarted.elapsedNow()}")
            assertEquals(1,server.pushRequestCount)
            assertEquals(if (changeLocal) 86L else 85L,scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows"))
            if (changeLocal) {
                assertTrue(changedLocal, "write occurs after a decision cache was reused")
                assertTrue(client.workSliceDiagnostics().replayDecisionInvalidations > 0)
                assertTrue(client.workSliceDiagnostics().replayDecisionBuilds > 1)
                assertEquals("changed during continuation", scalarText(db, "SELECT name FROM users WHERE id='user-001'"))
                assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows WHERE key_json='{\"id\":\"user-001\"}'"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_apply_plan"))
            }
            assertEquals(2L,client.workSliceDiagnostics().pushMetadataMeasurements)
            if (workTarget < 1_000_000) {
                assertTrue(client.workSliceDiagnostics().workSlices > 0)
                if (!changeLocal) {
                    assertEquals(176L, client.workSliceDiagnostics().replayDecisionReuses, "320 proven empty-table visits are eliminated")
                    assertEquals(1163L - (2L * 125L + 40L), client.workSliceDiagnostics().pushPlanningSteps, "independent nodes omit two DFS transitions and one empty propagation; quantum stays32")
                }
            }
            assertEquals(0L,scalarLong(db,"SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
            if (observeReservations) assertTrue(commitsObserved > 0)
            if (observeReservations) assertTrue(peakReservationEntries in 1..100_000, "fully charged replay stays within the unchanged hard entry limit")
            println("paired_multi_table rows=125 tables=8 max_push=40 target=$workTarget peak_reservation_entries=$peakReservationEntries peak_reservation_bytes=$peakReservationBytes commits=$commitsObserved diagnostics=${client.workSliceDiagnostics()}")
            client.setSnapshotApplyFaultInjectorForTest(null)
            if (drain) {
            val syncStarted = kotlin.time.TimeSource.Monotonic.markNow()
            println("paired_phase sync_start target=$workTarget")
            client.sync().getOrThrow()
            println("paired_phase sync_complete target=$workTarget elapsed=${syncStarted.elapsedNow()}")
            assertEquals(4,server.pushRequestCount)
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(125L,scalarLong(db,"SELECT count(*) FROM users"))
            assertEquals(0L,scalarLong(db,"SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"), "each outer operation releases its reservation")
            }
        } finally { client.closeFromCoordinatedOwner();http.close();db.close() }
    }
}
