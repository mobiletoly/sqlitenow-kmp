package dev.goquick.sqlitenow.oversqlite

import kotlin.test.assertFailsWith
import kotlinx.coroutines.CancellationException
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

internal abstract class ConflictReservationTestSupport : SurvivorTestSupport() {
    protected fun conflictReservation(rows: Int, target: Long, entries: Long, automatic: Boolean, cancelResolution: Boolean = false) = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, http, seed ->
            if (rows > 4) {
                db.execSQL("WITH RECURSIVE n(i) AS(SELECT 4 UNION ALL SELECT i+1 FROM n WHERE i<${rows-1}) INSERT INTO nodes SELECT printf('established-%04d',i),'established','P' FROM n")
                seed.sync().getOrThrow()
            }
            seed.closeFromCoordinatedOwner()
            fun createClient() = newClient(db, http, syncTables = nodeTables, workUnits = target, workspaceEntries = entries,
                resolver = object : Resolver {
                    override fun resolve(conflict: ConflictContext) = if (automatic) MergeResult.AcceptServer else MergeResult.DeferConflict
                })
            var client = createClient()
            try {
                client.openAndConnect("user-1").getOrThrow()
                db.execSQL("CREATE TABLE reservation_witness(peak_entries INTEGER,peak_bytes INTEGER,releases INTEGER)")
                db.execSQL("INSERT INTO reservation_witness VALUES(0,0,0)")
                val reservationWitnessTrigger = "CREATE TRIGGER reservation_witness_update AFTER UPDATE OF workspace_context ON _sync_operation_state BEGIN UPDATE reservation_witness SET peak_entries=max(peak_entries,NEW.reserved_entries),peak_bytes=max(peak_bytes,NEW.reserved_bytes),releases=releases+(OLD.workspace_context<>'' AND NEW.workspace_context=''); END"
                db.execSQL(reservationWitnessTrigger)
                db.execSQL("UPDATE nodes SET name='local' WHERE id='P'")
                server.conflictOverride = { row, _, _ -> PushConflictDetails(row.schema,row.table,row.key,row.op,row.baseRowVersion,2,false,
                    buildJsonObject { put("id","P");put("name","remote");put("parent_id",JsonNull) }) }
                suspend fun resolve() {
                    if (automatic) client.pushPending().getOrThrow() else {
                        assertIs<ConflictNeedsResolutionException>(client.pushPending().exceptionOrNull())
                        val handle = checkNotNull(client.pendingConflict().getOrThrow())
                        val requests = server.pushRequestCount
                        db.execSQL("UPDATE reservation_witness SET peak_entries=0,peak_bytes=0,releases=0")
                        client.resolveConflict(handle,MergeResult.AcceptServer).getOrThrow()
                        assertEquals(requests,server.pushRequestCount,"resolution keeps the frozen remote identity")
                    }
                }
                if (cancelResolution) {
                    var commits = 0
                    client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(beforeCommit = {
                        if (++commits == 2) throw CancellationException("cancel conflict after durable preparation")
                    }))
                    assertFailsWith<CancellationException> { resolve() }
                    assertEquals(2, commits)
                    val handle = checkNotNull(client.pendingConflict().getOrThrow())
                    val frozen = scalarText(db,"SELECT canonical_request_hash FROM _sync_outbox_bundle")
                    val sequence = scalarLong(db,"SELECT next_source_bundle_id FROM _sync_source_state")
                    assertTrue(scalarLong(db,"SELECT count(*) FROM _sync_apply_plan") > 0)
                    assertEquals(0L,scalarLong(db,"SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                    // Opening correctly rejects unrecognized internal-table
                    // triggers; remove only this test instrumentation.
                    db.execSQL("DROP TRIGGER reservation_witness_update")
                    client.closeFromCoordinatedOwner()
                    client = createClient()
                    client.openAndConnect("user-1").getOrThrow()
                    db.execSQL(reservationWitnessTrigger)
                    assertEquals(handle,client.pendingConflict().getOrThrow())
                    assertEquals(frozen,scalarText(db,"SELECT canonical_request_hash FROM _sync_outbox_bundle"))
                    val requests = server.pushRequestCount
                    client.resolveConflict(handle,MergeResult.AcceptServer).getOrThrow()
                    assertEquals(requests,server.pushRequestCount)
                    assertEquals(sequence,scalarLong(db,"SELECT next_source_bundle_id FROM _sync_source_state"))
                } else resolve()
                val peak = scalarLong(db,"SELECT peak_entries FROM reservation_witness")
                assertTrue(peak <= 512,"temporary reservation must not accumulate across slices: $peak")
                assertEquals(if (cancelResolution) 2L else 1L,scalarLong(db,"SELECT releases FROM reservation_witness"),"one outer release")
                assertEquals(rows.toLong(),scalarLong(db,"SELECT count(*) FROM nodes"))
                assertEquals("remote",scalarText(db,"SELECT name FROM nodes WHERE id='P'"))
                assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows"))
                assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_outbox_rows"))
                assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_apply_plan"))
                assertEquals(0L,scalarLong(db,"SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                assertEquals(0L,scalarLong(db,"SELECT count(*) FROM pragma_foreign_key_check"))
                if (target < 1_000_000) assertTrue(client.workSliceDiagnostics().workSlices > rows)
                println("conflict_reservation rows=$rows target=$target automatic=$automatic cancelled=$cancelResolution peak_entries=$peak diagnostics=${client.workSliceDiagnostics()}")
            } finally { client.closeFromCoordinatedOwner() }
        }
    }
}
