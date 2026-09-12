package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.*
import kotlin.test.*

internal class SnapshotRecoveryTest : SurvivorTestSupport() {
    @Test fun firstDiscoveredInPull() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server,db,_,client ->
            db.execSQL("UPDATE _sync_attachment_state SET last_bundle_seq_seen=0")
            server.retainedBundleFloor=1
            assertEquals(0L,scalarLong(db,"SELECT rebuild_required FROM _sync_attachment_state"))
            val before=baselineQualificationVector(db,listOf("nodes"))
            client.pullToStable().getOrThrow()
            assertEquals(1L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals(4L,scalarLong(db,"SELECT count(*) FROM nodes"))
            assertEquals("grandchild",scalarText(db,"SELECT name FROM nodes WHERE id='D'"))
            assertEquals("C",scalarText(db,"SELECT parent_id FROM nodes WHERE id='D'"))
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM pragma_foreign_key_check"))
            val after=baselineQualificationVector(db,listOf("nodes"))
            assertEquals(before.rows("nodes"),after.rows("nodes"),"complete snapshot hydration payloads")
            for (table in listOf("_sync_dirty_rows","_sync_outbox_rows","_sync_outbox_bundle","_sync_source_state"))
                assertEquals(before.rows(table),after.rows(table),table)
            val oldConflict=before.rows("_sync_conflict_state").single()
            val newConflict=after.rows("_sync_conflict_state").single()
            assertEquals(oldConflict - "baseline_revision",newConflict - "baseline_revision","only authoritative snapshot revision advances")
            assertEquals(oldConflict.text("baseline_revision")!!.toLong()+8,newConflict.text("baseline_revision")!!.toLong(),"four A deletes plus four installs")
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_outbox_rows"))
            for (id in listOf("P","Q","C","D")) {
                val old=before.row("_sync_row_state",id);val installed=after.row("_sync_row_state",id)
                assertEquals(old.payload("server_payload"),installed.payload("server_payload"),"complete A $id")
                assertEquals(old.text("row_version"),installed.text("row_version"),"version $id")
                assertEquals("live",installed.text("server_kind"))
                assertBaselineProvenance(installed,"server_provenance",installed.text("row_version")!!.toLong())
            }
            assertEquals(before.rows("_sync_relationships").map {it - "owner_revision"},
                after.rows("_sync_relationships").map {it - "owner_revision"},"relationship identities; snapshot owns new revisions")
            assertTrue(server.snapshotSessionCreateRequestCount>0)
        }
    }

    @Test fun preparedOutbox() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server,db,_,client ->
            db.execSQL("UPDATE nodes SET name='frozen L' WHERE id='P'")
            server.beforePushCommit={throw CancellationException("freeze original before commit")}
            assertFailsWith<CancellationException> {client.pushPending().getOrThrow()}
            val rows=OversqliteOutboxStateStore(db).loadRows()
            val hash=scalarText(db,"SELECT canonical_request_hash FROM _sync_outbox_bundle")
            markSourceRecoveryRequired(db,SourceRecoveryReason.HISTORY_PRUNED)
            client.rebuild().getOrThrow()
            val recovered=OversqliteOutboxStateStore(db).loadRows()
            assertEquals(rows.map { listOf(it.base,it.wirePayload,it.localPayload,it.keyJson,it.wireKeyJson,it.op,it.baseRowVersion,it.rowOrdinal) },
                recovered.map { listOf(it.base,it.wirePayload,it.localPayload,it.keyJson,it.wireKeyJson,it.op,it.baseRowVersion,it.rowOrdinal) })
            assertEquals(hash,scalarText(db,"SELECT canonical_request_hash FROM _sync_outbox_bundle"))
            assertEquals(1L,scalarLong(db,"SELECT source_bundle_id FROM _sync_outbox_bundle"))
            assertEquals("frozen L",scalarText(db,"SELECT name FROM nodes WHERE id='P'"))
            assertEquals("parent",scalarText(db,"SELECT json_extract(server_payload,'$.name.value') FROM _sync_row_state WHERE json_extract(key_json,'$.id')='P'"))
            client.pushPending().getOrThrow()
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM pragma_foreign_key_check"))
        }
    }
}
