package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import kotlinx.coroutines.CancellationException
import kotlin.test.*

internal open class SettlementTest : SurvivorTestSupport() {
    protected open suspend fun measuredPush(client: DefaultOversqliteClient) {
        client.pushPending().getOrThrow()
    }
    @Test fun backlogBound() = runOwnedWebDatabaseTest(timeout=kotlin.time.Duration.parse("480s")) {
        for(n in listOf(1000,10000)) for(width in listOf(16,1024)) {
            withSurvivorFixture(workUnits=1_000_000) { server,db,_,client ->
                db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
                server.beforePushCommit={throw CancellationException("freeze exact original outbox")}
                assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
                val frozen=scalarText(db,"SELECT canonical_request_hash FROM _sync_outbox_bundle")
                assertEquals(2L,scalarLong(db,"SELECT source_bundle_id FROM _sync_outbox_bundle"))
                val payload="x".repeat(width)
                db.withPreparedStatement("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<?) INSERT INTO nodes SELECT printf('Z%05d',i),?,NULL FROM n") {
                    it.bindLong(1,n.toLong());it.bindText(2,"b".repeat(width));it.step()
                }
                // Published A is fixture setup; managed DML captures historical B.
                db.execSQL("""UPDATE _sync_row_state SET row_version=1,server_kind='live',
                    server_payload=(SELECT payload FROM _sync_dirty_rows d WHERE d.schema_name=_sync_row_state.schema_name AND d.table_name=_sync_row_state.table_name AND d.key_json=_sync_row_state.key_json),
                    server_provenance=(SELECT server_provenance FROM _sync_row_state WHERE key_json='{"id":"P"}'),relationships_known=1
                    WHERE json_extract(key_json,'$.id') LIKE 'Z%'""")
                db.execSQL("DELETE FROM _sync_dirty_rows WHERE json_extract(key_json,'$.id') LIKE 'Z%'")
                db.withPreparedStatement("UPDATE nodes SET name=? WHERE id LIKE 'Z%'") {
                    it.bindText(1,payload);it.step()
                }
                assertEquals(n.toLong(),scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows WHERE base_kind='present' AND base_payload IS NOT NULL"))
                val retained=unrelatedIntentDigest(db)
                // All setup above is excluded. Measure one public invocation;
                // the frozen outbox forbids replacing the original unit.
                server.normalizePushEffects={effects()}
                measuredPush(client)
                assertEquals(retained,unrelatedIntentDigest(db),"complete unrelated L, B, provenance, ordinal and timestamps")
                requireSurvivors(db)
                val mismatches=db.withPreparedStatement("SELECT count(*) FROM nodes n LEFT JOIN _sync_dirty_rows d ON d.table_name='nodes' AND d.key_json=json_object('id',n.id) WHERE n.id LIKE 'Z%' AND (n.name<>? OR n.parent_id IS NOT NULL OR d.payload IS NULL OR json_extract(d.payload,'$.id.value')<>n.id OR json_extract(d.payload,'$.name.value')<>? OR json_extract(d.payload,'$.parent_id.storage')<>'null')") {
                    it.bindText(1,payload);it.bindText(2,payload);check(it.step());it.getLong(0)
                }
                assertEquals(0L,mismatches)
                assertEquals(n.toLong(),scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows"))
                assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_outbox_rows"))
                assertEquals(2L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                assertEquals(3L,scalarLong(db,"SELECT next_source_bundle_id FROM _sync_source_state"))
                assertEquals(0L,scalarLong(db,"SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                val stats=client.workspaceDiagnostics()
                assertTrue(stats.savedCopies<=5,"$stats");assertTrue(stats.savedBytes<4096,"$stats")
                assertTrue(stats.peakPrimaryCopies<=14,"$stats");assertTrue(stats.peakPrimaryBytes<64L*(width+1024),"$stats")
                assertTrue(stats.primaryBytes>=n.toLong()*width,"$stats")
                assertTrue(stats.serializedBytes>0 && stats.serializedBytes<=4*stats.primaryBytes+4096,"cumulative streamed live-row conversions, not retained copies: $stats")
                assertTrue(stats.primaryReads>=n && stats.primaryReads<=5L*n+10,"$stats")
                assertEquals(stats.primaryReads*2,stats.indexedVisits)
                assertTrue(stats.restorationPasses<=3 && stats.duplicateChecks>0,"$stats")
                assertTrue(stats.work>=1024L*n && stats.work<2048L*n+10000,"$stats")
                val plan=db.withPreparedStatement("EXPLAIN QUERY PLAN SELECT payload FROM _sync_dirty_rows WHERE (schema_name,table_name,key_json)>(?,?,?) ORDER BY schema_name,table_name,key_json LIMIT 1") {
                    it.bindText(1,"");it.bindText(2,"");it.bindText(3,"");check(it.step());it.getText(3)
                }
                assertContains(plan,"SEARCH");assertContains(plan,"INDEX");assertFalse(plan.contains("TEMP"))
                println("A3 $n/$width public PushPending; setup excluded; frozen=$frozen stats=$stats")
            }
        }
    }

    private suspend fun unrelatedIntentDigest(db: dev.goquick.sqlitenow.core.SafeSQLiteConnection): String {
        val hash=Sha256Accumulator()
        for ((table,key) in listOf("nodes" to "id", "_sync_dirty_rows" to "json_extract(key_json,'$.id')",
            "_sync_row_state" to "json_extract(key_json,'$.id')", "_sync_relationships" to "json_extract(child_key,'$.id')")) {
            val columns=db.queryList("PRAGMA table_info($table)") {it.getText(1)}
            val projection=columns.joinToString(",") {"'$it',json_object('storage',typeof(\"$it\"),'value',CASE WHEN typeof(\"$it\")='blob' THEN lower(hex(\"$it\")) WHEN \"$it\" IS NULL THEN NULL ELSE CAST(\"$it\" AS TEXT) END)"}
            hash.append(table.encodeToByteArray());hash.appendAscii('\n')
            db.withPreparedStatement("SELECT json_object($projection) FROM $table WHERE $key LIKE 'Z%' ORDER BY ${columns.joinToString(",") { "\"$it\"" }}") {
                while(it.step()) {hash.append(it.getText(0).encodeToByteArray());hash.appendAscii('\n')}
            }
        }
        return hash.finishHex()
    }

    @Test fun triggerRestoration() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server,db,_,client ->
            db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
            server.beforePushCommit={throw CancellationException("freeze original")}
            assertFailsWith<CancellationException> {client.pushPending().getOrThrow()}
            db.execSQL("INSERT INTO nodes VALUES('U','new L',NULL)")
            db.execSQL("UPDATE _sync_apply_state SET apply_mode=1")
            db.execSQL("UPDATE nodes SET name='live exception' WHERE id='U'")
            db.execSQL("UPDATE _sync_apply_state SET apply_mode=0")
            db.execSQL("CREATE TRIGGER alter_u AFTER DELETE ON nodes WHEN OLD.id='P' BEGIN UPDATE nodes SET name='trigger damage' WHERE id='U'; END")
            server.normalizePushEffects={effects()}
            client.pushPending().getOrThrow()
            assertEquals("live exception",scalarText(db,"SELECT name FROM nodes WHERE id='U'"))
            assertContains(scalarText(db,"SELECT payload FROM _sync_dirty_rows WHERE key_json='{\"id\":\"U\"}'"),"new L")
            requireSurvivors(db)
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_apply_rows"))
        }
    }
}
