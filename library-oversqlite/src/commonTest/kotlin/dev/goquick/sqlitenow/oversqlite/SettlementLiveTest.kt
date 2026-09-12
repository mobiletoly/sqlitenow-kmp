package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import io.ktor.client.statement.HttpReceivePipeline
import kotlinx.coroutines.CancellationException
import kotlin.test.*

internal class SettlementLiveTest : RealServerSupport() {
    @Test fun settlementCancellationRetainsHistoricalIntent() = runRealServerTest {
        val config=requireRealServerConfig() ?: return@runRealServerTest
        val tables=listOf(SyncTable("categories",syncKeyColumnName="id"))
        val db=newDb();val user=randomRealServerId("a3")
        val p=randomRealServerUuid();val q=randomRealServerUuid();val c=randomRealServerUuid();val d=randomRealServerUuid();val u=randomRealServerUuid()
        var lose=false
        var client:DefaultOversqliteClient?=null
        var http:io.ktor.client.HttpClient?=null
        try {
            db.execSQL("CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
            val source=bootstrapManagedSourceId(db,config.baseUrl,syncTables=tables)
            val original=newRealServerHttpClient(config.baseUrl,issueDummySigninToken(config.baseUrl,user,source))
            val transport=original.config {install("A3CommittedResponseLoss") {
                receivePipeline.intercept(HttpReceivePipeline.Before) {response ->
                    if(lose && response.call.request.url.encodedPath=="/sync/push") {
                        assertTrue(response.status.value in 200..299)
                        lose=false;db.execSQL("UPDATE categories SET name='new independent L' WHERE id='$u'")
                        throw CancellationException("committed response loss")
                    }
                }
            }}
            original.close();http=transport
            val active=newRealServerClient(db,transport,syncTables=tables,transientRetryPolicy=OversqliteTransientRetryPolicy(maxAttempts=1));client=active
            active.open().getOrThrow();active.attach(user).getOrThrow()
            db.execSQL("INSERT INTO categories VALUES('$p','parent',NULL),('$q','a1-other',NULL),('$c','child','$p'),('$d','grandchild','$c'),('$u','historical',NULL)")
            active.pushPending().getOrThrow()
            val historical=scalarText(db,"SELECT server_payload FROM _sync_row_state WHERE key_json='{\"id\":\"$u\"}'")
            db.execSQL("UPDATE categories SET name='a1-delete' WHERE id='$p'")
            lose=true
            assertFailsWith<CancellationException> {active.pushPending().getOrThrow()}
            assertFalse(lose)
            val frozen=baselineQualificationVector(db,listOf("categories"))
            assertEquals("2",frozen.rows("_sync_outbox_bundle").single().text("source_bundle_id"))
            assertEquals("1",frozen.rows("_sync_attachment_state").single().text("last_bundle_seq_seen"))
            assertEquals(0L,scalarLong(db,"SELECT reserved_entries FROM _sync_operation_state"))
            active.pushPending().getOrThrow()
            assertEquals("grandchild",scalarText(db,"SELECT name FROM categories WHERE id='$d'"))
            assertEquals(c,scalarText(db,"SELECT parent_id FROM categories WHERE id='$d'"))
            assertEquals(q,scalarText(db,"SELECT parent_id FROM categories WHERE id='$c'"))
            assertEquals(historical,scalarText(db,"SELECT base_payload FROM _sync_dirty_rows WHERE key_json='{\"id\":\"$u\"}'"))
            assertContains(scalarText(db,"SELECT payload FROM _sync_dirty_rows WHERE key_json='{\"id\":\"$u\"}'"),"new independent L")
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM pragma_foreign_key_check"))
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(3L,scalarLong(db,"SELECT next_source_bundle_id FROM _sync_source_state"))
            assertEquals(2L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        } finally {client?.closeFromCoordinatedOwner();http?.close();db.close()}
    }

    @Test fun backlogBound() = runOwnedWebDatabaseTest(timeout=kotlin.time.Duration.parse("900s")) {
        val authority=requireRealServerConfig() ?: return@runOwnedWebDatabaseTest
        for (size in listOf(1000,10000)) for (width in listOf(16,1024)) {
            val db=newDb();var http:io.ktor.client.HttpClient?=null;var client:DefaultOversqliteClient?=null
            try {
                val tables=listOf(SyncTable("categories",syncKeyColumnName="id"))
                db.execSQL("CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
                val user=randomRealServerId("a3-heavy");val source=bootstrapManagedSourceId(db,authority.baseUrl,syncTables=tables)
                var lose=false
                val original=newRealServerHttpClient(authority.baseUrl,issueDummySigninToken(authority.baseUrl,user,source))
                val transport=original.config {install("A3HeavyResponseLoss") {
                    receivePipeline.intercept(HttpReceivePipeline.Before) {response ->
                        if(lose && response.call.request.url.encodedPath=="/sync/push") {
                            assertTrue(response.status.value in 200..299);lose=false
                            throw CancellationException("committed response loss")
                        }
                    }
                }}
                original.close();http=transport
                val active=DefaultOversqliteClient.forTesting(db,OversqliteConfig(generationId=1,databaseId="fixture",serviceId="business",syncTables=tables,
                    maxSyncWorkUnitsPerInvocation=100000000,transientRetryPolicy=OversqliteTransientRetryPolicy(maxAttempts=1)),transport)
                client=active;active.open().getOrThrow();active.attach(user).getOrThrow()
                val p=randomRealServerUuid();val q=randomRealServerUuid();val c=randomRealServerUuid();val d=randomRealServerUuid()
                db.execSQL("INSERT INTO categories VALUES('$p','parent',NULL),('$q','a1-other',NULL),('$c','child','$p'),('$d','grandchild','$c')")
                active.pushPending().getOrThrow();db.execSQL("UPDATE categories SET name='a1-delete' WHERE id='$p'")
                lose=true;assertFailsWith<CancellationException> {active.pushPending().getOrThrow()};assertFalse(lose)
                val hash=scalarText(db,"SELECT canonical_request_hash FROM _sync_outbox_bundle")
                val predicate="json_extract(key_json,'$.id') LIKE '00000000-0000-4000-8000-%'"
                db.execSQL("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<$size) INSERT INTO categories SELECT printf('00000000-0000-4000-8000-%012x',i),'${"b".repeat(width)}',NULL FROM n")
                db.execSQL("""UPDATE _sync_row_state SET row_version=1,server_kind='live',server_payload=(SELECT payload FROM _sync_dirty_rows d WHERE d.schema_name=_sync_row_state.schema_name AND d.table_name=_sync_row_state.table_name AND d.key_json=_sync_row_state.key_json),
                    server_provenance=(SELECT server_provenance FROM _sync_row_state WHERE key_json='{"id":"$q"}'),relationships_known=1 WHERE $predicate""")
                db.execSQL("DELETE FROM _sync_dirty_rows WHERE $predicate")
                db.execSQL("UPDATE categories SET name='${"x".repeat(width)}' WHERE id LIKE '00000000-0000-4000-8000-%'")
                assertEquals(size.toLong(),scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows WHERE base_kind='present' AND base_payload IS NOT NULL"))
                val before=heavyIntentDigest(db)
                active.pushPending().getOrThrow()
                assertEquals(before,heavyIntentDigest(db))
                assertEquals("grandchild",scalarText(db,"SELECT name FROM categories WHERE id='$d'"));assertEquals(c,scalarText(db,"SELECT parent_id FROM categories WHERE id='$d'"))
                assertEquals("child",scalarText(db,"SELECT name FROM categories WHERE id='$c'"));assertEquals(q,scalarText(db,"SELECT parent_id FROM categories WHERE id='$c'"))
                assertEquals(size.toLong(),scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows"))
                assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_outbox_rows"));assertEquals(3L,scalarLong(db,"SELECT next_source_bundle_id FROM _sync_source_state"))
                assertEquals(2L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"));assertEquals(0L,scalarLong(db,"SELECT count(*) FROM pragma_foreign_key_check"))
                assertEquals(0L,scalarLong(db,"SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
                val stats=active.workspaceDiagnostics()
                assertTrue(stats.savedCopies<=5 && stats.savedBytes<4096,"$stats")
                assertTrue(stats.peakPrimaryCopies<=14 && stats.peakPrimaryBytes<64L*(width+1024),"$stats")
                assertTrue(stats.primaryReads>=size && stats.primaryReads<=5L*size+10,"$stats")
                assertEquals(stats.primaryReads*2,stats.indexedVisits)
                assertTrue(stats.serializedBytes>0 && stats.serializedBytes<=4*stats.primaryBytes+4096,"$stats")
                assertTrue(stats.restorationPasses<=3 && stats.duplicateChecks>0,"$stats")
                println("A3 actual live heavy $size/$width frozen=$hash stats=$stats; preparation/digest excluded; no RSS claim")
            } finally {client?.closeFromCoordinatedOwner();http?.close();db.close()}
        }
    }

    private suspend fun heavyIntentDigest(db:dev.goquick.sqlitenow.core.SafeSQLiteConnection):String {
        val hash=Sha256Accumulator()
        for((table,key) in listOf("categories" to "id","_sync_dirty_rows" to "json_extract(key_json,'$.id')","_sync_row_state" to "json_extract(key_json,'$.id')","_sync_relationships" to "json_extract(child_key,'$.id')")) {
            val columns=db.queryList("PRAGMA table_info($table)") {it.getText(1)}
            val projection=columns.joinToString(",") {"'$it',json_object('storage',typeof(\"$it\"),'value',CASE WHEN typeof(\"$it\")='blob' THEN lower(hex(\"$it\")) WHEN \"$it\" IS NULL THEN NULL ELSE CAST(\"$it\" AS TEXT) END)"}
            hash.append(table.encodeToByteArray());hash.appendAscii('\n')
            db.withPreparedStatement("SELECT json_object($projection) FROM $table WHERE $key LIKE '00000000-0000-4000-8000-%' ORDER BY ${columns.joinToString(",") {"\"$it\""}}") {
                while(it.step()) {hash.append(it.getText(0).encodeToByteArray());hash.appendAscii('\n')}
            }
        }
        return hash.finishHex()
    }
}
