package dev.goquick.sqlitenow.oversqlite

import io.ktor.client.HttpClient
import io.ktor.client.statement.HttpReceivePipeline
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.assertNotNull

internal class A1LiveSurvivorsTest : RealServerSupport() {
    @Test
    fun baselineConfirmedTombstoneReinsertion() = runRealServerTest {
        val config = requireRealServerConfig("OVERSQLITE_CATEGORIES_REAL_SERVER_SMOKE_BASE_URL") ?: return@runRealServerTest
        val tables = listOf(SyncTable("categories",syncKeyColumnName="id"))
        val db = newDb()
        val user = randomRealServerId("baseline-tombstone")
        val id = randomRealServerUuid()
        var http: HttpClient? = null
        var client: DefaultOversqliteClient? = null
        try {
            createBaselineTable(db)
            val source = bootstrapManagedSourceId(db,config.baseUrl,syncTables=tables)
            http = newRealServerHttpClient(config.baseUrl,issueDummySigninToken(config.baseUrl,user,source))
            client = newRealServerClient(db,http,syncTables=tables)
            client.open().getOrThrow();client.attach(user).getOrThrow()
            db.execSQL("INSERT INTO categories VALUES('$id','original',NULL)")
            client.pushPending().getOrThrow()
            val published=baselineLiveVector(db)
            assertTextPayload(published.row("_sync_row_state",id).payload("server_payload"),mapOf("id" to id,"name" to "original","parent_id" to null))
            db.execSQL("DELETE FROM categories WHERE id='$id'")
            client.pushPending().getOrThrow()
            val version = scalarLong(db,"SELECT row_version FROM _sync_row_state")
            assertTrue(version>0)
            assertEquals("deleted",scalarText(db,"SELECT server_kind FROM _sync_row_state"))
            assertEquals("[]",baselineRemoteRows(config,user,tables))
            val deleted=baselineLiveVector(db)
            assertEquals(null,deleted.row("_sync_row_state",id).payload("server_payload"))
            assertEquals("2",deleted.row("_sync_row_state",id).text("row_version"))
            assertTrue(deleted.rows("categories").isEmpty());assertTrue(deleted.rows("_sync_dirty_rows").isEmpty());assertBaselineCounters(deleted,source,3,2)
            db.execSQL("INSERT INTO categories VALUES('$id','reinserted',NULL)")
            assertEquals("deleted",scalarText(db,"SELECT base_kind FROM _sync_dirty_rows"))
            assertEquals(version,scalarLong(db,"SELECT base_row_version FROM _sync_dirty_rows"))
            val retry=baselineLiveVector(db)
            assertEquals(deleted.row("_sync_row_state",id).payload("server_provenance"),retry.row("_sync_dirty_rows",id).payload("base_provenance"))
            assertEquals(null,retry.row("_sync_dirty_rows",id).payload("base_payload"))
            assertTextPayload(retry.row("_sync_dirty_rows",id).payload("payload"),mapOf("id" to id,"name" to "reinserted","parent_id" to null))
            client.pushPending().getOrThrow()
            assertEquals("live",scalarText(db,"SELECT server_kind FROM _sync_row_state"))
            assertTrue(scalarLong(db,"SELECT row_version FROM _sync_row_state")>version)
            assertEquals("reinserted",scalarText(db,"SELECT json_extract(server_payload,'$.name.value') FROM _sync_row_state"))
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_relationships"))
            assertEquals(source,scalarText(db,"SELECT source_id FROM _sync_source_state"))
            assertEquals(4L,scalarLong(db,"SELECT next_source_bundle_id FROM _sync_source_state"))
            assertEquals(3L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals("[[\"$id\",\"reinserted\",null]]",baselineRemoteRows(config,user,tables))
            assertTextPayload(baselineLiveVector(db).row("_sync_row_state",id).payload("server_payload"),mapOf("id" to id,"name" to "reinserted","parent_id" to null))
            println("baseline-live: confirmed tombstone reinsertion settled")
        } finally { client?.closeFromCoordinatedOwner();http?.close();db.close() }
        println("RECOVERY_HTTP_BODY A1LiveSurvivorsTest.baselineConfirmedTombstoneReinsertion PASS")
    }

    @Test
    fun baselineLostResponsePhysicalReopen() = runRealServerTest {
        if (!supportsDurableTestDatabaseRestart()) {
            println("NOT-APPLICABLE: physical restart on transient Node; baselineLostResponseTransientReplay retains live replay obligations")
            return@runRealServerTest
        }
        baselineLiveReplay(true)
    }

    @Test
    fun baselineLostResponseTransientReplay() = runRealServerTest { baselineLiveReplay(false) }

    private suspend fun baselineLiveReplay(physicalRestart: Boolean) {
        val config = requireRealServerConfig("OVERSQLITE_CATEGORIES_REAL_SERVER_SMOKE_BASE_URL") ?: return
        val tables = listOf(SyncTable("categories",syncKeyColumnName="id"))
        val path = if (physicalRestart) createTempSqliteNowTestDbPath("baseline-live-replay") else null
        var db = if (path == null) newDb() else newFileBackedDb(path)
        val user = randomRealServerId("baseline-replay")
        val id = randomRealServerUuid()
        var http: HttpClient? = null
        var client: DefaultOversqliteClient? = null
        var loseResponse = false
        try {
            createBaselineTable(db)
            val source = bootstrapManagedSourceId(db,config.baseUrl,syncTables=tables)
            val transport = newRealServerHttpClient(config.baseUrl,issueDummySigninToken(config.baseUrl,user,source))
            http = transport.config {
                install("BaselineResponseLoss") {
                    receivePipeline.intercept(HttpReceivePipeline.Before) { response ->
                        if (loseResponse && response.call.request.url.encodedPath=="/sync/push") {
                            assertTrue(response.status.value in 200..299)
                            loseResponse=false
                            db.execSQL("UPDATE categories SET name='newer' WHERE id='$id'")
                            throw CancellationException("test-owned lost committed response")
                        }
                    }
                }
            }
            transport.close()
            client = newRealServerClient(db,http,syncTables=tables,transientRetryPolicy=OversqliteTransientRetryPolicy(maxAttempts=1))
            client.open().getOrThrow();client.attach(user).getOrThrow()
            db.execSQL("INSERT INTO categories VALUES('$id','original',NULL)")
            client.pushPending().getOrThrow()
            val original = scalarText(db,"SELECT server_payload FROM _sync_row_state")
            db.execSQL("UPDATE categories SET name='frozen' WHERE id='$id'")
            loseResponse=true
            var failed = false
            try { failed = client.pushPending().isFailure } catch (_: CancellationException) { failed = true }
            assertTrue(failed,"the committed response must be lost before client settlement")
            assertTrue(!loseResponse,"the test-owned response loss must execute")
            assertEquals(original,scalarText(db,"SELECT base_payload FROM _sync_outbox_rows"))
            val hash = scalarText(db,"SELECT canonical_request_hash FROM _sync_outbox_bundle")
            assertEquals(64,hash.length)
            val before = baselineLiveVector(db)
            val originalA=before.row("_sync_row_state",id)
            val frozenB=before.row("_sync_outbox_rows",id)
            val newerB=before.row("_sync_dirty_rows",id)
            assertTextPayload(originalA.payload("server_payload"),mapOf("id" to id,"name" to "original","parent_id" to null))
            assertEquals(originalA.payload("server_payload"),frozenB.payload("base_payload"));assertEquals(originalA.payload("server_provenance"),frozenB.payload("base_provenance"))
            assertEquals(originalA.payload("server_payload"),newerB.payload("base_payload"));assertEquals(originalA.payload("server_provenance"),newerB.payload("base_provenance"))
            assertTextPayload(frozenB.payload("local_payload"),mapOf("id" to id,"name" to "frozen","parent_id" to null))
            assertTextPayload(newerB.payload("payload"),mapOf("id" to id,"name" to "newer","parent_id" to null));assertBaselineCounters(before,source,2,1)
            assertEquals("[[\"$id\",\"frozen\",null]]",baselineRemoteRows(config,user,tables))
            if (path != null) {
                client.closeFromCoordinatedOwner();db.close()
                db = newFileBackedDb(path)
                assertEquals(before,baselineLiveVector(db))
                client = newRealServerClient(db,http,syncTables=tables,transientRetryPolicy=OversqliteTransientRetryPolicy(maxAttempts=1))
                client.open().getOrThrow();client.attach(user).getOrThrow()
                assertEquals(hash,scalarText(db,"SELECT canonical_request_hash FROM _sync_outbox_bundle"))
            }
            client.pushPending().getOrThrow()
            assertEquals("frozen",scalarText(db,"SELECT json_extract(server_payload,'$.name.value') FROM _sync_row_state"))
            assertEquals("frozen",scalarText(db,"SELECT json_extract(base_payload,'$.name.value') FROM _sync_dirty_rows"))
            assertEquals("newer",scalarText(db,"SELECT name FROM categories"))
            assertEquals(source,scalarText(db,"SELECT source_id FROM _sync_source_state"))
            assertEquals(3L,scalarLong(db,"SELECT next_source_bundle_id FROM _sync_source_state"))
            assertEquals(2L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_relationships"))
            assertEquals("[[\"$id\",\"frozen\",null]]",baselineRemoteRows(config,user,tables))
            val settled=baselineLiveVector(db)
            assertTextPayload(settled.row("_sync_row_state",id).payload("server_payload"),mapOf("id" to id,"name" to "frozen","parent_id" to null))
            assertEquals(settled.row("_sync_row_state",id).payload("server_payload"),settled.row("_sync_dirty_rows",id).payload("base_payload"))
            assertEquals(settled.row("_sync_row_state",id).payload("server_provenance"),settled.row("_sync_dirty_rows",id).payload("base_provenance"))
            assertEquals(before.rows("categories"),settled.rows("categories"))
            println("baseline-live: replay settled; physicalRestart=$physicalRestart")
        } finally {
            client?.closeFromCoordinatedOwner();http?.close();db.close()
            if (path != null) deleteTempSqliteNowTestDbArtifacts(path)
        }
        val method = if (physicalRestart) "baselineLostResponsePhysicalReopen" else "baselineLostResponseTransientReplay"
        println("RECOVERY_HTTP_BODY A1LiveSurvivorsTest.$method PASS")
    }

    private suspend fun createBaselineTable(db: SafeSQLiteConnection) {
        db.execSQL("CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
    }

    // Observation-only wire projection. The primary/second attached SDK clients
    // below remain the physical apply witnesses; this oracle needs no snapshot.
    private suspend fun baselineRemoteRows(config:RealServerConfig,user:String,tables:List<SyncTable>):String {
        val source=randomRealServerId("baseline-observer")
        val http=newRealServerHttpClient(config.baseUrl,issueDummySigninToken(config.baseUrl,user,source))
        lateinit var generation:SyncGeneration
        val remote=OversqliteRemoteApi(http,Json {ignoreUnknownKeys=false},generation={generation},log={})
        try {
            val admitted=remote.fetchGeneration(source,1);generation=admitted.generation
            assertEquals(tables.map {it.tableName},generation.tables.map {it.table})
            remote.connect(source,false)
            val rows=mutableMapOf<String,JsonObject>();var after=0L
            do {
                val page=remote.sendPullRequest(after,admitted.limits.maxPullBundles,0,source,admitted.limits.maxPullDecodedBytes)
                for(bundle in page.bundles) {
                    for(row in bundle.rows) {
                        val key=row.key.getValue("id")
                        if(row.op=="DELETE")rows.remove(key) else rows[key]=row.payload!!.jsonObject
                    }
                    after=bundle.bundleSeq
                }
            } while(page.hasMore)
            return JsonArray(rows.entries.sortedBy {it.key}.map {it.value}.map {JsonArray(listOf(it.getValue("id"),it.getValue("name"),it.getValue("parent_id")))}).toString()
        } finally {http.close()}
    }

    private suspend fun baselineLiveVector(db: SafeSQLiteConnection) = baselineQualificationVector(db,listOf("categories"))

    // Replaces the obsolete snapshot-based prepared-source rebind assertion.
    // C03 settles and acknowledges F on its original source; C04 owns clean T.
    @Test fun baselineSourceRecoveryCannotBypassFrozenOutcome() = runRealServerTest {
        val config=requireRealServerConfig("OVERSQLITE_CATEGORIES_REAL_SERVER_SMOKE_BASE_URL") ?: return@runRealServerTest
        val db=newDb();val user=randomRealServerId("baseline-recovery");val id=randomRealServerUuid()
        val tables=listOf(SyncTable("categories",syncKeyColumnName="id"))
        var http:HttpClient?=null;var client:DefaultOversqliteClient?=null;var lose=false;var snapshots=0
        try {
            createBaselineTable(db);val source=bootstrapManagedSourceId(db,config.baseUrl,syncTables=tables)
            val transport=newRealServerHttpClient(config.baseUrl,issueDummySigninToken(config.baseUrl,user,source),beforeRequest={if(it=="/sync/snapshot-sessions")snapshots++})
            http=transport.config {install("BaselineRecoveryResponseLoss") {receivePipeline.intercept(HttpReceivePipeline.Before) {response ->
                if(lose && response.call.request.url.encodedPath=="/sync/push") {assertTrue(response.status.value in 200..299);lose=false;throw CancellationException("lost committed recovery response")}
            }}};transport.close()
            client=newRealServerClient(db,http,syncTables=tables,transientRetryPolicy=OversqliteTransientRetryPolicy(maxAttempts=1))
            client.open().getOrThrow();client.attach(user).getOrThrow();db.execSQL("INSERT INTO categories VALUES('$id','original',NULL)");client.pushPending().getOrThrow()
            db.execSQL("UPDATE categories SET name='frozen' WHERE id='$id'");lose=true
            var failed=false;try {failed=client.pushPending().isFailure}catch(_:CancellationException){failed=true}
            assertTrue(failed && !lose)
            val originalF=scalarText(db,"SELECT hex(request_body) FROM _sync_upload_journal")
            val before=baselineLiveVector(db);val old=before.row("_sync_row_state",id);val capture=before.row("_sync_outbox_rows",id)
            assertTextPayload(old.payload("server_payload"),mapOf("id" to id,"name" to "original","parent_id" to null))
            assertEquals(old.payload("server_payload"),capture.payload("base_payload"));assertEquals(old.payload("server_provenance"),capture.payload("base_provenance"))
            markSourceRecoveryRequired(db,replacementSourceId=randomRealServerUuid())
            // Source recovery never retargets the frozen request. Ordinary settlement resolves and
            // acknowledges it under the original source, without a snapshot.
            assertEquals(originalF,scalarText(db,"SELECT hex(request_body) FROM _sync_upload_journal"));assertEquals(0,snapshots)
            client.pushPending().getOrThrow()
            val settled=baselineLiveVector(db)
            assertTrue(settled.rows("_sync_outbox_rows").isEmpty());assertTrue(settled.rows("_sync_dirty_rows").isEmpty())
            assertBaselineCounters(settled,source,3,2);assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_upload_journal"))
            assertTextPayload(settled.row("_sync_row_state",id).payload("server_payload"),mapOf("id" to id,"name" to "frozen","parent_id" to null))
            assertEquals(before.rows("categories"),settled.rows("categories"));assertEquals(0,snapshots)
            println("A1_C03_REPLACEMENT_COMPLETE source_recovery_did_not_retarget_F original_source=$source resolve_ack_complete=true snapshot=0")
        } finally {client?.closeFromCoordinatedOwner();http?.close();db.close()}
        println("RECOVERY_HTTP_BODY A1LiveSurvivorsTest.baselineSourceRecoveryCannotBypassFrozenOutcome PASS")
    }

    @Test
    fun settlementAndPullPreserveGrandchild() = runRealServerTest {
        val config = requireRealServerConfig("OVERSQLITE_CATEGORIES_REAL_SERVER_SMOKE_BASE_URL") ?: return@runRealServerTest
        val tables = listOf(SyncTable("categories", syncKeyColumnName = "id"))
        val user = randomRealServerId("a1")
        val databases = listOf(newDb(), newDb())
        val transports = mutableListOf<HttpClient>()
        val clients = mutableListOf<DefaultOversqliteClient>()
        try {
            for (db in databases) {
                db.execSQL("CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
                val source = bootstrapManagedSourceId(db, config.baseUrl, syncTables = tables)
                val http = newRealServerHttpClient(config.baseUrl, issueDummySigninToken(config.baseUrl, user, source))
                transports += http
                val client = newRealServerClient(db, http, syncTables = tables)
                clients += client
                client.open().getOrThrow()
                client.attach(user).getOrThrow()
            }
            val p = randomRealServerUuid()
            val q = randomRealServerUuid()
            val c = randomRealServerUuid()
            val d = randomRealServerUuid()
            databases[0].execSQL("INSERT INTO categories VALUES('$p','parent',NULL),('$q','a1-other',NULL),('$c','child','$p'),('$d','grandchild','$c')")
            clients[0].pushPending().getOrThrow()
            clients[1].pullToStable().getOrThrow()
            databases[0].execSQL("UPDATE categories SET name='a1-delete' WHERE id='$p'")
            clients[0].pushPending().getOrThrow()
            clients[1].pullToStable().getOrThrow()
            for (db in databases) {
                assertEquals("grandchild", scalarText(db, "SELECT name FROM categories WHERE id='$d'"))
                assertEquals(c, scalarText(db, "SELECT parent_id FROM categories WHERE id='$d'"))
                assertEquals(q, scalarText(db, "SELECT parent_id FROM categories WHERE id='$c'"))
                for (query in listOf("SELECT count(*) FROM categories WHERE id='$p'",
                    "SELECT count(*) FROM pragma_foreign_key_check", "SELECT count(*) FROM _sync_dirty_rows",
                    "SELECT count(*) FROM _sync_outbox_rows", "SELECT count(*) FROM _sync_apply_rows",
                    "SELECT count(*) FROM _sync_apply_plan")) assertEquals(0L, scalarLong(db, query))
                assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            }
        } finally {
            clients.forEach { it.closeFromCoordinatedOwner() }
            transports.forEach { it.close() }
            databases.forEach { it.close() }
        }
        println("RECOVERY_HTTP_BODY A1LiveSurvivorsTest.settlementAndPullPreserveGrandchild PASS")
    }
}
