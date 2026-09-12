package dev.goquick.sqlitenow.oversqlite

import io.ktor.client.HttpClient
import io.ktor.client.statement.HttpReceivePipeline
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

internal class A1LiveSurvivorsTest : RealServerSupport() {
    @Test
    fun baselineConfirmedTombstoneReinsertion() = runRealServerTest {
        val config = requireRealServerConfig() ?: return@runRealServerTest
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
        val config = requireRealServerConfig() ?: return
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
    }

    private suspend fun createBaselineTable(db: SafeSQLiteConnection) {
        db.execSQL("CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
    }

    private suspend fun baselineRemoteRows(config: RealServerConfig,user: String,tables: List<SyncTable>): String {
        val db = newDb()
        var http: HttpClient? = null
        var client: DefaultOversqliteClient? = null
        try {
            createBaselineTable(db)
            val source = bootstrapManagedSourceId(db,config.baseUrl,syncTables=tables)
            http = newRealServerHttpClient(config.baseUrl,issueDummySigninToken(config.baseUrl,user,source))
            client = newRealServerClient(db,http,syncTables=tables)
            client.open().getOrThrow();client.attach(user).getOrThrow();client.pullToStable().getOrThrow()
            return scalarText(db,"SELECT json_group_array(json_array(id,name,parent_id)) FROM (SELECT * FROM categories ORDER BY id)")
        } finally { client?.closeFromCoordinatedOwner();http?.close();db.close() }
    }

    private suspend fun baselineLiveVector(db: SafeSQLiteConnection) = baselineQualificationVector(db,listOf("categories"))

    @Test fun baselineSourceRecoverySnapshotPreservesFrozenOwners() = runRealServerTest {
        val config=requireRealServerConfig() ?: return@runRealServerTest
        val db=newDb();val tables=listOf(SyncTable("categories",syncKeyColumnName="id"));val user=randomRealServerId("baseline-recovery");val id=randomRealServerUuid()
        var http:HttpClient?=null;var client:DefaultOversqliteClient?=null;var lose=false
        try {
            createBaselineTable(db);val source=bootstrapManagedSourceId(db,config.baseUrl,syncTables=tables)
            val transport=newRealServerHttpClient(config.baseUrl,issueDummySigninToken(config.baseUrl,user,source))
            http=transport.config {install("BaselineRecoveryResponseLoss") {receivePipeline.intercept(HttpReceivePipeline.Before) {response ->
                if(lose && response.call.request.url.encodedPath=="/sync/push") {assertTrue(response.status.value in 200..299);lose=false;throw CancellationException("lost committed recovery response")}
            } } };transport.close()
            client=newRealServerClient(db,http,syncTables=tables,transientRetryPolicy=OversqliteTransientRetryPolicy(maxAttempts=1))
            client.open().getOrThrow();client.attach(user).getOrThrow();db.execSQL("INSERT INTO categories VALUES('$id','original',NULL)");client.pushPending().getOrThrow()
            db.execSQL("UPDATE categories SET name='frozen' WHERE id='$id'");lose=true
            var failed=false;try {failed=client.pushPending().isFailure} catch(_:CancellationException) {failed=true}
            assertTrue(failed && !lose);val frozen=baselineLiveVector(db)
            assertTrue(frozen.rows("_sync_dirty_rows").isEmpty())
            val original=frozen.row("_sync_row_state",id);val outbox=frozen.row("_sync_outbox_rows",id)
            assertTextPayload(original.payload("server_payload"),mapOf("id" to id,"name" to "original","parent_id" to null))
            assertEquals(original.payload("server_payload"),outbox.payload("base_payload"));assertEquals(original.payload("server_provenance"),outbox.payload("base_provenance"))
            assertEquals("[[\"$id\",\"frozen\",null]]",baselineRemoteRows(config,user,tables))
            val replacement=randomRealServerUuid();markSourceRecoveryRequired(db,replacementSourceId=replacement);client.rebuild().getOrThrow()
            val recovered=baselineLiveVector(db);val a=recovered.row("_sync_row_state",id);val retained=recovered.row("_sync_outbox_rows",id)
            assertTextPayload(a.payload("server_payload"),mapOf("id" to id,"name" to "frozen","parent_id" to null));assertEquals("2",a.text("row_version"))
            assertEquals("snapshot",a.payload("server_provenance")!!.jsonObject["origin"]!!.jsonPrimitive.content)
            for(c in listOf("key_json","wire_key_json","op","base_row_version","base_kind","base_payload","base_provenance","wire_payload","local_payload","row_ordinal")) assertEquals(outbox[c],retained[c],c)
            assertEquals(frozen.rows("_sync_outbox_bundle").single().text("canonical_request_hash"),recovered.rows("_sync_outbox_bundle").single().text("canonical_request_hash"))
            assertEquals(frozen.rows("categories"),recovered.rows("categories"));assertTrue(recovered.rows("_sync_dirty_rows").isEmpty());assertTrue(recovered.rows("_sync_relationships").isEmpty())
            assertBaselineCounters(recovered,replacement,2,2)
            assertEquals("1",recovered.rows("_sync_outbox_bundle").single().text("source_bundle_id"));assertEquals(replacement,recovered.rows("_sync_outbox_bundle").single().text("source_id"))
            assertEquals("[[\"$id\",\"frozen\",null]]",baselineRemoteRows(config,user,tables))
            println("baseline-live: public recovery newer A and immutable original frozen B/wire verified")
        } finally {client?.closeFromCoordinatedOwner();http?.close();db.close()}
    }

    @Test
    fun settlementAndPullPreserveGrandchild() = runRealServerTest {
        val config = requireRealServerConfig() ?: return@runRealServerTest
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
    }
}
