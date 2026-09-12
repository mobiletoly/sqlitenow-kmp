package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.DatabaseMigrations
import dev.goquick.sqlitenow.core.SqliteNowDatabase
import dev.goquick.sqlitenow.core.TransactionMode
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.*
import kotlin.test.*

internal class BaselineQualificationTest : CrossTargetSyncTestSupport() {
    @Test fun cleanupAndAccountGuardsPreserveCompleteReferencedOwners() = runOwnedWebDatabaseTest {
        seeded { db,server,_,client ->
            db.execSQL("UPDATE posts SET title='pending' WHERE id='C'")
            val dirty=baselineQualificationVector(db)
            assertEquals(DetachOutcome.BLOCKED_UNSYNCED_DATA,client.detach().getOrThrow())
            assertEquals(dirty,baselineQualificationVector(db))
            assertTrue(client.attach("different-account").isFailure)
            assertEquals(dirty,baselineQualificationVector(db))
            server.beforePushCommit={throw CancellationException("definitely uncommitted")}
            try {client.pushPending()} catch (_:CancellationException) { }
            val frozen=baselineQualificationVector(db)
            assertTrue(frozen.rows("_sync_outbox_rows").isNotEmpty())
            assertEquals(DetachOutcome.BLOCKED_UNSYNCED_DATA,client.detach().getOrThrow())
            assertEquals(frozen,baselineQualificationVector(db))
            client.pushPending().getOrThrow()
            val settled=baselineQualificationVector(db)
            assertTextPayload(settled.row("_sync_row_state","C").payload("server_payload"),mapOf("id" to "C","user_id" to "P","title" to "pending"))
            assertEquals(DetachOutcome.DETACHED,client.detach().getOrThrow())
            val detached=baselineQualificationVector(db)
            for(t in listOf("users","posts","_sync_row_state","_sync_dirty_rows","_sync_relationships","_sync_outbox_rows")) assertTrue(detached.rows(t).isEmpty(),t)
            val source=detached.rows("_sync_attachment_state").single().text("current_source_id")!!
            assertNotEquals(settled.rows("_sync_attachment_state").single().text("current_source_id"),source)
            assertEquals("anonymous",detached.rows("_sync_attachment_state").single().text("binding_state"))
            assertBaselineCounters(detached,source,1,0)
            client.closeFromCoordinatedOwner()
            val otherServer=MockSyncServer();val otherHttp=otherServer.newHttpClient();val other=newClient(db,otherHttp)
            try {
                other.openAndConnect("user-2").getOrThrow()
                assertTrue(baselineQualificationVector(db).rows("_sync_row_state").isEmpty())
                insertUser(db,"U","new-account")
                val adopted=baselineQualificationVector(db)
                val b=adopted.row("_sync_dirty_rows","U")
                assertEquals("absent",b.text("base_kind"));assertEquals(null,b.payload("base_payload"))
                assertEquals("user-2",b.payload("base_provenance")!!.jsonObject["account_scope"]!!.jsonObject["account_id"]!!.jsonPrimitive.content)
                assertBaselineCounters(adopted,source,1,0)
            } finally {other.closeFromCoordinatedOwner();otherHttp.close()}
            println("baseline-qualification: blocked cleanup preserves all owners; clean detach rotates source and prevents cross-account base reuse")
        }
    }

    @Test fun publishedIntermediateRekeyRetainsItsOwnDeletionBase() = runOwnedWebDatabaseTest {
        seeded { db,server,_,client ->
            val original=baselineQualificationVector(db)
            db.execSQL("UPDATE users SET id='R' WHERE id='Q'");client.pushPending().getOrThrow()
            val published=baselineQualificationVector(db)
            assertEquals("live",published.row("_sync_row_state","R").text("server_kind"))
            assertEquals("2",published.row("_sync_row_state","R").text("row_version"))
            db.execSQL("UPDATE users SET id='S' WHERE id='R'")
            val chained=baselineQualificationVector(db)
            val deletion=chained.row("_sync_dirty_rows","R")
            assertEquals("DELETE",deletion.text("op"));assertEquals("present",deletion.text("base_kind"))
            assertEquals(published.row("_sync_row_state","R").payload("server_payload"),deletion.payload("base_payload"))
            assertEquals(published.row("_sync_row_state","R").payload("server_provenance"),deletion.payload("base_provenance"))
            assertEquals("absent",chained.row("_sync_dirty_rows","S").text("base_kind"))
            assertEquals(published.rows("_sync_row_state"),chained.rows("_sync_row_state").filter {it.text("key_json")!="{\"id\":\"S\"}"})
            val successor=chained.row("_sync_row_state","S")
            assertEquals("unpublished",successor.text("server_kind"));assertEquals("0",successor.text("row_version"));assertEquals(null,successor.payload("server_payload"))
            assertBaselineProvenance(successor,"server_provenance",0)
            assertEquals("never_published",successor.payload("server_provenance")!!.jsonObject["origin"]!!.jsonPrimitive.content)
            assertEquals(original.row("_sync_row_state","C"),chained.row("_sync_row_state","C"))
            client.pushPending().getOrThrow()
            val settled=baselineQualificationVector(db)
            assertEquals("deleted",settled.row("_sync_row_state","R").text("server_kind"))
            assertTextPayload(settled.row("_sync_row_state","S").payload("server_payload"),mapOf("id" to "S","name" to "other"))
            assertTrue(settled.rows("_sync_dirty_rows").isEmpty());assertTrue(settled.rows("_sync_outbox_rows").isEmpty())
            assertTrue("users/id=R" !in server.remoteRowsForBaselineTest());assertTrue("users/id=S" in server.remoteRowsForBaselineTest())
        }
    }

    @Test fun frozenIntermediateRekeyReplaysExactRequestAcrossPhysicalRestart() = runOwnedWebDatabaseTest {
        val path=if(supportsDurableTestDatabaseRestart()) createTempSqliteNowTestDbPath("baseline-frozen-rekey") else null
        try {seeded(path) { db,server,http,client ->
            db.execSQL("UPDATE users SET id='R' WHERE id='Q'")
            server.beforePushResponse={throw CancellationException("committed intermediate response lost")}
            try {client.pushPending()} catch (_:CancellationException) { }
            val frozen=baselineQualificationVector(db)
            db.execSQL("UPDATE users SET id='S' WHERE id='R'")
            val chained=baselineQualificationVector(db)
            assertEquals(frozen.rows("_sync_outbox_rows"),chained.rows("_sync_outbox_rows"));assertEquals(frozen.rows("_sync_outbox_bundle"),chained.rows("_sync_outbox_bundle"))
            val replayDb=if(path==null) db else {client.closeFromCoordinatedOwner();db.close();newFileBackedDb(path)}
            val replay=if(path==null) client else newClient(replayDb,http,resolver=DeferConflictResolver)
            try {
                assertEquals(chained,baselineQualificationVector(replayDb))
                if(path!=null) replay.openAndConnect("user-1").getOrThrow()
                assertEquals(chained,baselineQualificationVector(replayDb))
                replay.pushPending().getOrThrow()
                val acknowledged=baselineQualificationVector(replayDb)
                val deletion=acknowledged.row("_sync_dirty_rows","R")
                assertEquals("DELETE",deletion.text("op"));assertEquals("present",deletion.text("base_kind"))
                assertEquals(acknowledged.row("_sync_row_state","R").payload("server_payload"),deletion.payload("base_payload"))
                assertEquals(acknowledged.row("_sync_row_state","R").payload("server_provenance"),deletion.payload("base_provenance"))
                assertEquals(chained.rows("users"),acknowledged.rows("users"));assertEquals(chained.rows("posts"),acknowledged.rows("posts"))
                assertEquals(server.pushBodies[1],server.pushBodies[2])
                replay.pushPending().getOrThrow()
                val complete=baselineQualificationVector(replayDb)
                assertEquals("deleted",complete.row("_sync_row_state","R").text("server_kind"))
                assertTextPayload(complete.row("_sync_row_state","S").payload("server_payload"),mapOf("id" to "S","name" to "other"))
                assertTrue(complete.rows("_sync_dirty_rows").isEmpty());assertTrue(complete.rows("_sync_outbox_rows").isEmpty())
                assertTrue("users/id=R" !in server.remoteRowsForBaselineTest());assertTrue("users/id=S" in server.remoteRowsForBaselineTest())
                println("baseline-qualification: frozen intermediate exact replay and distinct successor bases established; durable="+(path!=null))
            } finally {if(path!=null) {replay.closeFromCoordinatedOwner();replayDb.close()}}
        }} finally {if(path!=null) deleteTempSqliteNowTestDbArtifacts(path)}
    }

    @Test fun definitelyUncommittedRequeuePreservesNewerIntentAcrossPhysicalRestart() = runOwnedWebDatabaseTest {
        val path=if(supportsDurableTestDatabaseRestart()) createTempSqliteNowTestDbPath("baseline-requeue") else null
        try {seeded(path) { db,server,http,client ->
            val original=baselineQualificationVector(db)
            db.execSQL("UPDATE posts SET title='frozen' WHERE id='C'")
            server.beforePushCommit={throw CancellationException("definitely uncommitted")}
            try {client.pushPending()} catch (_:CancellationException) { }
            val frozen=baselineQualificationVector(db)
            db.execSQL("UPDATE posts SET title='newer' WHERE id='C'")
            val newer=baselineQualificationVector(db)
            assertEquals(original.rows("_sync_row_state"),newer.rows("_sync_row_state"))
            assertEquals(frozen.rows("_sync_outbox_rows"),newer.rows("_sync_outbox_rows"))
            assertEquals("child",Json.parseToJsonElement(server.remoteRowsForBaselineTest().getValue("posts/id=C")).jsonObject["title"]!!.jsonPrimitive.content)
            requeueDefinitelyUncommittedBaselineOutbox(db)
            val queued=baselineQualificationVector(db)
            val b=queued.row("_sync_dirty_rows","C")
            for(c in listOf("base_kind","base_row_version","base_payload","base_provenance")) assertEquals(frozen.rows("_sync_outbox_rows").single()[c],b[c],c)
            assertTextPayload(b.payload("payload"),mapOf("id" to "C","user_id" to "P","title" to "newer"))
            for(t in listOf("users","posts","_sync_row_state","_sync_source_state","_sync_attachment_state")) assertEquals(newer.rows(t),queued.rows(t),t)
            assertTrue(queued.rows("_sync_outbox_rows").isEmpty())
            val replayDb=if(path==null) db else {client.closeFromCoordinatedOwner();db.close();newFileBackedDb(path)}
            val replay=if(path==null) client else newClient(replayDb,http)
            try {
                assertEquals(queued,baselineQualificationVector(replayDb));if(path!=null) replay.openAndConnect("user-1").getOrThrow()
                assertEquals(queued,baselineQualificationVector(replayDb));replay.pushPending().getOrThrow()
                val settled=baselineQualificationVector(replayDb)
                assertTextPayload(settled.row("_sync_row_state","C").payload("server_payload"),mapOf("id" to "C","user_id" to "P","title" to "newer"))
                assertTrue(settled.rows("_sync_dirty_rows").isEmpty());assertTrue(settled.rows("_sync_outbox_rows").isEmpty())
                assertEquals("newer",Json.parseToJsonElement(server.remoteRowsForBaselineTest().getValue("posts/id=C")).jsonObject["title"]!!.jsonPrimitive.content)
                println("baseline-qualification: original historical B/newer L requeue and public settlement established; durable="+(path!=null))
            } finally {if(path!=null) {replay.closeFromCoordinatedOwner();replayDb.close()}}
        }} finally {if(path!=null) deleteTempSqliteNowTestDbArtifacts(path)}
    }

    @Test fun independentlyAcknowledgedOldDeletePreservesSuccessorBaseAcrossPhysicalRestart() = runOwnedWebDatabaseTest {
        val path=if(supportsDurableTestDatabaseRestart()) createTempSqliteNowTestDbPath("baseline-old-delete") else null
        try {seeded(path) { db,server,http,client ->
            val original=baselineQualificationVector(db)
            // Ordinary equivalent captures give R the first ready ordinal;
            // no rename association or old table/key heuristic controls it.
            db.execSQL("INSERT INTO users SELECT 'R',name FROM users WHERE id='Q'")
            db.execSQL("DELETE FROM users WHERE id='Q'")
            val before=baselineQualificationVector(db)
            assertEquals(original.row("_sync_row_state","Q").payload("server_payload"),before.row("_sync_dirty_rows","Q").payload("base_payload"))
            client.closeFromCoordinatedOwner()
            val single=newClient(db,http,maxPushRows=1)
            var restartDb:SafeSQLiteConnection?=null;var restarted:DefaultOversqliteClient?=null
            try {
                single.openAndConnect("user-1").getOrThrow();assertEquals(before,baselineQualificationVector(db))
                single.pushPending().getOrThrow()
                val published=baselineQualificationVector(db)
                assertEquals("live",published.row("_sync_row_state","R").text("server_kind"))
                assertEquals("DELETE",published.row("_sync_dirty_rows","Q").text("op"))
                server.beforePushResponse={throw CancellationException("independent old delete committed response lost")}
                try {single.pushPending()} catch(_:CancellationException) { }
                val frozen=baselineQualificationVector(db)
                assertEquals("{\"id\":\"Q\"}",frozen.rows("_sync_outbox_rows").single().text("key_json"))
                assertEquals("DELETE",frozen.rows("_sync_outbox_rows").single().text("op"))
                db.execSQL("UPDATE users SET id='S' WHERE id='R'")
                val chained=baselineQualificationVector(db)
                assertEquals(frozen.rows("_sync_outbox_rows"),chained.rows("_sync_outbox_rows"))
                single.closeFromCoordinatedOwner()
                val activeDb=if(path==null) db else {db.close();newFileBackedDb(path).also {restartDb=it}}
                val active=newClient(activeDb,http).also {restarted=it}
                assertEquals(chained,baselineQualificationVector(activeDb));active.openAndConnect("user-1").getOrThrow()
                assertEquals(chained,baselineQualificationVector(activeDb));active.pushPending().getOrThrow()
                assertEquals(server.pushBodies[2],server.pushBodies[3])
                val committed=baselineQualificationVector(activeDb)
                assertEquals("deleted",committed.row("_sync_row_state","Q").text("server_kind"))
                assertEquals(published.row("_sync_row_state","R").payload("server_payload"),committed.row("_sync_dirty_rows","R").payload("base_payload"))
                assertEquals(published.row("_sync_row_state","R").payload("server_provenance"),committed.row("_sync_dirty_rows","R").payload("base_provenance"))
                assertEquals("absent",committed.row("_sync_dirty_rows","S").text("base_kind"))
                assertEquals(null,committed.row("_sync_dirty_rows","S").payload("base_payload"))
                assertEquals(chained.rows("users"),committed.rows("users"));assertEquals(chained.rows("posts"),committed.rows("posts"))
                active.closeFromCoordinatedOwner()
                val finalDb=if(path==null) activeDb else {activeDb.close();newFileBackedDb(path).also {restartDb=it}}
                val finalClient=newClient(finalDb,http).also {restarted=it}
                assertEquals(committed,baselineQualificationVector(finalDb));finalClient.openAndConnect("user-1").getOrThrow()
                assertEquals(committed,baselineQualificationVector(finalDb));finalClient.pushPending().getOrThrow()
                val settled=baselineQualificationVector(finalDb)
                assertTextPayload(settled.row("_sync_row_state","S").payload("server_payload"),mapOf("id" to "S","name" to "other"))
                assertEquals("deleted",settled.row("_sync_row_state","R").text("server_kind"))
                assertTrue(settled.rows("_sync_dirty_rows").isEmpty());assertTrue(settled.rows("_sync_outbox_rows").isEmpty())
                assertTrue("users/id=Q" !in server.remoteRowsForBaselineTest());assertTrue("users/id=R" !in server.remoteRowsForBaselineTest());assertTrue("users/id=S" in server.remoteRowsForBaselineTest())
                println("baseline-qualification: independently frozen/committed old-delete retains distinct R/S bases across reopen; durable="+(path!=null))
            } finally {restarted?.closeFromCoordinatedOwner();restartDb?.close();single.closeFromCoordinatedOwner()}
        }} finally {if(path!=null) deleteTempSqliteNowTestDbArtifacts(path)}
    }

    @Test fun targetGenerationSnapshotBaselineIsCompleteBeforeReadyPublication() = runOwnedWebDatabaseTest {
        if(!supportsDurableTestDatabaseRestart()) {
            println("NOT-APPLICABLE: generation replacement across owner close requires durable storage; transient Node supported traces execute separately")
            return@runOwnedWebDatabaseTest
        }
        val path=createTempSqliteNowTestDbPath("baseline-generation")
        fun generation(id:Long)=SyncGeneration(id.toString(),"",listOf(GenerationTable("main","notes",listOf("id"),listOf(GenerationColumn("id","uuid",false),GenerationColumn("title","text",false)),emptyList()))).let {it.copy(fingerprint=it.computeFingerprint())}
        val old=generation(1);val target=generation(2)
        val id="33333333-3333-4333-8333-333333333333"
        val rows=listOf(BundleRow("main","notes",mapOf("id" to id),"INSERT",9,buildJsonObject {put("id",id);put("title","target-authority")}))
        fun authority(targetMode:Boolean)=HttpClient(MockEngine { request ->
            val content=when(request.url.encodedPath) {
                "/sync/generations/1" -> Json.encodeToString(GenerationDiscovery("v1",old,testWireLimits(),false))
                "/sync/generations/2" -> Json.encodeToString(GenerationDiscovery("v1",target,testWireLimits(),false))
                "/sync/connect" -> Json.encodeToString(ConnectResponse(resolution=if(targetMode) "remote_authoritative" else "initialize_empty"))
                "/sync/snapshot-sessions" -> Json.encodeToString(SnapshotSession("baseline-generation-snapshot",9,1,OversqliteCompactCodec.snapshotLogicalByteCount(target,rows),"2099-01-01T00:00:00Z"))
                "/sync/snapshot-sessions/baseline-generation-snapshot" -> if(request.method==HttpMethod.Delete) "" else buildJsonObject {
                    put("rows",OversqliteCompactCodec.encodeBundleRows(target,rows,firstOrdinal=1,snapshot=true));put("next_row_ordinal",1);put("has_more",false);put("logical_byte_count",OversqliteCompactCodec.snapshotLogicalByteCount(target,rows))
                }.toString()
                else -> error("unexpected generation baseline request ${request.url.encodedPath}")
            }
            respond(content,if(request.method==HttpMethod.Delete) HttpStatusCode.NoContent else HttpStatusCode.OK,headersOf(HttpHeaders.ContentType,"application/json"))
        }) { install(ContentNegotiation) {json()};defaultRequest {url("https://sync.test/")} }
        fun database()=SqliteNowDatabase(path,object:DatabaseMigrations {
            override val targetVersion=2
            override suspend fun applyMigration(conn:SafeSQLiteConnection,currentVersion:Int):Int {
                if(currentVersion==-1) conn.execSQL("CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL,title TEXT NOT NULL)")
                return 2
            }
        },syncManaged=true).apply {connectionConfig=createSqliteNowTestConnectionConfig(path)}
        val config=OversqliteConfig(generationId=1,databaseId="fixture",serviceId="main",syncTables=listOf(SyncTable("notes",syncKeyColumnName="id")))
        val oldHttp=authority(false);var initial:OversqliteOpening<SqliteNowDatabase>?=null
        val targetHttp=authority(true);var opening:OversqliteOpening<SqliteNowDatabase>?=null
        try {
            initial=database().openForSync(config,oldHttp);initial.advance();initial.readyOrNull()!!.client.attach("user-1").getOrThrow()
            val oldSource=initial.readyOrNull()!!.client.sourceInfo().getOrThrow().currentSourceId
            initial.close();initial=null
            opening=database().openForSync(config.copy(generationId=2),targetHttp,upgradePlan=OversqliteUpgradePlan("baseline-generation",1,2,2,2,emptySet()))
            assertEquals(null,opening.readyOrNull())
            val outcome=opening.advance();assertEquals(OversqliteOpeningOutcome.READY,outcome.outcome)
            assertTrue(outcome.drainComplete==true)
            val ready=opening.readyOrNull() ?: error("target ready owner required")
            val vector=baselineQualificationVector(ready.database.connection(),listOf("notes"))
            val a=vector.row("_sync_row_state",id)
            assertTextPayload(a.payload("server_payload"),mapOf("id" to id,"title" to "target-authority"));assertEquals("9",a.text("row_version"));assertEquals("live",a.text("server_kind"))
            val p=a.payload("server_provenance")!!.jsonObject
            assertEquals("2",p["generation_id"]!!.jsonPrimitive.content);assertEquals(target.fingerprint,p["generation_fingerprint"]!!.jsonPrimitive.content)
            assertEquals("snapshot",p["origin"]!!.jsonPrimitive.content);assertEquals("9",p["bundle_sequence"]!!.jsonPrimitive.content);assertEquals("9",p["authoritative_version"]!!.jsonPrimitive.content)
            assertEquals("user-1",p["account_scope"]!!.jsonObject["account_id"]!!.jsonPrimitive.content)
            assertEquals("target-authority",vector.rows("notes").single().text("title"))
            for(t in listOf("_sync_dirty_rows","_sync_outbox_rows","_sync_relationships")) assertTrue(vector.rows(t).isEmpty(),t)
            val source=vector.rows("_sync_attachment_state").single().text("current_source_id")!!;assertTrue(source!=oldSource);assertBaselineCounters(vector,source,1,9)
            println("baseline-qualification: generation2 full snapshot A/provenance/L installed before ready; old intent drained")
        } finally {opening?.close();initial?.close();targetHttp.close();oldHttp.close();deleteTempSqliteNowTestDbArtifacts(path)}
    }
    @Test fun adoptionHasAbsentHistoricalBaseBeforeFirstPublication() = runOwnedWebDatabaseTest {
        val db=newDb();val server=MockSyncServer();val http=server.newHttpClient();val client=newClient(db,http)
        try {
            createUsersAndPostsTables(db);insertUser(db,"P","parent");insertPost(db,"C","P","adopted")
            client.openAndConnect("user-1").getOrThrow()
            val adopted=baselineQualificationVector(db)
            for(k in listOf("P","C")) {
                val a=adopted.row("_sync_row_state",k);val b=adopted.row("_sync_dirty_rows",k)
                assertEquals("unpublished",a.text("server_kind"));assertEquals("0",a.text("row_version"));assertEquals(null,a.payload("server_payload"))
                assertEquals("absent",b.text("base_kind"));assertEquals("0",b.text("base_row_version"));assertEquals(null,b.payload("base_payload"))
                assertEquals("never_published",b.payload("base_provenance")!!.jsonObject["origin"]!!.jsonPrimitive.content)
            }
            assertTextPayload(adopted.row("_sync_dirty_rows","C").payload("payload"),mapOf("id" to "C","user_id" to "P","title" to "adopted"))
            assertBaselineEdges(adopted,"server","C",null);assertBaselineEdges(adopted,"desired","C","P")
            assertTrue(adopted.rows("_sync_outbox_rows").isEmpty());assertEquals(0,server.pushRequestCount);assertTrue(server.remoteRowsForBaselineTest().isEmpty())
            val source=adopted.rows("_sync_attachment_state").single().text("current_source_id")!!;assertBaselineCounters(adopted,source,1,0)
            client.pushPending().getOrThrow()
            val published=baselineQualificationVector(db)
            assertTextPayload(published.row("_sync_row_state","C").payload("server_payload"),mapOf("id" to "C","user_id" to "P","title" to "adopted"))
            assertTrue(published.rows("_sync_dirty_rows").isEmpty());assertBaselineEdges(published,"server","C","P");assertBaselineEdges(published,"desired","C",null)
            assertBaselineCounters(published,source,2,1)
            println("baseline-qualification: adoption and first publication complete owners verified")
        } finally {client.closeFromCoordinatedOwner();http.close();db.close()}
    }

    @Test fun validatedCleanSnapshotAndOlderPullNeverFabricateOrRegressBaseline() = runOwnedWebDatabaseTest {
        seeded { db,server,_,client ->
            val original=baselineQualificationVector(db)
            val config=OversqliteConfig(generationId=1,databaseId="fixture",serviceId="main",syncTables=listOf(SyncTable("users",syncKeyColumnName="id"),SyncTable("posts",syncKeyColumnName="id")))
            val validated=SyncRuntimeInitializer(config,TableInfoCache()).validateConfig(db).withGeneration(OversqliteContractStore(db).readGeneration(1))
            val local=OversqliteLocalStore(db,Json) {validated}
            val store=OversqliteSyncStateStore(db) {local}
            db.transaction(TransactionMode.IMMEDIATE) {
                db.execSQL("UPDATE _sync_apply_state SET apply_mode=1")
                db.execSQL("UPDATE posts SET title='newer-authority' WHERE id='C'")
                store.installAuthoritativeBaseline("main","posts","{\"id\":\"C\"}","live",3,local.captureAuthoritativePayload("posts",Json.parseToJsonElement("""{"id":"C","user_id":"P","title":"newer-authority"}""").jsonObject),"explicit_conflict",null)
                db.execSQL("UPDATE _sync_apply_state SET apply_mode=0")
            }
            val newer=baselineQualificationVector(db)
            server.addRemoteBundleForTest(listOf(BundleRow("main","posts",mapOf("id" to "C"),"UPDATE",2,Json.parseToJsonElement("""{"id":"C","user_id":"Q","title":"older-pull"}"""))))
            client.pullToStable().getOrThrow()
            val skipped=baselineQualificationVector(db)
            for(t in listOf("users","posts","_sync_row_state","_sync_relationships","_sync_dirty_rows","_sync_outbox_rows","_sync_source_state")) assertEquals(newer.rows(t),skipped.rows(t),t)
            assertEquals("3",skipped.row("_sync_row_state","C").text("row_version"))
            assertEquals(newer.rows("_sync_attachment_state").single().text("last_bundle_seq_seen")!!.toLong()+1,skipped.rows("_sync_attachment_state").single().text("last_bundle_seq_seen")!!.toLong())
            server.addRemoteBundleForTest(listOf(BundleRow("main","users",mapOf("id" to "Q"),"DELETE",3,null)))
            // Remote mock's current row is older after the historical bundle: explicitly
            // install newer snapshot authority; incremental client A must remain unchanged.
            server.addRemoteBundleForTest(listOf(BundleRow("main","posts",mapOf("id" to "C"),"UPDATE",4,Json.parseToJsonElement("""{"id":"C","user_id":"P","title":"snapshot-complete"}"""))))
            client.rebuild().getOrThrow()
            val snapshot=baselineQualificationVector(db)
            assertEquals(setOf("{\"id\":\"P\"}","{\"id\":\"C\"}"),snapshot.rows("_sync_row_state").map {it.text("key_json")}.toSet())
            assertTextPayload(snapshot.row("_sync_row_state","C").payload("server_payload"),mapOf("id" to "C","user_id" to "P","title" to "snapshot-complete"))
            assertEquals("4",snapshot.row("_sync_row_state","C").text("row_version"));assertBaselineProvenance(snapshot.row("_sync_row_state","C"),"server_provenance",4)
            assertEquals("snapshot-complete",snapshot.rows("posts").single().text("title"));assertEquals(1,snapshot.rows("users").size)
            assertTrue(snapshot.rows("_sync_dirty_rows").isEmpty());assertTrue(snapshot.rows("_sync_outbox_rows").isEmpty())
            assertBaselineEdges(snapshot,"server","C","P");assertBaselineEdges(snapshot,"desired","C",null)
            assertEquals(original.rows("_sync_source_state"),snapshot.rows("_sync_source_state"))
            assertEquals(2,server.remoteRowsForBaselineTest().size)
            println("baseline-qualification: older validated pull skipped and complete snapshot exclusion verified")
        }
    }

    @Test fun mismatchedPullVersionRejectsBeforeAnyBaselineOrCheckpointMutation() = runOwnedWebDatabaseTest {
        seeded {db,server,_,client ->
            val before=baselineQualificationVector(db)
            server.addRemoteBundleForTest(listOf(BundleRow("main","posts",mapOf("id" to "C"),"UPDATE",3,Json.parseToJsonElement("""{"id":"C","user_id":"P","title":"future-version"}"""))))
            val result=client.pullToStable()
            val after=baselineQualificationVector(db)
            println("baseline-invalid-pull: before="+before.tables)
            println("baseline-invalid-pull: after="+after.tables)
            assertTrue(result.isFailure,"row_version=3 inside bundle_seq=2 must reject before mutation; accepted response installed future A and advanced checkpoint")
            assertEquals(before,after,"invalid pull response must not mutate any owner")
        }
    }

    @Test fun malformedLaterPullBundleRejectsBeforeApplyingAnyPageEffects() = runOwnedWebDatabaseTest {
        seeded { db, server, _, client ->
            val before = baselineQualificationVector(db)
            server.addRemoteBundleForTest(listOf(BundleRow("main", "posts", mapOf("id" to "C"), "UPDATE", 2,
                Json.parseToJsonElement("""{"id":"C","user_id":"Q","title":"valid-first-bundle"}"""))))
            server.addRemoteBundleForTest(listOf(BundleRow("main", "posts", mapOf("id" to "C"), "UPDATE", 4,
                Json.parseToJsonElement("""{"id":"C","user_id":"P","title":"invalid-later-bundle"}"""))))
            assertTrue(client.pullToStable().isFailure, "malformed bundle3 must reject the page before applying valid bundle2")
            assertEquals(before, baselineQualificationVector(db), "all business, baseline, ownership and checkpoint state must remain unchanged")
        }
    }

    @Test fun typedBaseIsCompleteAtomicAndNeverAliasedToCurrentLocalState() = runOwnedWebDatabaseTest {
        val db=newDb();val server=MockSyncServer();val http=server.newHttpClient()
        val columns=listOf(GenerationColumn("id","text",false),GenerationColumn("n","int64",false),GenerationColumn("d","decimal",false),GenerationColumn("b","bytes",false),GenerationColumn("j","json",false),GenerationColumn("z","text",true))
        server.generationOverride=SyncGeneration("1","",listOf(GenerationTable("main","typed",listOf("id"),columns,emptyList()))).let {it.copy(fingerprint=it.computeFingerprint())}
        val client=newClient(db,http,syncTables=listOf(SyncTable("typed",syncKeyColumnName="id")))
        try {
            db.execSQL("CREATE TABLE typed(id TEXT PRIMARY KEY NOT NULL,n INTEGER NOT NULL,d TEXT NOT NULL,b BLOB NOT NULL,j TEXT NOT NULL,z TEXT)")
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("""INSERT INTO typed VALUES('T',9223372036854775807,'1234567890.123456789',X'00ff','{"exact":"9007199254740993","nested":{"x":null}}',NULL)""")
            client.pushPending().getOrThrow();val before=baselineQualificationVector(db,listOf("typed"))
            val expected=Json.parseToJsonElement("""{"id":{"storage":"text","value":"T"},"n":{"storage":"integer","value":"9223372036854775807"},"d":{"storage":"text","value":"1234567890.123456789"},"b":{"storage":"blob","value":"00ff"},"j":{"storage":"text","value":"{\"exact\":\"9007199254740993\",\"nested\":{\"x\":null}}"},"z":{"storage":"null","value":null}}""")
            assertEquals(expected,before.row("_sync_row_state","T").payload("server_payload"))
            db.execSQL("""UPDATE typed SET n=1,j='{"replacement":true}' WHERE id='T'""")
            val edited=baselineQualificationVector(db,listOf("typed"));val b=edited.row("_sync_dirty_rows","T")
            assertEquals(expected,b.payload("base_payload"));assertEquals(before.row("_sync_row_state","T").payload("server_provenance"),b.payload("base_provenance"))
            assertEquals(before.rows("_sync_row_state"),edited.rows("_sync_row_state"))
            assertEquals("{\"replacement\":true}",b.payload("payload")!!.jsonObject["j"]!!.jsonObject["value"]!!.jsonPrimitive.content)
            assertEquals("1",edited.rows("typed").single().text("n"))
            for(t in listOf("_sync_relationships","_sync_outbox_rows","_sync_source_state","_sync_attachment_state")) assertEquals(before.rows(t),edited.rows(t),t)
            println("baseline-qualification: complete int64/decimal/blob/atomic JSON/null historical base verified")
        } finally {client.closeFromCoordinatedOwner();http.close();db.close()}
    }
    private suspend fun seeded(path: String? = null, block: suspend (SafeSQLiteConnection, MockSyncServer, HttpClient, DefaultOversqliteClient) -> Unit) {
        val db=if(path==null) newDb() else newFileBackedDb(path);val server=MockSyncServer();val http=server.newHttpClient();val client=newClient(db,http,resolver=DeferConflictResolver)
        try {
            createUsersAndPostsTables(db);client.openAndConnect("user-1").getOrThrow()
            insertUser(db,"P","parent");insertUser(db,"Q","other");insertPost(db,"C","P","child")
            client.pushPending().getOrThrow()
            block(db,server,http,client)
        } finally { client.closeFromCoordinatedOwner();http.close();db.close() }
    }

    @Test fun chainedRekeyKeepsDistinctHistoricalBasesAcrossPhysicalReopenBeforeFreeze() = runOwnedWebDatabaseTest {
        val path=if(supportsDurableTestDatabaseRestart()) createTempSqliteNowTestDbPath("baseline-rekey") else null
        try {
            seeded(path) {db,server,http,client ->
                val initial=baselineQualificationVector(db)
                db.execSQL("UPDATE users SET id='R' WHERE id='Q'");db.execSQL("UPDATE users SET id='S' WHERE id='R'")
                val chain=baselineQualificationVector(db)
                assertEquals(initial.row("_sync_row_state","Q"),chain.row("_sync_row_state","Q"))
                for(k in listOf("Q","R","S")) {
                    val b=chain.row("_sync_dirty_rows",k)
                    if(k=="Q") {
                        assertEquals("present",b.text("base_kind"));assertEquals(initial.row("_sync_row_state","Q").payload("server_payload"),b.payload("base_payload"));assertEquals(initial.row("_sync_row_state","Q").payload("server_provenance"),b.payload("base_provenance"))
                    } else {
                        assertEquals("absent",b.text("base_kind"));assertEquals("0",b.text("base_row_version"));assertEquals(null,b.payload("base_payload"))
                        assertEquals("never_published",b.payload("base_provenance")!!.jsonObject["origin"]!!.jsonPrimitive.content)
                    }
                }
                assertEquals("DELETE",chain.row("_sync_dirty_rows","R").text("op"));assertEquals(null,chain.row("_sync_dirty_rows","R").payload("payload"))
                assertTextPayload(chain.row("_sync_dirty_rows","S").payload("payload"),mapOf("id" to "S","name" to "other"))
                assertEquals(initial.rows("_sync_source_state"),chain.rows("_sync_source_state"));assertEquals(initial.rows("_sync_attachment_state"),chain.rows("_sync_attachment_state"))
                assertBaselineEdges(chain,"server","C","P");assertBaselineEdges(chain,"desired","C",null)
                assertEquals(1,server.pushRequestCount)
                val replayDb=if(path==null) db else {client.closeFromCoordinatedOwner();db.close();newFileBackedDb(path)}
                val replay=if(path==null) client else newClient(replayDb,http,resolver=DeferConflictResolver)
                try {
                    assertEquals(chain,baselineQualificationVector(replayDb))
                    if(path!=null) replay.openAndConnect("user-1").getOrThrow()
                    assertEquals(chain,baselineQualificationVector(replayDb))
                    val remoteBefore=server.remoteRowsForBaselineTest()
                    val source=chain.rows("_sync_attachment_state").single().text("current_source_id")!!
                    server.beforePushCommit={throw CancellationException("hold unpublished rekey before remote mutation")}
                    assertFailsWith<CancellationException> {replay.pushPending()}
                    val frozen=baselineQualificationVector(replayDb)
                    assertEquals(setOf("{\"id\":\"Q\"}","{\"id\":\"S\"}"),frozen.rows("_sync_outbox_rows").map {it.text("key_json")}.toSet())
                    assertEquals(2,frozen.rows("_sync_outbox_rows").size)
                    assertTrue(frozen.rows("_sync_dirty_rows").isEmpty())
                    for(t in listOf("users","posts","_sync_row_state","_sync_relationships","_sync_source_state","_sync_attachment_state","_sync_operation_state")) assertEquals(chain.rows(t),frozen.rows(t),t)
                    val conflictBefore=chain.rows("_sync_conflict_state").single();val conflictFrozen=frozen.rows("_sync_conflict_state").single()
                    for((column,value) in conflictBefore) {
                        if(column=="dirty_revision") assertEquals((conflictBefore.text(column)!!.toLong()+3).toString(),conflictFrozen.text(column))
                        else assertEquals(value,conflictFrozen[column],column)
                    }
                    for(key in listOf("Q","S")) {
                        val b=chain.row("_sync_dirty_rows",key);val o=frozen.row("_sync_outbox_rows",key)
                        for(column in listOf("op","base_kind","base_row_version","base_payload","base_provenance")) assertEquals(b[column],o[column],"$key/$column")
                        assertEquals(b["payload"],o["local_payload"])
                    }
                    val request=Json.decodeFromString(CompletePushRequest.serializer(),server.pushBodies[1])
                    val generation=testGeneration(server.registeredTableSpecs)
                    val rows=OversqliteCompactCodec.decodeBundleRows(generation,request.rows)
                    assertEquals(2,rows.size)
                    val byKey=rows.associateBy {it.key.getValue("id")}
                    assertEquals(setOf("Q","S"),byKey.keys)
                    assertEquals("DELETE",byKey.getValue("Q").op);assertEquals(1L,byKey.getValue("Q").rowVersion);assertEquals(null,byKey.getValue("Q").payload)
                    assertEquals("INSERT",byKey.getValue("S").op);assertEquals(0L,byKey.getValue("S").rowVersion)
                    assertEquals(Json.parseToJsonElement("""{"id":"S","name":"other"}"""),byKey.getValue("S").payload)
                    assertEquals(byKey.getValue("S").payload,frozen.row("_sync_outbox_rows","S").payload("wire_payload"))
                    val logical=rows.map {PushRequestRow(it.schema,it.table,it.key,it.op,it.rowVersion,it.payload)}
                    assertEquals(computePushRequestHashForTest(logical,generation),request.canonicalRequestHash)
                    assertEquals(request.canonicalRequestHash,frozen.rows("_sync_outbox_bundle").single().text("canonical_request_hash"))
                    assertEquals("2",frozen.rows("_sync_outbox_bundle").single().text("row_count"))
                    assertEquals(source,frozen.rows("_sync_outbox_bundle").single().text("source_id"))
                    assertEquals(2L,request.sourceBundleId);assertBaselineCounters(frozen,source,2,1)
                    assertEquals(remoteBefore,server.remoteRowsForBaselineTest())
                    replay.pushPending().getOrThrow()
                    assertEquals(server.pushBodies[1],server.pushBodies[2]);assertEquals(3,server.pushRequestCount)
                    val settled=baselineQualificationVector(replayDb)
                    assertTrue(settled.rows("_sync_dirty_rows").isEmpty());assertTrue(settled.rows("_sync_outbox_rows").isEmpty())
                    assertEquals("none",settled.rows("_sync_outbox_bundle").single().text("state"))
                    assertEquals("0",settled.rows("_sync_outbox_bundle").single().text("row_count"));assertEquals("",settled.rows("_sync_outbox_bundle").single().text("canonical_request_hash"))
                    for(t in listOf("users","posts","_sync_relationships","_sync_operation_state")) assertEquals(chain.rows(t),settled.rows(t),t)
                    val conflictSettled=settled.rows("_sync_conflict_state").single()
                    for((column,value) in conflictFrozen) {
                        if(column=="baseline_revision") assertEquals((conflictFrozen.text(column)!!.toLong()+2).toString(),conflictSettled.text(column))
                        else assertEquals(value,conflictSettled[column],column)
                    }
                    assertEquals(chain.row("_sync_row_state","R"),settled.row("_sync_row_state","R"))
                    assertEquals("deleted",settled.row("_sync_row_state","Q").text("server_kind"))
                    assertEquals("2",settled.row("_sync_row_state","Q").text("row_version"));assertBaselineProvenance(settled.row("_sync_row_state","Q"),"server_provenance",2)
                    assertTextPayload(settled.row("_sync_row_state","S").payload("server_payload"),mapOf("id" to "S","name" to "other"))
                    assertEquals("2",settled.row("_sync_row_state","S").text("row_version"));assertBaselineProvenance(settled.row("_sync_row_state","S"),"server_provenance",2)
                    assertBaselineCounters(settled,source,3,2)
                    val expectedRemote=remoteBefore.filterKeys {it!="users/id=Q"}+mapOf("users/id=S" to """{"id":"S","name":"other"}""")
                    assertEquals(expectedRemote.mapValues {Json.parseToJsonElement(it.value)},server.remoteRowsForBaselineTest().mapValues {Json.parseToJsonElement(it.value)})
                    assertTrue(server.remoteRowsForBaselineTest().keys.none {it=="users/id=R"})
                    println("baseline-unpublished-rekey: frozen Q/S only, distinct B/wire/hash, exact replay, counters and remote exclusion verified; durable="+(path!=null))
                } finally {if(path!=null) {replay.closeFromCoordinatedOwner();replayDb.close()}}
            }
        } finally {if(path!=null) deleteTempSqliteNowTestDbArtifacts(path)}
    }

    @Test fun localReparentDeleteAndUnpublishedNoopKeepCompleteHistoricalOwners() = runOwnedWebDatabaseTest {
        seeded { db,server,_,client ->
            val before=baselineQualificationVector(db)
            val source=before.rows("_sync_attachment_state").single().text("current_source_id")!!
            db.execSQL("UPDATE posts SET user_id='Q',title='first' WHERE id='C'")
            db.execSQL("UPDATE posts SET title='second' WHERE id='C'")
            val edited=baselineQualificationVector(db)
            assertEquals(before.rows("_sync_row_state"),edited.rows("_sync_row_state"))
            val dirty=edited.rows("_sync_dirty_rows").single()
            assertEquals(before.row("_sync_row_state","C").payload("server_payload"),dirty.payload("base_payload"))
            assertEquals(before.row("_sync_row_state","C").payload("server_provenance"),dirty.payload("base_provenance"))
            assertTextPayload(dirty.payload("payload"),mapOf("id" to "C","user_id" to "Q","title" to "second"))
            assertBaselineEdges(edited,"server","C","P");assertBaselineEdges(edited,"desired","C","Q")
            db.execSQL("DELETE FROM posts WHERE id='C'")
            val deleted=baselineQualificationVector(db)
            for(c in listOf("base_kind","base_row_version","base_payload","base_provenance","dirty_ordinal")) assertEquals(dirty[c],deleted.rows("_sync_dirty_rows").single()[c])
            assertEquals("DELETE",deleted.rows("_sync_dirty_rows").single().text("op"))
            assertEquals(null,deleted.rows("_sync_dirty_rows").single().payload("payload"))
            assertEquals(before.rows("_sync_row_state"),deleted.rows("_sync_row_state"))
            assertBaselineEdges(deleted,"server","C","P");assertBaselineEdges(deleted,"desired","C",null)
            assertBaselineCounters(deleted,source,2,1)
            assertEquals("child",Json.parseToJsonElement(server.remoteRowsForBaselineTest().getValue("posts/id=C")).jsonObject["title"]!!.jsonPrimitive.content)
            client.pushPending().getOrThrow()
            val clean=baselineQualificationVector(db)
            val requests=server.pushRequestCount
            insertUser(db,"N","unpublished");db.execSQL("DELETE FROM users WHERE id='N'")
            val noop=baselineQualificationVector(db)
            assertEquals("unpublished",noop.row("_sync_row_state","N").text("server_kind"))
            assertEquals("absent",noop.row("_sync_dirty_rows","N").text("base_kind"))
            assertEquals(null,noop.row("_sync_dirty_rows","N").payload("base_payload"))
            client.pushPending().getOrThrow()
            val pruned=baselineQualificationVector(db)
            assertTrue(pruned.rows("_sync_dirty_rows").isEmpty());assertTrue(pruned.rows("_sync_outbox_rows").isEmpty())
            for(t in listOf("users","posts","_sync_source_state","_sync_attachment_state")) assertEquals(clean.rows(t),pruned.rows(t),t)
            assertEquals(requests,server.pushRequestCount)
            assertTrue(server.remoteRowsForBaselineTest().keys.none { it.endsWith("id=N") })
            println("baseline-qualification: local-reparent/delete and unpublished-noop complete owners verified")
        }
    }

    @Test fun normalizedSettlementAndExtraEffectRebaseOnlyAffectedIntent() = runOwnedWebDatabaseTest {
        seeded { db,server,_,client ->
            val before=baselineQualificationVector(db)
            val source=before.rows("_sync_attachment_state").single().text("current_source_id")!!
            db.execSQL("CREATE TRIGGER disturb_unrelated AFTER UPDATE ON posts WHEN NEW.title='normalized' BEGIN UPDATE users SET name='trigger-disturbance' WHERE id='P'; END")
            db.execSQL("UPDATE posts SET title='frozen' WHERE id='C'")
            server.normalizePushEffects={ effects -> effects.map { r -> if(r.table=="posts") r.copy(payload=Json.parseToJsonElement("""{"id":"C","user_id":"Q","title":"normalized"}""")) else r } }
            server.extraPushEffects={ _,seq -> listOf(BundleRow("main","users",mapOf("id" to "Q"),"UPDATE",seq,Json.parseToJsonElement("""{"id":"Q","name":"extra-effect"}"""))) }
            server.beforePushResponse={
                db.execSQL("UPDATE posts SET title='newer' WHERE id='C'")
                db.execSQL("UPDATE users SET name='unrelated-local' WHERE id='P'")
                db.execSQL("UPDATE users SET name='affected-local' WHERE id='Q'")
                throw CancellationException("committed normalized response lost")
            }
            try {client.pushPending()} catch(_:CancellationException) { }
            val frozen=baselineQualificationVector(db)
            assertEquals(before.rows("_sync_row_state"),frozen.rows("_sync_row_state"))
            val unrelated=frozen.row("_sync_dirty_rows","P")
            val outbox=frozen.rows("_sync_outbox_rows").single()
            assertTextPayload(outbox.payload("local_payload"),mapOf("id" to "C","user_id" to "P","title" to "frozen"))
            assertEquals(before.row("_sync_row_state","C").payload("server_payload"),outbox.payload("base_payload"))
            client.pushPending().getOrThrow()
            val settled=baselineQualificationVector(db)
            assertTextPayload(settled.row("_sync_row_state","C").payload("server_payload"),mapOf("id" to "C","user_id" to "Q","title" to "normalized"))
            assertTextPayload(settled.row("_sync_row_state","Q").payload("server_payload"),mapOf("id" to "Q","name" to "extra-effect"))
            for(k in listOf("C","Q")) {
                val a=settled.row("_sync_row_state",k);val b=settled.row("_sync_dirty_rows",k)
                assertEquals(a.payload("server_payload"),b.payload("base_payload"));assertEquals(a.payload("server_provenance"),b.payload("base_provenance"))
                assertEquals("2",a.text("row_version"));assertBaselineProvenance(a,"server_provenance",2)
            }
            assertEquals(before.row("_sync_row_state","P"),settled.row("_sync_row_state","P"))
            assertEquals(unrelated,settled.row("_sync_dirty_rows","P"))
            assertEquals(frozen.rows("users"),settled.rows("users"));assertEquals(frozen.rows("posts"),settled.rows("posts"))
            assertBaselineEdges(settled,"server","C","Q");assertBaselineEdges(settled,"desired","C","P")
            assertBaselineCounters(settled,source,3,2);assertTrue(settled.rows("_sync_outbox_rows").isEmpty())
            assertEquals(server.pushBodies[1],server.pushBodies[2])
            assertEquals("normalized",Json.parseToJsonElement(server.remoteRowsForBaselineTest().getValue("posts/id=C")).jsonObject["title"]!!.jsonPrimitive.content)
            val again=baselineQualificationVector(db)
            assertEquals(settled,again,"observation cannot advance settlement twice")
            println("baseline-qualification: normalized settlement/extra-effect/trigger full A/B/L verified")
        }
    }

    @Test fun deferredConflictPreservesBaselineUntilExplicitKeepLocalRebase() = runOwnedWebDatabaseTest {
        seeded { db,server,_,client ->
            db.execSQL("UPDATE posts SET title='desired' WHERE id='C'")
            db.execSQL("UPDATE users SET name='unrelated' WHERE id='Q'")
            val before=baselineQualificationVector(db)
            val remote=Json.parseToJsonElement("""{"id":"C","user_id":"P","title":"remote"}""")
            server.conflictOverride={ r,_,_ -> if(r.table=="posts") PushConflictDetails(r.schema,r.table,r.key,r.op,r.baseRowVersion,2,false,remote) else null }
            assertTrue(client.pushPending().isFailure)
            val pending=baselineQualificationVector(db)
            assertEquals(before.rows("_sync_row_state"),pending.rows("_sync_row_state"))
            for(t in listOf("users","posts","_sync_source_state","_sync_attachment_state")) assertEquals(before.rows(t),pending.rows(t),t)
            assertTrue(pending.rows("_sync_dirty_rows").isEmpty())
            assertBaselineEdges(pending,"server","C","P");assertBaselineEdges(pending,"desired","C",null)
            for(k in listOf("C","Q")) {
                val b=before.row("_sync_dirty_rows",k);val frozen=pending.row("_sync_outbox_rows",k)
                for(c in listOf("base_kind","base_row_version","base_payload","base_provenance")) assertEquals(b[c],frozen[c])
                assertEquals(b.payload("payload"),frozen.payload("local_payload"))
            }
            val handle=client.pendingConflict().getOrThrow() ?: error("typed pending conflict required")
            client.resolveConflict(handle,MergeResult.KeepLocal).getOrThrow()
            val resolved=baselineQualificationVector(db)
            assertTextPayload(resolved.row("_sync_row_state","C").payload("server_payload"),mapOf("id" to "C","user_id" to "P","title" to "remote"))
            assertEquals("2",resolved.row("_sync_row_state","C").text("row_version"))
            assertEquals(resolved.row("_sync_row_state","C").payload("server_payload"),resolved.row("_sync_dirty_rows","C").payload("base_payload"))
            assertEquals(resolved.row("_sync_row_state","C").payload("server_provenance"),resolved.row("_sync_dirty_rows","C").payload("base_provenance"))
            assertTextPayload(resolved.row("_sync_dirty_rows","C").payload("payload"),mapOf("id" to "C","user_id" to "P","title" to "desired"))
            assertEquals(before.row("_sync_row_state","Q"),resolved.row("_sync_row_state","Q"))
            for(c in listOf("base_kind","base_row_version","base_payload","base_provenance","payload")) assertEquals(before.row("_sync_dirty_rows","Q")[c],resolved.row("_sync_dirty_rows","Q")[c])
            for(t in listOf("users","posts","_sync_source_state","_sync_attachment_state")) assertEquals(before.rows(t),resolved.rows(t),t)
            assertBaselineEdges(resolved,"server","C","P");assertBaselineEdges(resolved,"desired","C","P")
            assertTrue(resolved.rows("_sync_outbox_rows").isEmpty())
            println("baseline-qualification: defer then explicit KeepLocal full rebase and unrelated owners verified")
        }
    }

    @Test fun sourceRecoverySnapshotPreservesOriginalFrozenBaseAndWire() = runOwnedWebDatabaseTest {
        seeded { db,server,_,client ->
            val initial=baselineQualificationVector(db)
            val source=initial.rows("_sync_attachment_state").single().text("current_source_id")!!
            assertBaselineCounters(initial,source,2,1)
            val original=initial.row("_sync_row_state","C")
            assertTextPayload(original.payload("server_payload"),mapOf("id" to "C","user_id" to "P","title" to "child"))
            assertBaselineProvenance(original,"server_provenance",1)
            db.execSQL("UPDATE posts SET title='frozen' WHERE id='C'")
            server.beforePushResponse={ throw CancellationException("committed response lost") }
            try { client.pushPending() } catch (_: CancellationException) { }
            val frozen=baselineQualificationVector(db)
            assertEquals(initial.rows("_sync_row_state"),frozen.rows("_sync_row_state"))
            assertBaselineCounters(frozen,source,2,1)
            val outbox=frozen.rows("_sync_outbox_rows").single()
            assertEquals(original.payload("server_payload"),outbox.payload("base_payload"))
            assertEquals(original.payload("server_provenance"),outbox.payload("base_provenance"))
            assertTextPayload(outbox.payload("local_payload"),mapOf("id" to "C","user_id" to "P","title" to "frozen"))
            assertTrue(frozen.rows("_sync_dirty_rows").isEmpty(), "direct recovery requires no unfrozen dirty work")
            assertBaselineEdges(frozen,"server","C","P");assertBaselineEdges(frozen,"desired","C",null)
            assertEquals("frozen",Json.parseToJsonElement(server.remoteRowsForBaselineTest().getValue("posts/id=C")).jsonObject.getValue("title").jsonPrimitive.content)
            server.addRemoteBundleForTest(listOf(BundleRow("main","posts",mapOf("id" to "C"),"UPDATE",3,Json.parseToJsonElement("""{"id":"C","user_id":"Q","title":"snapshot-newer"}"""))))
            markSourceRecoveryRequired(db,replacementSourceId="qualification-replacement")
            client.rebuild().getOrThrow()
            val recovered=baselineQualificationVector(db)
            println("baseline-recovery-complete-vector: " + recovered.tables)
            val authoritative=recovered.row("_sync_row_state","C")
            assertTextPayload(authoritative.payload("server_payload"),mapOf("id" to "C","user_id" to "Q","title" to "snapshot-newer"))
            assertEquals("3",authoritative.text("row_version"))
            assertBaselineProvenance(authoritative,"server_provenance",3)
            val retained=recovered.rows("_sync_outbox_rows").single()
            for(column in listOf("key_json","wire_key_json","op","base_row_version","base_kind","base_payload","base_provenance","wire_payload","local_payload")) assertEquals(outbox.getValue(column),retained.getValue(column),"immutable frozen $column across recovery")
            assertEquals(frozen.rows("_sync_outbox_bundle").single().text("canonical_request_hash"),recovered.rows("_sync_outbox_bundle").single().text("canonical_request_hash"))
            assertEquals(frozen.rows("posts"),recovered.rows("posts"),"snapshot must replay frozen L")
            assertBaselineEdges(recovered,"server","C","Q");assertBaselineEdges(recovered,"desired","C",null)
            assertBaselineCounters(recovered,"qualification-replacement",2,3)
            println("baseline-qualification: source recovery full frozen B/wire and newer A and frozen L verified")
        }
    }
}
