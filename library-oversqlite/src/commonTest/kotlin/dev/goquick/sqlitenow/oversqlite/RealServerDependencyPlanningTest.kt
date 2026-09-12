package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import io.ktor.client.request.HttpSendPipeline
import io.ktor.http.HttpHeaders
import io.ktor.http.content.OutgoingContent
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

internal class RealServerDependencyPlanningTest : RealServerSupport() {
    @Test
    fun reparentBeyondActualHardLookaheadThroughPublicPush() = runRealServerTest {
        if (!realServerEnv("OVERSQLITE_REALSERVER_TESTS").equals("true",ignoreCase=true)) {
            println("NOT-APPLICABLE: narrow A2 dependency witness requires its isolated enabled authority; no live pass claimed")
            return@runRealServerTest
        }
        val server=assertNotNull(requireRealServerConfig(),"narrow live authority must be enabled and ready")
        check(supportsDurableTestDatabaseRestart()) {"narrow live witness requires physical storage"}
        val path=createTempSqliteNowTestDbPath("dependency-live")
        val db=newFileBackedDb(path)
        val mirrorDb=newDb()
        val tables=listOf(SyncTable("categories",syncKeyColumnName="id"))
        db.execSQL("PRAGMA foreign_keys=ON")
        val ddl="CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) DEFERRABLE INITIALLY DEFERRED)"
        db.execSQL(ddl);mirrorDb.execSQL(ddl)
        val user=randomRealServerId("a2-kotlin")
        val source=bootstrapManagedSourceId(db,server.baseUrl,syncTables=tables)
        val mirrorSource=bootstrapManagedSourceId(mirrorDb,server.baseUrl,syncTables=tables)
        val baseHttp=newRealServerHttpClient(server.baseUrl,issueDummySigninToken(server.baseUrl,user,source))
        val mirrorHttp=newRealServerHttpClient(server.baseUrl,issueDummySigninToken(server.baseUrl,user,mirrorSource))
        val bodies=mutableListOf<String>()
        val http=baseHttp.config {
          install("A2DependencyWireObserver") {
            sendPipeline.intercept(HttpSendPipeline.Monitoring) {
            if(context.url.build().encodedPath=="/sync/push") {
                val content=subject as OutgoingContent.ByteArrayContent
                val bytes=content.bytes()
                val decoded=if(context.headers[HttpHeaders.ContentEncoding]=="gzip") decodeOversqliteSingleMemberGzip(bytes,1024*1024) else bytes
                bodies+=decoded.decodeToString()
            }
            proceed()
            }
          }
        }
        val client=newRealServerClient(db,http,syncTables=tables,maxPushRows=2)
        val mirror=newRealServerClient(mirrorDb,mirrorHttp,syncTables=tables,maxPushRows=2)
        try {
            client.openAndConnect(user).getOrThrow()
            val ids=List(8) {randomRealServerUuid()}
            val (parent,other,child,clean)=ids.take(4)
            db.execSQL("INSERT INTO categories VALUES('$parent','parent',NULL),('$other','other',NULL),('$child','child','$parent'),('$clean','clean','$other')")
            for(i in 4..7) db.execSQL("INSERT INTO categories VALUES('${ids[i]}','$i',NULL)")
            repeat(4) {client.pushPending().getOrThrow()}
            assertEquals(4,bodies.size)
            mirror.openAndConnect(user).getOrThrow()
            db.execSQL("BEGIN")
            db.execSQL("DELETE FROM categories WHERE id='$parent'")
            for(i in 4..7) db.execSQL("UPDATE categories SET name=name||'!' WHERE id='${ids[i]}'")
            db.execSQL("UPDATE categories SET parent_id='$other' WHERE id='$child'")
            db.execSQL("COMMIT")
            val parentOrdinal=scalarLong(db,"""SELECT dirty_ordinal FROM _sync_dirty_rows WHERE key_json='{"id":"$parent"}'""")
            val childOrdinal=scalarLong(db,"""SELECT dirty_ordinal FROM _sync_dirty_rows WHERE key_json='{"id":"$child"}'""")
            assertEquals(5L,childOrdinal-parentOrdinal);assertTrue(childOrdinal-parentOrdinal>3)
            val before=baselineQualificationVector(db,listOf("categories"));val cleanBefore=before.row("_sync_row_state",clean)
            repeat(3) {call ->
                client.pushPending().getOrThrow();assertEquals(5+call,bodies.size)
                mirror.pullToStable().getOrThrow()
                val local=baselineQualificationVector(db,listOf("categories"))
                val remote=baselineQualificationVector(mirrorDb,listOf("categories"))
                assertEquals(cleanBefore,local.row("_sync_row_state",clean));assertTrue(local.rows("_sync_outbox_rows").isEmpty())
                val wire=json.parseToJsonElement(bodies.last()).jsonObject
                assertEquals((5+call).toString(),wire.getValue("source_bundle_id").jsonPrimitive.content)
                assertEquals(64,wire.getValue("canonical_request_hash").jsonPrimitive.content.length)
                if(call<2) {
                    assertEquals(parent,scalarText(mirrorDb,"SELECT parent_id FROM categories WHERE id='$child'"))
                    assertEquals(1L,scalarLong(mirrorDb,"SELECT COUNT(*) FROM categories WHERE id='$parent'"))
                    assertEquals(before.row("_sync_dirty_rows",child),local.row("_sync_dirty_rows",child))
                } else {
                    assertEquals(other,scalarText(mirrorDb,"SELECT parent_id FROM categories WHERE id='$child'"))
                    assertEquals(0L,scalarLong(mirrorDb,"SELECT COUNT(*) FROM categories WHERE id='$parent'"))
                }
                println("A2-03 real Kotlin call=${call+1} selected=${bodies.last()} local=${local.tables} authoritativeMirror=${remote.tables}")
            }
            assertTrue(baselineQualificationVector(db,listOf("categories")).rows("_sync_dirty_rows").isEmpty())
            assertEquals(0L,scalarLong(db,"SELECT COUNT(*) FROM pragma_foreign_key_check"))
        } finally {
            client.closeFromCoordinatedOwner();mirror.closeFromCoordinatedOwner();http.close();baseHttp.close();mirrorHttp.close()
            db.close();mirrorDb.close();deleteTempSqliteNowTestDbArtifacts(path)
        }
    }

    private data class PublicSyncFixture(
        val path: String, val user: String, val baseUrl: String,
        var db: SafeSQLiteConnection, var client: DefaultOversqliteClient,
        val mirrorDb: SafeSQLiteConnection, val mirror: DefaultOversqliteClient,
        val http: HttpClient, val bodies: MutableList<String>,
    )

    // Mechanical real-authority/file/client setup only; named owners below
    // declare the transition and expected outcome of their one public call.
    private suspend fun withPublicSyncFixture(body: suspend (PublicSyncFixture) -> Unit) {
        val server=assertNotNull(requireRealServerConfig(),"A2-05 live authority must be enabled and ready")
        check(supportsDurableTestDatabaseRestart()) {"A2-05 scoped JVM witness requires durable storage; Node transient"}
        val path=createTempSqliteNowTestDbPath("a205-public-sync")
        val db=newFileBackedDb(path);val mirrorDb=newDb()
        val tables=listOf(SyncTable("categories",syncKeyColumnName="id"))
        val ddl="CREATE TABLE categories(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES categories(id) DEFERRABLE INITIALLY DEFERRED)"
        db.execSQL("PRAGMA foreign_keys=ON");db.execSQL(ddl);mirrorDb.execSQL(ddl)
        val user=randomRealServerId("a205-kotlin")
        val source=bootstrapManagedSourceId(db,server.baseUrl,syncTables=tables)
        val mirrorSource=bootstrapManagedSourceId(mirrorDb,server.baseUrl,syncTables=tables)
        val mirrorHttp=newRealServerHttpClient(server.baseUrl,issueDummySigninToken(server.baseUrl,user,mirrorSource))
        val mirror=newRealServerClient(mirrorDb,mirrorHttp,syncTables=tables,maxPushRows=2)
        val bodies=mutableListOf<String>()
        lateinit var fixture: PublicSyncFixture
        val baseHttp=newRealServerHttpClient(server.baseUrl,issueDummySigninToken(server.baseUrl,user,source),afterResponse={route ->
            if(route=="/sync/push") {
                mirror.pullToStable().getOrThrow()
                println("A2-05 authoritativeMirror=${baselineQualificationVector(mirrorDb,listOf("categories")).tables}")
            }
        })
        val http=baseHttp.config {
            install("A205PublicSyncWireObserver") {
                sendPipeline.intercept(HttpSendPipeline.Monitoring) {
                    if(context.url.build().encodedPath=="/sync/push") {
                        val bytes=(subject as OutgoingContent.ByteArrayContent).bytes()
                        val decoded=if(context.headers[HttpHeaders.ContentEncoding]=="gzip") decodeOversqliteSingleMemberGzip(bytes,1024*1024) else bytes
                        bodies+=decoded.decodeToString()
                        println("A2-05 pre-push local=${baselineQualificationVector(fixture.db,listOf("categories")).tables} wire=${bodies.last()}")
                    }
                    proceed()
                }
            }
        }
        fixture=PublicSyncFixture(path,user,server.baseUrl,db,newRealServerClient(db,http,syncTables=tables,maxPushRows=2),mirrorDb,mirror,http,bodies)
        try {
            mirror.openAndConnect(user).getOrThrow();fixture.client.openAndConnect(user).getOrThrow();body(fixture)
        } finally {
            fixture.client.closeFromCoordinatedOwner();mirror.closeFromCoordinatedOwner();http.close();baseHttp.close();mirrorHttp.close()
            fixture.db.close();mirrorDb.close();deleteTempSqliteNowTestDbArtifacts(path)
        }
    }

    private suspend fun assertPublicSyncBatches(f:PublicSyncFixture,start:Int,next:Long,count:Int) {
        assertEquals(start+count,f.bodies.size)
        for((index,raw) in f.bodies.drop(start).withIndex()) {
            val wire=json.parseToJsonElement(raw).jsonObject
            val rowCount=wire.getValue("rows").jsonArray.sumOf {it.jsonArray[2].jsonArray.size}
            assertTrue(rowCount in 1..2);assertEquals((next+index).toString(),wire.getValue("source_bundle_id").jsonPrimitive.content)
            assertEquals(64,wire.getValue("canonical_request_hash").jsonPrimitive.content.length)
        }
        val v=baselineQualificationVector(f.db,listOf("categories"));val source=v.rows("_sync_attachment_state").single().text("current_source_id")!!
        assertBaselineCounters(v,source,next+count,next+count-1)
    }
    private suspend fun assertPublicSyncDrained(f:PublicSyncFixture) {
        val v=baselineQualificationVector(f.db,listOf("categories"))
        assertTrue(v.rows("_sync_dirty_rows").isEmpty());assertTrue(v.rows("_sync_outbox_rows").isEmpty())
        assertEquals("none",v.rows("_sync_outbox_bundle").single().text("state"))
        assertEquals(0L,scalarLong(f.db,"SELECT COUNT(*) FROM pragma_foreign_key_check"))
    }
    private suspend fun publicSyncNext(f:PublicSyncFixture):Long = scalarLong(f.db,"SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id=(SELECT current_source_id FROM _sync_attachment_state)")

    @Test
    fun publicSyncReparentBeyondActualHardLookahead() = runRealServerTest {
        if(!realServerEnv("OVERSQLITE_REALSERVER_TESTS").equals("true",ignoreCase=true)) {println("NOT-APPLICABLE: A2-05 live authority disabled; no live pass claimed");return@runRealServerTest}
        for(newParent in listOf(false,true)) withPublicSyncFixture { f ->
            val db=f.db;val ids=List(9){randomRealServerUuid()};val(p,q,c,d)=ids.take(4)
            db.execSQL("INSERT INTO categories VALUES('$p','parent',NULL),('$q','other',NULL),('$c','child','$p'),('$d','clean','$c')")
            for(i in 4..7) db.execSQL("INSERT INTO categories VALUES('${ids[i]}','$i',NULL)")
            f.client.sync().getOrThrow();assertPublicSyncDrained(f);assertEquals(4,f.bodies.size)
            val seeded=baselineQualificationVector(db,listOf("categories"));val clean=seeded.row("_sync_row_state",d)
            val cleanBusiness=scalarText(db,"SELECT json_array(id,name,parent_id) FROM categories WHERE id='$d'")
            val cleanEdges=seeded.rows("_sync_relationships").filter{it.text("child_key")=="{\"id\":\"$d\"}"}
            db.execSQL("BEGIN");db.execSQL("DELETE FROM categories WHERE id='$p'")
            for(i in 4..7) db.execSQL("UPDATE categories SET name=name||'!' WHERE id='${ids[i]}'")
            val parent=if(newParent) ids[8] else q
            db.execSQL("UPDATE categories SET parent_id='$parent' WHERE id='$c'")
            if(newParent) db.execSQL("INSERT INTO categories VALUES('$parent','new',NULL)")
            db.execSQL("COMMIT")
            val po=scalarLong(db,"""SELECT dirty_ordinal FROM _sync_dirty_rows WHERE key_json='{"id":"$p"}'""")
            val co=scalarLong(db,"""SELECT dirty_ordinal FROM _sync_dirty_rows WHERE key_json='{"id":"$c"}'""")
            assertEquals(5L,co-po);assertTrue(co-po>3)
            val before=baselineQualificationVector(db,listOf("categories"));val start=f.bodies.size;val next=publicSyncNext(f)
            // Exactly one measured public call drains the dependency backlog.
            val report=f.client.sync().getOrThrow();assertEquals(PushOutcome.COMMITTED,report.pushOutcome)
            assertPublicSyncBatches(f,start,next,if(newParent)4 else 3);assertPublicSyncDrained(f)
            val after=baselineQualificationVector(db,listOf("categories"));assertEquals(clean,after.row("_sync_row_state",d))
            assertEquals(cleanBusiness,scalarText(db,"SELECT json_array(id,name,parent_id) FROM categories WHERE id='$d'"))
            assertEquals(cleanEdges,after.rows("_sync_relationships").filter{it.text("child_key")=="{\"id\":\"$d\"}"})
            assertEquals(0L,scalarLong(f.mirrorDb,"SELECT COUNT(*) FROM categories WHERE id='$p'"))
            assertEquals(parent,scalarText(f.mirrorDb,"SELECT parent_id FROM categories WHERE id='$c'"))
            assertEquals(c,scalarText(f.mirrorDb,"SELECT parent_id FROM categories WHERE id='$d'"))
            assertEquals("clean",scalarText(f.mirrorDb,"SELECT name FROM categories WHERE id='$d'"))
            println("A2-05 ONE-CALL reparent newParent=$newParent before=${before.tables} after=${after.tables} remote=${baselineQualificationVector(f.mirrorDb,listOf("categories")).tables} report=$report")
        }
    }

    @Test
    fun publicSyncIndependentDeletePairs() = runRealServerTest {
        if(!realServerEnv("OVERSQLITE_REALSERVER_TESTS").equals("true",ignoreCase=true)) {println("NOT-APPLICABLE: A2-05 live authority disabled; no live pass claimed");return@runRealServerTest}
        withPublicSyncFixture { f ->
            val parents=List(3){randomRealServerUuid()};val children=List(3){randomRealServerUuid()}
            for(i in 0..2) f.db.execSQL("INSERT INTO categories VALUES('${parents[i]}','parent',NULL),('${children[i]}','child','${parents[i]}')")
            f.client.sync().getOrThrow();assertPublicSyncDrained(f)
            f.db.execSQL("BEGIN");for(p in parents) f.db.execSQL("DELETE FROM categories WHERE id='$p'")
            for(c in children) f.db.execSQL("DELETE FROM categories WHERE id='$c'");f.db.execSQL("COMMIT")
            val before=baselineQualificationVector(f.db,listOf("categories"));val start=f.bodies.size;val next=publicSyncNext(f)
            val report=f.client.sync().getOrThrow();assertEquals(PushOutcome.COMMITTED,report.pushOutcome)
            assertPublicSyncBatches(f,start,next,3);assertPublicSyncDrained(f)
            assertEquals(0L,scalarLong(f.db,"SELECT COUNT(*) FROM categories"));assertEquals(0L,scalarLong(f.mirrorDb,"SELECT COUNT(*) FROM categories"))
            println("A2-05 ONE-CALL independent-pairs before=${before.tables} after=${baselineQualificationVector(f.db,listOf("categories")).tables} remote=${baselineQualificationVector(f.mirrorDb,listOf("categories")).tables} report=$report")
        }
    }

    @Test
    fun publicSyncConfirmedPrefixBeforeOversizedCycle() = runRealServerTest {
        if(!realServerEnv("OVERSQLITE_REALSERVER_TESTS").equals("true",ignoreCase=true)) {println("NOT-APPLICABLE: A2-05 live authority disabled; no live pass claimed");return@runRealServerTest}
        withPublicSyncFixture { f ->
            val ids=List(3){randomRealServerUuid()};val u=randomRealServerUuid()
            for(id in ids+u) f.db.execSQL("INSERT INTO categories VALUES('$id','old',NULL)")
            f.client.sync().getOrThrow();f.db.execSQL("BEGIN")
            for((index,id) in ids.withIndex()) f.db.execSQL("UPDATE categories SET parent_id='${ids[(index+1)%3]}' WHERE id='$id'")
            f.db.execSQL("COMMIT");f.client.sync().getOrThrow();assertPublicSyncDrained(f)
            f.db.execSQL("BEGIN");for(id in ids) f.db.execSQL("DELETE FROM categories WHERE id='$id'")
            f.db.execSQL("UPDATE categories SET name='safe' WHERE id='$u'");f.db.execSQL("COMMIT")
            val before=baselineQualificationVector(f.db,listOf("categories"));val start=f.bodies.size;val next=publicSyncNext(f)
            val result=f.client.sync();val error=result.exceptionOrNull();assertTrue(error is PushDependencyGroupExceedsAdmittedLimitException)
            assertEquals("rows",error.dimension);assertEquals(3L,error.componentRows);assertEquals(1L,error.progress.committedBundles);assertEquals(3L,error.progress.dirtyRows)
            assertPublicSyncBatches(f,start,next,1)
            val after=baselineQualificationVector(f.db,listOf("categories"))
            for(id in ids) for(table in listOf("_sync_dirty_rows","_sync_row_state")) assertEquals(before.row(table,id),after.row(table,id))
            assertTrue(after.rows("_sync_outbox_rows").isEmpty());assertEquals("none",after.rows("_sync_outbox_bundle").single().text("state"))
            assertEquals("safe",scalarText(f.mirrorDb,"SELECT name FROM categories WHERE id='$u'"));assertEquals(4L,scalarLong(f.mirrorDb,"SELECT COUNT(*) FROM categories"))
            val remote=baselineQualificationVector(f.mirrorDb,listOf("categories"));val calls=f.bodies.size
            f.client.closeFromCoordinatedOwner();f.db.close();f.db=newFileBackedDb(f.path)
            f.client=newRealServerClient(f.db,f.http,syncTables=listOf(SyncTable("categories",syncKeyColumnName="id")),maxPushRows=2)
            f.client.openAndConnect(f.user).getOrThrow();assertEquals(after,baselineQualificationVector(f.db,listOf("categories")))
            val repeated=f.client.sync().exceptionOrNull();assertTrue(repeated is PushDependencyGroupExceedsAdmittedLimitException);assertEquals(0L,repeated.progress.committedBundles)
            assertEquals(calls,f.bodies.size);assertEquals(after,baselineQualificationVector(f.db,listOf("categories")));assertEquals(remote,baselineQualificationVector(f.mirrorDb,listOf("categories")))
            println("A2-05 ONE-CALL confirmed-prefix before=${before.tables} after=${after.tables} remote=${remote.tables} error=$error repeated=$repeated")
        }
    }
}
