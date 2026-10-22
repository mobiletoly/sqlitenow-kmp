package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import com.sun.net.httpserver.HttpServer
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.request
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsBytes
import io.ktor.client.statement.HttpReceivePipeline
import io.ktor.client.request.HttpSendPipeline
import io.ktor.http.HttpMethod
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.*
import java.net.InetSocketAddress
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.*

private fun BaselineQualificationVector.journalEncoded()=JsonObject(tables.mapValues {JsonArray(it.value)}).toString()

/** Parent HTTP authority survives the actual upload-owner SIGKILL. */
internal class RecoveryJournalJvmTest : CrossTargetSyncTestSupport() {
    @Test fun processDeathKeepsOriginalUploadAndAtomicDisposition() = runOwnedWebDatabaseTest {
        for(cut in listOf("frozen_before_dispatch","committed_before_response","during_C_publication","partial_stage","after_C","during_settlement","before_ack","after_ack")) {
            val root=Files.createTempDirectory("upload-journal-death-")
            val path=root.resolve("durable.sqlite").toString();val marker=root.resolve("ready")
            val authority=MockSyncServer()
            authority.normalizePushEffects={rows -> rows.map {it.copy(payload=JsonObject(it.payload!!.jsonObject+("name" to JsonPrimitive("normalized"))))}}
            val fixtureHttp=authority.newHttpClient()
            val executor=Executors.newCachedThreadPool();val release=CountDownLatch(1)
            val server=HttpServer.create(InetSocketAddress("127.0.0.1",0),0)
            var child:Process?=null
            try {
                val owner=recoveryJournalDatabase(path).openForSync(recoveryJournalConfig(),fixtureHttp);owner.advance()
                val db=owner.readyOrNull()!!.database.connection();val bootstrap=owner.readyOrNull()!!.client
                try {
                    bootstrap.openAndConnect("user-1").getOrThrow()
                    db.execSQL("INSERT INTO users(id,name) VALUES('item','frozen')")
                } finally {owner.close()}
                fun pause() {Files.writeString(marker,cut);check(release.await(15,TimeUnit.SECONDS))}
                server.executor=executor
                server.createContext("/") { exchange ->
                    try {
                        val route=exchange.requestURI.toString()
                        if(cut=="before_ack" && route=="/sync/push-outcomes/ack") pause()
                        val result=runBlocking {
                            val response=fixtureHttp.request("https://sync.test$route") {
                                method=HttpMethod.parse(exchange.requestMethod)
                                for((name,values) in exchange.requestHeaders) if(!name.equals("Host",true) && !name.equals("Content-Length",true)) headers.appendAll(name,values)
                                if(exchange.requestMethod=="POST")setBody(exchange.requestBody.readAllBytes())
                            }
                            Triple(response.status.value,response.headers,response.bodyAsBytes())
                        }
                        if((cut=="committed_before_response" && route=="/sync/push") || (cut=="after_ack" && route=="/sync/push-outcomes/ack")) pause()
                        result.second.forEach {name,values -> if(!name.equals("Content-Length",true)) exchange.responseHeaders.put(name,values)}
                        exchange.sendResponseHeaders(result.first,if(result.first==204)-1 else result.third.size.toLong())
                        if(result.first!=204)exchange.responseBody.write(result.third)
                    } catch(failure:Throwable) {
                        // The killed client closes its transport while the authority remains.
                        if(!Files.exists(marker)) {Files.writeString(root.resolve("http-error"),failure.stackTraceToString());runCatching {exchange.sendResponseHeaders(500,-1)}}
                    } finally {exchange.close()}
                }
                server.start()
                val base="http://127.0.0.1:${server.address.port}"
                child=start(root.resolve("killed.log"),path,base,cut,marker.toString())
                val deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10)
                while(!Files.exists(marker) && child.isAlive && System.nanoTime()<deadline)Thread.sleep(10)
                assertTrue(Files.exists(marker),"$cut: ${Files.readString(root.resolve("killed.log"))}; HTTP=${root.resolve("http-error").toFile().takeIf {it.exists()}?.readText()}")
                val peer=newFileBackedDb(path)
                val before=try {recoveryJournalVector(peer).journalEncoded()} finally {peer.close()}
                child.destroyForcibly();assertTrue(child.waitFor(3,TimeUnit.SECONDS));assertNotEquals(0,child.exitValue())
                child=null;release.countDown()
                val expected=root.resolve("expected.json");Files.writeString(expected,before)
                child=start(root.resolve("reopened.log"),path,base,"reopen",expected.toString())
                assertTrue(child.waitFor(15,TimeUnit.SECONDS),"$cut reopen deadline")
                val output=Files.readString(root.resolve("reopened.log"));assertEquals(0,child.exitValue(),"$cut: $output")
                assertTrue(output.contains("JOURNAL_PROCESS_COMPLETE"),output);child=null
                assertEquals(1,authority.pushRequestCount,"one business application: $cut")
                assertEquals(0,authority.snapshotSessionCreateRequestCount)
                println("RECOVERY_JOURNAL_PROCESS_COMPLETE $cut actual_sigkill=true raw_vector_verified=true")
            } finally {
                child?.let {if(it.isAlive){it.destroyForcibly();assertTrue(it.waitFor(3,TimeUnit.SECONDS))}}
                release.countDown();server.stop(0);executor.shutdownNow();assertTrue(executor.awaitTermination(3,TimeUnit.SECONDS))
                fixtureHttp.close();assertTrue(root.toFile().deleteRecursively())
            }
        }
    }
    @Test fun preU0DischargesOriginalBeforeRetiredIntentAdmission() = runOwnedWebDatabaseTest {
        for(phase in listOf("frozen","ack_pending")) {
            val root=Files.createTempDirectory("journal-pre-u0-");val path=root.resolve("durable.sqlite").toString()
            val authority=MockSyncServer();val base=authority.newHttpClient();var freezeUpload=false
            val http=base.config {install("JournalPredispatch") {sendPipeline.intercept(HttpSendPipeline.Before) {
                if(freezeUpload && context.url.build().encodedPath=="/sync/push") {freezeUpload=false;throw kotlinx.coroutines.CancellationException("before engine")}
            }}}
            var owner=recoveryJournalDatabase(path).openForSync(recoveryJournalConfig(),http)
            try {
                owner.advance();val ready=owner.readyOrNull()!!;val db=ready.database.connection();val client=ready.client
                client.attach("user-1").getOrThrow();db.execSQL("INSERT INTO users(id,name) VALUES('item','frozen')")
                if(phase=="frozen")freezeUpload=true
                else authority.beforeAck={throw kotlinx.coroutines.CancellationException("ack unavailable")}
                try {client.pushPending().getOrThrow();fail("fault must be reached")}catch(_:kotlinx.coroutines.CancellationException) {}
                owner.close();authority.retireOldGeneration=true;authority.unavailableSecondGeneration=true
                authority.generationDiscoveryOverrides["2"]=testGeneration(authority.registeredTableSpecs).copy(id="2",fingerprint="").let {it.copy(fingerprint=it.computeFingerprint())}
                owner=recoveryJournalDatabase(path,2).openForSync(recoveryJournalConfig(2),http,
                    upgradePlan=OversqliteUpgradePlan("journal-u0",1,2,1,2,emptySet()))
                val status=try {owner.advance()} catch(failure:OversqliteRemoteContractException) {
                    assertEquals("ack_pending",phase);assertEquals("unknown_schema_generation",failure.code);assertEquals(2L,failure.generationId);null
                }
                val peer=newFileBackedDb(path)
                try {
                    assertEquals(0L,scalarLong(peer,"SELECT count(*) FROM _sync_upload_journal"))
                    assertEquals(1L,scalarLong(peer,"PRAGMA user_version"))
                    assertEquals(0L,scalarLong(peer,"SELECT count(*) FROM _sync_opening_state WHERE phase='upgrade_started'"))
                    assertEquals("frozen",scalarText(peer,"SELECT name FROM users WHERE id='item'"))
                    if(phase=="frozen") {
                        assertEquals("retired_schema_generation",status!!.diagnosticCode)
                        assertEquals(OversqliteOpeningOutcome.OLD_GENERATION_UNAVAILABLE,status.outcome)
                        assertEquals(OversqliteOpeningNextAction.RESTORE_SUPPORT,status.nextAction)
                        assertEquals(1L,scalarLong(peer,"SELECT count(*) FROM _sync_dirty_rows"))
                    }
                } finally {peer.close()}
                assertEquals(if(phase=="frozen")1 else 0,authority.resolveRequests.size)
                assertEquals(if(phase=="frozen")0 else 1,authority.pushRequestCount)
                assertEquals(if(phase=="frozen")1 else 2,authority.ackRequests.size)
                assertEquals(0,authority.snapshotSessionCreateRequestCount)
                println("RECOVERY_JOURNAL_COMPLETE pre_u0 $phase original_before_new_admission=true")
            } finally {owner.close();http.close();base.close();assertTrue(root.toFile().deleteRecursively())}
        }
    }
    @Test fun preU0RetainedRejectionsReachExistingOpeningResolution() = runOwnedWebDatabaseTest {
        for(kind in listOf("application","row_conflict")) {
            val root=Files.createTempDirectory("journal-pre-policy-");val path=root.resolve("durable.sqlite").toString()
            val authority=MockSyncServer();val base=authority.newHttpClient();var lose=false
            val target=testGeneration(testRegisteredTableSpecs("users","posts")).copy(id="2",fingerprint="").let {it.copy(fingerprint=it.computeFingerprint())}
            authority.generationDiscoveryOverrides["2"]=target
            val http=base.config {install("JournalLostRejection") {receivePipeline.intercept(HttpReceivePipeline.Before) {response ->
                if(lose && response.call.request.url.encodedPath=="/sync/push") {lose=false;throw kotlinx.coroutines.CancellationException("lost rejected response")}
            }}}
            var owner=recoveryJournalDatabase(path).openForSync(recoveryJournalConfig(),http)
            try {
                owner.advance();val ready=owner.readyOrNull()!!;val db=ready.database.connection()
                ready.client.attach("user-1").getOrThrow();db.execSQL("INSERT INTO users(id,name) VALUES('item','frozen')")
                if(kind=="application")authority.applicationRejectionCode="application_rejected"
                else authority.conflictOverride={row,_,_->PushConflictDetails(row.schema,row.table,row.key,row.op,row.baseRowVersion,1,false,buildJsonObject {put("id","item");put("name","remote")})}
                lose=true
                try {ready.client.pushPending().getOrThrow();fail("lost response must be reached")}catch(_:kotlinx.coroutines.CancellationException) {}
                owner.close()
                owner=recoveryJournalDatabase(path,2).openForSync(recoveryJournalConfig(2),http,
                    upgradePlan=OversqliteUpgradePlan("journal-policy",1,2,1,2,emptySet()))
                val status=owner.advance();assertEquals(OversqliteOpeningOutcome.NEEDS_RESOLUTION,status.outcome)
                assertEquals("resolving_old_intent",status.phase)
                assertEquals(1,authority.ackRequests.size);assertEquals(1,authority.resolveRequests.size);assertEquals(1,authority.pushRequestCount)
                if(kind=="application") {
                    val pending=owner.pendingRejection();assertNotNull(pending)
                    assertEquals(OversqliteOpeningNextAction.ADVANCE,owner.retryRejected(pending).nextAction)
                } else {
                    val pending=owner.pendingConflict().getOrThrow();assertNotNull(pending)
                    owner.retryConflict(pending).getOrThrow()
                    assertEquals(OversqliteOpeningNextAction.ADVANCE,owner.status().nextAction)
                }
                val peer=newFileBackedDb(path)
                try {
                    assertEquals(0L,scalarLong(peer,"SELECT count(*) FROM _sync_upload_journal"))
                    assertEquals(1L,scalarLong(peer,"SELECT count(*) FROM _sync_dirty_rows"))
                    assertEquals("frozen",scalarText(peer,"SELECT name FROM users WHERE id='item'"))
                    assertEquals(1L,scalarLong(peer,"PRAGMA user_version"))
                } finally {peer.close()}
                assertEquals(1,authority.pushRequestCount);assertEquals(1,authority.ackRequests.size)
                println("RECOVERY_JOURNAL_COMPLETE pre_u0_policy $kind existing_resolution_usable=true")
            } finally {owner.close();http.close();base.close();assertTrue(root.toFile().deleteRecursively())}
        }
    }
    private fun start(log:Path,vararg args:String):Process=ProcessBuilder(
        Path.of(System.getProperty("java.home"),"bin","java").toString(),"-cp",
        checkNotNull(System.getProperty("coordinatedopening.jvmTest.classpath")),RecoveryJournalProcess::class.java.name,*args)
        .redirectErrorStream(true).redirectOutput(log.toFile()).start()
}

internal object RecoveryJournalProcess : CrossTargetSyncTestSupport() {
    @JvmStatic fun main(args:Array<String>) = runBlocking {
        var db=newFileBackedDb(args[0]);val cut=args[2]
        val http=HttpClient(CIO) {defaultRequest {url(args[1])}}
        if(cut=="reopen")assertEquals(Json.parseToJsonElement(Files.readString(Path.of(args[3]))),Json.parseToJsonElement(recoveryJournalVector(db).journalEncoded()),"raw durable vector before opening")
        db.close()
        val owner=recoveryJournalDatabase(args[0]).openForSync(recoveryJournalConfig(),http);owner.advance()
        db=owner.readyOrNull()!!.database.connection();val client=owner.readyOrNull()!!.client
        try {
            client.openAndConnect("user-1").getOrThrow()
            if(cut!="reopen") {
                val field=db.javaClass.getDeclaredField("ref").apply {isAccessible=true}
                val raw=field.get(db) as SQLiteConnection
                var frozen=false;var captured=false
                fun pause(){Files.writeString(Path.of(args[3]),cut);while(true)Thread.sleep(1000)}
                db.withExclusiveAccess {
                    field.set(db,object:SQLiteConnection by raw {
                        override fun prepare(sql:String):SQLiteStatement {
                            val statement=raw.prepare(sql)
                            return object:SQLiteStatement by statement {
                                override fun step():Boolean {
                                    val result=statement.step()
                                    if(sql.startsWith("INSERT INTO _sync_upload_journal"))frozen=true
                                    if(sql.startsWith("UPDATE _sync_upload_journal SET outcome_body")) {
                                        captured=true;if(cut=="during_C_publication")pause()
                                    }
                                    if(cut=="partial_stage" && sql.startsWith("INSERT INTO _sync_apply_rows"))pause()
                                    if(cut=="during_settlement" && captured && Regex("(?is)^\\s*(UPDATE|INSERT|DELETE).*users.*").matches(sql))pause()
                                    if(sql.trim().equals("COMMIT",true)) {
                                        if(cut=="frozen_before_dispatch" && frozen && !captured)pause()
                                        if(cut=="after_C" && captured)pause()
                                    }
                                    return result
                                }
                            }
                        }
                    })
                }
            }
            client.pushPending().getOrThrow()
            if(cut!="reopen")error("death boundary not reached: $cut")
            // Cancellation proves noncommit, retaining intent for an explicit successor.
            client.pushPending().getOrThrow()
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_upload_journal"))
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals("normalized",scalarText(db,"SELECT name FROM users WHERE id='item'"))
            println("JOURNAL_PROCESS_COMPLETE")
        } finally {owner.close();http.close()}
    }
}
