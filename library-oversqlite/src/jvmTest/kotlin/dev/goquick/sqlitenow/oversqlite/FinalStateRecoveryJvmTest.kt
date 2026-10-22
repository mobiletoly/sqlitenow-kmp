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

private fun snapshotProcessConfig() = recoveryJournalConfig().copy(maxSyncWorkUnitsPerInvocation = 1,
    transferTargets = OversqliteTransferTargets(maxSnapshotChunkRows = 1))
private fun BaselineQualificationVector.snapshotEncoded() = JsonObject(tables.mapValues { JsonArray(it.value) }).toString()

/** Separate native PIDs own a real SQLite file; the HTTP authority survives. */
internal class FinalStateRecoveryJvmTest : CrossTargetSyncTestSupport() {
    @Test fun processDeathKeepsSealedStageAndAtomicDelta() = runOwnedWebDatabaseTest {
        for (cut in listOf("create_receipt","paging","full_hash","verified_expired","preparation","ready","during_dml","post_commit","auto_captured","auto_stage_baseline","auto_prepare_transition","auto_ready","auto_completed","auto_blocked","auto_retry_armed")) {
            val root = Files.createTempDirectory("snapshot-death-")
            val path = root.resolve("durable.sqlite").toString()
            val marker = root.resolve("ready")
            val authority = MockSyncServer()
            val fixture = authority.newHttpClient()
            val executor = Executors.newCachedThreadPool()
            val release = CountDownLatch(1)
            val server = HttpServer.create(InetSocketAddress("127.0.0.1",0),0)
            var child: Process? = null
            try {
                val owner = recoveryJournalDatabase(path).openForSync(snapshotProcessConfig(),fixture)
                owner.advance()
                try {
                    val ready = assertNotNull(owner.readyOrNull())
                    ready.client.attach("user-1").getOrThrow()
                    ready.database.connection().execSQL("PRAGMA synchronous=FULL")
                    ready.database.connection().execSQL("INSERT INTO users(id,name) VALUES('A','first'),('B','second')")
                    ready.client.pushPending().getOrThrow()
                    ready.database.connection().execSQL("CREATE UNIQUE INDEX unique_name ON users(name)")
                } finally { owner.close() }
                authority.addRemoteBundleForTest(listOf(
                    BundleRow("main","users",mapOf("id" to "A"),"UPDATE",2,buildJsonObject {put("id","A");put("name","second")}),
                    BundleRow("main","users",mapOf("id" to "B"),"UPDATE",2,buildJsonObject {put("id","B");put("name","first")}),
                ))
                if (cut.startsWith("auto_")) authority.retainedBundleFloor=2
                fun pause() { Files.writeString(marker,cut); check(release.await(15,TimeUnit.SECONDS)) }
                server.executor = executor
                server.createContext("/") { exchange ->
                    try {
                        val route = exchange.requestURI.toString()
                        if (cut == "paging" && route.contains("after_row_ordinal=1")) pause()
                        val result = runBlocking {
                            val response = fixture.request("https://sync.test$route") {
                                method = HttpMethod.parse(exchange.requestMethod)
                                for ((name,values) in exchange.requestHeaders)
                                    if (!name.equals("Host",true) && !name.equals("Content-Length",true)) headers.appendAll(name,values)
                                if (exchange.requestMethod == "POST") setBody(exchange.requestBody.readAllBytes())
                            }
                            Triple(response.status.value,response.headers,response.bodyAsBytes())
                        }
                        if (cut == "create_receipt" && exchange.requestMethod == "POST" && route == "/sync/snapshot-sessions") pause()
                        result.second.forEach { name,values -> if (!name.equals("Content-Length",true)) exchange.responseHeaders.put(name,values) }
                        exchange.sendResponseHeaders(result.first,if (result.first == 204) -1 else result.third.size.toLong())
                        if (result.first != 204) exchange.responseBody.write(result.third)
                    } catch (failure: Throwable) {
                        if (!Files.exists(marker)) {
                            Files.writeString(root.resolve("http-error"),failure.stackTraceToString())
                            runCatching { exchange.sendResponseHeaders(500,-1) }
                        }
                    } finally { exchange.close() }
                }
                server.start()
                val base = "http://127.0.0.1:${server.address.port}"
                child = start(root.resolve("killed.log"),path,base,cut,marker.toString(),if(cut.startsWith("auto_"))"automatic" else "explicit")
                val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
                while (!Files.exists(marker) && child.isAlive && System.nanoTime() < deadline) Thread.sleep(10)
                assertTrue(Files.exists(marker),"$cut: ${Files.readString(root.resolve("killed.log"))}")
                val peer = newFileBackedDb(path)
                val before = try { recoveryJournalVector(peer).snapshotEncoded() } finally { peer.close() }
                assertTrue(child.isAlive)
                child.destroyForcibly()
                assertTrue(child.waitFor(3,TimeUnit.SECONDS))
                assertEquals(137,child.exitValue(),"physical SIGKILL exit status")
                child = null
                release.countDown()
                val expected = root.resolve("expected.json")
                Files.writeString(expected,before)
                val oldCreates = authority.snapshotSessionCreateRequestCount
                val oldPages = authority.snapshotChunkRequests.size
                if (cut == "verified_expired") authority.snapshotSessionIdsForTest().forEach(authority::expireSnapshotSessionForTest)
                child = start(root.resolve("reopened.log"),path,base,"reopen",expected.toString(),if(cut.startsWith("auto_"))"automatic" else "explicit")
                assertTrue(child.waitFor(15,TimeUnit.SECONDS),"$cut reopen deadline")
                val output = Files.readString(root.resolve("reopened.log"))
                assertEquals(0,child.exitValue(),"$cut: $output")
                assertTrue(output.contains("SNAPSHOT_PROCESS_COMPLETE"),output)
                child = null
                if (cut == "verified_expired") {
                    assertEquals(oldCreates,authority.snapshotSessionCreateRequestCount)
                    assertEquals(oldPages,authority.snapshotChunkRequests.size)
                }
                assertEquals(1,authority.pushRequestCount,"snapshot cannot freeze/upload/ack a new operation")
                println("SNAPSHOT_PROCESS_COMPLETE $cut actual_sigkill=true raw_vector_verified=true checkpoint=2")
            } finally {
                child?.let { if (it.isAlive) { it.destroyForcibly(); assertTrue(it.waitFor(3,TimeUnit.SECONDS)) } }
                release.countDown(); server.stop(0); executor.shutdownNow(); assertTrue(executor.awaitTermination(3,TimeUnit.SECONDS))
                fixture.close(); assertTrue(root.toFile().deleteRecursively(),"run-owned SQLite files removed")
            }
        }
    }

    private fun start(log: Path,vararg args: String): Process = ProcessBuilder(
        Path.of(System.getProperty("java.home"),"bin","java").toString(),"-cp",
        checkNotNull(System.getProperty("coordinatedopening.jvmTest.classpath")),SnapshotRecoveryProcess::class.java.name,*args)
        .redirectErrorStream(true).redirectOutput(log.toFile()).start()
}

internal object SnapshotRecoveryProcess : CrossTargetSyncTestSupport() {
    @JvmStatic fun main(args: Array<String>) = runBlocking {
        var db = newFileBackedDb(args[0])
        val cut = args[2]
        val automatic=args.getOrNull(4)=="automatic"
        val http = HttpClient(CIO) { defaultRequest { url(args[1]) } }
        if (cut == "reopen") assertEquals(Json.parseToJsonElement(Files.readString(Path.of(args[3]))),
            Json.parseToJsonElement(recoveryJournalVector(db).snapshotEncoded()),"raw vector before new opening lifetime")
        db.close()
        val owner = recoveryJournalDatabase(args[0]).openForSync(snapshotProcessConfig(),http)
        owner.advance()
        db = assertNotNull(owner.readyOrNull()).database.connection()
        val client = owner.readyOrNull()!!.client
        try {
            client.attach("user-1").getOrThrow()
            db.execSQL("PRAGMA synchronous=FULL");assertEquals(2L,scalarLong(db,"PRAGMA synchronous"))
            if (cut != "reopen") {
                val field = db.javaClass.getDeclaredField("ref").apply { isAccessible = true }
                val raw = field.get(db) as SQLiteConnection
                fun pause() { Files.writeString(Path.of(args[3]),cut); while (true) Thread.sleep(1000) }
                db.withExclusiveAccess {
                    field.set(db,object: SQLiteConnection by raw {
                        override fun prepare(sql: String): SQLiteStatement {
                            val statement = raw.prepare(sql)
                            var complete = false
                            var phase = ""
                            return object: SQLiteStatement by statement {
                                override fun bindLong(index: Int,value: Long) { if (index == 7) complete = value == 1L; statement.bindLong(index,value) }
                                override fun bindText(index: Int,value: String) { if (index == 3) phase = value; statement.bindText(index,value) }
                                override fun step(): Boolean {
                                    val result = statement.step()
                                    if (cut=="auto_ready" && sql.startsWith("UPDATE _sync_operation_state SET recovery_state")) {
                                        val observe=raw.prepare("SELECT json_extract(NULLIF(recovery_state,''),'$.status.phase') FROM _sync_operation_state")
                                        val phaseNow=try{check(observe.step());observe.getText(0)}finally{observe.close()}
                                        if(phaseNow=="ready") pause()
                                    }
                                    if (sql.trim().equals("COMMIT",true) && cut.startsWith("auto_")) {
                                        val observe=raw.prepare("SELECT json_extract(NULLIF(recovery_state,''),'$.status.phase') FROM _sync_operation_state")
                                        val phaseNow=try{check(observe.step());if(observe.isNull(0))"" else observe.getText(0)}finally{observe.close()}
                                        if(phaseNow==cut.removePrefix("auto_") && cut!="auto_ready")pause()
                                    }
                                    if (cut == "full_hash" && sql.trimStart().startsWith("UPDATE _sync_operation_state") && complete) pause()
                                    if (cut == "preparation" && sql.startsWith("INSERT INTO _sync_apply_rows")) pause()
                                    if (cut == "ready" && sql.startsWith("INSERT INTO _sync_apply_plan") && phase == "ready") pause()
                                    if (cut == "during_dml" && Regex("(?is)^\\s*(INSERT INTO|UPDATE|DELETE FROM) \\\"?users\\b.*").matches(sql)) pause()
                                    if (sql.trim().equals("COMMIT",true) && cut in setOf("verified_expired","post_commit")) {
                                        val observe = raw.prepare("SELECT snapshot_stage_complete,last_bundle_seq_seen FROM _sync_operation_state CROSS JOIN _sync_attachment_state")
                                        val flags = try { check(observe.step()); observe.getLong(0) to observe.getLong(1) } finally { observe.close() }
                                        if ((cut == "verified_expired" && flags.first == 1L) || (cut == "post_commit" && flags.second == 2L && flags.first == 0L)) pause()
                                    }
                                    return result
                                }
                            }
                        }
                    })
                }
            }
            if (cut in setOf("auto_blocked","auto_retry_armed")) db.execSQL("CREATE TEMP TRIGGER block_recovery AFTER UPDATE ON users BEGIN SELECT 1;END")
            if(automatic) {
                val result=client.sync()
                if(cut=="auto_retry_armed") {
                    assertTrue(result.isFailure);client.retryRecovery().getOrThrow()
                    Files.writeString(Path.of(args[3]),cut);while(true)Thread.sleep(1000)
                }
                result.getOrThrow()
            } else client.rebuild().getOrThrow()
            check(cut == "reopen") { "physical boundary not reached: $cut" }
            assertEquals(2L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals("second",scalarText(db,"SELECT name FROM users WHERE id='A'"))
            assertEquals("first",scalarText(db,"SELECT name FROM users WHERE id='B'"))
            for (table in listOf("_sync_dirty_rows","_sync_upload_journal","_sync_snapshot_stage","_sync_apply_rows","_sync_apply_edges","_sync_apply_plan"))
                assertEquals(0L,scalarLong(db,"SELECT count(*) FROM $table"))
            println("SNAPSHOT_PROCESS_COMPLETE")
        } finally { owner.close(); http.close() }
    }
}
