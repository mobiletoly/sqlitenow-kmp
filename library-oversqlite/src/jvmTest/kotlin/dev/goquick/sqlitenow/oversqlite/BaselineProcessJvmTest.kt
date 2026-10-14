package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.*
import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import androidx.sqlite.async.step
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlin.test.*

private fun BaselineQualificationVector.encoded() = JsonObject(tables.mapValues { JsonArray(it.value) }).toString()

internal class BaselineProcessJvmTest : CrossTargetSyncTestSupport() {
    @Test fun dependencyEntryLookupIsBoundedAndCharged() = runOwnedWebDatabaseTest {
        for(count in listOf(0,1,10000)) for(oneStep in listOf(false,true)) {
            val db=newDb();val server=MockSyncServer();val http=server.newHttpClient()
            var client=newClient(db,http)
            val raw=db.ref
            val rawField=db.javaClass.getDeclaredField("ref").apply {isAccessible=true}
            try {
                createUsersAndPostsTables(db);client.openAndConnect("user-1").getOrThrow()
                if(count>0) db.execSQL("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<$count) INSERT INTO users(id,name) SELECT CAST(i AS TEXT),'value' FROM n")
                val before=baselineQualificationVector(db)
                val cache=TableInfoCache();var step=32L
                for(table in listOf("users","posts")) {val info=cache.get(db,table);step+=4+16*info.columns.size+32*info.foreignKeys.size}
                client.closeFromCoordinatedOwner();client=newClient(db,http,workUnits=if(oneStep) step else 1)
                client.openAndConnect("user-1").getOrThrow()
                fun field(name:String)=client.javaClass.getDeclaredField(name).apply {isAccessible=true}.get(client)
                val stages=field("stageStore") as OversqliteStageStore
                val operations=field("operationController") as OversqliteClientOperationController
                val state=(field("sessionState") as OversqliteClientSessionState).requireConnectedRuntimeState("entry regression")
                val dirtyQueries=mutableListOf<String>()
                rawField.set(db,object:SQLiteConnection by raw {
                    override fun prepare(sql:String):SQLiteStatement {
                        if(sql.contains("FROM _sync_dirty_rows",ignoreCase=true)) {
                            dirtyQueries+=sql
                            assertEquals(step,operations.usedSyncWork(),"dirty query must follow full step reservation")
                            assertTrue(sql.trimEnd().endsWith("LIMIT 1"),sql)
                            assertTrue(!sql.contains("count(",ignoreCase=true),sql)
                        }
                        return raw.prepare(sql)
                    }
                })
                val result=runCatching {stages.preparePushPlanSlice(state) { step }}
                rawField.set(db,raw)
                if(count==0) {assertTrue(result.isSuccess);assertEquals(null,result.getOrThrow())}
                else assertTrue(result.exceptionOrNull() is SyncWorkLimitException)
                assertEquals(step,operations.usedSyncWork())
                assertEquals(if(count==0) 1 else 3,dirtyQueries.size)
                assertEquals(before,baselineQualificationVector(db));assertEquals(0,server.pushRequestCount)
                assertEquals(if(count>0) 1L else 0L,scalarLong(db,"SELECT COUNT(*) FROM _sync_push_nodes"))
                if(oneStep) {
                    val opcodes=mutableListOf<String>()
                    db.withPreparedStatement("EXPLAIN "+dirtyQueries.first()) {st -> while(st.step()) opcodes+=st.getText(1)}
                    assertTrue("DecrJumpZero" in opcodes);assertTrue("Count" !in opcodes);assertTrue("SorterOpen" !in opcodes)
                }
                println("A2 bounded-entry dirty=$count allowance=${if(oneStep) step else 1} used=${operations.usedSyncWork()} dirtyQueries=${dirtyQueries.size}")
            } finally {rawField.set(db,raw);client.closeFromCoordinatedOwner();http.close();db.close()}
        }
    }

    @Test fun dependencySecondConnectionInvalidatesPreparedGuard() = runOwnedWebDatabaseTest {
        val root=Files.createTempDirectory("dependency-second-writer-")
        val path=root.resolve("durable.sqlite").toString()
        val db=newFileBackedDb(path);val server=MockSyncServer();val http=server.newHttpClient()
        var client=newClient(db,http)
        try {
            createUsersAndPostsTables(db);client.openAndConnect("user-1").getOrThrow()
            repeat(8) {insertUser(db,"$it","original")}
            val cache=TableInfoCache();var step=32L
            for(table in listOf("users","posts")) {val metadata=cache.get(db,table);step+=4+16*metadata.columns.size+32*metadata.foreignKeys.size}
            client.closeFromCoordinatedOwner();client=newClient(db,http,workUnits=2*step);client.openAndConnect("user-1").getOrThrow()
            val before=baselineQualificationVector(db)
            assertTrue(preparePushSlice(db,http,2*step).exceptionOrNull() is SyncWorkLimitException)
            assertEquals(before,baselineQualificationVector(db));assertEquals(2L,scalarLong(db,"SELECT COUNT(*) FROM _sync_push_nodes"))
            val oldUnit=scalarText(db,"SELECT unit FROM _sync_push_plan")
            val other=newFileBackedDb(path)
            try {other.execSQL("UPDATE users SET name='second-writer' WHERE id='0'")} finally {other.close()}
            val changed=baselineQualificationVector(db)
            assertNotEquals(before.row("_sync_dirty_rows","0"),changed.row("_sync_dirty_rows","0"))
            for(key in (1..7).map(Int::toString)) assertEquals(before.row("_sync_dirty_rows",key),changed.row("_sync_dirty_rows",key))
            client.closeFromCoordinatedOwner();client=newClient(db,http);client.openAndConnect("user-1").getOrThrow()
            client.pushPending().getOrThrow();assertEquals(1,server.pushRequestCount)
            val wire=Json.decodeFromString(CompletePushRequest.serializer(),server.pushBodies.single())
            val generation=testGeneration(server.registeredTableSpecs)
            val rows=OversqliteCompactCodec.decodeBundleRows(generation,wire.rows)
            assertEquals(8,rows.size);assertEquals("second-writer",rows.single {it.key["id"]=="0"}.payload!!.jsonObject.getValue("name").jsonPrimitive.content)
            assertEquals(computePushRequestHashForTest(rows.map {PushRequestRow(it.schema,it.table,it.key,it.op,it.rowVersion,it.payload)},generation),wire.canonicalRequestHash)
            assertEquals(1L,wire.sourceBundleId)
            val final=baselineQualificationVector(db)
            assertEquals(changed.rows("users"),final.rows("users"));assertTrue(final.rows("_sync_dirty_rows").isEmpty());assertTrue(final.rows("_sync_outbox_rows").isEmpty())
            assertEquals(0L,scalarLong(db,"SELECT COUNT(*) FROM _sync_push_plan"));assertEquals(8,server.remoteRowsForBaselineTest().size)
            assertEquals("second-writer",Json.parseToJsonElement(checkNotNull(server.remoteRowsForBaselineTest()["users/id=0"])).jsonObject.getValue("name").jsonPrimitive.content)
            println("A2 second-writer invalidated=$oldUnit before=${before.tables} changed=${changed.tables} final=${final.tables} wire=${server.pushBodies} remote=${server.remoteRowsForBaselineTest()}")
        } finally {client.closeFromCoordinatedOwner();http.close();db.close();root.toFile().deleteRecursively()}
    }

    @Test fun dependencyPlanResumesAcrossFreshJvmProcess() = runOwnedWebDatabaseTest {
        val root=Files.createTempDirectory("dependency-fresh-jvm-")
        val path=root.resolve("durable.sqlite").toString()
        val db=newFileBackedDb(path);val server=MockSyncServer();val http=server.newHttpClient()
        var client=newClient(db,http)
        try {
            createUsersAndPostsTables(db);client.openAndConnect("user-1").getOrThrow()
            repeat(8) {insertUser(db,"$it","value")}
            val before=baselineQualificationVector(db)
            val cache=TableInfoCache();var step=32L
            for(table in listOf("users","posts")) {val metadata=cache.get(db,table);step+=4+16*metadata.columns.size+32*metadata.foreignKeys.size}
            client.closeFromCoordinatedOwner();client=newClient(db,http,workUnits=2*step);client.openAndConnect("user-1").getOrThrow()
            assertTrue(preparePushSlice(db,http,2*step).exceptionOrNull() is SyncWorkLimitException)
            assertEquals(before,baselineQualificationVector(db));assertEquals(2L,scalarLong(db,"SELECT COUNT(*) FROM _sync_push_nodes"));assertEquals(0,server.pushRequestCount)
            val expected=root.resolve("expected.json")
            Files.writeString(expected,buildJsonObject {put("vector",Json.parseToJsonElement(before.encoded()));put("unit",JsonPrimitive(scalarText(db,"SELECT unit FROM _sync_push_plan")));put("step",JsonPrimitive(step))}.toString())
            client.closeFromCoordinatedOwner();db.close()
            verifyFreshJvm(path,expected,root.resolve("child.log"),DependencyPlanProcessReader::class.java.name)
        } finally {client.closeFromCoordinatedOwner();http.close();db.close();root.toFile().deleteRecursively()}
    }

    @Test fun frozenAndNewerIntentSurviveFreshJvmProcess() = runOwnedWebDatabaseTest {
        val root=Files.createTempDirectory("baseline-fresh-jvm-")
        val dbPath=root.resolve("durable.sqlite").toString()
        val db=newFileBackedDb(dbPath);val server=MockSyncServer();val http=server.newHttpClient();val client=newClient(db,http)
        try {
            createUsersAndPostsTables(db);client.openAndConnect("user-1").getOrThrow()
            insertUser(db,"P","parent");insertUser(db,"Q","other");insertPost(db,"C","P","child");client.pushPending().getOrThrow()
            db.execSQL("UPDATE posts SET title='frozen' WHERE id='C'")
            server.beforePushResponse={throw CancellationException("lost committed response")}
            try {client.pushPending()} catch(_:CancellationException) { }
            db.execSQL("UPDATE posts SET title='newer' WHERE id='C'")
            db.execSQL("UPDATE users SET id='R' WHERE id='Q'");db.execSQL("UPDATE users SET id='S' WHERE id='R'")
            val expected=baselineQualificationVector(db).encoded()
            val expectedPath=root.resolve("expected.json");Files.writeString(expectedPath,expected)
            client.closeFromCoordinatedOwner();db.close()
            verifyFreshJvm(dbPath,expectedPath,root.resolve("child.log"))
        } finally {
            client.closeFromCoordinatedOwner();http.close();db.close();root.toFile().deleteRecursively()
        }
    }
    private fun verifyFreshJvm(dbPath:String,expectedPath:Path,log:Path,reader:String=BaselineProcessReader::class.java.name) {
        val process=ProcessBuilder(Path.of(System.getProperty("java.home"),"bin","java").toString(),"-cp",
            checkNotNull(System.getProperty("coordinatedopening.jvmTest.classpath")),reader,
            dbPath,expectedPath.toString()).redirectErrorStream(true).redirectOutput(log.toFile()).start()
        try {
            assertTrue(process.waitFor(30,TimeUnit.SECONDS),"fresh JVM child deadline")
            val output=Files.readString(log);assertEquals(0,process.exitValue(),output)
            assertTrue(output.contains("baseline-process: complete durable vector verified"),output);println(output)
        } finally {
            if(process.isAlive) {process.destroyForcibly();assertTrue(process.waitFor(3,TimeUnit.SECONDS))}
            process.inputStream.close();process.errorStream.close();process.outputStream.close()
        }
    }

    @Test fun beforeFreezeAndRequeueSurviveFreshJvmProcesses() = runOwnedWebDatabaseTest {
        val root=Files.createTempDirectory("baseline-phases-jvm-");val path=root.resolve("durable.sqlite").toString()
        var db=newFileBackedDb(path);val server=MockSyncServer();val http=server.newHttpClient();var client=newClient(db,http)
        suspend fun checkpoint(label:String) {
            val vector=baselineQualificationVector(db);val expected=root.resolve("$label.json");Files.writeString(expected,vector.encoded())
            client.closeFromCoordinatedOwner();db.close()
            verifyFreshJvm(path,expected,root.resolve("$label.log"))
            db=newFileBackedDb(path);assertEquals(vector,baselineQualificationVector(db));client=newClient(db,http)
            client.openAndConnect("user-1").getOrThrow();assertEquals(vector,baselineQualificationVector(db))
        }
        try {
            createUsersAndPostsTables(db);client.openAndConnect("user-1").getOrThrow()
            insertUser(db,"P","parent");insertUser(db,"Q","other");insertPost(db,"C","P","child");client.pushPending().getOrThrow()
            db.execSQL("UPDATE users SET id='R' WHERE id='Q'");db.execSQL("UPDATE users SET id='S' WHERE id='R'")
            checkpoint("before-freeze")
            server.beforePushCommit={throw CancellationException("definitely uncommitted")}
            try {client.pushPending()} catch(_:CancellationException) { }
            db.execSQL("UPDATE users SET name='newer' WHERE id='S'")
            requeueDefinitelyUncommittedBaselineOutbox(db)
            checkpoint("requeued")
            client.pushPending().getOrThrow()
            val settled=baselineQualificationVector(db)
            assertTextPayload(settled.row("_sync_row_state","S").payload("server_payload"),mapOf("id" to "S","name" to "newer"))
            assertTrue(settled.rows("_sync_dirty_rows").isEmpty());assertTrue(settled.rows("_sync_outbox_rows").isEmpty())
        } finally {client.closeFromCoordinatedOwner();http.close();db.close();root.toFile().deleteRecursively()}
    }

}

internal object DependencyPlanProcessReader : CrossTargetSyncTestSupport() {
    @JvmStatic fun main(args:Array<String>) = runBlocking {
        val db=newFileBackedDb(args[0]);val server=MockSyncServer();val http=server.newHttpClient()
        // Restore the independently initialized empty authority through its
        // ordinary connect owner before reconnecting the retained attachment.
        val bootstrapDb=newDb();val bootstrap=newClient(bootstrapDb,http)
        try {createUsersAndPostsTables(bootstrapDb);bootstrap.openAndConnect("user-1").getOrThrow()}
        finally {bootstrap.closeFromCoordinatedOwner();bootstrapDb.close()}
        val expected=Json.parseToJsonElement(Files.readString(Path.of(args[1]))).jsonObject
        var client=newClient(db,http,workUnits=2*expected.getValue("step").jsonPrimitive.long)
        try {
            assertEquals(expected.getValue("vector"),Json.parseToJsonElement(baselineQualificationVector(db).encoded()))
            val unit=expected.getValue("unit").jsonPrimitive.content
            assertEquals(unit,scalarText(db,"SELECT unit FROM _sync_push_plan"))
            client.openAndConnect("user-1").getOrThrow()
            val error=preparePushSlice(db,http,2*expected.getValue("step").jsonPrimitive.long).exceptionOrNull();assertTrue(error is SyncWorkLimitException);assertEquals(unit,error.unit)
            assertEquals(4L,scalarLong(db,"SELECT COUNT(*) FROM _sync_push_nodes"))
            assertEquals(expected.getValue("vector"),Json.parseToJsonElement(baselineQualificationVector(db).encoded()));assertEquals(0,server.pushRequestCount)
            client.closeFromCoordinatedOwner();client=newClient(db,http);client.openAndConnect("user-1").getOrThrow();client.pushPending().getOrThrow()
            val final=baselineQualificationVector(db);assertTrue(final.rows("_sync_dirty_rows").isEmpty());assertTrue(final.rows("_sync_outbox_rows").isEmpty());assertEquals(1,server.pushRequestCount);assertEquals(8,server.remoteRowsForBaselineTest().size)
            println("dependency-process: same unit=$unit resumed2-to4 nodes, final=${final.tables} wire=${server.pushBodies} remote=${server.remoteRowsForBaselineTest()}")
            println("baseline-process: complete durable vector verified")
        } finally {client.closeFromCoordinatedOwner();http.close();db.close()}
    }
}

internal object BaselineProcessReader : CrossTargetSyncTestSupport() {
    @JvmStatic fun main(args:Array<String>) = runBlocking {
        val db=newFileBackedDb(args[0])
        try {
            val expected=Json.parseToJsonElement(Files.readString(Path.of(args[1])))
            assertEquals(expected,Json.parseToJsonElement(baselineQualificationVector(db).encoded()))
            assertEquals("CURRENT",inspectRetainedRuntime(db))
            println("baseline-process: complete durable vector verified")
        } finally {db.close()}
    }
}
