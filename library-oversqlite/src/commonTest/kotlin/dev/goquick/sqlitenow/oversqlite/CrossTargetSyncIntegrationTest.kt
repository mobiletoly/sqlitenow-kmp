package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

internal class CrossTargetSyncIntegrationTest : CrossTargetSyncTestSupport() {
    @Test
    fun dependencyRekeyEquivalentFactsAndPartialFailureAcrossPhysicalRestart() = runOwnedWebDatabaseTest {
        if(!supportsDurableTestDatabaseRestart()) {println("NOT-APPLICABLE: equivalence physical copies require durable storage; Node transient");return@runOwnedWebDatabaseTest}
        val seedPath=createTempSqliteNowTestDbPath("a2-rekey-seed")
        val paths=listOf(createTempSqliteNowTestDbPath("a2-rekey-update"),createTempSqliteNowTestDbPath("a2-rekey-independent"))
        val server=MockSyncServer();val http=server.newHttpClient();val seedDb=newFileBackedDb(seedPath)
        val seed=newClient(seedDb,http)
        val facts=mutableListOf<BaselineQualificationVector>();val wires=mutableListOf<String>()
        try {
            createUsersAndPostsTables(seedDb);seed.openAndConnect("user-1").getOrThrow();insertUser(seedDb,"P","same");seed.pushPending().getOrThrow()
            seed.closeFromCoordinatedOwner()
            for(path in paths) seedDb.execSQL("VACUUM INTO '$path'")
            server.wireLimits=testWireLimits().copy(maxPushRows=1)
            for((index,path) in paths.withIndex()) {
                var db=newFileBackedDb(path);var client=newClient(db,http,maxPushRows=1)
                try {
                    client.openAndConnect("user-1").getOrThrow()
                    db.execSQL("BEGIN")
                    if(index==0) db.execSQL("UPDATE users SET id='Q' WHERE id='P'") else {db.execSQL("DELETE FROM users WHERE id='P'");insertUser(db,"Q","same")}
                    db.execSQL("COMMIT")
                    // Fix only nonsemantic wall-clock fields so these independent
                    // physical copies have exactly equal durable facts.
                    db.execSQL("UPDATE _sync_row_state SET updated_at='2026-01-01T00:00:00.000Z' WHERE key_json='{\"id\":\"Q\"}'")
                    db.execSQL("UPDATE _sync_dirty_rows SET updated_at='2026-01-01T00:00:00.000Z'")
                    val before=baselineQualificationVector(db);facts+=before
                    val q=before.row("_sync_dirty_rows","Q");assertEquals("absent",q.text("base_kind"));assertEquals(null,q.payload("base_payload"))
                    assertEquals("present",before.row("_sync_dirty_rows","P").text("base_kind"));assertTrue(before.rows("_sync_relationships").isEmpty())
                    val source=before.rows("_sync_attachment_state").single().text("current_source_id")!!
                    client.closeFromCoordinatedOwner();db.close();db=newFileBackedDb(path);client=newClient(db,http,maxPushRows=1)
                    client.openAndConnect("user-1").getOrThrow();assertEquals(before,baselineQualificationVector(db))
                    client.pushPending().getOrThrow();wires+=server.pushBodies.last()
                    val prefix=baselineQualificationVector(db)
                    assertEquals(q,prefix.row("_sync_dirty_rows","Q"));assertEquals("deleted",prefix.row("_sync_row_state","P").text("server_kind"))
                    assertBaselineCounters(prefix,source,3,2);assertTrue(server.remoteRowsForBaselineTest().isEmpty())
                    server.beforePushCommit={error("qualification insertion validation refusal")}
                    val refused=client.pushPending();assertTrue(refused.isFailure)
                    val failed=baselineQualificationVector(db)
                    for(owner in listOf("users","posts","_sync_row_state","_sync_relationships","_sync_source_state","_sync_attachment_state")) assertEquals(prefix.rows(owner),failed.rows(owner),owner)
                    assertEquals(1,failed.rows("_sync_outbox_rows").size)
                    val frozenQ=failed.row("_sync_outbox_rows","Q")
                    for(column in listOf("op","base_kind","base_payload","base_provenance","base_row_version")) assertEquals(q[column],frozenQ[column],column)
                    assertEquals(q["payload"],frozenQ["local_payload"])
                    assertTrue(server.remoteRowsForBaselineTest().isEmpty())
                    val frozenWire=server.pushBodies.last();val calls=server.pushRequestCount
                    client.closeFromCoordinatedOwner();db.close();db=newFileBackedDb(path);client=newClient(db,http,maxPushRows=1)
                    client.openAndConnect("user-1").getOrThrow();assertEquals(failed,baselineQualificationVector(db))
                    server.beforePushCommit={error("qualification repeated validation refusal")}
                    assertTrue(client.pushPending().isFailure)
                    assertEquals(frozenWire,server.pushBodies.last());assertEquals(calls+1,server.pushRequestCount)
                    assertEquals(failed,baselineQualificationVector(db));assertTrue(server.remoteRowsForBaselineTest().isEmpty())
                    println("A2-04 rekey history=$index before=${before.tables} prefix=${prefix.tables} failure=${failed.tables} wire=${server.pushBodies} remote=${server.remoteRowsForBaselineTest()}")
                } finally {client.closeFromCoordinatedOwner();db.close()}
            }
            assertEquals(facts[0],facts[1],"identical copied source contexts and complete durable facts")
            assertEquals(wires[0],wires[1],"actual primitive compact selection; second mock receipt is exact replay")
        } finally {seed.closeFromCoordinatedOwner();seedDb.close();http.close();for(path in paths+seedPath) deleteTempSqliteNowTestDbArtifacts(path)}
    }


    @Test
    fun dependencyThreeIndependentDeletePairs() = runOwnedWebDatabaseTest {
        val db=newDb(); val server=MockSyncServer(); val http=server.newHttpClient()
        var client=newClient(db,http)
        try {
            createUsersAndPostsTables(db); client.openAndConnect("user-1").getOrThrow()
            for(i in 1..3) { insertUser(db,"P$i","parent"); insertPost(db,"C$i","P$i","child") }
            client.pushPending().getOrThrow();client.closeFromCoordinatedOwner()
            server.wireLimits=testWireLimits().copy(maxPushRows=2)
            client=newClient(db,http,maxPushRows=2);client.openAndConnect("user-1").getOrThrow()
            db.execSQL("BEGIN");db.execSQL("PRAGMA defer_foreign_keys=ON");db.execSQL("DELETE FROM users");db.execSQL("DELETE FROM posts");db.execSQL("COMMIT")
            val before=baselineQualificationVector(db)
            val remaining=mutableSetOf("P1","P2","P3","C1","C2","C3")
            val source=before.rows("_sync_attachment_state").single().text("current_source_id")!!
            repeat(3) { index ->
                client.pushPending().getOrThrow();assertEquals(index+2,server.pushRequestCount)
                val request=json.parseToJsonElement(server.pushBodies.last()).jsonObject
                val rows=OversqliteCompactCodec.decodeBundleRows(testGeneration(server.registeredTableSpecs),request.getValue("rows"))
                assertEquals(2,rows.size)
                for(row in rows) {assertEquals("DELETE",row.op);assertTrue(remaining.remove(row.key.getValue("id")))}
                val after=baselineQualificationVector(db)
                assertEquals(4-2*index,after.rows("_sync_dirty_rows").size)
                assertTrue(after.rows("_sync_outbox_rows").isEmpty());assertTrue(after.rows("users").isEmpty());assertTrue(after.rows("posts").isEmpty())
                for(key in remaining) {
                    assertEquals(before.row("_sync_dirty_rows",key),after.row("_sync_dirty_rows",key))
                    assertEquals(before.row("_sync_row_state",key),after.row("_sync_row_state",key))
                }
                assertBaselineCounters(after,source,index+3L,index+2L)
                assertEquals(remaining.size,server.remoteRowsForBaselineTest().size)
                println("A2-04 independent-pairs call=${index+1} state=${after.tables} wire=${server.pushBodies.last()} remote=${server.remoteRowsForBaselineTest()}")
            }
            assertTrue(remaining.isEmpty())
        } finally {client.closeFromCoordinatedOwner();http.close();db.close()}
    }

    @Test
    fun dependencyThreeRowCycleKeepsConfirmedIndependentPrefix() = runOwnedWebDatabaseTest {
        val db=newDb();val server=MockSyncServer().apply {registeredTableSpecs=testRegisteredTableSpecs("employees")};val http=server.newHttpClient()
        val tables=listOf(SyncTable("employees",syncKeyColumnName="id"))
        var client=newClient(db,http,syncTables=tables)
        try {
            db.execSQL("PRAGMA foreign_keys=ON")
            db.execSQL("CREATE TABLE employees(id TEXT PRIMARY KEY NOT NULL,manager_id TEXT REFERENCES employees(id) DEFERRABLE INITIALLY DEFERRED,name TEXT NOT NULL)")
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO employees VALUES('A','B','a'),('B','C','b'),('C','A','c'),('U',NULL,'old')")
            client.pushPending().getOrThrow();client.closeFromCoordinatedOwner()
            server.wireLimits=testWireLimits().copy(maxPushRows=2)
            client=newClient(db,http,maxPushRows=2,syncTables=tables);client.openAndConnect("user-1").getOrThrow()
            db.execSQL("BEGIN");db.execSQL("DELETE FROM employees WHERE id!='U'");db.execSQL("UPDATE employees SET name='safe' WHERE id='U'");db.execSQL("COMMIT")
            val before=baselineQualificationVector(db,listOf("employees"));val remoteBefore=server.remoteRowsForBaselineTest()
            val error=client.sync().exceptionOrNull();assertTrue(error is PushDependencyGroupExceedsAdmittedLimitException)
            assertEquals(3L,error.componentRows);assertEquals(SyncProgress(dirtyRows=3,committedBundles=1),error.progress)
            assertEquals(2,server.pushRequestCount)
            val after=baselineQualificationVector(db,listOf("employees"))
            for(key in listOf("A","B","C")) {
                assertEquals(before.row("_sync_dirty_rows",key),after.row("_sync_dirty_rows",key))
                assertEquals(before.row("_sync_row_state",key),after.row("_sync_row_state",key))
                assertEquals(remoteBefore["employees/id=$key"],server.remoteRowsForBaselineTest()["employees/id=$key"])
            }
            assertTrue(after.rows("_sync_outbox_rows").isEmpty())
            assertTrue(client.pushPending().exceptionOrNull() is PushDependencyGroupExceedsAdmittedLimitException)
            assertEquals(after,baselineQualificationVector(db,listOf("employees")));assertEquals(2,server.pushRequestCount)
            println("A2-04 confirmed-prefix error=$error state=${after.tables} wire=${server.pushBodies} remote=${server.remoteRowsForBaselineTest()}")
        } finally {client.closeFromCoordinatedOwner();http.close();db.close()}
    }

    @Test
    fun dependencyCycleCompleteByteBoundary() = runOwnedWebDatabaseTest {
        suspend fun fixture(limit:Long?=null,expectRefusal:Boolean=false):Long {
            val db=newDb();val server=MockSyncServer().apply {registeredTableSpecs=testRegisteredTableSpecs("employees");wireLimits=testWireLimits().copy(maxPushRows=2,maxPushDecodedBytes=limit?:testWireLimits().maxPushDecodedBytes,maxRowDecodedBytes=limit?.minus(1)?:testWireLimits().maxRowDecodedBytes)}
            val http=server.newHttpClient();val client=newClient(db,http,maxPushRows=2,syncTables=listOf(SyncTable("employees",syncKeyColumnName="id")))
            try {
                db.execSQL("PRAGMA foreign_keys=ON")
                db.execSQL("CREATE TABLE employees(id TEXT PRIMARY KEY NOT NULL,manager_id TEXT REFERENCES employees(id) DEFERRABLE INITIALLY DEFERRED,name TEXT NOT NULL)")
                client.openAndConnect("user-1").getOrThrow()
                db.execSQL("INSERT INTO employees VALUES('X','Y','${"x".repeat(512)}'),('Y','X','${"y".repeat(512)}')")
                val before=baselineQualificationVector(db,listOf("employees"))
                val result=client.pushPending()
                if(expectRefusal) {
                    assertTrue(result.isFailure,"SCC must refuse the configured byte limit $limit")
                    val error=result.exceptionOrNull();assertTrue(error is PushDependencyGroupExceedsAdmittedLimitException)
                    assertEquals("decoded_bytes",error.dimension);assertEquals(2L,error.componentRows)
                    assertEquals(before,baselineQualificationVector(db,listOf("employees")));assertEquals(0,server.pushRequestCount)
                    println("A2-04 cycle byte refusal limit=$limit error=$error state=${before.tables}")
                    return assertNotNull(limit)
                }
                assertTrue(result.isSuccess,"SCC must fit the configured byte limit $limit: ${result.exceptionOrNull()}")
                result.getOrThrow();assertEquals(1,server.pushRequestCount)
                val bytes=server.pushBodies.single().encodeToByteArray().size.toLong()
                if(limit!=null) assertEquals(limit,bytes)
                assertEquals(2,server.remoteRowsForBaselineTest().size)
                assertTrue(baselineQualificationVector(db,listOf("employees")).rows("_sync_dirty_rows").isEmpty())
                println("A2-04 cycle bytes=$bytes state=${baselineQualificationVector(db,listOf("employees")).tables} wire=${server.pushBodies} remote=${server.remoteRowsForBaselineTest()}")
                return bytes
            } finally {client.closeFromCoordinatedOwner();http.close();db.close()}
        }
        val exact=fixture();fixture(exact);fixture(exact-1,expectRefusal=true)
    }


    @Test
    fun dependencyExactEncodedLimitsAndDigitWidth() = runOwnedWebDatabaseTest {
        var exactBytes=0L
        var exactRowBytes=0L
        suspend fun fixture(limit: Long?=null,rowLimit: Long?=null,id: Long=9,probe: Boolean=false): Pair<String,Long> {
            val db=newDb();val server=MockSyncServer();val http=server.newHttpClient()
            if(limit!=null) server.wireLimits=testWireLimits().copy(maxPushDecodedBytes=limit,maxRowDecodedBytes=checkNotNull(rowLimit))
            val client=newClient(db,http)
            try {
                createUsersAndPostsTables(db);client.openAndConnect("user-1").getOrThrow()
                insertUser(db,"one","quoted\" unicode ü")
                db.execSQL("UPDATE _sync_source_state SET next_source_bundle_id=$id")
                val before=baselineQualificationVector(db)
                if(probe) server.beforePushCommit={throw CancellationException("measure actual pre-commit wire")}
                val result=try {client.pushPending()} catch(error: CancellationException) {if(!probe) throw error;Result.failure(error)}
                if(probe) {
                    assertEquals(1,server.pushBodies.size)
                    val generation=testGeneration(server.registeredTableSpecs)
                    val wire=json.parseToJsonElement(server.pushBodies.single()).jsonObject
                    val row=OversqliteCompactCodec.decodeBundleRows(generation,wire.getValue("rows")).single()
                    val expanded=OversqliteCompactCodec.expandedRowDecodedBytes(PushRequestRow(row.schema,row.table,row.key,row.op,row.rowVersion,row.payload))
                    return server.pushBodies.single() to expanded
                }
                if(id==9L && limit==exactBytes && rowLimit==exactRowBytes) {
                    result.getOrThrow();assertEquals(1,server.pushRequestCount)
                    assertEquals(limit,server.pushBodies.single().encodeToByteArray().size.toLong())
                    assertTrue(baselineQualificationVector(db).rows("_sync_dirty_rows").isEmpty())
                } else {
                    assertTrue(result.isFailure)
                    if(rowLimit==exactRowBytes-1) assertTrue(result.exceptionOrNull() is PushRowExceedsAdmittedLimitException)
                    else assertTrue(result.exceptionOrNull() is InvalidAdmittedContractException)
                    assertEquals(before,baselineQualificationVector(db));assertEquals(0,server.pushRequestCount)
                }
                println("A2 exact id=$id limit=$limit rowLimit=$rowLimit state=${baselineQualificationVector(db).tables} wire=${server.pushBodies}")
                return "" to 0L
            } finally {client.closeFromCoordinatedOwner();http.close();db.close()}
        }
        val (wire,row)=fixture(probe=true)
        exactBytes=wire.encodeToByteArray().size.toLong();exactRowBytes=row
        fixture(exactBytes,row)
        fixture(exactBytes-1,row)
        fixture(exactBytes,row-1)
        fixture(exactBytes,row,id=10)
    }

    @Test
    fun dependencySharedWorkspaceAndDerivedLayoutRefusal() = runOwnedWebDatabaseTest {
        for(mode in listOf("entries","bytes","missing-plan","missing-node","missing-index","extra-column","version-only")) {
            val db=newDb();val server=MockSyncServer();val http=server.newHttpClient()
            var client=newClient(db,http)
            try {
                createUsersAndPostsTables(db);client.openAndConnect("user-1").getOrThrow();insertUser(db,"one","value")
                if(mode=="entries" || mode=="bytes") {
                    db.execSQL("INSERT INTO _sync_apply_plan(singleton_key,unit,guard,phase,root_cursor,visit_cursor,table_cursor,fk_cursor,row_cursor,entries,bytes,edge_entries,edge_bytes) VALUES(1,'independent-apply','retained','roots',0,0,0,0,NULL,0,0,0,0)")
                    client.closeFromCoordinatedOwner();client=newClient(db,http,workspaceEntries=if(mode=="entries") 49 else 100000,workspaceBytes=if(mode=="bytes") 8192 else 256L*1024*1024)
                    client.openAndConnect("user-1").getOrThrow();val before=baselineQualificationVector(db)
                    val error=client.pushPending().exceptionOrNull();assertTrue(error is SyncWorkspaceLimitException);assertEquals(mode,error.dimension)
                    assertEquals(before,baselineQualificationVector(db));assertEquals("independent-apply",scalarText(db,"SELECT unit FROM _sync_apply_plan"));assertEquals(0,server.pushRequestCount)
                    client.closeFromCoordinatedOwner();client=newClient(db,http);client.openAndConnect("user-1").getOrThrow();client.pushPending().getOrThrow()
                    assertEquals(1,server.pushRequestCount);assertTrue(baselineQualificationVector(db).rows("_sync_dirty_rows").isEmpty())
                } else {
                    client.closeFromCoordinatedOwner()
                    db.execSQL(when(mode) {"missing-plan" -> "DROP TABLE _sync_push_plan";"missing-node" -> "DROP TABLE _sync_push_nodes";"missing-index" -> "DROP INDEX _sync_push_nodes_frontier_idx";"extra-column" -> "ALTER TABLE _sync_push_plan ADD COLUMN extension TEXT";else -> "DROP TABLE _sync_row_state"})
                    if(mode=="version-only") db.execSQL("CREATE TABLE _sync_row_state(schema_name TEXT,table_name TEXT,key_json TEXT,row_version INTEGER,deleted INTEGER)")
                    val before=baselineQualificationVector(db);val objects=scalarLong(db,"SELECT COUNT(*) FROM sqlite_schema")
                    val schemaBefore=scalarText(db,"SELECT json_group_array(json_array(type,name,tbl_name,rootpage,sql)) FROM (SELECT * FROM sqlite_schema ORDER BY type,name)")
                    val journalBefore=scalarText(db,"PRAGMA journal_mode")
                    client=newClient(db,http);assertTrue(client.open().isFailure)
                    assertEquals(before,baselineQualificationVector(db));assertEquals(objects,scalarLong(db,"SELECT COUNT(*) FROM sqlite_schema"));assertEquals(0,server.pushRequestCount)
                    assertEquals(schemaBefore,scalarText(db,"SELECT json_group_array(json_array(type,name,tbl_name,rootpage,sql)) FROM (SELECT * FROM sqlite_schema ORDER BY type,name)"))
                    assertEquals(journalBefore,scalarText(db,"PRAGMA journal_mode"))
                }
            } finally {client.closeFromCoordinatedOwner();http.close();db.close()}
        }
    }

    @Test
    fun dependencyInsertionComponentsAndUnavailableParentProgress() = runOwnedWebDatabaseTest {
        for(shape in listOf("chain","self","cycle","absent","unknown")) {
            val server=MockSyncServer().apply {registeredTableSpecs=testRegisteredTableSpecs("employees")}
            val db=newDb();val http=server.newHttpClient()
            val tables=listOf(SyncTable("employees",syncKeyColumnName="id"))
            var client: DefaultOversqliteClient?=null
            try {
                db.execSQL("PRAGMA foreign_keys=ON")
                db.execSQL("CREATE TABLE employees(id TEXT PRIMARY KEY NOT NULL,manager_id TEXT REFERENCES employees(id) DEFERRABLE INITIALLY DEFERRED,name TEXT NOT NULL)")
                if(shape=="absent" || shape=="unknown") {
                    client=newClient(db,http,syncTables=tables);client.openAndConnect("user-1").getOrThrow()
                    db.execSQL("INSERT INTO employees VALUES('P',NULL,'parent'),('C','P','child'),('D',NULL,'clean')")
                    client.pushPending().getOrThrow()
                    val known=if(shape=="unknown") 0 else 1
                    db.execSQL("""UPDATE _sync_row_state SET server_kind='$shape',row_version=0,server_payload=NULL,relationships_known=$known WHERE key_json='{"id":"P"}'""")
                    db.execSQL("UPDATE employees SET name='changed' WHERE id='C'")
                    db.execSQL("INSERT INTO employees VALUES('I',NULL,'independent')")
                    val before=baselineQualificationVector(db,listOf("employees"))
                    val pulls=server.pullRequestCount
                    val error=client.sync().exceptionOrNull();assertTrue(error is FKReconciliationRequiredException)
                    assertEquals(SyncProgress(dirtyRows=1,committedBundles=1),error.progress)
                    assertEquals(2,server.pushRequestCount);assertEquals(pulls,server.pullRequestCount)
                    val after=baselineQualificationVector(db,listOf("employees"))
                    for(key in listOf("P","C","D")) assertEquals(before.row("_sync_row_state",key),after.row("_sync_row_state",key))
                    assertEquals(before.row("_sync_dirty_rows","C"),after.row("_sync_dirty_rows","C"))
                    assertTrue(after.rows("_sync_outbox_rows").isEmpty())
                    println("A2 unavailable=$shape progress=${error.progress} local=${after.tables} wire=${server.pushBodies} remote=${server.remoteRowsForBaselineTest()}")
                } else {
                    server.wireLimits=testWireLimits().copy(maxPushRows=1)
                    client=newClient(db,http,maxPushRows=1,syncTables=tables);client.openAndConnect("user-1").getOrThrow()
                    val values=when(shape) {"self" -> "('S','S','self')";"cycle" -> "('X','Y','x'),('Y','X','y')";else -> "('C','B','child'),('B','A','middle'),('A',NULL,'root')"}
                    db.execSQL("INSERT INTO employees VALUES$values")
                    val before=baselineQualificationVector(db,listOf("employees"))
                    if(shape=="cycle") {
                        val error=client.pushPending().exceptionOrNull();assertTrue(error is PushDependencyGroupExceedsAdmittedLimitException)
                        assertEquals(2L,error.componentRows);assertEquals(before,baselineQualificationVector(db,listOf("employees")));assertEquals(0,server.pushRequestCount)
                    } else {
                        val keys=if(shape=="self") listOf("S") else listOf("A","B","C")
                        keys.forEachIndexed { index,key ->
                            client.pushPending().getOrThrow();assertEquals(index+1,server.pushRequestCount)
                            assertEquals(keys.take(index+1).map {"employees/id=$it"}.toSet(),server.remoteRowsForBaselineTest().keys)
                            println("A2 insert=$shape selected=$key local=${baselineQualificationVector(db,listOf("employees")).tables} wire=${server.pushBodies.last()} remote=${server.remoteRowsForBaselineTest()}")
                        }
                        assertTrue(baselineQualificationVector(db,listOf("employees")).rows("_sync_dirty_rows").isEmpty())
                    }
                }
            } finally {client?.closeFromCoordinatedOwner();http.close();db.close()}
        }
    }

    @Test
    fun dependencyWorkResumesAcrossPhysicalReopen() = runOwnedWebDatabaseTest {
        if(!supportsDurableTestDatabaseRestart()) {
            println("NOT-APPLICABLE: physical planner restart requires durable storage; Node remains transient")
            return@runOwnedWebDatabaseTest
        }
        val path=createTempSqliteNowTestDbPath("dependency-work-resume")
        val server=MockSyncServer();val http=server.newHttpClient()
        var db=newFileBackedDb(path)
        var client: DefaultOversqliteClient?=null
        try {
            createUsersAndPostsTables(db)
            client=newClient(db,http);client.openAndConnect("user-1").getOrThrow()
            for(i in 0..7) insertUser(db,"$i","value $i")
            val before=baselineQualificationVector(db)
            val info=TableInfoCache()
            var stepWork=32L
            for(table in listOf("users","posts")) {
                val metadata=info.get(db,table)
                stepWork+=4+16*metadata.columns.size+32*metadata.foreignKeys.size
            }
            client.closeFromCoordinatedOwner()
            client=newClient(db,http,workUnits=2*stepWork);client.openAndConnect("user-1").getOrThrow()
            val error=client.pushPending().exceptionOrNull()
            assertTrue(error is SyncWorkLimitException);assertEquals(SyncProgress(dirtyRows=8),error.progress)
            assertEquals(before,baselineQualificationVector(db));assertEquals(2L,scalarLong(db,"SELECT COUNT(*) FROM _sync_push_nodes"))
            assertEquals(0,server.pushRequestCount)
            val unit=scalarText(db,"SELECT unit FROM _sync_push_plan")
            client.closeFromCoordinatedOwner();db.close();db=newFileBackedDb(path)
            client=newClient(db,http,workUnits=2*stepWork);client.openAndConnect("user-1").getOrThrow()
            assertEquals(before,baselineQualificationVector(db))
            val resumed=client.pushPending().exceptionOrNull()
            assertTrue(resumed is SyncWorkLimitException);assertEquals(error.unit,resumed.unit)
            assertEquals(unit,scalarText(db,"SELECT unit FROM _sync_push_plan"))
            assertEquals(4L,scalarLong(db,"SELECT COUNT(*) FROM _sync_push_nodes"))
            assertEquals(before,baselineQualificationVector(db));assertEquals(0,server.pushRequestCount)
            client.closeFromCoordinatedOwner();client=newClient(db,http)
            client.openAndConnect("user-1").getOrThrow();client.pushPending().getOrThrow()
            val final=baselineQualificationVector(db)
            assertTrue(final.rows("_sync_dirty_rows").isEmpty());assertTrue(final.rows("_sync_outbox_rows").isEmpty())
            assertEquals(before.rows("users"),final.rows("users"));assertEquals(1,server.pushRequestCount)
            assertEquals(8,server.remoteRowsForBaselineTest().size)
        } finally {client?.closeFromCoordinatedOwner();db.close();http.close();deleteTempSqliteNowTestDbArtifacts(path)}
    }

    @Test
    fun attachmentEpochIsExactOnceAndOverflowPreservesState() = runOwnedWebDatabaseTest {
        val server=MockSyncServer();val http=server.newHttpClient();val db=newDb()
        val client=newClient(db,http)
        try {
            createUsersAndPostsTables(db);client.open().getOrThrow()
            val owner=OversqliteAttachmentStateStore(db)
            val initial=owner.loadState()
            client.attach("user-1").getOrThrow()
            val attached=owner.loadState();assertEquals(initial.attachmentEpoch+1,attached.attachmentEpoch)
            owner.persistState(attached.copy(lastBundleSeqSeen=1))
            assertEquals(attached.attachmentEpoch,owner.loadState().attachmentEpoch)
            client.detach().getOrThrow()
            val detached=owner.loadState();assertEquals(attached.attachmentEpoch+1,detached.attachmentEpoch)
            client.attach("user-1").getOrThrow()
            assertEquals(detached.attachmentEpoch+1,owner.loadState().attachmentEpoch)
            val before=baselineQualificationVector(db)
            db.execSQL("BEGIN")
            owner.advanceAttachmentEpoch();db.execSQL("ROLLBACK")
            assertEquals(before,baselineQualificationVector(db))
            db.execSQL("UPDATE _sync_attachment_state SET attachment_epoch=9223372036854775807")
            val atLimit=baselineQualificationVector(db)
            assertFailsWith<Exception> { owner.advanceAttachmentEpoch() }
            assertEquals(atLimit,baselineQualificationVector(db))
            assertFailsWith<Exception> { owner.persistState(owner.loadState().copy(attachedUserId="another-user")) }
            assertEquals(atLimit,baselineQualificationVector(db))
        } finally {client.closeFromCoordinatedOwner();db.close();http.close()}
    }

    @Test
    fun dependencyReparentBeyondActualHardLookahead() = runOwnedWebDatabaseTest {
        for (newParent in listOf(false,true)) {
            val server=MockSyncServer().apply {registeredTableSpecs=testRegisteredTableSpecs("employees")}
            val db=newDb();val http=server.newHttpClient()
            try {
                db.execSQL("PRAGMA foreign_keys=ON")
                db.execSQL("CREATE TABLE employees(id TEXT PRIMARY KEY NOT NULL,manager_id TEXT REFERENCES employees(id) DEFERRABLE INITIALLY DEFERRED,name TEXT NOT NULL)")
                val tables=listOf(SyncTable("employees",syncKeyColumnName="id"))
                val seed=newClient(db,http,syncTables=tables)
                seed.openAndConnect("user-1").getOrThrow()
                db.execSQL("INSERT INTO employees VALUES('P',NULL,'parent'),('Q',NULL,'other'),('C','P','child'),('D','C','clean'),('f1',NULL,'one'),('f2',NULL,'two'),('f3',NULL,'three'),('f4',NULL,'four')")
                seed.pushPending().getOrThrow();seed.closeFromCoordinatedOwner()
                assertTrue(server.remoteRowsForBaselineTest().containsKey("employees/id=P"))
                server.wireLimits=testWireLimits().copy(maxPushRows=2)
                val client=newClient(db,http,maxPushRows=2,syncTables=tables)
                client.openAndConnect("user-1").getOrThrow()
                db.execSQL("BEGIN")
                db.execSQL("DELETE FROM employees WHERE id='P'")
                for(i in 1..4) db.execSQL("UPDATE employees SET name=name||'!' WHERE id='f$i'")
                val parent=if(newParent) "N" else "Q"
                db.execSQL("UPDATE employees SET manager_id='$parent' WHERE id='C'")
                if(newParent) db.execSQL("INSERT INTO employees VALUES('N',NULL,'new')")
                db.execSQL("COMMIT")
                val pOrdinal=scalarLong(db,"""SELECT dirty_ordinal FROM _sync_dirty_rows WHERE key_json='{"id":"P"}'""")
                val cOrdinal=scalarLong(db,"""SELECT dirty_ordinal FROM _sync_dirty_rows WHERE key_json='{"id":"C"}'""")
                assertEquals(5L,cOrdinal-pOrdinal);assertTrue(cOrdinal-pOrdinal>3)
                val before=baselineQualificationVector(db,listOf("employees"))
                val clean=before.row("_sync_row_state","D")
                val cleanBusiness=scalarText(db,"SELECT json_array(id,manager_id,name) FROM employees WHERE id='D'")
                val cleanEdges=scalarText(db,"""SELECT json_group_array(json_array(view,child_key,parent_key,owner_revision)) FROM _sync_relationships WHERE child_key='{"id":"D"}'""")
                repeat(if(newParent) 4 else 3) { call ->
                    client.pushPending().getOrThrow()
                    val after=baselineQualificationVector(db,listOf("employees"))
                    assertEquals(clean,after.row("_sync_row_state","D"))
                    assertEquals(cleanBusiness,scalarText(db,"SELECT json_array(id,manager_id,name) FROM employees WHERE id='D'"))
                    assertEquals(cleanEdges,scalarText(db,"""SELECT json_group_array(json_array(view,child_key,parent_key,owner_revision)) FROM _sync_relationships WHERE child_key='{"id":"D"}'"""))
                    assertTrue(after.rows("_sync_outbox_rows").isEmpty())
                    if(call<2) {
                        assertTrue(server.remoteRowsForBaselineTest().containsKey("employees/id=P"))
                        assertEquals("live",after.row("_sync_row_state","P").text("server_kind"))
                        assertEquals(before.row("_sync_dirty_rows","C"),after.row("_sync_dirty_rows","C"))
                    }
                    println("A2-03 reparent newParent=$newParent call=${call+1} local=${after.tables} wire=${server.pushBodies.last()} remote=${server.remoteRowsForBaselineTest()}")
                }
                val final=baselineQualificationVector(db,listOf("employees"))
                assertTrue(final.rows("_sync_dirty_rows").isEmpty())
                assertEquals("deleted",final.row("_sync_row_state","P").text("server_kind"))
                assertEquals(parent,scalarText(db,"""SELECT json_extract(server_payload,'$.manager_id.value') FROM _sync_row_state WHERE key_json='{"id":"C"}'"""))
                assertTrue(!server.remoteRowsForBaselineTest().containsKey("employees/id=P"))
            } finally {http.close();db.close()}
        }
    }

    @Test
    fun dependencyDiagnosticsPreserveBlockedIntent() = runOwnedWebDatabaseTest {
        val server=MockSyncServer().apply {registeredTableSpecs=testRegisteredTableSpecs("authors","profiles","users")}
        val db=newDb();val http=server.newHttpClient()
        try {
            db.execSQL("PRAGMA foreign_keys=ON")
            db.execSQL("CREATE TABLE authors(id TEXT PRIMARY KEY NOT NULL,profile_id TEXT NOT NULL REFERENCES profiles(id) DEFERRABLE INITIALLY DEFERRED,name TEXT NOT NULL)")
            db.execSQL("CREATE TABLE profiles(id TEXT PRIMARY KEY NOT NULL,author_id TEXT NOT NULL REFERENCES authors(id) DEFERRABLE INITIALLY DEFERRED,bio TEXT NOT NULL)")
            db.execSQL("CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)")
            val tables=listOf(SyncTable("users",syncKeyColumnName="id"),SyncTable("authors",syncKeyColumnName="id"),SyncTable("profiles",syncKeyColumnName="id"))
            val seed=newClient(db,http,syncTables=tables);seed.openAndConnect("user-1").getOrThrow()
            db.execSQL("BEGIN");db.execSQL("INSERT INTO authors VALUES('a','p','Ada')");db.execSQL("INSERT INTO profiles VALUES('p','a','bio')");db.execSQL("COMMIT")
            seed.pushPending().getOrThrow();seed.closeFromCoordinatedOwner()
            server.wireLimits=testWireLimits().copy(maxPushRows=1)
            var client=newClient(db,http,maxPushRows=1,syncTables=tables);client.openAndConnect("user-1").getOrThrow()
            db.execSQL("BEGIN");db.execSQL("DELETE FROM authors WHERE id='a'");db.execSQL("DELETE FROM profiles WHERE id='p'");db.execSQL("INSERT INTO users VALUES('safe','independent')");db.execSQL("COMMIT")
            client.pushPending().getOrThrow();assertEquals(2,server.pushRequestCount)
            val before=baselineQualificationVector(db,listOf("authors","profiles","users"))
            val error=client.pushPending().exceptionOrNull();assertTrue(error is PushDependencyGroupExceedsAdmittedLimitException)
            assertEquals(SyncProgress(dirtyRows=2),error.progress);assertEquals(2L,error.componentRows);assertEquals(2,error.sample.size)
            assertEquals(before,baselineQualificationVector(db,listOf("authors","profiles","users")))
            val first=client.readPushDependencyDiagnostic(error.diagnosticId,pageSize=1).getOrThrow()
            assertEquals(1,first.identities.size);assertNotNull(first.nextCursor)
            val second=client.readPushDependencyDiagnostic(error.diagnosticId,first.nextCursor,1).getOrThrow()
            assertEquals(1,second.identities.size);assertEquals(null,second.nextCursor)
            assertTrue(client.readPushDependencyDiagnostic(error.diagnosticId,first.nextCursor+"x",1).exceptionOrNull() is StalePushDependencyDiagnosticException)
            var retained=scalarLong(db,"SELECT (SELECT COUNT(*) FROM _sync_push_nodes)+(SELECT COUNT(*) FROM _sync_push_edges)")
            var interruptions=0
            repeat(16) {
                if(scalarLong(db,"SELECT COUNT(*) FROM _sync_push_plan")==0L) return@repeat
                client.closeFromCoordinatedOwner()
                client=newClient(db,http,maxPushRows=1,syncTables=tables,workspaceEntries=1,workspaceBytes=1,workUnits=8)
                assertTrue(client.open().isFailure)
                val released=client.releasePushDependencyDiagnostic(error.diagnosticId)
                val remaining=scalarLong(db,"SELECT (SELECT COUNT(*) FROM _sync_push_nodes)+(SELECT COUNT(*) FROM _sync_push_edges)")
                if(released.isFailure) {
                    val work=released.exceptionOrNull();assertTrue(work is SyncWorkLimitException);assertEquals(8L,work.used)
                    assertEquals(retained-1,remaining)
                    assertEquals(error.diagnosticId,scalarText(db,"SELECT json_extract(state,'$.cleanupDiagnosticId') FROM _sync_push_plan"))
                    interruptions++
                } else assertEquals(0L,remaining)
                retained=remaining;assertEquals(before,baselineQualificationVector(db,listOf("authors","profiles","users")))
                assertEquals(2,server.pushRequestCount)
            }
            assertTrue(interruptions>0);assertEquals(0L,scalarLong(db,"SELECT COUNT(*) FROM _sync_push_plan"))
            client.releasePushDependencyDiagnostic(error.diagnosticId).getOrThrow()
            assertEquals(before,baselineQualificationVector(db,listOf("authors","profiles","users")))
            client.closeFromCoordinatedOwner();client=newClient(db,http,maxPushRows=1,syncTables=tables);client.openAndConnect("user-1").getOrThrow()
            assertTrue(client.readPushDependencyDiagnostic(error.diagnosticId).exceptionOrNull() is StalePushDependencyDiagnosticException)
        } finally {http.close();db.close()}
    }

    @Test
    fun pushReplayPreservesDirtyExtraEffectAndTriggerAffectedUnrelatedRow() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val http = server.newHttpClient()
        val restartedHttp = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val client = newClient(db, http)
            client.openAndConnect("user-1").getOrThrow()
            for (id in listOf("a", "b", "c")) insertUser(db, id, "Original $id")
            client.sync().getOrThrow()
            db.execSQL("""
                CREATE TRIGGER mutate_c_after_a_apply AFTER UPDATE ON users
                WHEN NEW.id = 'a' AND NEW.name = 'Server A'
                BEGIN UPDATE users SET name='Trigger C' WHERE id='c'; END
            """.trimIndent())
            updateUserName(db, "a", "Local A")
            server.normalizePushEffects = { effects ->
                effects.map { row -> if (row.table == "users" && row.key["id"] == "a")
                    row.copy(payload = buildJsonObject { put("id", JsonPrimitive("a")); put("name", JsonPrimitive("Server A")) }) else row }
            }
            server.extraPushEffects = { rows, bundleSeq ->
                if (rows.none { it.table == "users" && it.key["id"] == "a" }) emptyList()
                else listOf(BundleRow("main", "users", mapOf("id" to "b"), "UPDATE", bundleSeq,
                    buildJsonObject { put("id", JsonPrimitive("b")); put("name", JsonPrimitive("Server B")) }))
            }
            val unrelatedBase = scalarText(db, """SELECT server_payload FROM _sync_row_state WHERE key_json='{"id":"c"}'""")
            server.beforePushResponse = {
                updateUserName(db, "b", "Desired B")
                updateUserName(db, "c", "Desired C")
            }

            val report = client.pushPending().getOrThrow()
            assertEquals(PushOutcome.COMMITTED, report.outcome)
            assertEquals("Desired B", scalarText(db, "SELECT name FROM users WHERE id='b'"))
            assertEquals("Desired C", scalarText(db, "SELECT name FROM users WHERE id='c'"))
            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            val bKey = """{"id":"b"}"""
            val cKey = """{"id":"c"}"""
            assertEquals(2L, scalarLong(db, "SELECT base_row_version FROM _sync_dirty_rows WHERE key_json='$bKey'"))
            assertContains(scalarText(db, "SELECT payload FROM _sync_dirty_rows WHERE key_json='$bKey'"), "Desired B")
            assertContains(scalarText(db, "SELECT payload FROM _sync_dirty_rows WHERE key_json='$cKey'"), "Desired C")
            assertEquals("Server A", scalarText(db, """SELECT json_extract(server_payload,'$.name.value') FROM _sync_row_state WHERE key_json='{"id":"a"}'"""))
            assertEquals("Server B", scalarText(db, """SELECT json_extract(server_payload,'$.name.value') FROM _sync_row_state WHERE key_json='{"id":"b"}'"""))
            assertEquals(scalarText(db, "SELECT server_payload FROM _sync_row_state WHERE key_json='$bKey'"),
                scalarText(db, "SELECT base_payload FROM _sync_dirty_rows WHERE key_json='$bKey'"))
            assertEquals(unrelatedBase, scalarText(db, "SELECT base_payload FROM _sync_dirty_rows WHERE key_json='$cKey'"))
            assertEquals(unrelatedBase, scalarText(db, "SELECT server_payload FROM _sync_row_state WHERE key_json='$cKey'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_relationships"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(3L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))

            client.closeFromCoordinatedOwner()
            val restarted = newClient(db, restartedHttp)
            restarted.openAndConnect("user-1").getOrThrow()
            restarted.sync().getOrThrow()
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(3, server.pushRequestCount)
        } finally {
            http.close()
            restartedHttp.close()
            db.close()
        }
    }

    @Test
    fun oneSyncDrainsWriteCommittedAfterPullResponseWasSelected() = runOwnedWebDatabaseTest {
        for (sameKey in listOf(true, false)) {
            val server = MockSyncServer()
            val leaderDb = newDb()
            val followerDb = newDb()
            val leaderHttp = server.newHttpClient()
            val followerHttp = server.newHttpClient()
            try {
                createUsersAndPostsTables(leaderDb)
                createUsersAndPostsTables(followerDb)
                val leader = newClient(leaderDb, leaderHttp)
                val follower = newClient(followerDb, followerHttp, resolver = ClientWinsResolver)
                leader.openAndConnect("user-1").getOrThrow()
                follower.openAndConnect("user-1").getOrThrow()
                insertUser(leaderDb, "shared", "Original")
                leader.pushPending().getOrThrow()
                follower.pullToStable().getOrThrow()
                leaderDb.execSQL("UPDATE users SET name='Server Update' WHERE id='shared'")
                leader.pushPending().getOrThrow()
                var lateWriteRan = false
                server.beforePullResponse = {
                    lateWriteRan = true
                    if (sameKey) {
                        followerDb.execSQL("UPDATE users SET name='Local Final' WHERE id='shared'")
                    } else {
                        insertUser(followerDb, "unrelated", "Local Final")
                    }
                    assertEquals(1L, scalarLong(followerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
                }

                val report = follower.sync().getOrThrow()
                assertTrue(lateWriteRan)
                assertEquals(PushOutcome.COMMITTED, report.pushOutcome,
                    "sameKey=$sameKey remote=${report.remoteOutcome} pushes=${server.pushRequestCount} dirty=${scalarLong(followerDb, "SELECT COUNT(*) FROM _sync_dirty_rows")}")
                assertEquals(0L, scalarLong(followerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
                assertEquals(0L, scalarLong(followerDb, "SELECT COUNT(*) FROM _sync_outbox_rows"))
                assertEquals(
                    if (sameKey) "Local Final" else "Server Update",
                    scalarText(followerDb, "SELECT name FROM users WHERE id='shared'"),
                )
                leader.pullToStable().getOrThrow()
                assertEquals(
                    if (sameKey) "Local Final" else "Server Update",
                    scalarText(leaderDb, "SELECT name FROM users WHERE id='shared'"),
                )
                if (!sameKey) {
                    assertEquals("Local Final", scalarText(leaderDb, "SELECT name FROM users WHERE id='unrelated'"))
                }
            } finally {
                leaderHttp.close()
                followerHttp.close()
                leaderDb.close()
                followerDb.close()
            }
        }
    }

    @Test
    fun oneSyncDrainsBacklogAcrossBoundedPushes() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val client = newClient(db, http, maxPushRows = 2)
            client.openAndConnect("user-1").getOrThrow()
            repeat(5) { insertUser(db, "user-$it", "Local $it") }
            val pushesBefore = server.pushRequestCount
            val pullsBefore = server.pullRequestCount

            val report = client.sync().getOrThrow()
            assertEquals(PushOutcome.COMMITTED, report.pushOutcome)
            assertEquals(3, server.pushRequestCount - pushesBefore)
            assertEquals(1, server.pullRequestCount - pullsBefore)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(5L, scalarLong(db, "SELECT COUNT(*) FROM users"))
        } finally {
            http.close()
            db.close()
        }
    }

    @Test
    fun oneSyncDrainsWritesAfterPullDiscoversPrunedHistory() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val client = newClient(db, http, maxPushRows = 1)
            client.openAndConnect("user-1").getOrThrow()
            insertUser(db, "seed", "Remote seed")
            client.pushPending().getOrThrow()
            db.execSQL("UPDATE _sync_attachment_state SET last_bundle_seq_seen = 0 WHERE singleton_key = 1")
            assertEquals(0L, scalarLong(db, "SELECT rebuild_required FROM _sync_attachment_state"),
                "recovery must first be discovered by pull")
            assertEquals(0L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            server.retainedBundleFloor = 1
            var lateWriteRan = false
            server.beforeSnapshotChunkResponse = {
                lateWriteRan = true
                insertUser(db, "late-a", "Local A")
                insertUser(db, "late-b", "Local B")
                assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            }
            val pushesBefore = server.pushRequestCount
            val pullsBefore = server.pullRequestCount
            val snapshotsBefore = server.snapshotSessionCreateRequestCount
            val sourceBefore = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")

            val report = client.sync().getOrThrow()
            assertTrue(lateWriteRan)
            assertEquals(PushOutcome.COMMITTED, report.pushOutcome)
            assertEquals(RemoteSyncOutcome.APPLIED_SNAPSHOT, report.remoteOutcome)
            assertEquals(2, server.pushRequestCount - pushesBefore)
            assertEquals(1, server.pullRequestCount - pullsBefore)
            assertEquals(2, server.snapshotSessionCreateRequestCount - snapshotsBefore)
            assertEquals(sourceBefore, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
            assertEquals(0L, scalarLong(db, "SELECT rebuild_required FROM _sync_attachment_state"))
            assertEquals(3L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("none", scalarText(db, "SELECT state FROM _sync_outbox_bundle"))
            assertEquals(4L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
            assertEquals(3L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals("Remote seed", scalarText(db, "SELECT name FROM users WHERE id='seed'"))
            assertEquals("Local A", scalarText(db, "SELECT name FROM users WHERE id='late-a'"))
            assertEquals("Local B", scalarText(db, "SELECT name FROM users WHERE id='late-b'"))
        } finally {
            http.close()
            db.close()
        }
    }

    @Test
    fun oneSyncRecoveryDrainsSeveralBatchesAndLateSnapshotGateWrite() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val client = newClient(db, http, maxPushRows = 1)
            client.openAndConnect("user-1").getOrThrow()
            repeat(3) { insertUser(db, "recovery-$it", "Local $it") }
            db.execSQL("UPDATE _sync_attachment_state SET rebuild_required = 1 WHERE singleton_key = 1")
            var lateWriteRan = false
            server.beforeSnapshotChunkResponse = {
                lateWriteRan = true
                insertUser(db, "recovery-late", "Late local")
            }
            val sourceBefore = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            val pushesBefore = server.pushRequestCount
            val snapshotsBefore = server.snapshotSessionCreateRequestCount

            val report = client.sync().getOrThrow()
            assertTrue(lateWriteRan)
            assertEquals(PushOutcome.COMMITTED, report.pushOutcome)
            assertEquals(4, server.pushRequestCount - pushesBefore)
            assertTrue(server.snapshotSessionCreateRequestCount - snapshotsBefore >= 2)
            assertEquals(sourceBefore, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
            assertEquals(0L, scalarLong(db, "SELECT rebuild_required FROM _sync_attachment_state"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("Late local", scalarText(db, "SELECT name FROM users WHERE id='recovery-late'"))
        } finally {
            http.close()
            db.close()
        }
    }

    @Test
    fun explicitSyncContinuesAcrossBatchesWhenBackgroundWorkIsPaused() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val client = newClient(db, http, maxPushRows = 1)
            client.openAndConnect("user-1").getOrThrow()
            repeat(3) { insertUser(db, "paused-$it", "Local $it") }
            client.pauseUploads()
            client.pauseDownloads()
            val pushesBefore = server.pushRequestCount
            val pullsBefore = server.pullRequestCount
            val report = client.sync().getOrThrow()
            assertEquals(PushOutcome.COMMITTED, report.pushOutcome)
            assertEquals(3, server.pushRequestCount - pushesBefore)
            assertEquals(1, server.pullRequestCount - pullsBefore)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            http.close()
            db.close()
        }
    }

    @Test
    fun oneSyncMayFinishOnAttempt64AndPull() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val client = newClient(db, http, maxPushRows = 1,workUnits=100_000_000)
            client.openAndConnect("user-1").getOrThrow()
            repeat(64) { insertUser(db, "user-$it", "Local $it") }
            val pushesBefore = server.pushRequestCount
            val pullsBefore = server.pullRequestCount

            val report = client.sync().getOrThrow()
            assertEquals(PushOutcome.COMMITTED, report.pushOutcome)
            assertEquals(64, server.pushRequestCount - pushesBefore)
            assertEquals(1, server.pullRequestCount - pullsBefore)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            http.close()
            db.close()
        }
    }

    @Test
    fun oneSyncStopsBeforeAttempt65AndPreservesRemainingIntent() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val client = newClient(db, http, maxPushRows = 1,workUnits=100_000_000)
            client.openAndConnect("user-1").getOrThrow()
            repeat(65) { insertUser(db, "user-$it", "Local $it") }
            val pushesBefore = server.pushRequestCount
            val pullsBefore = server.pullRequestCount

            val incomplete = client.sync().exceptionOrNull() as? SyncIncompleteException
            assertNotNull(incomplete)
            assertEquals(SyncIncompleteReason.PUSH_ATTEMPT_LIMIT, incomplete.reason)
            assertEquals(64, incomplete.pushAttempts)
            assertEquals(64, incomplete.confirmedBundleCount)
            assertEquals(1L, incomplete.dirtyCount)
            assertEquals(64, server.pushRequestCount - pushesBefore)
            assertEquals(0, server.pullRequestCount - pullsBefore)
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))

            val report = client.sync().getOrThrow()
            assertEquals(PushOutcome.COMMITTED, report.pushOutcome)
            assertEquals(65, server.pushRequestCount - pushesBefore)
            assertEquals(1, server.pullRequestCount - pullsBefore)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            http.close()
            db.close()
        }
    }

    @Test
    fun oneSyncCountsConflictReentryAgainstAttempt65() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val client = newClient(db, http, maxPushRows = 1, resolver = ClientWinsResolver,workUnits=100_000_000)
            client.openAndConnect("user-1").getOrThrow()
            insertUser(db, "zzzz", "Original")
            client.pushPending().getOrThrow()
            repeat(63) { insertUser(db, "a-${it.toString().padStart(3, '0')}", "Backlog") }
            updateUserName(db, "zzzz", "Local")
            server.conflictOverride = { row, _, _ ->
                if (row.table == "users" && row.key["id"] == "zzzz") {
                    PushConflictDetails(
                        schema = row.schema, table = row.table, key = row.key, op = row.op,
                        baseRowVersion = row.baseRowVersion, serverRowVersion = 2,
                        serverRowDeleted = false,
                        serverRow = buildJsonObject {
                            put("id", JsonPrimitive("zzzz"))
                            put("name", JsonPrimitive("Server"))
                        },
                    )
                } else null
            }
            val pushesBefore = server.pushRequestCount
            val pullsBefore = server.pullRequestCount

            val incomplete = client.sync().exceptionOrNull() as? SyncIncompleteException
            assertNotNull(incomplete)
            assertEquals(SyncIncompleteReason.PUSH_ATTEMPT_LIMIT, incomplete.reason)
            assertEquals(64, incomplete.pushAttempts)
            assertEquals(63, incomplete.confirmedBundleCount)
            assertEquals(1L, incomplete.dirtyCount)
            assertEquals(0L, incomplete.outboundCount)
            assertEquals(64, server.pushRequestCount - pushesBefore)
            assertEquals(0, server.pullRequestCount - pullsBefore)
            assertEquals("Local", scalarText(db, "SELECT name FROM users WHERE id='zzzz'"))
        } finally {
            http.close()
            db.close()
        }
    }

    @Test
    fun oneSyncWholeOperationRetryKeepsAttemptAllowance() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val client = newClient(db, http, maxPushRows = 1,workUnits=100_000_000)
            client.openAndConnect("user-1").getOrThrow()
            repeat(64) { insertUser(db, "retry-$it", "Local $it") }
            server.beforePullResponse = { insertUser(db, "retry-late", "Late") }
            server.remainingTransientPullFailures = 1
            val pushesBefore = server.pushRequestCount

            val incomplete = client.sync().exceptionOrNull() as? SyncIncompleteException
            assertNotNull(incomplete)
            assertEquals(SyncIncompleteReason.PUSH_ATTEMPT_LIMIT, incomplete.reason)
            assertEquals(64, incomplete.pushAttempts)
            assertEquals(64, incomplete.confirmedBundleCount)
            assertEquals(1L, incomplete.dirtyCount)
            assertEquals(64, server.pushRequestCount - pushesBefore)
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals("Late", scalarText(db, "SELECT name FROM users WHERE id='retry-late'"))
        } finally {
            http.close()
            db.close()
        }
    }

    @Test
    fun foreignKeyCycleAboveSoftByteTargetSendsIntactGroup() = runOwnedWebDatabaseTest {
        val specs = testRegisteredTableSpecs("authors", "profiles")
        val server = MockSyncServer().apply { registeredTableSpecs = specs }
        val db = newDb()
        val http = server.newHttpClient()
        try {
            db.execSQL("PRAGMA foreign_keys = ON")
            db.execSQL("CREATE TABLE authors(id TEXT PRIMARY KEY NOT NULL, profile_id TEXT NOT NULL REFERENCES profiles(id) DEFERRABLE INITIALLY DEFERRED, name TEXT NOT NULL)")
            db.execSQL("CREATE TABLE profiles(id TEXT PRIMARY KEY NOT NULL, author_id TEXT NOT NULL REFERENCES authors(id) DEFERRABLE INITIALLY DEFERRED, bio TEXT NOT NULL)")
            val client = newClient(db, http, maxPushRows = 2, maxPushDecodedBytes = 64,
                syncTables = listOf(SyncTable("authors", syncKeyColumnName = "id"),
                    SyncTable("profiles", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("BEGIN")
            db.execSQL("INSERT INTO authors(id, profile_id, name) VALUES('a1', 'p1', 'Ada')")
            db.execSQL("INSERT INTO profiles(id, author_id, bio) VALUES('p1', 'a1', 'bio')")
            db.execSQL("COMMIT")

            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            client.pushPending().getOrThrow()
            assertEquals(1, server.pushRequestCount)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(2L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
        } finally {
            http.close()
            db.close()
        }
    }

    @Test
    fun cyclicDeletesStayInOnePushAcrossSoftRowAndByteTargets() = runOwnedWebDatabaseTest {
        for (target in listOf("rows", "bytes", "legacy")) {
            val specs = testRegisteredTableSpecs("authors", "profiles")
            val server = MockSyncServer().apply { registeredTableSpecs = specs }
            val db = newDb()
            val http = server.newHttpClient()
            try {
                db.execSQL("PRAGMA foreign_keys = ON")
                db.execSQL("CREATE TABLE authors(id TEXT PRIMARY KEY NOT NULL, profile_id TEXT NOT NULL REFERENCES profiles(id) DEFERRABLE INITIALLY DEFERRED, name TEXT NOT NULL)")
                db.execSQL("CREATE TABLE profiles(id TEXT PRIMARY KEY NOT NULL, author_id TEXT NOT NULL REFERENCES authors(id) DEFERRABLE INITIALLY DEFERRED, bio TEXT NOT NULL)")
                val client = newClient(db, http, maxPushRows = if (target != "bytes") 1 else 2,
                    maxPushDecodedBytes = if (target == "bytes") 64 else 1_000_000,
                    syncTables = listOf(SyncTable("authors", syncKeyColumnName = "id"),
                        SyncTable("profiles", syncKeyColumnName = "id")))
                client.openAndConnect("user-1").getOrThrow()
                db.execSQL("BEGIN")
                db.execSQL("INSERT INTO authors VALUES('a1','p1','Ada')")
                db.execSQL("INSERT INTO profiles VALUES('p1','a1','bio')")
                db.execSQL("COMMIT")
                client.pushPending().getOrThrow()
                assertEquals(1, server.pushRequestCount)

                db.execSQL("BEGIN")
                db.execSQL("DELETE FROM authors WHERE id='a1'")
                db.execSQL("DELETE FROM profiles WHERE id='p1'")
                db.execSQL("COMMIT")
                if (target == "legacy") db.execSQL("UPDATE _sync_dirty_rows SET payload=NULL WHERE op='DELETE'")
                assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows WHERE op='DELETE'"))
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
                assertEquals(2L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
                client.pushPending().getOrThrow()
                assertEquals(2, server.pushRequestCount, target)
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
                assertEquals(3L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
            } finally {
                http.close()
                db.close()
            }
        }
    }

    @Test
    fun cyclicDeleteAboveAdmittedRowLimitFailsBeforeFreeze() = runOwnedWebDatabaseTest {
        val specs = testRegisteredTableSpecs("authors", "profiles")
        val server = MockSyncServer().apply { registeredTableSpecs = specs }
        val db = newDb()
        val http = server.newHttpClient()
        try {
            db.execSQL("PRAGMA foreign_keys = ON")
            db.execSQL("CREATE TABLE authors(id TEXT PRIMARY KEY NOT NULL, profile_id TEXT NOT NULL REFERENCES profiles(id) DEFERRABLE INITIALLY DEFERRED, name TEXT NOT NULL)")
            db.execSQL("CREATE TABLE profiles(id TEXT PRIMARY KEY NOT NULL, author_id TEXT NOT NULL REFERENCES authors(id) DEFERRABLE INITIALLY DEFERRED, bio TEXT NOT NULL)")
            val tables = listOf(SyncTable("authors", syncKeyColumnName = "id"),
                SyncTable("profiles", syncKeyColumnName = "id"))
            val seed = newClient(db, http, syncTables = tables)
            seed.openAndConnect("user-1").getOrThrow()
            db.execSQL("BEGIN")
            db.execSQL("INSERT INTO authors VALUES('a1','p1','Ada')")
            db.execSQL("INSERT INTO profiles VALUES('p1','a1','bio')")
            db.execSQL("COMMIT")
            seed.pushPending().getOrThrow()
            seed.closeFromCoordinatedOwner()

            server.wireLimits = testWireLimits().copy(maxPushRows = 1)
            val client = newClient(db, http, maxPushRows = 1, syncTables = tables)
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("BEGIN")
            db.execSQL("DELETE FROM authors WHERE id='a1'")
            db.execSQL("DELETE FROM profiles WHERE id='p1'")
            db.execSQL("COMMIT")
            val nextSource = scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state")
            val error = client.pushPending().exceptionOrNull()
            assertTrue(error is PushDependencyGroupExceedsAdmittedLimitException)
            assertEquals("rows", error.dimension)
            assertEquals(2L, error.actual)
            assertEquals(2L, error.componentRows)
            assertEquals(1, server.pushRequestCount)
            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(nextSource, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
        } finally {
            http.close()
            db.close()
        }
    }

    @Test
    fun updateThenDeleteOrdersSplittableServerBaseDependencies() = runOwnedWebDatabaseTest {
        val server = MockSyncServer().apply { registeredTableSpecs = testRegisteredTableSpecs("employees") }
        val db = newDb()
        val http = server.newHttpClient()
        try {
            db.execSQL("PRAGMA foreign_keys = ON")
            db.execSQL("CREATE TABLE employees(id TEXT PRIMARY KEY NOT NULL, manager_id TEXT REFERENCES employees(id) DEFERRABLE INITIALLY DEFERRED, name TEXT NOT NULL)")
            val tables = listOf(SyncTable("employees", syncKeyColumnName = "id"))
            val seed = newClient(db, http, maxPushRows = 2, syncTables = tables)
            seed.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO employees VALUES('P',NULL,'parent'),('C','P','child')")
            seed.pushPending().getOrThrow()
            seed.closeFromCoordinatedOwner()

            val client = newClient(db, http, maxPushRows = 1, syncTables = tables)
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("UPDATE employees SET name='changed' WHERE id='P'")
            db.execSQL("UPDATE employees SET manager_id=NULL WHERE id='C'")
            db.execSQL("DELETE FROM employees WHERE id='P'")
            db.execSQL("DELETE FROM employees WHERE id='C'")
            assertEquals("P", scalarText(db, """SELECT json_extract(base_payload,'$.manager_id.value') FROM _sync_dirty_rows WHERE key_json='{"id":"C"}'"""))
            val desired = mutableListOf<String?>()
            db.withStatementCache { cache ->
                OversqliteSyncStateStore(db) { error("intent enumeration does not decode payloads") }.forEachDirtyIntent(cache) { _, _, _, payload -> desired += payload }
            }
            assertEquals(listOf<String?>(null, null), desired)
            val before=baselineQualificationVector(db,listOf("employees"))
            client.pushPending().getOrThrow()
            assertEquals(2, server.pushRequestCount)
            val afterChild=baselineQualificationVector(db,listOf("employees"))
            assertEquals(before.row("_sync_dirty_rows","P"),afterChild.row("_sync_dirty_rows","P"))
            assertEquals(before.row("_sync_row_state","P"),afterChild.row("_sync_row_state","P"))
            assertTrue(server.remoteRowsForBaselineTest().keys.none {it=="employees/id=C"})
            assertTrue(server.remoteRowsForBaselineTest().keys.any {it=="employees/id=P"})
            client.pushPending().getOrThrow();assertEquals(3,server.pushRequestCount)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(4L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
        } finally {
            http.close()
            db.close()
        }
    }

    @Test
    fun fkDeletePeerBeyondInitialBatchPageIsIncluded() = runOwnedWebDatabaseTest {
        val server = MockSyncServer().apply { registeredTableSpecs = testRegisteredTableSpecs("employees") }
        val db = newDb()
        val http = server.newHttpClient()
        try {
            db.execSQL("PRAGMA foreign_keys = ON")
            db.execSQL("CREATE TABLE employees(id TEXT PRIMARY KEY NOT NULL, manager_id TEXT REFERENCES employees(id) DEFERRABLE INITIALLY DEFERRED, name TEXT NOT NULL)")
            val tables = listOf(SyncTable("employees", syncKeyColumnName = "id"))
            val seed = newClient(db, http, syncTables = tables)
            seed.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO employees VALUES('P',NULL,'parent'),('C','P','child'),('U1',NULL,'one'),('U2',NULL,'two'),('U3',NULL,'three')")
            seed.pushPending().getOrThrow()
            seed.closeFromCoordinatedOwner()

            server.wireLimits = testWireLimits().copy(maxPushRows = 2)
            val client = newClient(db, http, maxPushRows = 1, syncTables = tables)
            client.openAndConnect("user-1").getOrThrow()
            for (statement in listOf(
                "UPDATE employees SET name='changed' WHERE id='P'",
                "UPDATE employees SET name='changed' WHERE id='U1'",
                "UPDATE employees SET name='changed' WHERE id='U2'",
                "UPDATE employees SET name='changed' WHERE id='U3'",
                "UPDATE employees SET manager_id=NULL WHERE id='C'",
                "DELETE FROM employees WHERE id='P'",
                "DELETE FROM employees WHERE id='C'",
                "DELETE FROM employees WHERE id='U1'",
                "DELETE FROM employees WHERE id='U2'",
                "DELETE FROM employees WHERE id='U3'",
            )) db.execSQL(statement)
            val before=baselineQualificationVector(db,listOf("employees"))
            val generation=testGeneration(server.registeredTableSpecs)
            listOf("U1","U2","U3","C","P").forEachIndexed { index,key ->
                client.pushPending().getOrThrow();assertEquals(index+2,server.pushRequestCount)
                val wire=json.parseToJsonElement(server.pushBodies.last()).jsonObject
                val rows=OversqliteCompactCodec.decodeBundleRows(generation,wire.getValue("rows"))
                assertEquals(1,rows.size);assertEquals(key,rows.single().key["id"])
                assertEquals((4-index).toLong(),scalarLong(db,"SELECT COUNT(*) FROM _sync_dirty_rows"));assertEquals(0L,scalarLong(db,"SELECT COUNT(*) FROM _sync_outbox_rows"))
                if(index<3) {
                    val after=baselineQualificationVector(db,listOf("employees"))
                    for(blocked in listOf("P","C")) {
                        assertEquals(before.row("_sync_dirty_rows",blocked),after.row("_sync_dirty_rows",blocked))
                        assertEquals(before.row("_sync_row_state",blocked),after.row("_sync_row_state",blocked))
                    }
                }
            }
        } finally {
            http.close()
            db.close()
        }
    }

    @Test
    fun foreignKeyCycleAboveHardByteLimitFailsBeforeFreezeOrSourceIdConsumption() = runOwnedWebDatabaseTest {
        val specs = testRegisteredTableSpecs("authors", "profiles")
        val generation = testGeneration(specs)
        val author = PushRequestRow("main", "authors", mapOf("id" to "a1"), "INSERT", 0,
            json.parseToJsonElement("""{"id":"a1","profile_id":"p1","name":"Ada"}"""))
        val profile = PushRequestRow("main", "profiles", mapOf("id" to "p1"), "INSERT", 0,
            json.parseToJsonElement("""{"id":"p1","author_id":"a1","bio":"bio"}"""))
        fun completeBytes(rows: List<PushRequestRow>): Long = canonicalizeJsonElement(buildJsonObject {
            put("source_bundle_id", JsonPrimitive(1))
            put("canonical_request_hash", JsonPrimitive("a".repeat(64)))
            put("rows", OversqliteCompactCodec.encodePushRows(generation, rows))
        }).encodeToByteArray().size.toLong()
        val oneRowBytes = completeBytes(listOf(author))
        val twoRowBytes = completeBytes(listOf(author, profile))
        val hardLimit = (oneRowBytes + twoRowBytes) / 2
        assertTrue(oneRowBytes < hardLimit && hardLimit < twoRowBytes)

        val server = MockSyncServer().apply {
            registeredTableSpecs = specs
            wireLimits = testWireLimits().copy(
                maxPushDecodedBytes = hardLimit,
                maxRowDecodedBytes = hardLimit - 1,
            )
        }
        val db = newDb()
        val http = server.newHttpClient()
        try {
            db.execSQL("PRAGMA foreign_keys = ON")
            db.execSQL("CREATE TABLE authors(id TEXT PRIMARY KEY NOT NULL, profile_id TEXT NOT NULL REFERENCES profiles(id) DEFERRABLE INITIALLY DEFERRED, name TEXT NOT NULL)")
            db.execSQL("CREATE TABLE profiles(id TEXT PRIMARY KEY NOT NULL, author_id TEXT NOT NULL REFERENCES authors(id) DEFERRABLE INITIALLY DEFERRED, bio TEXT NOT NULL)")
            val client = newClient(db, http, maxPushRows = 2, maxPushDecodedBytes = 64,
                syncTables = listOf(SyncTable("authors", syncKeyColumnName = "id"),
                    SyncTable("profiles", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("BEGIN")
            db.execSQL("INSERT INTO authors(id, profile_id, name) VALUES('a1', 'p1', 'Ada')")
            db.execSQL("INSERT INTO profiles(id, author_id, bio) VALUES('p1', 'a1', 'bio')")
            db.execSQL("COMMIT")

            val error = client.pushPending().exceptionOrNull()
            assertTrue(error is PushDependencyGroupExceedsAdmittedLimitException)
            assertEquals(0, server.pushRequestCount)
            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(1L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
        } finally {
            http.close()
            db.close()
        }
    }

    private data class ThreeClientSyncEnv(
        val dbA: SafeSQLiteConnection,
        val dbB: SafeSQLiteConnection,
        val observerDb: SafeSQLiteConnection,
        val clientA: DefaultOversqliteClient,
        val clientB: DefaultOversqliteClient,
        val observer: DefaultOversqliteClient,
    )

    private fun updatedAtResolver(): Resolver = Resolver { conflict ->
        val serverUpdatedAt = conflict.serverRow
            ?.jsonObject
            ?.get("updated_at")
            ?.jsonPrimitive
            ?.contentOrNull
        val localUpdatedAt = conflict.localPayload
            ?.jsonObject
            ?.get("updated_at")
            ?.jsonPrimitive
            ?.contentOrNull
        if (serverUpdatedAt != null && localUpdatedAt != null && localUpdatedAt > serverUpdatedAt) {
            MergeResult.KeepLocal
        } else {
            MergeResult.AcceptServer
        }
    }

    @Test
    fun sync_cancellationPropagates_releasesGate_andRecoversFrozenBundle() = runOwnedWebDatabaseTest {
        val db = newDb()
        val server = MockSyncServer()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val client = newClient(db, http)
            val unopenedResult = client.sourceInfo()
            assertTrue(unopenedResult.isFailure)
            assertTrue(unopenedResult.exceptionOrNull() is OpenRequiredException)

            client.open().getOrThrow()
            client.attach("user-1").getOrThrow()
            insertUser(db, "user-1", "Ada")
            val sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            val nextSourceBundleId = scalarLong(
                db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$sourceId'",
            )
            assertEquals(1L, nextSourceBundleId)
            assertEquals(outboxStateNone, scalarText(db, "SELECT state FROM _sync_outbox_bundle"))

            server.cancelNextPush = true
            assertFailsWith<CancellationException> {
                client.sync()
            }

            assertEquals(1, server.pushRequestCount)
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(outboxStatePrepared, scalarText(db, "SELECT state FROM _sync_outbox_bundle"))
            assertEquals(sourceId, scalarText(db, "SELECT source_id FROM _sync_outbox_bundle"))
            assertEquals(nextSourceBundleId, scalarLong(db, "SELECT source_bundle_id FROM _sync_outbox_bundle"))
            assertEquals(sourceId, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
            assertEquals(
                nextSourceBundleId,
                scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$sourceId'"),
            )

            client.sync().getOrThrow()

            assertEquals(2, server.pushRequestCount)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(outboxStateNone, scalarText(db, "SELECT state FROM _sync_outbox_bundle"))
            assertEquals(
                nextSourceBundleId + 1L,
                scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$sourceId'"),
            )
        } finally {
            http.close()
            db.close()
        }
    }

    private suspend fun withThreeClientSync(
        clientBResolver: Resolver = ServerWinsResolver,
        generation: SyncGeneration? = null,
        createTables: suspend (SafeSQLiteConnection) -> Unit = ::createUsersAndPostsTables,
        block: suspend ThreeClientSyncEnv.() -> Unit,
    ) {
        val server = MockSyncServer().apply { generationOverride = generation }
        val dbA = newDb()
        val dbB = newDb()
        val observerDb = newDb()
        val httpA = server.newHttpClient()
        val httpB = server.newHttpClient()
        val observerHttp = server.newHttpClient()
        try {
            createTables(dbA)
            createTables(dbB)
            createTables(observerDb)

            val clientA = newClient(dbA, httpA)
            val clientB = newClient(dbB, httpB, resolver = clientBResolver)
            val observer = newClient(observerDb, observerHttp)

            clientA.openAndConnect(userId = "user-1").getOrThrow()
            clientB.openAndConnect(userId = "user-1").getOrThrow()
            observer.openAndConnect(userId = "user-1").getOrThrow()
            clientA.rebuild().getOrThrow()
            clientB.rebuild().getOrThrow()
            observer.rebuild().getOrThrow()

            ThreeClientSyncEnv(dbA, dbB, observerDb, clientA, clientB, observer).block()
        } finally {
            httpA.close()
            httpB.close()
            observerHttp.close()
            dbA.close()
            dbB.close()
            observerDb.close()
        }
    }

    private suspend fun assertUpdatedAtConflictResolution(
        serverName: String,
        serverUpdatedAt: String,
        localName: String,
        localUpdatedAt: String,
        expectedName: String,
        expectedUpdatedAt: String,
    ) {
        val updatedAtGeneration = testGeneration(testRegisteredTableSpecs("users", "posts")).let { generation ->
            generation.copy(tables = generation.tables.map { table ->
                if (table.table == "users") table.copy(columns = table.columns + GenerationColumn("updated_at", "text", false)) else table
            })
        }
        withThreeClientSync(
            generation = updatedAtGeneration.copy(fingerprint = updatedAtGeneration.computeFingerprint()),
            clientBResolver = updatedAtResolver(),
            createTables = ::createUsersWithUpdatedAtAndPostsTables,
        ) {
            dbA.execSQL(
                """
                INSERT INTO users(id, name, updated_at)
                VALUES('u1', 'Original', '2026-03-24T00:00:00Z')
                """.trimIndent(),
            )
            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()
            observer.pullToStable().getOrThrow()

            dbA.execSQL("UPDATE users SET name = '$serverName', updated_at = '$serverUpdatedAt' WHERE id = 'u1'")
            dbB.execSQL("UPDATE users SET name = '$localName', updated_at = '$localUpdatedAt' WHERE id = 'u1'")
            clientA.pushPending().getOrThrow()
            clientB.pushPending().getOrThrow()
            observer.pullToStable().getOrThrow()

            assertEquals(expectedName, scalarText(dbB, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals(expectedUpdatedAt, scalarText(dbB, "SELECT updated_at FROM users WHERE id = 'u1'"))
            assertEquals(expectedName, scalarText(observerDb, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals(expectedUpdatedAt, scalarText(observerDb, "SELECT updated_at FROM users WHERE id = 'u1'"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        }
    }

    private suspend fun ThreeClientSyncEnv.prepareUserNameConflict(
        localName: String,
        beforeServerPush: suspend ThreeClientSyncEnv.() -> Unit = {},
    ) {
        insertUser(dbA, "u1", "Original")
        clientA.pushPending().getOrThrow()
        clientB.pullToStable().getOrThrow()
        observer.pullToStable().getOrThrow()

        updateUserName(dbA, "u1", "Server Name")
        updateUserName(dbB, "u1", localName)
        beforeServerPush()
        clientA.pushPending().getOrThrow()
    }

    private suspend fun ThreeClientSyncEnv.pushClientBAndObserveUser(
        expectedClientName: String? = null,
        expectedObserverName: String? = expectedClientName,
        assertCleanOutbox: Boolean = true,
    ) {
        clientB.pushPending().getOrThrow()
        observer.pullToStable().getOrThrow()

        if (expectedClientName != null) {
            assertEquals(expectedClientName, scalarText(dbB, "SELECT name FROM users WHERE id = 'u1'"))
        }
        if (expectedObserverName != null) {
            assertEquals(expectedObserverName, scalarText(observerDb, "SELECT name FROM users WHERE id = 'u1'"))
        }
        assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        if (assertCleanOutbox) {
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        }
    }

    @Test
    fun chunkedPushPullAndHydrate_convergeAcrossClients() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val dbA = newDb()
        val dbB = newDb()
        val dbC = newDb()
        val httpA = server.newHttpClient()
        val httpB = server.newHttpClient()
        val httpC = server.newHttpClient()
        try {
            createUsersAndPostsTables(dbA)
            createUsersAndPostsTables(dbB)
            createUsersAndPostsTables(dbC)

            val clientA = newClient(dbA, httpA, maxPushRows = 2, maxPullBundles = 2)
            val clientB = newClient(dbB, httpB, maxPushRows = 2, maxPullBundles = 2)
            val clientC = newClient(dbC, httpC, maxPushRows = 2, maxPullBundles = 2)

            clientA.openAndConnect(userId = "user-1").getOrThrow()
            clientB.openAndConnect(userId = "user-1").getOrThrow()
            clientC.openAndConnect(userId = "user-1").getOrThrow()

            clientA.rebuild().getOrThrow()
            clientB.rebuild().getOrThrow()
            clientC.rebuild().getOrThrow()

            insertUser(dbA, "u1", "Ada")
            insertPost(dbA, "p1", "u1", "Ada Post")
            insertUser(dbA, "u2", "Grace")
            insertPost(dbA, "p2", "u2", "Grace Post")
            insertUser(dbA, "u3", "Linus")
            insertPost(dbA, "p3", "u3", "Linus Post")

            repeat(3) {
                clientA.pushPending().getOrThrow()
            }
            assertEquals(3, server.pushRequestCount)

            clientB.pullToStable().getOrThrow()
            clientC.rebuild().getOrThrow()

            assertEquals(3L, clientA.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(3L, clientB.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(3L, clientC.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(3L, scalarLong(dbB, "SELECT COUNT(*) FROM users"))
            assertEquals(3L, scalarLong(dbB, "SELECT COUNT(*) FROM posts"))
            assertEquals(3L, scalarLong(dbC, "SELECT COUNT(*) FROM users"))
            assertEquals(3L, scalarLong(dbC, "SELECT COUNT(*) FROM posts"))
            assertEquals("Grace Post", scalarText(dbB, "SELECT title FROM posts WHERE id = 'p2'"))
            assertEquals("Linus", scalarText(dbC, "SELECT name FROM users WHERE id = 'u3'"))
            assertEquals(0L, scalarLong(dbA, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbC, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            httpA.close()
            httpB.close()
            httpC.close()
            dbA.close()
            dbB.close()
            dbC.close()
        }
    }

    @Test
    fun pullToStable_succeedsWithoutSqliteNowInvalidationBinding() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val leaderDb = newDb()
        val followerDb = newDb()
        val leaderHttp = server.newHttpClient()
        val followerHttp = server.newHttpClient()
        try {
            createUsersAndPostsTables(leaderDb)
            createUsersAndPostsTables(followerDb)

            val leader = newClient(leaderDb, leaderHttp)
            val follower = newClient(followerDb, followerHttp)

            leader.openAndConnect(userId = "user-1").getOrThrow()
            follower.openAndConnect(userId = "user-1").getOrThrow()
            leader.rebuild().getOrThrow()
            follower.rebuild().getOrThrow()

            insertUser(leaderDb, "u1", "Ada")
            leader.pushPending().getOrThrow()

            follower.pullToStable().getOrThrow()

            assertEquals("Ada", scalarText(followerDb, "SELECT name FROM users WHERE id = 'u1'"))
        } finally {
            leaderHttp.close()
            followerHttp.close()
            leaderDb.close()
            followerDb.close()
        }
    }

    @Test
    fun pushPending_doesNotSkipEarlierUnseenPeerBundles() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val dbA = newDb()
        val dbB = newDb()
        try {
            createUsersAndPostsTables(dbA)
            createUsersAndPostsTables(dbB)

            val clientA = newClient(dbA, server.newHttpClient())
            val clientB = newClient(dbB, server.newHttpClient())

            clientA.openAndConnect(userId = "user-1").getOrThrow()
            clientB.openAndConnect(userId = "user-1").getOrThrow()
            clientA.rebuild().getOrThrow()
            clientB.rebuild().getOrThrow()

            insertUser(dbA, "u1", "From A")
            clientA.pushPending().getOrThrow()

            insertUser(dbB, "u2", "From B")
            clientB.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()

            assertEquals(2L, clientB.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals("From A", scalarText(dbB, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("From B", scalarText(dbB, "SELECT name FROM users WHERE id = 'u2'"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            dbA.close()
            dbB.close()
        }
    }

    @Test
    fun threeDevices_pushWithoutPull_thenAllPull_converge() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val dbA = newDb()
        val dbB = newDb()
        val dbC = newDb()
        val httpA = server.newHttpClient()
        val httpB = server.newHttpClient()
        val httpC = server.newHttpClient()
        try {
            createUsersAndPostsTables(dbA)
            createUsersAndPostsTables(dbB)
            createUsersAndPostsTables(dbC)

            val clientA = newClient(dbA, httpA)
            val clientB = newClient(dbB, httpB)
            val clientC = newClient(dbC, httpC)

            clientA.openAndConnect(userId = "user-1").getOrThrow()
            clientB.openAndConnect(userId = "user-1").getOrThrow()
            clientC.openAndConnect(userId = "user-1").getOrThrow()
            clientA.rebuild().getOrThrow()
            clientB.rebuild().getOrThrow()
            clientC.rebuild().getOrThrow()

            insertUser(dbA, "u1", "From A")
            clientA.pushPending().getOrThrow()

            insertUser(dbB, "u2", "From B")
            clientB.pushPending().getOrThrow()

            insertUser(dbC, "u3", "From C")
            clientC.pushPending().getOrThrow()

            clientA.pullToStable().getOrThrow()
            clientB.pullToStable().getOrThrow()
            clientC.pullToStable().getOrThrow()

            assertEquals(3L, clientA.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(3L, clientB.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(3L, clientC.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(3L, scalarLong(dbA, "SELECT COUNT(*) FROM users"))
            assertEquals(3L, scalarLong(dbB, "SELECT COUNT(*) FROM users"))
            assertEquals(3L, scalarLong(dbC, "SELECT COUNT(*) FROM users"))
            assertEquals("From A", scalarText(dbB, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("From B", scalarText(dbC, "SELECT name FROM users WHERE id = 'u2'"))
            assertEquals("From C", scalarText(dbA, "SELECT name FROM users WHERE id = 'u3'"))
            assertEquals(0L, scalarLong(dbA, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbC, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbA, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(dbC, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            httpA.close()
            httpB.close()
            httpC.close()
            dbA.close()
            dbB.close()
            dbC.close()
        }
    }

    @Test
    fun restartAfterOwnPushBeforePull_stillFetchesEarlierPeerBundles() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val dbA = newDb()
        val dbB = newDb()
        val httpA = server.newHttpClient()
        val httpB = server.newHttpClient()
        val restartedHttpB = server.newHttpClient()
        val sourceIdA = "restart-a-source"
        val sourceIdB = "restart-b-source"
        try {
            createUsersAndPostsTables(dbA)
            createUsersAndPostsTables(dbB)

            val clientA = newClient(dbA, httpA)
            val clientB = newClient(dbB, httpB)

            clientA.openAndConnect(userId = "user-1").getOrThrow()
            clientB.openAndConnect(userId = "user-1").getOrThrow()
            clientA.rebuild().getOrThrow()
            clientB.rebuild().getOrThrow()

            insertUser(dbA, "u1", "From A")
            clientA.pushPending().getOrThrow()

            insertUser(dbB, "u2", "From B")
            clientB.pushPending().getOrThrow()

            clientB.closeFromCoordinatedOwner()
            val restartedClientB = newClient(dbB, restartedHttpB)
            restartedClientB.openAndConnect(userId = "user-1").getOrThrow()
            restartedClientB.pullToStable().getOrThrow()

            assertEquals(2L, restartedClientB.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals("From A", scalarText(dbB, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("From B", scalarText(dbB, "SELECT name FROM users WHERE id = 'u2'"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            httpA.close()
            httpB.close()
            restartedHttpB.close()
            dbA.close()
            dbB.close()
        }
    }

    @Test
    fun conflictingPush_withUnseenPeerBundles_clientWinsStillConverges() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val dbA = newDb()
        val dbB = newDb()
        val dbC = newDb()
        val httpA = server.newHttpClient()
        val httpB = server.newHttpClient()
        val httpC = server.newHttpClient()
        try {
            createUsersAndPostsTables(dbA)
            createUsersAndPostsTables(dbB)
            createUsersAndPostsTables(dbC)

            val clientA = newClient(dbA, httpA)
            val clientB = newClient(dbB, httpB, resolver = ClientWinsResolver)
            val clientC = newClient(dbC, httpC)

            clientA.openAndConnect(userId = "user-1").getOrThrow()
            clientB.openAndConnect(userId = "user-1").getOrThrow()
            clientC.openAndConnect(userId = "user-1").getOrThrow()
            clientA.rebuild().getOrThrow()
            clientB.rebuild().getOrThrow()
            clientC.rebuild().getOrThrow()

            insertUser(dbA, "u1", "Original")
            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()
            clientC.pullToStable().getOrThrow()

            updateUserName(dbA, "u1", "Server Name")
            clientA.pushPending().getOrThrow()

            insertUser(dbC, "u2", "Peer Row")
            clientC.pushPending().getOrThrow()

            updateUserName(dbB, "u1", "Client Wins")
            clientB.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()
            clientA.pullToStable().getOrThrow()
            clientC.pullToStable().getOrThrow()

            assertEquals(4L, clientA.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(4L, clientB.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(4L, clientC.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals("Client Wins", scalarText(dbA, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("Client Wins", scalarText(dbB, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("Client Wins", scalarText(dbC, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("Peer Row", scalarText(dbA, "SELECT name FROM users WHERE id = 'u2'"))
            assertEquals("Peer Row", scalarText(dbB, "SELECT name FROM users WHERE id = 'u2'"))
            assertEquals("Peer Row", scalarText(dbC, "SELECT name FROM users WHERE id = 'u2'"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            httpA.close()
            httpB.close()
            httpC.close()
            dbA.close()
            dbB.close()
            dbC.close()
        }
    }

    @Test
    fun conflictingPush_keepLocalResolverAutoRetriesAndWins() = runOwnedWebDatabaseTest {
        withThreeClientSync(clientBResolver = Resolver { MergeResult.KeepLocal }) {
            prepareUserNameConflict(localName = "Local Name")

            pushClientBAndObserveUser(expectedObserverName = null)
            assertEquals("Local Name", scalarText(dbB, "SELECT name FROM users WHERE id = 'u1'"))
            clientB.pullToStable().getOrThrow()

            observer.pullToStable().getOrThrow()
            assertEquals("Local Name", scalarText(observerDb, "SELECT name FROM users WHERE id = 'u1'"))
        }
    }

    @Test
    fun conflictingPush_updatedAtResolverKeepsNewerLocalIntent() = runOwnedWebDatabaseTest {
        assertUpdatedAtConflictResolution(
            serverName = "Server Older",
            serverUpdatedAt = "2026-03-24T00:00:10Z",
            localName = "Local Newer",
            localUpdatedAt = "2026-03-24T00:00:20Z",
            expectedName = "Local Newer",
            expectedUpdatedAt = "2026-03-24T00:00:20Z",
        )
    }

    @Test
    fun conflictingPush_updatedAtResolverAcceptsNewerServerState() = runOwnedWebDatabaseTest {
        assertUpdatedAtConflictResolution(
            serverName = "Server Newer",
            serverUpdatedAt = "2026-03-24T00:00:20Z",
            localName = "Local Older",
            localUpdatedAt = "2026-03-24T00:00:10Z",
            expectedName = "Server Newer",
            expectedUpdatedAt = "2026-03-24T00:00:20Z",
        )
    }

    @Test
    fun conflictingPush_clientWinsResolverAutoRetriesAndWins() = runOwnedWebDatabaseTest {
        withThreeClientSync(clientBResolver = ClientWinsResolver) {
            prepareUserNameConflict(localName = "Client Wins")
            pushClientBAndObserveUser(expectedClientName = "Client Wins")
        }
    }

    @Test
    fun conflictingPush_keepMergedResolverCommitsMergedPayload() = runOwnedWebDatabaseTest {
        withThreeClientSync(
            clientBResolver = Resolver {
                MergeResult.KeepMerged(
                    buildJsonObject {
                        put("id", JsonPrimitive("u1"))
                        put("name", JsonPrimitive("Merged Name"))
                    },
                )
            },
        ) {
            prepareUserNameConflict(localName = "Local Name")
            pushClientBAndObserveUser(expectedClientName = "Merged Name", assertCleanOutbox = false)
        }
    }

    @Test
    fun conflictingPush_preservesSiblingRowsFromRejectedBundle() = runOwnedWebDatabaseTest {
        withThreeClientSync(clientBResolver = ClientWinsResolver) {
            prepareUserNameConflict(localName = "Client Name") {
                insertPost(dbB, "p1", "u1", "Sibling Post")
            }

            pushClientBAndObserveUser(expectedObserverName = "Client Name")
            assertEquals("Sibling Post", scalarText(observerDb, "SELECT title FROM posts WHERE id = 'p1'"))
        }
    }

    @Test
    fun conflictingPush_retryExhaustionLeavesReplayableDirtyState() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val dbA = newDb()
        val dbB = newDb()
        val httpA = server.newHttpClient()
        val httpB = server.newHttpClient()
        try {
            createUsersAndPostsTables(dbA)
            createUsersAndPostsTables(dbB)

            var forcedConflicts = 0
            server.conflictOverride = { row, _, _ ->
                if (row.table == "users" && row.key["id"] == "u1" && forcedConflicts in 1..3) {
                    forcedConflicts++
                    PushConflictDetails(
                        schema = row.schema,
                        table = row.table,
                        key = row.key,
                        op = row.op,
                        baseRowVersion = row.baseRowVersion,
                        serverRowVersion = 7L + forcedConflicts,
                        serverRowDeleted = false,
                        serverRow = buildJsonObject {
                            put("id", JsonPrimitive("u1"))
                            put("name", JsonPrimitive("Server $forcedConflicts"))
                        },
                    )
                } else {
                    null
                }
            }

            val clientA = newClient(dbA, httpA)
            val clientB = newClient(dbB, httpB, resolver = ClientWinsResolver)

            clientA.openAndConnect(userId = "user-1").getOrThrow()
            clientB.openAndConnect(userId = "user-1").getOrThrow()
            clientA.rebuild().getOrThrow()
            clientB.rebuild().getOrThrow()

            insertUser(dbA, "u1", "Original")
            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()

            updateUserName(dbB, "u1", "Replay Me")
            forcedConflicts = 1

            val exhausted = clientB.pushPending().exceptionOrNull()
            assertNotNull(exhausted)
            assertTrue(exhausted is PushConflictRetryExhaustedException)
            assertEquals(2, exhausted.retryCount)
            assertEquals(1, exhausted.remainingDirtyCount)
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(1L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals("Replay Me", scalarText(dbB, "SELECT name FROM users WHERE id = 'u1'"))

            server.conflictOverride = null
            clientB.pushPending().getOrThrow()
            clientA.pullToStable().getOrThrow()

            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("Replay Me", scalarText(dbA, "SELECT name FROM users WHERE id = 'u1'"))
        } finally {
            httpA.close()
            httpB.close()
            dbA.close()
            dbB.close()
        }
    }

    @Test
    fun historyPruned_pullRebuildsThroughSnapshotWithoutSourceRotation() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val leaderDb = newDb()
        val followerDb = newDb()
        val leaderHttp = server.newHttpClient()
        val followerHttp = server.newHttpClient()
        try {
            createUsersAndPostsTables(leaderDb)
            createUsersAndPostsTables(followerDb)

            val leader = newClient(leaderDb, leaderHttp, maxPushRows = 2, maxPullBundles = 2)
            val follower = newClient(followerDb, followerHttp, maxPushRows = 2, maxPullBundles = 2)

            leader.openAndConnect(userId = "user-1").getOrThrow()
            follower.openAndConnect(userId = "user-1").getOrThrow()
            leader.rebuild().getOrThrow()
            follower.rebuild().getOrThrow()

            insertUser(leaderDb, "u1", "Seed")
            leader.pushPending().getOrThrow()
            follower.pullToStable().getOrThrow()

            val sourceBefore = scalarText(
                followerDb,
                "SELECT current_source_id FROM _sync_attachment_state",
            )

            updateUserName(leaderDb, "u1", "Latest")
            insertPost(leaderDb, "p1", "u1", "Latest Post")
            leader.pushPending().getOrThrow()

            server.retainedBundleFloor = 2
            follower.pullToStable().getOrThrow()

            assertEquals(2L, follower.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(sourceBefore, scalarText(followerDb, "SELECT current_source_id FROM _sync_attachment_state"))
            assertEquals("Latest", scalarText(followerDb, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("Latest Post", scalarText(followerDb, "SELECT title FROM posts WHERE id = 'p1'"))
            assertEquals(0L, scalarLong(followerDb, "SELECT rebuild_required FROM _sync_attachment_state"))
            assertEquals(0L, scalarLong(followerDb, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            assertEquals(0L, scalarLong(followerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            leaderHttp.close()
            followerHttp.close()
            leaderDb.close()
            followerDb.close()
        }
    }

    @Test
    fun recover_rotatesSourceAndAllowsFollowupPush() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val seedDb = newDb()
        val recoverDb = newDb()
        val verifyDb = newDb()
        val seedHttp = server.newHttpClient()
        val recoverHttp = server.newHttpClient()
        val verifyHttp = server.newHttpClient()
        try {
            createUsersAndPostsTables(seedDb)
            createUsersAndPostsTables(recoverDb)
            createUsersAndPostsTables(verifyDb)

            val seedClient = newClient(seedDb, seedHttp)
            val recoverClient = newClient(recoverDb, recoverHttp)
            val verifyClient = newClient(verifyDb, verifyHttp)

            seedClient.openAndConnect(userId = "user-1").getOrThrow()
            recoverClient.openAndConnect(userId = "user-1").getOrThrow()
            verifyClient.openAndConnect(userId = "user-1").getOrThrow()

            insertUser(seedDb, "u1", "Recover Seed")
            insertPost(seedDb, "p1", "u1", "Recover Seed Post")
            seedClient.pushPending().getOrThrow()

            val sourceBefore = scalarText(
                recoverDb,
                "SELECT current_source_id FROM _sync_attachment_state",
            )
            markSourceRecoveryRequired(recoverDb)
            recoverClient.rebuild().getOrThrow()

            val sourceAfter = scalarText(
                recoverDb,
                "SELECT current_source_id FROM _sync_attachment_state",
            )
            assertNotEquals(sourceBefore, sourceAfter)
            assertEquals(
                1L,
                scalarLong(
                    recoverDb,
                    "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$sourceAfter'",
                ),
            )
            assertEquals("Recover Seed", scalarText(recoverDb, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("Recover Seed Post", scalarText(recoverDb, "SELECT title FROM posts WHERE id = 'p1'"))

            insertUser(recoverDb, "u2", "Recover Writer")
            insertPost(recoverDb, "p2", "u2", "Recover Followup")
            recoverClient.pushPending().getOrThrow()
            verifyClient.pullToStable().getOrThrow()

            assertEquals(2L, verifyClient.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(2L, scalarLong(verifyDb, "SELECT COUNT(*) FROM users"))
            assertEquals(2L, scalarLong(verifyDb, "SELECT COUNT(*) FROM posts"))
            assertEquals("Recover Followup", scalarText(verifyDb, "SELECT title FROM posts WHERE id = 'p2'"))
            assertEquals(0L, scalarLong(recoverDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(verifyDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            seedHttp.close()
            recoverHttp.close()
            verifyHttp.close()
            seedDb.close()
            recoverDb.close()
            verifyDb.close()
        }
    }

    @Test
    fun sourceRetiredOnPush_persistsReplacementAcrossRestartAndRotatedRebuildReusesIt() =
        runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val verifyDb = newDb()
        var http = server.newHttpClient()
        val verifyHttp = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            createUsersAndPostsTables(verifyDb)

            val client = newClient(db, http)
            val verifyClient = newClient(verifyDb, verifyHttp)

            client.openAndConnect(userId = "user-1").getOrThrow()
            verifyClient.openAndConnect(userId = "user-1").getOrThrow()
            client.rebuild().getOrThrow()
            verifyClient.rebuild().getOrThrow()

            val sourceBefore = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            insertUser(db, "u1", "Rotated Writer")
            server.sourceRetiredOnPush = SourceRetiredResponse(
                error = "source_retired",
                message = "source retired on create",
                sourceId = sourceBefore,
                replacedBySourceId = "server-rotated-device",
            )

            val pushError = assertFailsWith<SourceRecoveryRequiredException> {
                client.pushPending().getOrThrow()
            }
            assertEquals(SourceRecoveryReason.SOURCE_RETIRED, pushError.reason)
            assertEquals("source_retired", scalarText(db, "SELECT reason FROM _sync_operation_state"))
            assertEquals("server-rotated-device", scalarText(db, "SELECT replacement_source_id FROM _sync_operation_state"))
            assertEquals(1L, scalarLong(db, "SELECT rebuild_required FROM _sync_attachment_state"))

            client.closeFromCoordinatedOwner()
            http.close()
            server.sourceRetiredOnPush = null
            http = server.newHttpClient()
            val restartedClient = newClient(db, http)
            restartedClient.open().getOrThrow()
            restartedClient.attach("user-1").getOrThrow()
            restartedClient.rebuild().getOrThrow()

            assertEquals(
                listOf("server-rotated-device"),
                server.requestedSnapshotSourceReplacements.map { it.newSourceId },
            )
            assertEquals(
                listOf(sourceBefore),
                server.requestedSnapshotSourceReplacements.map { it.previousSourceId },
            )
            assertEquals("server-rotated-device", scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
            assertEquals("", scalarText(db, "SELECT replacement_source_id FROM _sync_operation_state"))

            restartedClient.pushPending().getOrThrow()
            verifyClient.pullToStable().getOrThrow()

            assertEquals("Rotated Writer", scalarText(verifyDb, "SELECT name FROM users WHERE id = 'u1'"))
        } finally {
            http.close()
            verifyHttp.close()
            db.close()
            verifyDb.close()
        }
    }

    @Test
    fun sourceRetiredOnPush_recordsDurableSourceRecovery() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)

            val client = newClient(db, http)
            client.openAndConnect(userId = "user-1").getOrThrow()
            client.rebuild().getOrThrow()

            val sourceBefore = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            insertUser(db, "u1", "Commit Rotation")
            server.sourceRetiredOnPush = SourceRetiredResponse(
                error = "source_retired",
                message = "source retired on commit",
                sourceId = sourceBefore,
                replacedBySourceId = "commit-rotated-device",
            )

            val pushError = assertFailsWith<SourceRecoveryRequiredException> {
                client.pushPending().getOrThrow()
            }
            assertEquals(SourceRecoveryReason.SOURCE_RETIRED, pushError.reason)
            assertEquals("source_retired", scalarText(db, "SELECT reason FROM _sync_operation_state"))
            assertEquals("commit-rotated-device", scalarText(db, "SELECT replacement_source_id FROM _sync_operation_state"))
            assertEquals(sourceBefore, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
        } finally {
            http.close()
            db.close()
        }
    }

    @Test
    fun sourceRetiredOnSnapshotCreateFromPull_adoptsReplacementAndKeepsCurrentSourceUnchanged() =
        runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val leaderDb = newDb()
        val followerDb = newDb()
        val leaderHttp = server.newHttpClient()
        val followerHttp = server.newHttpClient()
        try {
            createUsersAndPostsTables(leaderDb)
            createUsersAndPostsTables(followerDb)

            val leader = newClient(leaderDb, leaderHttp)
            val follower = newClient(followerDb, followerHttp)

            leader.openAndConnect(userId = "user-1").getOrThrow()
            follower.openAndConnect(userId = "user-1").getOrThrow()
            leader.rebuild().getOrThrow()
            follower.rebuild().getOrThrow()

            insertUser(leaderDb, "u1", "Seed")
            leader.pushPending().getOrThrow()
            follower.pullToStable().getOrThrow()

            val followerSourceBefore = scalarText(followerDb, "SELECT current_source_id FROM _sync_attachment_state")
            insertUser(leaderDb, "u2", "Latest")
            leader.pushPending().getOrThrow()

            server.retainedBundleFloor = 2
            server.sourceRetiredOnSnapshotCreate = SourceRetiredResponse(
                error = "source_retired",
                message = "source retired during snapshot create",
                sourceId = followerSourceBefore,
                replacedBySourceId = "adopted-rotated-device",
            )

            val rebuildError = assertFailsWith<SourceRecoveryRequiredException> {
                follower.pullToStable().getOrThrow()
            }
            assertEquals(SourceRecoveryReason.SOURCE_RETIRED, rebuildError.reason)
            assertEquals("source_retired", scalarText(followerDb, "SELECT reason FROM _sync_operation_state"))
            assertEquals("adopted-rotated-device", scalarText(followerDb, "SELECT replacement_source_id FROM _sync_operation_state"))
            assertEquals(followerSourceBefore, scalarText(followerDb, "SELECT current_source_id FROM _sync_attachment_state"))
        } finally {
            leaderHttp.close()
            followerHttp.close()
            leaderDb.close()
            followerDb.close()
        }
    }

    @Test
    fun rotatedSnapshotSourceReplacementInvalid_failsClosedWithoutSwitchingCurrentSource() =
        runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)

            val client = newClient(db, http)
            client.openAndConnect(userId = "user-1").getOrThrow()
            client.rebuild().getOrThrow()

            val sourceBefore = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            markSourceRecoveryRequired(db, replacementSourceId = "local-rotated-device")
            server.sourceReplacementInvalidOnSnapshotCreate = ErrorResponse(
                error = "source_replacement_invalid",
                message = "replacement source conflicts with server lineage",
            )

            val error = assertFailsWith<SourceReplacementInvalidException> {
                client.rebuild().getOrThrow()
            }
            assertContains(error.message.orEmpty(), "server rejected the source replacement request")
            assertEquals(
                listOf("local-rotated-device"),
                server.requestedSnapshotSourceReplacements.map { it.newSourceId },
            )
            assertEquals(sourceBefore, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
            assertEquals("local-rotated-device", scalarText(db, "SELECT replacement_source_id FROM _sync_operation_state"))
        } finally {
            http.close()
            db.close()
        }
    }

    @Test
    fun rotatedSnapshotSourceRetiredWithDifferentReplacement_failsClosedOnDivergence() =
        runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)

            val client = newClient(db, http)
            client.openAndConnect(userId = "user-1").getOrThrow()
            client.rebuild().getOrThrow()

            val sourceBefore = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            markSourceRecoveryRequired(db, replacementSourceId = "local-rotated-device")
            server.sourceRetiredOnSnapshotCreate = SourceRetiredResponse(
                error = "source_retired",
                message = "source retired to another replacement",
                sourceId = sourceBefore,
                replacedBySourceId = "server-rotated-device",
            )

            val error = assertFailsWith<SourceReplacementDivergedException> {
                client.rebuild().getOrThrow()
            }
            assertContains(error.message.orEmpty(), "replacement source diverged")
            assertTrue("local-rotated-device" !in error.toString())
            assertTrue("server-rotated-device" !in error.toString())
            assertEquals(sourceBefore, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
            assertEquals("local-rotated-device", scalarText(db, "SELECT replacement_source_id FROM _sync_operation_state"))
        } finally {
            http.close()
            db.close()
        }
    }
}
