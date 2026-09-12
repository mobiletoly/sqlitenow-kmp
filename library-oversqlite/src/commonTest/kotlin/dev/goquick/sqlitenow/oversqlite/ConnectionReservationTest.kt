package dev.goquick.sqlitenow.oversqlite

import kotlin.test.*

internal class ConnectionReservationTest : SurvivorTestSupport() {
    @Test fun secondConnection() = runOwnedWebDatabaseTest {
        if (!supportsDurableTestDatabaseRestart()) {
            println("A3 independent file connection NOT-APPLICABLE to transient Node; shared-handle reservation witness is WorkspaceAdmissionTest.cleanupAndHandles")
            return@runOwnedWebDatabaseTest
        }
        for ((scenario,sql) in listOf(
            "edit" to "UPDATE nodes SET name='new independent L' WHERE id='D'",
            "new-dependent" to "INSERT INTO nodes VALUES('E','new child','D')",
            "reparent" to "UPDATE nodes SET parent_id='Q' WHERE id='D'",
            "parent-write" to "UPDATE nodes SET name='new independent parent' WHERE id='Q'",
        )) {
            val path=createTempSqliteNowTestDbPath("a3-concurrent-$scenario")
            try {withSurvivorFixture(dbPath=path) { server,db,_,client ->
                val other=newFileBackedDb(path)
                try {
                    db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
                    server.normalizePushEffects={effects()}
                    var captured: BaselineQualificationVector?=null
                    // Independent DML at the response barrier proves no HTTP writer lock.
                    server.beforePushResponse={
                        other.execSQL(sql)
                        captured=baselineQualificationVector(db,listOf("nodes"))
                    }
                    client.pushPending().getOrThrow()
                    val after=baselineQualificationVector(db,listOf("nodes"))
                    for (table in listOf("nodes","_sync_dirty_rows","_sync_row_state","_sync_relationships")) {
                        val column=when(table) {"nodes"->"id";"_sync_relationships"->"child_key";else->"key_json"}
                        val excluded=setOf("P","C","{\"id\":\"P\"}","{\"id\":\"C\"}")
                        assertEquals(assertNotNull(captured).rows(table).filter {it.text(column) !in excluded},
                            after.rows(table).filter {it.text(column) !in excluded},"$scenario complete independent writer vector: $table")
                    }
                    assertEquals(1L,scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows"))
                    assertEquals(0L,scalarLong(db,"SELECT count(*) FROM pragma_foreign_key_check"))
                    assertEquals(3L,scalarLong(db,"SELECT next_source_bundle_id FROM _sync_source_state"))
                    assertEquals(2L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_outbox_rows"))
                    println("A3 independent writer $scenario full vector preserved")
                } finally {other.close()}
            }} finally {deleteTempSqliteNowTestDbArtifacts(path)}
        }
    }

    @Test fun secondConnectionReadyGuard() = runOwnedWebDatabaseTest {
        if (!supportsDurableTestDatabaseRestart()) {
            println("A3 ready guard independent file connection NOT-APPLICABLE to transient Node")
            return@runOwnedWebDatabaseTest
        }
        for ((scenario,sql) in listOf(
            "edit" to "UPDATE nodes SET name='new independent L' WHERE id='D'",
            "new-dependent" to "INSERT INTO nodes VALUES('E','new child','D')",
            "reparent" to "UPDATE nodes SET parent_id='Q' WHERE id='D'",
            "parent-write" to "UPDATE nodes SET name='new independent parent' WHERE id='Q'",
        )) {
            val path=createTempSqliteNowTestDbPath("a3-ready-guard-$scenario")
            try {withSurvivorFixture(dbPath=path) { _,db,http,_ ->
                val other=newFileBackedDb(path)
                try {
                    val config=OversqliteConfig(1,nodeTables,"fixture","main")
                    val generation=OversqliteContractStore(db).readGeneration(1)
                    val validated=SyncRuntimeInitializer(config,TableInfoCache()).validateConfig(db).withGeneration(generation)
                    val state=RuntimeState(validated,AdmittedOversqliteContract("v1",generation,testWireLimits(),false),
                        "user-1",scalarText(db,"SELECT current_source_id FROM _sync_attachment_state"))
                    val resources=OversqliteClientResourceOwner(db,http)
                    val local=OversqliteLocalStore(db,json) {validated}
                    val unit=applyUnit("snapshot",2,"ready-independent-writer")
                    suspend fun prepare(): String {
                        val operations=OversqliteClientOperationController(config,resources,SnapshotDiagnosticsState()) {}
                        val owner=OversqliteApplySurvivors(db,local,config,operations,json)
                        db.transaction(dev.goquick.sqlitenow.core.TransactionMode.IMMEDIATE) {
                            owner.prepare(state,unit,0,"snapshot",2) {error("empty snapshot has no roots")}
                        }
                        assertEquals("ready",scalarText(db,"SELECT phase FROM _sync_apply_plan"))
                        return scalarText(db,"SELECT guard FROM _sync_apply_plan")
                    }
                    val guard=prepare()
                    other.execSQL(sql)
                    val before=protectedPrimaryVector(db)
                    assertNotEquals(guard,prepare(),"$scenario invalidates captured ready context")
                    assertEquals(before,protectedPrimaryVector(db),
                        "$scenario revalidation preserves complete primary vector")
                    println("A3 ready guard $scenario invalidated under writer; primary vector preserved")
                } finally {other.close()}
            }} finally {deleteTempSqliteNowTestDbArtifacts(path)}
        }
    }
}
