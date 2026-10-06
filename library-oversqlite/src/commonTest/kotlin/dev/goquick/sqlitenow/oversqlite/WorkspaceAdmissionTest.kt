package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.CancellationException
import kotlin.test.*

internal class WorkspaceAdmissionTest : SurvivorTestSupport() {
    @Test fun retainedValidationCompletesAboveSoftWorkTarget() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, http, seed ->
            for (ordinal in 1..32) {
                db.execSQL("INSERT INTO _sync_snapshot_stage(snapshot_id,row_ordinal,schema_name,table_name,key_json,row_version,payload) " +
                    "VALUES('retained',${ordinal},'main','nodes','{\"id\":\"retained-${ordinal}\"}',1,'{}')")
            }
            val config = OversqliteConfig(1, nodeTables, "fixture", "main", maxSyncWorkUnitsPerInvocation = 1)
            val workspace = OversqliteWorkspace(db, config)
            workspace.reconcileRetainedOwners()
            seed.closeFromCoordinatedOwner()
            val reopened = newClient(db, http, syncTables = nodeTables, workUnits = 1)
            reopened.open().getOrThrow()
            assertEquals(32L, scalarLong(db, "SELECT count(*) FROM _sync_snapshot_stage"))
            reopened.closeFromCoordinatedOwner()
            val used = workspace.usage(applyUnit("reopen", 0, "retained"))
            assertEquals(32L, scalarLong(db, "SELECT snapshot_stage_entries FROM _sync_operation_state"))
            assertFailsWith<SyncWorkspaceLimitException> {
                OversqliteWorkspace(db, config.copy(maxSyncWorkspaceEntries = used.entries - 1)).check("reopen")
            }
            db.execSQL("UPDATE _sync_snapshot_stage SET charge=charge+1 WHERE row_ordinal=17")
            assertFails { workspace.reconcileRetainedOwners() }
        }
    }

    @Test fun boundaries() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _,db,http,_ ->
            db.execSQL("INSERT INTO _sync_snapshot_stage(snapshot_id,row_ordinal,schema_name,table_name,key_json,row_version,payload) VALUES('snapshot',1,'main','nodes','{\"id\":\"X\"}',1,'{}')")
            val config=OversqliteConfig(generationId=1,databaseId="fixture",serviceId="main",syncTables=nodeTables)
            val workspace=OversqliteWorkspace(db,config)
            workspace.reconcileRetainedOwners()
            val unit=applyUnit("a3-boundary",0,"")
            val used=workspace.usage(unit)
            for(delta in listOf(-1L,0L,1L)) {
                val bounded=OversqliteWorkspace(db,config.copy(maxSyncWorkspaceEntries=used.entries+delta))
                if(delta<0) assertFailsWith<SyncWorkspaceLimitException> {bounded.check(unit)} else bounded.check(unit)
                val bytes=OversqliteWorkspace(db,config.copy(maxSyncWorkspaceBytes=used.bytes+delta))
                if(delta<0) assertFailsWith<SyncWorkspaceLimitException> {bytes.check(unit)} else bytes.check(unit)
            }
            assertFailsWith<SyncWorkspaceLimitException> {workspace.add(unit,WorkspaceCharge(Long.MAX_VALUE),WorkspaceCharge(1))}
            for (work in listOf(16L,17L,18L)) {
                val owner=OversqliteClientOperationController(config.copy(maxSyncWorkUnitsPerInvocation=17),
                    OversqliteClientResourceOwner(db,http),SnapshotDiagnosticsState()) {}
                owner.reserveSyncWork(unit, work)
                assertEquals(work, owner.usedSyncWork())
                if (work == 16L) owner.reserveSyncWork(unit, 1)
                val pending = assertFailsWith<SyncWorkLimitException> { owner.reserveSyncWork(unit, 1) }
                assertEquals(17L, pending.limit)
            }
            db.execSQL("UPDATE _sync_snapshot_stage SET charge=charge+1")
            assertFails {workspace.reconcileRetainedOwners()}
        }
    }

    @Test fun cleanupAndHandles() = runOwnedWebDatabaseTest {
        val group=Any()
        withSurvivorFixture(leaseGroup=group) { server,db,http,client ->
            db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
            server.beforePushCommit={
                val held=scalarLong(db,"SELECT reserved_entries FROM _sync_operation_state")
                assertTrue(held>0)
                val other=newClient(db,http,syncTables=nodeTables,leaseGroup=group)
                other.openAndConnect("user-1").getOrThrow()
                assertEquals(held,scalarLong(db,"SELECT reserved_entries FROM _sync_operation_state"))
                other.closeFromCoordinatedOwner()
                throw CancellationException("cancel after second handle inspection")
            }
            assertFailsWith<CancellationException> {client.pushPending().getOrThrow()}
            assertEquals(0L,scalarLong(db,"SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
            client.pushPending().getOrThrow()
            assertEquals(0L,scalarLong(db,"SELECT reserved_entries FROM _sync_operation_state"))
        }
    }

    @Test fun atomicResume() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server,db,http,seed ->
            db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
            server.beforePushCommit={throw CancellationException("frozen original")}
            assertFailsWith<CancellationException> {seed.pushPending().getOrThrow()}
            seed.closeFromCoordinatedOwner()
            val limited=newClient(db,http,syncTables=nodeTables,workUnits=1000)
            limited.openAndConnect("user-1").getOrThrow()
            server.normalizePushEffects={effects()}
            val failure=limited.pushPending().exceptionOrNull()
            assertTrue(failure is SyncWorkLimitException || failure is SyncWorkspaceLimitException,"$failure")
            requireSurvivors(db)
            assertEquals(3L,scalarLong(db,"SELECT next_source_bundle_id FROM _sync_source_state"))
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(2L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals(0L,scalarLong(db,"SELECT reserved_entries FROM _sync_operation_state"))
            limited.closeFromCoordinatedOwner()
            val resumed=newClient(db,http,syncTables=nodeTables)
            resumed.openAndConnect("user-1").getOrThrow();resumed.pushPending().getOrThrow()
            requireSurvivors(db)
            assertEquals(3L,scalarLong(db,"SELECT next_source_bundle_id FROM _sync_source_state"))
            resumed.closeFromCoordinatedOwner()
        }
    }
}

// Keep recovery identity while excluding only admitted derived accounting progress.
internal suspend fun protectedPrimaryVector(db: dev.goquick.sqlitenow.core.SafeSQLiteConnection): BaselineQualificationVector {
    val vector=baselineQualificationVector(db,listOf("nodes"))
    val derived=setOf("workspace_context","reserved_entries","reserved_bytes","snapshot_stage_entries","snapshot_stage_bytes",
        "snapshot_cleanup_ordinal","push_stage_entries","push_stage_bytes","push_cleanup_ordinal")
    return BaselineQualificationVector(vector.tables.mapValues { (table,rows) ->
        if (table=="_sync_operation_state") rows.map {kotlinx.serialization.json.JsonObject(it.filterKeys {key -> key !in derived})} else rows
    })
}
