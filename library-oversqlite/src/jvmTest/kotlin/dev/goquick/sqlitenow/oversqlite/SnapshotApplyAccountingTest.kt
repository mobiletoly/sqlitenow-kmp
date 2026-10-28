package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlin.test.*

internal class SnapshotApplyAccountingTest : SurvivorTestSupport() {
    @Test fun externalReadsAreBoundedAndOwnedCountersAndTransientChargesStayCurrent() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _,db,_,client -> try {
            val cfg=OversqliteConfig(1,nodeTables,"fixture","main");val ws=OversqliteWorkspace(db,cfg)
            val observation=ApplyStatementObservation.install(db)
            fun reads()=observation.steps.filterKeys { it.startsWith("SELECT o.reserved_entries") }.values.sum()
            try { for(rows in listOf(16,64))withContext(OversqliteWorkSlice()) { db.transaction { db.withStatementCache { cache ->
                assertTrue(SnapshotApplyAccounting.eligible(ws,cache))
                val baseline=ws.usage("accounting",apply=WorkspaceCharge(),statementCache=cache)
                var own=WorkspaceCharge(1,512);val identity=Any()
                val before=reads()
                val cap=assertNotNull(SnapshotApplyAccounting.create(ws,"accounting",cache,identity){own})
                try {
                    repeat(rows) {
                        own=ws.add("accounting",own,WorkspaceCharge(1,64))
                        assertEquals(ws.add("accounting",ws.add("accounting",baseline,own),snapshotApplyAccountingCharge),cap.usage(ws,cache,identity))
                    }
                    assertEquals(1L,reads()-before,"one scalar base per uninterrupted segment")
                    SnapshotRowLease.use(db,ApplySurvivor("nodes","P",null),0,0,"accounting",cache,ws,Json,null) { lease ->
                        assertTrue(lease.tryRetain(WorkspaceCharge(2,128)))
                        assertEquals(ws.add("accounting",ws.add("accounting",ws.add("accounting",baseline,own),snapshotApplyAccountingCharge),WorkspaceCharge(2,128)),cap.usage(ws,cache,identity))
                    }
                    assertFails { cap.usage(ws,cache,Any()) }
                    assertFails { cap.usage(OversqliteWorkspace(db,cfg.copy()),cache,identity) }
                    db.withStatementCache { wrong -> assertFails { cap.usage(ws,wrong,identity) } }
                    withContext(OversqliteWorkSlice()) { assertFails { cap.usage(ws,cache,identity) } }
                } finally { cap.close() }
                assertFails { cap.usage(ws,cache,identity) }
                val fresh=reads();repeat(3) { ws.check("fresh",statementCache=cache) };assertEquals(3L,reads()-fresh)
            } } } } finally { observation.restore();observation.assertClosed() }
        } finally { client.closeFromCoordinatedOwner() } }
    }

    @Test fun exactHolderBoundsAndLatePressureFallBackBeforeRefusingRealWork() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _,db,_,client -> try {
            val config=OversqliteConfig(1,nodeTables,"fixture","main")
            val base=OversqliteWorkspace(db,config).usage("bounds",apply=WorkspaceCharge())
            for(dimension in listOf("entries","bytes"))for(fits in listOf(false,true)) {
                val cfg=if(dimension=="entries")config.copy(maxSyncWorkspaceEntries=base.entries+8-if(fits)0 else 1)
                    else config.copy(maxSyncWorkspaceBytes=base.bytes+1024-if(fits)0 else 1)
                val ws=OversqliteWorkspace(db,cfg)
                withContext(OversqliteWorkSlice()) { db.transaction { db.withStatementCache { cache ->
                    val identity=Any();val cap=SnapshotApplyAccounting.create(ws,"bounds",cache,identity){WorkspaceCharge()}
                    if(!fits)assertNull(cap) else {
                        val held=assertNotNull(cap)
                        try {
                            ws.readAndCheck("bounds",snapshotApplyAccountingCharge,statementCache=cache,accounting=held)
                            assertEquals(WorkspaceCharge(),held.retainedCharge,"release the optional holder before retrying the same admission")
                            assertEquals(base,held.usage(ws,cache,identity))
                            assertFailsWith<SyncWorkspaceLimitException> { ws.readAndCheck("bounds",WorkspaceCharge(Long.MAX_VALUE,Long.MAX_VALUE),statementCache=cache,accounting=held) }
                        } finally { held.close() }
                    }
                } } }
            }
        } finally { client.closeFromCoordinatedOwner() } }
    }

    @Test fun mainAndTemporaryTriggersDisableOwnedMutationReuse() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _,db,_,client -> try {
            val ws=OversqliteWorkspace(db,OversqliteConfig(1,nodeTables,"fixture","main"))
            assertFalse(SnapshotApplyAccounting.eligible(ws,null))
            for(temp in listOf(false,true))for(table in listOf("_sync_apply_rows","_sync_apply_edges","_sync_apply_plan")) {
                db.execSQL("CREATE ${if(temp)"TEMP " else ""}TRIGGER fixture_accounting AFTER INSERT ON $table BEGIN UPDATE _sync_operation_state SET reason=reason||'changed'; END")
                try { withContext(OversqliteWorkSlice()) { db.transaction { db.withStatementCache { cache ->
                    assertFalse(SnapshotApplyAccounting.eligible(ws,cache))
                } } } } finally { db.execSQL("DROP TRIGGER ${if(temp)"temp" else "main"}.fixture_accounting") }
            }
        } finally { client.closeFromCoordinatedOwner() } }
    }

    @Test fun cancellationRollbackAndNextWriterRebuildExternalState() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _,db,_,client -> try {
            val ws=OversqliteWorkspace(db,OversqliteConfig(1,nodeTables,"fixture","main"));val before=ws.usage("cancel")
            var expired:SnapshotApplyAccounting?=null
            withContext(OversqliteWorkSlice()) {
                assertFailsWith<CancellationException> { db.transaction { db.withStatementCache { cache ->
                    val cap=assertNotNull(SnapshotApplyAccounting.create(ws,"cancel",cache,Any()){WorkspaceCharge()});expired=cap
                    try { db.execSQL("UPDATE _sync_operation_state SET reason='rolled-back'");throw CancellationException("fixture") }
                    finally { cap.close() }
                } } }
                assertEquals(before,ws.usage("cancel"))
                db.transaction { db.withStatementCache { cache ->
                    assertFails { checkNotNull(expired).usage(ws,cache) }
                    db.execSQL("UPDATE _sync_operation_state SET reason='changed-writer'")
                    val fresh=ws.usage("cancel",apply=WorkspaceCharge(),statementCache=cache)
                    val cap=assertNotNull(SnapshotApplyAccounting.create(ws,"cancel",cache,Any()){WorkspaceCharge()})
                    try { assertEquals(ws.add("cancel",fresh,snapshotApplyAccountingCharge),cap.usage(ws,cache)) }
                    finally { cap.close() }
                } }
            }
        } finally { client.closeFromCoordinatedOwner() } }
    }

    @Test fun callbackCreatedTriggerCannotReuseEarlierEligibility() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _,db,http,client -> try {
            val cfg=OversqliteConfig(1,nodeTables,"fixture","main",maxSyncWorkspaceBytes=2_000_000)
            val field=DefaultOversqliteClient::class.java.getDeclaredField("sessionState").also { it.isAccessible=true }
            val state=(field.get(client) as OversqliteClientSessionState).requireConnectedRuntimeState("trigger callback")
            val owner=OversqliteApplySurvivors(db,OversqliteLocalStore(db,Json){state.validated},cfg,
                OversqliteClientOperationController(cfg,OversqliteClientResourceOwner(db,http),SnapshotDiagnosticsState()) {},Json)
            var calls=0
            withContext(OversqliteWorkSlice()) { assertFailsWith<SyncWorkspaceLimitException> { db.transaction { db.withStatementCache { cache ->
                owner.prepare(state,applyUnit("snapshot",0,"trigger-callback"),3,"snapshot",workspaceStatementCache=cache,replayStatementCache=cache) { n ->
                    calls++
                    if(n==1)db.execSQL("CREATE TEMP TRIGGER callback_trigger AFTER INSERT ON _sync_apply_rows BEGIN UPDATE _sync_operation_state SET reason=hex(zeroblob(250000)); END")
                    ApplySurvivor("nodes","callback-$n",null,role="intended")
                }
            } } } }
            assertEquals(2,calls,"the post-insert check must observe trigger work before another root callback")
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_apply_plan"))
        } finally { client.closeFromCoordinatedOwner() } }
    }

    @Test fun arbitraryRootCallbacksRefreshExternalAccountingAndRollbackIdentically() = runOwnedWebDatabaseTest {
        for(cached in listOf(false,true))withSurvivorFixture { _,db,http,client -> try {
            val cfg=OversqliteConfig(1,nodeTables,"fixture","main",maxSyncWorkspaceBytes=2_000_000)
            val field=DefaultOversqliteClient::class.java.getDeclaredField("sessionState").also { it.isAccessible=true }
            val state=(field.get(client) as OversqliteClientSessionState).requireConnectedRuntimeState("callback proof")
            val operations=OversqliteClientOperationController(cfg,OversqliteClientResourceOwner(db,http),SnapshotDiagnosticsState()) {}
            val owner=OversqliteApplySurvivors(db,OversqliteLocalStore(db,Json){state.validated},cfg,operations,Json)
            val before=baselineQualificationVector(db,listOf("nodes","_sync_apply_plan","_sync_apply_rows"))
            withContext(OversqliteWorkSlice()) {
                val result=assertFailsWith<SyncWorkspaceLimitException> { db.transaction { db.withStatementCache { cache ->
                    owner.prepare(state,applyUnit("snapshot",0,"callback"),2,"snapshot",workspaceStatementCache=if(cached)cache else null,replayStatementCache=if(cached)cache else null) { n ->
                        if(n==1)db.withPreparedStatement("UPDATE _sync_operation_state SET reason=?") { it.bindText(1,"x".repeat(500_000));it.step() }
                        ApplySurvivor("nodes","callback-$n",null,role="intended")
                    }
                } } }
                assertEquals("bytes",result.dimension)
            }
            assertEquals(before,baselineQualificationVector(db,listOf("nodes","_sync_apply_plan","_sync_apply_rows")))
        } finally { client.closeFromCoordinatedOwner() } }
    }
}
