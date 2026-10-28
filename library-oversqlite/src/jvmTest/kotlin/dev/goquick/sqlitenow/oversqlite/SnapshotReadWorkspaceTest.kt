package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.TransactionMode
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlin.test.*

internal class SnapshotReadWorkspaceTest : SurvivorTestSupport() {
    @Test fun freshRowCombinesRequiredAndOptionalAdmissionWithoutAnotherRead() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _,db,_,client -> try {
            val cfg=OversqliteConfig(1,nodeTables,"fixture","main");val ws=OversqliteWorkspace(db,cfg)
            val observation=ApplyStatementObservation.install(db)
            fun reads()=observation.steps.filterKeys { it.startsWith("SELECT o.reserved_entries") }.values.sum()
            try {
                for(rows in listOf(16,64))db.transaction { db.withStatementCache { cache ->
                    val before=reads()
                    repeat(rows) {
                        val required=WorkspaceCharge(8,512)
                        val used=ws.readAndCheck("row",required,statementCache=cache)
                        assertTrue(ws.admitOptionalFromFreshRow("row",used,required,WorkspaceCharge(4,256)))
                        assertFalse(ws.admitOptionalFromFreshRow("row",used,required,WorkspaceCharge(Long.MAX_VALUE,Long.MAX_VALUE)))
                    }
                    assertEquals(rows.toLong(),reads()-before)
                } }
            } finally { observation.restore();observation.assertClosed() }
        } finally { client.closeFromCoordinatedOwner() } }
    }
    @Test fun durableReadsAreConstantInsideRegionAndFreshOutside() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _,db,_,client -> try {
            val ws=OversqliteWorkspace(db,OversqliteConfig(1,nodeTables,"fixture","main"))
            val observed=ApplyStatementObservation.install(db)
            fun reads()=observed.steps.filterKeys { it.startsWith("SELECT o.reserved_entries") }.values.sum()
            try {
                for(rows in listOf(16,64))db.transaction(TransactionMode.IMMEDIATE) { db.withStatementCache { cache ->
                    val start=reads();var expired:SnapshotReadWorkspace?=null
                    SnapshotReadWorkspace.use(ws,"read",cache) { view ->
                        assertNotNull(view);expired=view
                        repeat(rows) { ws.check("read",statementCache=cache,read=view) }
                        val counter=SnapshotReadWorkspace::class.java.getDeclaredField("reuses").also { it.isAccessible=true }
                        assertEquals(0L,counter.getLong(view),"disabled diagnostics perform no view-counter aggregation")
                        db.withStatementCache { wrong -> assertFails { ws.usage("read",statementCache=wrong,read=view) } }
                    }
                    assertEquals(1L,reads()-start)
                    assertFails { ws.usage("read",statementCache=cache,read=expired) }
                    val before=reads();repeat(3) { ws.check("read",statementCache=cache) };assertEquals(3L,reads()-before)
                } }
            } finally { observed.restore();observed.assertClosed() }
        } finally { client.closeFromCoordinatedOwner() } }
    }

    @Test fun exactHolderBoundsFallbackAndCurrentTransientCharges() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _,db,_,client -> try {
            val cfg=OversqliteConfig(1,nodeTables,"fixture","main");val base=OversqliteWorkspace(db,cfg).usage("read")
            for(dimension in listOf("entries","bytes"))for(fits in listOf(false,true)) {
                val limit=if(dimension=="entries")base.entries+snapshotReadWorkspaceCharge.entries else base.bytes+snapshotReadWorkspaceCharge.bytes
                val config=if(dimension=="entries")cfg.copy(maxSyncWorkspaceEntries=limit-if(fits)0 else 1) else cfg.copy(maxSyncWorkspaceBytes=limit-if(fits)0 else 1)
                val ws=OversqliteWorkspace(db,config)
                db.transaction { db.withStatementCache { cache ->
                    SnapshotReadWorkspace.use(ws,"read",cache) { view ->
                        if(fits) {
                            assertNotNull(view)
                            assertEquals(ws.add("read",base,snapshotReadWorkspaceCharge),ws.usage("read",statementCache=cache,read=view))
                            // The optional view must retire before refusing an otherwise admissible row.
                            ws.check("read",snapshotReadWorkspaceCharge,statementCache=cache,read=view)
                            assertNull(view.base(ws,cache))
                            assertEquals(base,ws.usage("read",statementCache=cache,read=view))
                        } else assertNull(view)
                    }
                } }
            }
            val ws=OversqliteWorkspace(db,cfg)
            db.transaction { db.withStatementCache { cache -> withContext(OversqliteWorkSlice()) {
                SnapshotReadWorkspace.use(ws,"read",cache) { view ->
                    val initial=ws.usage("read",statementCache=cache,read=view)
                    SnapshotRowLease.use(db,ApplySurvivor("nodes","P",null),0,0,"read",cache,ws,Json,null,read=view) { lease ->
                        assertTrue(lease.tryRetain(WorkspaceCharge(2,128)))
                        assertEquals(ws.add("read",initial,WorkspaceCharge(2,128)),ws.usage("read",statementCache=cache,read=view))
                    }
                    assertEquals(initial,ws.usage("read",statementCache=cache,read=view))
                }
            } } }
        } finally { client.closeFromCoordinatedOwner() } }
    }

    @Test fun cancellationClosesViewAndNextWriterReadsChangedState() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _,db,_,client -> try {
            val ws=OversqliteWorkspace(db,OversqliteConfig(1,nodeTables,"fixture","main"));var expired:SnapshotReadWorkspace?=null
            val original=ws.usage("read")
            assertFailsWith<CancellationException> { db.transaction { db.withStatementCache { cache ->
                SnapshotReadWorkspace.use(ws,"read",cache) { view -> expired=view;throw CancellationException("read fixture") }
            } } }
            db.transaction { db.withStatementCache { cache ->
                assertFails { ws.usage("read",statementCache=cache,read=expired) }
                db.execSQL("UPDATE _sync_operation_state SET reason='changed-accounting' WHERE singleton_key=1")
                assertTrue(ws.usage("read",statementCache=cache).bytes>original.bytes)
            } }
        } finally { client.closeFromCoordinatedOwner() } }
    }
}
