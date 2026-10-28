package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.SqliteNowDiagnostics
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.*
import kotlin.test.*

internal class SnapshotReferenceLoadingTest : SurvivorTestSupport() {
    @Test fun referenceConsumersKeepValidationWithoutDiscardedText() = runOwnedWebDatabaseTest {
        val metadata = mutableListOf<Long>()
        for (rows in listOf(16,64)) withSurvivorFixture { server,db,http,seed ->
            seed.closeFromCoordinatedOwner()
            val capture=PipelineDiagnosticCapture();val previous=sqliteNowLogger;sqliteNowLogger=capture
            val client=DefaultOversqliteClient.forOpening(db,OversqliteConfig(1,nodeTables,"fixture","main",diagnostics=SqliteNowDiagnostics(true)),http,ServerWinsResolver,Any())
            try {
                client.openAndConnect("user-1").getOrThrow()
                server.addRemoteBundleForTest((0 until rows-4).map { n -> BundleRow("main","nodes",mapOf("id" to "R$n"),"INSERT",2,
                    buildJsonObject { put("id","R$n");put("name","Ž🧩-$n");put("parent_id",JsonNull) }) })
                capture.clear();client.rebuild().getOrThrow()
                val events=capture.records()
                val families=events.filter { it["kind"]?.jsonPrimitive?.content=="sql_family" }.associate { it["family"]!!.jsonPrimitive.content to it["name"]!!.jsonPrimitive.content }
                fun calls(name:String)=events.filter { it["kind"]?.jsonPrimitive?.content=="work_metric" && families[it["family"]?.jsonPrimitive?.content]==name }
                    .maxOfOrNull { it["counts"]!!.jsonObject["calls"]!!.jsonPrimitive.long } ?: 0
                assertEquals(0L,calls("row_processing.row_encode"),"no snapshot consumer requires an intermediate wire text")
                assertEquals(rows.toLong(),calls("snapshot.reference_load"),"each root is validated before retaining its reference")
                assertTrue(calls("row_processing.wire_decode")>=9L*rows,"all independent staged passes remain")
                assertTrue(calls("snapshot.plan_metadata_reuse")>=2L*rows)
                metadata+=calls("snapshot.plan_metadata_load")
                assertEquals(rows.toLong(),scalarLong(db,"SELECT count(*) FROM nodes"))
                assertEquals(0L,scalarLong(db,"SELECT count(*) FROM pragma_foreign_key_check"))
            } finally { client.closeFromCoordinatedOwner();sqliteNowLogger=previous }
        }
        assertEquals(metadata[0],metadata[1],"plan loads depend on lexical passes and work slices, not row population")
    }

    @Test fun readerBoundsFallbackIdentityAndLifetime() = runOwnedWebDatabaseTest {
        for(rows in listOf(16,64)) withSurvivorFixture { _,db,http,client -> try {
            val cfg=OversqliteConfig(1,nodeTables,"fixture","main")
            val state=state(client)
            stage(db,rows)
            val observed=ApplyStatementObservation.install(db)
            try {
                fun loads()=observed.steps.filterKeys { it.startsWith("SELECT unit,guard,phase,") }.values.sum()
                val slice=OversqliteWorkSlice()
                withContext(slice) { db.transaction { db.withStatementCache { cache ->
                    val owner=owner(db,http,cfg,state)
                    val before=loads();var expired:OversqliteApplySurvivors.PlannedRows?=null
                    owner.withPlannedRows(state,cache) { reader ->
                        expired=reader
                        repeat(rows) { n -> reader.withRow(n+1L) { lease ->
                            assertTrue(lease.hasPayload);assertNull(lease.row.payload)
                            assertEquals("R$n",lease.referenceRow().pk)
                            assertEquals(7L,lease.version)
                            assertEquals(utf8Size(checkNotNull(lease.payloadObject()).toString()),lease.encodedPayloadSize())
                        } }
                        assertTrue(reader.retainedCharge.bytes>=1024)
                        withContext(OversqliteWorkSlice()) { assertFails { reader.withRow(1) {} } }
                    }
                    assertEquals(1L,loads()-before)
                    assertFails { checkNotNull(expired).withRow(1) {} }
                    assertNull(slice.snapshotPlanReader)
                    val used=OversqliteWorkspace(db,cfg).usage("bounds")
                    val tight=cfg.copy(maxSyncWorkspaceEntries=used.entries+8)
                    val start=loads()
                    owner(db,http,tight,state).withPlannedRows(state,cache) { reader ->
                        repeat(rows) { reader.withRow(it+1L) { row -> assertTrue(row.hasPayload) } }
                        assertEquals(WorkspaceCharge(),reader.retainedCharge,"holder evicted at a row boundary before genuine admission")
                    }
                    assertTrue(loads()-start>=rows)
                    assertFailsWith<CancellationException> { owner.withPlannedRows(state,cache) { reader ->
                        reader.withRow(1) { throw CancellationException("fixture") }
                    } }
                    assertNull(slice.snapshotRowLease);assertNull(slice.snapshotPlanReader)
                    db.execSQL("UPDATE _sync_apply_rows SET local_pk='wrong' WHERE ordinal=1")
                    assertFails { owner.withPlannedRows(state,cache) { it.withRow(1) {} } }
                    db.execSQL("UPDATE _sync_apply_rows SET local_pk='R0' WHERE ordinal=1")
                    db.execSQL("UPDATE _sync_snapshot_stage SET row_version=0 WHERE row_ordinal=1")
                    assertFails { owner.withPlannedRows(state,cache) { it.withRow(1) {} } }
                    db.execSQL("UPDATE _sync_snapshot_stage SET row_version=7 WHERE row_ordinal=1")
                    db.execSQL("UPDATE _sync_snapshot_stage SET payload='{}' WHERE row_ordinal=1")
                    assertFails { owner.withPlannedRows(state,cache) { it.withRow(1) {} } }
                    db.execSQL("DELETE FROM _sync_snapshot_stage")
                    assertFails { owner.withPlannedRows(state,cache) { it.withRow(1) {} } }
                } } }
            } finally { observed.restore();observed.assertClosed() }
        } finally { client.closeFromCoordinatedOwner() } }
    }

    private suspend fun state(client:DefaultOversqliteClient):RuntimeState {
        val field=DefaultOversqliteClient::class.java.getDeclaredField("sessionState").also { it.isAccessible=true }
        return (field.get(client) as OversqliteClientSessionState).requireConnectedRuntimeState("reference proof")
    }
    private fun owner(db:SafeSQLiteConnection,http:io.ktor.client.HttpClient,cfg:OversqliteConfig,state:RuntimeState)=
        OversqliteApplySurvivors(db,OversqliteLocalStore(db,Json){state.validated},cfg,
            OversqliteClientOperationController(cfg,OversqliteClientResourceOwner(db,http),SnapshotDiagnosticsState()) {},Json)
    private suspend fun stage(db:SafeSQLiteConnection,rows:Int) {
        db.execSQL("INSERT INTO _sync_apply_plan(singleton_key,unit,guard,phase,root_cursor,visit_cursor,table_cursor,fk_cursor,entries,bytes,operation_kind,source_snapshot_id) VALUES(1,'unit','guard','ready',0,0,0,0,$rows,0,'snapshot','stage')")
        repeat(rows) { n ->
            val key="{\"id\":\"R$n\"}";val payload="{\"id\":\"R$n\",\"name\":\"Ž🧩-$n\",\"parent_id\":null}"
            db.withPreparedStatement("INSERT INTO _sync_snapshot_stage(snapshot_id,row_ordinal,schema_name,table_name,key_json,row_version,payload) VALUES('stage',?,'main','nodes',?,7,?)") {
                it.bindLong(1,n+1L);it.bindText(2,key);it.bindText(3,payload);it.step()
            }
            db.withPreparedStatement("INSERT INTO _sync_apply_rows(ordinal,table_name,local_pk,payload,wire,pending,bytes,role,source_ordinal) VALUES(?,'nodes',?,NULL,1,2,?,'snapshot_ref',?)") {
                it.bindLong(1,n+1L);it.bindText(2,"R$n");it.bindLong(3,96+6*utf8Size("nodes")+6*utf8Size("R$n"));it.bindLong(4,n+1L);it.step()
            }
        }
        db.execSQL("UPDATE _sync_apply_plan SET bytes=(SELECT sum(bytes) FROM _sync_apply_rows)")
    }
}
