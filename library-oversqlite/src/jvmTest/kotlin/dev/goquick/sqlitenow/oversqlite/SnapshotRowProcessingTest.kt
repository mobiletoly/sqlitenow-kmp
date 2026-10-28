package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.SqliteNowDiagnostics
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.*
import kotlin.test.*

internal class SnapshotRowProcessingTest : SurvivorTestSupport() {
    @Test fun finalValidationRetainsThreeFreshPassesWithoutWireTextRoundTrips() = runOwnedWebDatabaseTest {
        for (rows in listOf(16, 64)) withSurvivorFixture { server, db, http, seed ->
            seed.closeFromCoordinatedOwner()
            val capture = PipelineDiagnosticCapture(); val previous = sqliteNowLogger; sqliteNowLogger = capture
            val client = DefaultOversqliteClient.forOpening(db, OversqliteConfig(1, nodeTables, "fixture", "main", diagnostics=SqliteNowDiagnostics(true)), http, ServerWinsResolver, Any())
            try {
                client.openAndConnect("user-1").getOrThrow()
                server.addRemoteBundleForTest((0 until rows-4).map { n -> BundleRow("main","nodes",mapOf("id" to "R$n"),"INSERT",2,
                    buildJsonObject { put("id","R$n");put("name","Ž🧩-$n");put("parent_id",JsonNull) }) })
                capture.clear(); client.rebuild().getOrThrow()
                val events=capture.records();val families=events.filter { it["kind"]?.jsonPrimitive?.content=="sql_family" }.associate { it["family"]!!.jsonPrimitive.content to it["name"]!!.jsonPrimitive.content }
                val phase=events.single { it["kind"]?.jsonPrimitive?.content=="span_start" && it["name"]?.jsonPrimitive?.content=="final_validation" }["span"]
                val children=events.filter { it["kind"]?.jsonPrimitive?.content=="span_start" && it["parent"]==phase }.map { it["span"] }.toSet()
                assertEquals(4,children.size,"all final-verification phases remain under final_validation")
                fun count(name:String)=events.filter { it["kind"]?.jsonPrimitive?.content=="work_metric" && (it["span"]==phase || it["span"] in children) && families[it["family"]?.jsonPrimitive?.content]==name }.sumOf { it["counts"]!!.jsonObject["calls"]!!.jsonPrimitive.long }
                assertEquals(3L*rows,count("row_processing.wire_decode"),"restore, verify-only restore and final snapshot verification each reload every row")
                assertEquals(3L*rows,count("row_processing.wire_normalize"))
                assertEquals(0L,count("row_processing.row_encode"),"object consumers must not encode intermediate wire payloads")
                assertEquals(rows.toLong(),count("row_processing.authoritative_read"))
                assertEquals(rows.toLong(),count("snapshot.final.typed_projection"))
                assertEquals(0L,count("snapshot.final.typed_projection_fallback"))
                assertEquals(0L,count("row_processing.expected_projection_text"),"final expected projection must not build or read JSON")
                assertEquals(rows.toLong(),count("row_processing.authoritative_decode"),"stored authoritative evidence is still decoded independently")
                assertEquals(rows.toLong(),scalarLong(db,"SELECT count(*) FROM nodes"))
                assertEquals(0L,scalarLong(db,"SELECT count(*) FROM pragma_foreign_key_check"))
            } finally { client.closeFromCoordinatedOwner();sqliteNowLogger=previous }
        }
    }

    @Test fun changedStagedPayloadIsRejectedAfterSealingAndOnResume() = runOwnedWebDatabaseTest {
        for (resume in listOf(false, true)) withSurvivorFixture(workUnits = 64) { _, db, _, client -> try {
            var original: String? = null
            var ordinal = 0L
            suspend fun corrupt() {
                ordinal = scalarLong(db, "SELECT row_ordinal FROM _sync_snapshot_stage WHERE json_extract(payload,'$.id')='P'")
                original = scalarText(db, "SELECT payload FROM _sync_snapshot_stage WHERE row_ordinal=$ordinal")
                // Same length, valid wire data: only a fresh hash proves this mutation.
                db.execSQL("UPDATE _sync_snapshot_stage SET payload=replace(payload,'parent','tamper') WHERE row_ordinal=$ordinal")
            }
            val before = baselineQualificationVector(db, listOf("nodes", "_sync_row_state"))
            var commits = 0
            client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(beforeCommit = {
                if (++commits == 1) {
                    if (resume) throw CancellationException("retain sealed staging")
                    corrupt()
                }
            }))
            if (resume) {
                assertFailsWith<CancellationException> { client.rebuild().getOrThrow() }
                client.setSnapshotApplyFaultInjectorForTest(null)
                assertTrue(OversqliteOperationStateStore(db).loadState().snapshotStageComplete)
                corrupt()
            }
            val failure = client.rebuild().exceptionOrNull()
            assertIs<SnapshotSemanticException>(failure, "resume=$resume failure=${failure?.stackTraceToString()}")
            val after = baselineQualificationVector(db, listOf("nodes", "_sync_row_state"))
            for (table in listOf("nodes", "_sync_row_state")) assertEquals(before.rows(table), after.rows(table))
            client.setSnapshotApplyFaultInjectorForTest(null)
            db.withPreparedStatement("UPDATE _sync_snapshot_stage SET payload=? WHERE row_ordinal=?") { st ->
                st.bindText(1, checkNotNull(original)); st.bindLong(2, ordinal); st.step()
            }
            client.rebuild().getOrThrow()
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_snapshot_stage"))
        } finally { client.setSnapshotApplyFaultInjectorForTest(null); client.closeFromCoordinatedOwner() } }
    }

    @Test fun leaseChargesOptionalCopiesAtBothLimitsAndAlwaysReleasesThem() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _,db,_,client ->
            try {
                val base=OversqliteConfig(1,nodeTables,"fixture","main")
                val text="{\"note\":\"Ž🧩\"}"
                val extra=WorkspaceCharge(4,6*utf8Size(text))
                for (dimension in listOf("entries","bytes")) for (fits in listOf(false,true)) {
                    val slice=OversqliteWorkSlice()
                    withContext(slice) {
                        val used=OversqliteWorkspace(db,base).usage("lease")
                        val cfg=if(dimension=="entries")base.copy(maxSyncWorkspaceEntries=used.entries+if(fits)extra.entries else extra.entries-1)
                            else base.copy(maxSyncWorkspaceBytes=used.bytes+if(fits)extra.bytes else extra.bytes-1)
                        val ws=OversqliteWorkspace(db,cfg)
                        assertFailsWith<CancellationException> {
                            SnapshotRowLease.use(db,ApplySurvivor("nodes","P",text),0,utf8Size(text),"lease",null,ws,Json,null) { lease ->
                                val one=lease.payloadObject();val two=lease.payloadObject()
                                assertEquals(one,two)
                                if(fits)assertSame(one,two) else assertNotSame(one,two)
                                assertEquals(if(fits)extra else WorkspaceCharge(),lease.retainedCharge)
                                assertEquals(ws.add("lease",used,lease.retainedCharge),ws.usage("lease"))
                                assertEquals(one, lease.takePayloadObject())
                                assertEquals(WorkspaceCharge(), lease.retainedCharge, "required transient borrowing retires the optional holder before nested work")
                                assertEquals(used, ws.usage("lease"))
                                throw CancellationException("lease fixture")
                            }
                        }
                        assertNull(slice.snapshotRowLease)
                        assertEquals(used,ws.usage("lease"))
                        assertFails { SnapshotRowLease.use(db,ApplySurvivor("nodes","P","{"),0,1,"lease",null,ws,Json,null) { it.payloadObject() } }
                        assertNull(slice.snapshotRowLease)
                    }
                }
            } finally { client.closeFromCoordinatedOwner() }
        }
    }

    @Test fun wireLeasePreservesNullBlobNumericAndUnicodeStorage() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _,db,_,client ->
            try {
                val columns=listOf(ColumnInfo("id","TEXT",true,true,null,wireType="text"),ColumnInfo("data","BLOB",false,true,null,wireType="bytes"),
                    ColumnInfo("number","INTEGER",false,true,null,wireType="int64"),ColumnInfo("optional","TEXT",false,false,null,wireType="text"),ColumnInfo("note","TEXT",false,true,null,wireType="text"))
                val info=TableInfo("typed_fixture",columns)
                val wire=buildJsonObject { put("id","U");put("data","AAH/");put("number","-9223372036854775808");put("optional",JsonNull);put("note","Ž🧩") }
                db.execSQL("CREATE TABLE typed_fixture(id TEXT PRIMARY KEY,data BLOB NOT NULL,number INTEGER NOT NULL,optional TEXT,note TEXT NOT NULL)")
                db.withPreparedStatement("INSERT INTO typed_fixture VALUES(?,?,?,?,?)") { st -> columns.forEachIndexed { n,c -> OversqliteValueCodec.bindPayloadValue(st,n+1,c,wire.getValue(c.name),PayloadSource.AUTHORITATIVE_WIRE) };st.step() }
                val live=db.withPreparedStatement("SELECT id,data,number,optional,note FROM typed_fixture") { st -> check(st.step());buildJsonObject { columns.forEachIndexed { n,c -> put(c.name,OversqliteValueCodec.readLocalPayloadValue(st,n,c)) } }.toString() }
                val ws=OversqliteWorkspace(db,OversqliteConfig(1,nodeTables,"fixture","main"));val bytes=utf8Size(wire.toString())
                ws.check("wire",WorkspaceCharge(8,8*bytes))
                SnapshotRowLease.use(db,ApplySurvivor("typed_fixture","U",null,true,"snapshot_ref",1),42,bytes,"wire",null,ws,Json,wire) { lease ->
                    assertTrue(lease.matches(info,live));assertEquals(42,lease.version)
                    assertEquals(wire,Json.parseToJsonElement(checkNotNull(lease.legacyRow().payload)))
                }
                val invalid=JsonObject(wire+ ("data" to JsonPrimitive("not base64")))
                assertFails { SnapshotRowLease.use(db,ApplySurvivor("typed_fixture","U",null,true,"snapshot_ref",1),42,bytes,"wire",null,ws,Json,invalid) { it.matches(info,live) } }
            } finally { client.closeFromCoordinatedOwner() }
        }
    }

    @Test fun finalProjectionAndProvenanceCorruptionStillRollbackAndRetry() = runOwnedWebDatabaseTest {
        for (fault in listOf("projection","provenance","payload","duplicate","kind","context","owner_revision","relationships_unknown","changed_value")) withSurvivorFixture { server,db,_,client ->
            try {
                server.addRemoteBundleForTest(listOf(BundleRow("main","nodes",mapOf("id" to "P"),"UPDATE",2,
                    buildJsonObject { put("id","P");put("name","changed");put("parent_id",JsonNull) })))
                val before=baselineQualificationVector(db,listOf("nodes"));var applied=0
                client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(afterAppliedRow={
                    if(++applied==4) when(fault) {
                        "projection" -> db.execSQL("INSERT INTO _sync_relationships(view,child_schema,child_table,child_key,fk_column,parent_schema,parent_table,parent_key,owner_revision) SELECT 'server',schema_name,table_name,key_json,'bogus','main','nodes','{}',server_revision FROM _sync_row_state WHERE table_name='nodes' AND json_extract(key_json,'$.id')='P'")
                        "provenance" -> db.execSQL("UPDATE _sync_row_state SET row_version=row_version+1 WHERE table_name='nodes' AND json_extract(key_json,'$.id')='P'")
                        "duplicate" -> db.execSQL("UPDATE _sync_row_state SET server_payload='{\"id\":null,\"id\":null,\"name\":null,\"parent_id\":null}' WHERE table_name='nodes' AND json_extract(key_json,'$.id')='P'")
                        "kind" -> db.execSQL("UPDATE _sync_row_state SET server_kind='deleted' WHERE table_name='nodes' AND json_extract(key_json,'$.id')='P'")
                        "context" -> db.execSQL("UPDATE _sync_row_state SET server_context_id=NULL WHERE table_name='nodes' AND json_extract(key_json,'$.id')='P'")
                        "owner_revision" -> db.execSQL("UPDATE _sync_relationships SET owner_revision=owner_revision+1 WHERE view='server' AND child_table='nodes'")
                        "relationships_unknown" -> db.execSQL("UPDATE _sync_row_state SET relationships_known=0 WHERE table_name='nodes' AND json_extract(key_json,'$.id')='P'")
                        "changed_value" -> db.execSQL("UPDATE _sync_row_state SET server_payload=json_set(server_payload,'$.name.value','corrupted') WHERE table_name='nodes' AND json_extract(key_json,'$.id')='P'")
                        else -> db.execSQL("UPDATE _sync_row_state SET server_payload='{}' WHERE table_name='nodes' AND json_extract(key_json,'$.id')='P'")
                    }
                }))
                assertNotNull(client.rebuild().exceptionOrNull(),fault)
                val after=baselineQualificationVector(db,listOf("nodes"))
                for(table in listOf("nodes","_sync_row_state","_sync_relationships"))assertEquals(before.rows(table),after.rows(table),fault+table)
                client.setSnapshotApplyFaultInjectorForTest(null);client.rebuild().getOrThrow()
                assertEquals("changed",scalarText(db,"SELECT name FROM nodes WHERE id='P'"))
            } finally { client.setSnapshotApplyFaultInjectorForTest(null);client.closeFromCoordinatedOwner() }
        }
    }
}
