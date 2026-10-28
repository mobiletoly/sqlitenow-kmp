package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.withContext
import kotlinx.coroutines.currentCoroutineContext
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.SqliteNowDiagnostics
import dev.goquick.sqlitenow.core.SqliteNowDiagnosticContext
import kotlinx.serialization.json.*
import kotlin.test.*

internal class SnapshotTypedRowTest : SurvivorTestSupport() {
    @Test fun repeatedComparisonAndBindingDecodeOnlyFreshRepresentations() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _,db,_,client ->
            val previous=sqliteNowLogger;val capture=PipelineDiagnosticCapture();sqliteNowLogger=capture
            try {
                val info=TableInfo("nodes",listOf(ColumnInfo("id","TEXT",true,true,null,wireType="text"),ColumnInfo("name","TEXT",false,true,null,wireType="text"),ColumnInfo("parent_id","TEXT",false,false,null,wireType="text")))
                val wire=buildJsonObject { put("id","P");put("name","parent");put("parent_id",JsonNull) }
                val live=buildJsonObject { for((k,v) in wire)put(k,buildJsonObject { put("storage",if(v==JsonNull)"null" else "text");put("value",v) }) }
                SqliteNowDiagnostics(true).operation("typed-proof") {
                    val typed=SnapshotTypedValues.decode(db,info,PayloadSource.AUTHORITATIVE_WIRE,wire,SqliteNowDiagnosticContext.current(currentCoroutineContext())?.span)
                    try {
                        repeat(2) { assertTrue(typed.matches(live,PayloadSource.LOCAL_STATE)) }
                        val bad = JsonObject(live + ("id" to buildJsonObject { put("storage","integer");put("value","1") }))
                        assertFails { typed.matches(bad,PayloadSource.LOCAL_STATE) }
                        db.withPreparedStatement("SELECT ?,?,?") { st -> typed.bind(st);assertTrue(st.step()) }
                    } finally { typed.close() }
                }
                val records=capture.records();val family=records.single { it["kind"]?.jsonPrimitive?.content=="sql_family" && it["name"]?.jsonPrimitive?.content=="row_processing.typed_scalar_decode" }["family"]
                val calls=records.single { it["kind"]?.jsonPrimitive?.content=="work_metric" && it["family"]==family }["counts"]!!.jsonObject["calls"]!!.jsonPrimitive.long
                assertEquals(10L,calls,"three initial values, two fresh three-value live reads and one failed input; binding and retained-side comparison decode nothing")
                assertEquals(1L,records.single { it["kind"]?.jsonPrimitive?.content=="work_metric" && it["family"]==family }["counts"]!!.jsonObject["failures"]!!.jsonPrimitive.long)
                assertTrue(calls<2L*2*info.columns.size)
            } finally { sqliteNowLogger=previous;client.closeFromCoordinatedOwner() }
        }
    }
    @Test fun typedRetentionFitsExactlyOrFallsBackAtBothBounds() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _,db,_,client -> try {
            val info=TableInfo("nodes",listOf(ColumnInfo("id","TEXT",true,true,null,wireType="text"),ColumnInfo("name","TEXT",false,true,null,wireType="text"),ColumnInfo("parent_id","TEXT",false,false,null,wireType="text")))
            val wire=buildJsonObject { put("id","P");put("name","parent");put("parent_id",JsonNull) }
            val live=buildJsonObject { for((k,v) in wire)put(k,buildJsonObject { put("storage",if(v==JsonNull)"null" else "text");put("value",v) }) }.toString()
            val cfg=OversqliteConfig(1,nodeTables,"fixture","main");val original=OversqliteWorkspace(db,cfg)
            val bytes=utf8Size(wire.toString());val extra=SnapshotTypedValues.charge(original,"typed",info,bytes);val base=original.usage("typed")
            for(dimension in listOf("entries","bytes"))for(fits in listOf(false,true))withContext(OversqliteWorkSlice()) {
                val config=if(dimension=="entries")cfg.copy(maxSyncWorkspaceEntries=base.entries+extra.entries-if(fits)0 else 1)
                    else cfg.copy(maxSyncWorkspaceBytes=base.bytes+extra.bytes-if(fits)0 else 1)
                val ws=OversqliteWorkspace(db,config)
                SnapshotRowLease.use(db,ApplySurvivor("nodes","P",null,true),1,bytes,"typed",null,ws,Json,wire) { lease ->
                    assertTrue(lease.matches(info,live));assertTrue(lease.matches(info,live))
                    assertEquals(if(fits)extra else WorkspaceCharge(),lease.retainedCharge)
                    assertEquals(fits,lease.releaseOptionalTyped(db))
                    assertEquals(WorkspaceCharge(),lease.retainedCharge)
                }
                assertEquals(base,ws.usage("typed"))
            }
        } finally { client.closeFromCoordinatedOwner() } }
    }
    @Test fun typedRowBindsComparesAndExpiresWithoutChangingStorageChecks() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _,db,_,client -> try {
            val info=TableInfo("typed_fixture",listOf(
                ColumnInfo("id","TEXT",true,true,null,wireType="text"),ColumnInfo("number","INTEGER",false,true,null,wireType="int64"),
                ColumnInfo("data","BLOB",false,true,null,wireType="bytes"),ColumnInfo("note","TEXT",false,false,null,wireType="text")))
            val wire=buildJsonObject { put("id","Ž🧩");put("number",Long.MIN_VALUE.toString());put("data","AAH/");put("note",JsonNull) }
            val typed=SnapshotTypedValues.decode(db,info,PayloadSource.AUTHORITATIVE_WIRE,wire,null)
            db.execSQL("CREATE TABLE typed_fixture(id TEXT PRIMARY KEY,number INTEGER,data BLOB,note TEXT)")
            db.withPreparedStatement("INSERT INTO typed_fixture VALUES(?,?,?,?)") { typed.bind(it);it.step() }
            val packed=db.withPreparedStatement("SELECT id,number,data,note FROM typed_fixture") { check(it.step());OversqliteValueCodec.encodeLocalPayloadObject(info,it) }
            assertTrue(typed.matches(packed,PayloadSource.LOCAL_STATE))
            assertFails { typed.requireOwner(db,info.copy(),wire,PayloadSource.AUTHORITATIVE_WIRE) }
            assertFails { typed.requireOwner(db,info,JsonObject(wire.toMap()),PayloadSource.AUTHORITATIVE_WIRE) }
            val malformed=JsonObject(packed+("number" to buildJsonObject { put("storage","text");put("value","1") }))
            assertFails { typed.matches(malformed,PayloadSource.LOCAL_STATE) }
            typed.close();db.withPreparedStatement("SELECT 1") { st -> assertFails { typed.bind(st) } }
        } finally { client.closeFromCoordinatedOwner() } }
    }

    @Test fun retainedTypedValuesReleaseOnTransferCancellationAndMalformedInput() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _,db,_,client -> try {
            val info=TableInfo("nodes",listOf(ColumnInfo("id","TEXT",true,true,null,wireType="text"),ColumnInfo("name","TEXT",false,true,null,wireType="text"),ColumnInfo("parent_id","TEXT",false,false,null,wireType="text")))
            val wire=buildJsonObject { put("id","P");put("name","parent");put("parent_id",JsonNull) }
            val ws=OversqliteWorkspace(db,OversqliteConfig(1,nodeTables,"fixture","main"))
            val extra=SnapshotTypedValues.charge(ws,"typed",info,utf8Size(wire.toString()))
            ws.check("typed",extra)
            val typed=SnapshotTypedValues.decode(db,info,PayloadSource.AUTHORITATIVE_WIRE,wire,null)
            withContext(OversqliteWorkSlice()) { assertFailsWith<CancellationException> {
                SnapshotRowLease.use(db,ApplySurvivor("nodes","P",null,true),1,100,"typed",null,ws,Json,wire,typed,extra) { lease ->
                    assertEquals(extra,lease.retainedCharge)
                    assertSame(typed,lease.takeTypedValues());assertEquals(WorkspaceCharge(),lease.retainedCharge)
                    assertSame(wire,lease.takePayloadObject())
                    throw CancellationException("fixture cancellation")
                }
            }
            }
            assertFails { typed.requireOwner(db,info) }
            val rawTyped=SnapshotTypedValues.decode(db,info,PayloadSource.AUTHORITATIVE_WIRE,wire,null)
            SnapshotRowLease.use(db,ApplySurvivor("nodes","P",null,true),1,100,"typed",null,ws,Json,wire,rawTyped,extra) { lease ->
                assertEquals(WorkspaceCharge(),lease.retainedCharge)
                assertNull(lease.takeTypedValues())
                assertFails { rawTyped.requireOwner(db,info) }
            }
            assertFails { SnapshotTypedValues.decode(db,info,PayloadSource.AUTHORITATIVE_WIRE,JsonObject(wire+("id" to JsonNull)),null) }
        } finally { client.closeFromCoordinatedOwner() } }
    }
}
