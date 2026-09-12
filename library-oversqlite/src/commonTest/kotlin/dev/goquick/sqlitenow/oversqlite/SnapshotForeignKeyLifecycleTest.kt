package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.*
import kotlin.test.*

internal class SnapshotForeignKeyLifecycleTest : SurvivorTestSupport() {
    private fun row(id: String, parent: String?) = BundleRow("main","nodes",mapOf("id" to id),"UPDATE",2,
        buildJsonObject { put("id",id); put("name","S-$id"); put("parent_id",parent?.let(::JsonPrimitive) ?: JsonNull) })

    @Test fun admissionAndParents() = runOwnedWebDatabaseTest {
        val vectors=listOf(listOf(row("X","Y"),row("Y",null)),listOf(row("X","X")),
            listOf(row("X","Y"),row("Y","X")),listOf(row("C","Q"),row("Q",null),row("D","C")))
        for((index,rows) in vectors.withIndex()) withSurvivorFixture { server,db,_,client ->
            val before=baselineQualificationVector(db,listOf("nodes"))
            val expected=before.rows("nodes").associate { node ->
                checkNotNull(node.text("id")) to mapOf("id" to node.text("id"),"name" to node.text("name"),"parent_id" to node.text("parent_id"))
            }.toMutableMap()
            for(row in rows) {
                val payload=checkNotNull(row.payload).jsonObject
                expected[checkNotNull(row.key["id"])]=payload.mapValues { (_,value) -> value.jsonPrimitive.contentOrNull }
            }
            server.addRemoteBundleForTest(rows)
            client.rebuild().getOrThrow()
            val after=baselineQualificationVector(db,listOf("nodes"))
            assertEquals(expected.keys,after.rows("nodes").map { checkNotNull(it.text("id")) }.toSet(),"complete installed row inventory")
            assertEquals(expected.size,after.rows("_sync_row_state").size,"complete authoritative row inventory")
            for(node in after.rows("nodes")) {
                val id=checkNotNull(node.text("id"))
                assertTextPayload(node,expected.getValue(id))
                val authoritative=after.row("_sync_row_state",id)
                assertTextPayload(authoritative.payload("server_payload"),expected.getValue(id))
                assertEquals("live",authoritative.text("server_kind"))
                assertEquals("1",authoritative.text("relationships_known"))
                val version=rows.singleOrNull { it.key["id"]==id }?.rowVersion
                    ?: before.row("_sync_row_state",id).text("row_version")!!.toLong()
                assertEquals(version.toString(),authoritative.text("row_version"))
                assertBaselineProvenance(authoritative,"server_provenance",version)
            }
            val expectedEdges=expected.mapNotNull { (id,payload) ->
                payload["parent_id"]?.let { parent -> buildJsonObject {
                    put("view","server");put("child_schema","main");put("child_table","nodes")
                    put("child_key","{\"id\":\"$id\"}");put("fk_column","parent_id")
                    put("parent_schema","main");put("parent_table","nodes");put("parent_key","{\"id\":\"$parent\"}")
                } }
            }.toSet()
            val relationships=after.rows("_sync_relationships")
            assertEquals(expectedEdges.size,relationships.size,"exact relationship count; clean rows have no desired edges")
            assertEquals(expectedEdges,relationships.map { edge ->
                buildJsonObject { for(column in edge.keys - "owner_revision") put(column,edge.text(column)) }
            }.toSet(),"complete server relationship projection, including self/cycle and NULL absence")
            for(edge in relationships) {
                val id=Json.parseToJsonElement(checkNotNull(edge.text("child_key"))).jsonObject.getValue("id").jsonPrimitive.content
                assertEquals(after.row("_sync_row_state",id).text("server_revision"),edge.text("owner_revision"),"relationship owner revision")
                assertTrue(edge.text("owner_revision")!!.toLong()>0)
            }
            for(table in listOf("_sync_dirty_rows","_sync_outbox_rows","_sync_outbox_bundle","_sync_source_state"))
                assertEquals(before.rows(table),after.rows(table),"preserved clean/source state $table")
            assertEquals(rows.size.toLong()+4-rows.count { it.key["id"] in setOf("P","Q","C","D") },scalarLong(db,"SELECT count(*) FROM nodes"))
            for(row in rows) {
                val id=checkNotNull(row.key["id"])
                assertEquals("S-$id",scalarText(db,"SELECT name FROM nodes WHERE id='$id'"))
                assertEquals(row.rowVersion,scalarLong(db,"SELECT row_version FROM _sync_row_state WHERE json_extract(key_json,'$.id')='$id'"))
            }
            assertEquals(2L,scalarLong(db,"SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM pragma_foreign_key_check"))
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_apply_plan"))
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_snapshot_stage"))
            println("A3 snapshot topology $index complete payload and relationship projection verified")
        }
    }

    @Test fun actionsAndSurvivors() = runOwnedWebDatabaseTest {
        // SET DEFAULT is excluded by generation admission, not newly supported here.
        for(action in listOf("NO ACTION","RESTRICT","CASCADE","SET NULL")) withSurvivorFixture(action=action) { server,db,_,client ->
            db.execSQL("CREATE TRIGGER damage_d AFTER INSERT ON nodes WHEN NEW.id='Q' BEGIN UPDATE nodes SET name='trigger damage' WHERE id='D'; END")
            server.addRemoteBundleForTest(listOf(row("D","C"),row("C","Q"),row("Q",null),
                BundleRow("main","nodes",mapOf("id" to "P"),"DELETE",2,null)))
            client.rebuild().getOrThrow()
            assertEquals("S-D",scalarText(db,"SELECT name FROM nodes WHERE id='D'"))
            assertEquals("C",scalarText(db,"SELECT parent_id FROM nodes WHERE id='D'"))
            assertEquals("S-C",scalarText(db,"SELECT name FROM nodes WHERE id='C'"))
            assertEquals("Q",scalarText(db,"SELECT parent_id FROM nodes WHERE id='C'"))
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM nodes WHERE id='P'"))
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM pragma_foreign_key_check"))
        }
    }
}
