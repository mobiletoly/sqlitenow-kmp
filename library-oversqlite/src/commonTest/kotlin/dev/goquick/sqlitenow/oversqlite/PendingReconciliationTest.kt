package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.*
import kotlin.test.*

internal class PendingReconciliationTest : SurvivorTestSupport() {
    @Test fun publicResults() = runOwnedWebDatabaseTest {
        for(detail in listOf("dependency_unavailable","relationship_unknown","historical_repair_required"))
            withSurvivorFixture { server,db,_,client ->
                var parent="missing"
                if(detail=="relationship_unknown") {
                    parent="Q"
                    db.execSQL("UPDATE _sync_row_state SET server_kind='unknown',server_payload=NULL,row_version=0,relationships_known=0,server_provenance=json_set(server_provenance,'$.authoritative_version',0,'$.origin','explicit_conflict','$.bundle_sequence',NULL) WHERE json_extract(key_json,'$.id')='Q'")
                }
                if(detail=="historical_repair_required") {
                    parent="Q"
                    db.execSQL("UPDATE _sync_apply_state SET apply_mode=1")
                    db.execSQL("DELETE FROM nodes WHERE id='Q'")
                    db.execSQL("UPDATE _sync_row_state SET server_kind='deleted',server_payload=NULL,row_version=5,server_provenance=json_set(server_provenance,'$.authoritative_version',5,'$.origin','explicit_conflict','$.bundle_sequence',NULL) WHERE json_extract(key_json,'$.id')='Q'")
                    db.execSQL("UPDATE _sync_apply_state SET apply_mode=0")
                }
                server.addRemoteBundleForTest(listOf(BundleRow("main","nodes",mapOf("id" to "R"),"INSERT",2,
                    buildJsonObject { put("id","R");put("name","pending");put("parent_id",parent) })))
                val before=baselineQualificationVector(db,listOf("nodes"))
                val pending=assertIs<FKReconciliationRequiredException>(client.pullToStable().exceptionOrNull())
                assertEquals(detail,pending.detail)
                assertEquals(64,pending.unit.length)
                assertEquals(mapOf("id" to "R"),pending.sample.single().key)
                assertEquals(before,baselineQualificationVector(db,listOf("nodes")))
                assertEquals(pending.unit,assertIs<FKReconciliationRequiredException>(client.pullToStable().exceptionOrNull()).unit)
                assertIs<FKReconciliationRequiredException>(client.sync().exceptionOrNull())
                assertIs<FKReconciliationRequiredException>(client.syncThenDetach().exceptionOrNull())
                assertEquals(0,server.snapshotSessionCreateRequestCount)
                assertEquals(before,baselineQualificationVector(db,listOf("nodes")))
                assertEquals("attached",scalarText(db,"SELECT binding_state FROM _sync_attachment_state"))
            }
    }
}
