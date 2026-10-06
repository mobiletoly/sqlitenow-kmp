package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.TransactionMode
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertIs

internal class SnapshotStageReferenceTest : SurvivorTestSupport() {
    @Test fun retainedPlanReferencesTheCompleteStagePayload() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, http, _ ->
            val payload = """{"id":"R","name":"from-stage","parent_id":null}"""
            db.execSQL("INSERT INTO _sync_snapshot_stage(snapshot_id,row_ordinal,schema_name,table_name,key_json,row_version,payload) " +
                "VALUES('source',1,'main','nodes','{\"id\":\"R\"}',1,'$payload')")
            db.execSQL("UPDATE _sync_operation_state SET staged_snapshot_id='source',snapshot_row_count=1,snapshot_stage_complete=1")
            val config = OversqliteConfig(1, nodeTables, "fixture", "main", maxSyncWorkUnitsPerInvocation = 16)
            val generation = OversqliteContractStore(db).readGeneration(1)
            val validated = SyncRuntimeInitializer(config, TableInfoCache()).validateConfig(db).withGeneration(generation)
            val state = RuntimeState(validated, AdmittedOversqliteContract("v1", generation, testWireLimits(), false),
                "user-1", scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
            val local = OversqliteLocalStore(db, Json) { validated }
            val unit = applyUnit("snapshot", 2, "source")
            fun owner() = OversqliteApplySurvivors(db, local, config,
                OversqliteClientOperationController(config, OversqliteClientResourceOwner(db, http), SnapshotDiagnosticsState()) {}, Json)
            val first = owner()
            val lifecycle = OversqliteApplyLifecycle(db, config, local, first, Json)
            var pending: Throwable? = null
            db.transaction(TransactionMode.IMMEDIATE) {
                try { first.prepare(state, unit, 1, "snapshot", 2, sourceSnapshotId = "source") { index ->
                    lifecycle.readSnapshot(state, "source", index.toLong() + 1, unit).second
                } } catch (result: ApplyProgressPending) { pending = result.result }
            }
            assertIs<SyncWorkLimitException>(pending)
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_apply_rows WHERE role='snapshot_ref' AND source_ordinal=1 AND payload IS NULL"))
            assertEquals(payload, scalarText(db, "SELECT payload FROM _sync_snapshot_stage WHERE snapshot_id='source' AND row_ordinal=1"))
            owner().prepare(state, unit, 1, "snapshot", 2, sourceSnapshotId = "source") { error("retained root must not be re-read") }
            assertEquals("ready", scalarText(db, "SELECT phase FROM _sync_apply_plan"))
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_apply_rows WHERE payload IS NULL AND role='snapshot_ref'"))

            db.execSQL("DELETE FROM _sync_snapshot_stage WHERE snapshot_id='source'")
            assertFails { owner().prepare(state, unit, 1, "snapshot", 2, sourceSnapshotId = "source") { error("retained root") } }
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM nodes WHERE id='R'"))
        }
    }
}
