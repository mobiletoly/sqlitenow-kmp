package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Structural SQL observations are separate from latency and memory profiles. */
internal class PushPlanningWorkspaceObservationTest : SurvivorTestSupport() {
    @Test fun planningReadsWorkspaceMetadataWithoutRetainingHandles() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, _, client ->
            db.execSQL("UPDATE nodes SET name='updated parent' WHERE id='P'")
            val requests = server.pushRequestCount
            val observation = ReplaySqlObservation.install(db)
            try { client.pushPending().getOrThrow() } finally { observation.restore() }
            val reads = observation.counts.filterKeys { it.startsWith("SELECT o.reserved_entries") }.values.sumOf { it.steps }
            val guards = observation.counts.filterKeys { it.startsWith("SELECT i.database_id,i.service_id") }.values.sumOf { it.steps }
            observation.counts.forEach { (sql, count) -> assertEquals(count.prepares, count.closes, "released: $sql") }
            assertTrue(reads > 0)
            assertEquals(requests + 1, server.pushRequestCount)
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
            val planReads = observation.counts.filterKeys { it.startsWith("SELECT unit,guard,phase,entries,bytes,length(CAST(state AS BLOB))") }.values.sumOf { it.steps }
            val stateReads = observation.counts.filterKeys { it.startsWith("SELECT state FROM _sync_push_plan") }.values.sumOf { it.steps }
            val outboxReads = observation.counts.filterKeys { it.startsWith("SELECT state FROM _sync_outbox_bundle") }.values.sumOf { it.steps }
            println("push_workspace_observation reads=$reads guards=$guards plan_reads=$planReads state_reads=$stateReads outbox_reads=$outboxReads diagnostics=${client.workSliceDiagnostics()}")
        }
    }
}
