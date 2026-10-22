package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.*
import kotlin.test.*

internal class SnapshotVerificationStatementReuseTest : SurvivorTestSupport() {
    @Test fun freshOrdinaryRestorePreparesOnceForEachOfThreeVerificationOwners() = runOwnedWebDatabaseTest {
        for (rows in listOf(16, 64)) {
            withSurvivorFixture { server, db, _, client ->
                try {
                    addRows(server, rows - 4)
                    val observation = ReplaySqlObservation.install(db)
                    try {
                        client.rebuild().getOrThrow()
                        assertHotCounts(observation, owners = 3, rows = rows)
                        assertEquals(rows.toLong(), scalarLong(db, "SELECT count(*) FROM nodes"))
                        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_snapshot_stage"))
                        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                    } finally { observation.restore() }
                } finally { client.closeFromCoordinatedOwner() }
            }
        }
    }

    @Test fun completedOrdinaryStageResumeRetainsSealAndFinalVerification() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, _, client ->
            try {
                addRows(server, 12)
                client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(beforeCommit = { throw CancellationException("after staging") }))
                assertFailsWith<CancellationException> { client.rebuild().getOrThrow() }
                client.setSnapshotApplyFaultInjectorForTest(null)
                assertTrue(OversqliteOperationStateStore(db).loadState().snapshotStageComplete)
                val observation = ReplaySqlObservation.install(db)
                try {
                    client.rebuild().getOrThrow()
                    assertHotCounts(observation, owners = 2, rows = 16)
                    assertEquals(16L, scalarLong(db, "SELECT count(*) FROM nodes"))
                } finally { observation.restore() }
            } finally { client.closeFromCoordinatedOwner() }
        }
    }

    @Test fun consecutiveConnectionsAndRestoresDoNotShareVerificationHandles() = runOwnedWebDatabaseTest {
        repeat(2) {
            withSurvivorFixture { _, db, _, client ->
                try {
                    repeat(2) {
                        val observation = ReplaySqlObservation.install(db)
                        try { client.rebuild().getOrThrow(); assertHotCounts(observation, 3, 4) }
                        finally { observation.restore() }
                    }
                } finally { client.closeFromCoordinatedOwner() }
            }
        }
    }

    @Test fun generationStagingAndCompletedResumeOwnSeparateEarlyCaches() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, _, client ->
            try {
                fun field(name: String): Any = DefaultOversqliteClient::class.java.getDeclaredField(name).let {
                    it.isAccessible = true; it.get(client)
                }
                val session = field("sessionState") as OversqliteClientSessionState
                val workflow = field("downloadWorkflow") as OversqliteDownloadWorkflow
                val lifecycle = field("lifecycleCoordinator") as OversqliteClientLifecycleCoordinator
                val source = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
                val state = RuntimeState(replayValidatedOwner(client), session.requireAdmittedContract(), "user-1", source)
                // The public snapshot-session admission requires the clean rebuild marker.
                db.execSQL("UPDATE _sync_attachment_state SET rebuild_required=1")
                val snapshot = workflow.createGuardedSnapshotSession(state, source, SnapshotSessionCreateRequest(recovery = SnapshotRecovery(1)))
                db.transaction(dev.goquick.sqlitenow.core.TransactionMode.IMMEDIATE) {
                    workflow.retainGenerationSnapshot(state, snapshot, source)
                }
                val stagedObservation = ReplaySqlObservation.install(db)
                val totals = try {
                    lifecycle.stageGenerationSnapshot(snapshot, source, "user-1") {}.also {
                        assertHotCounts(stagedObservation, 1, 4)
                    }
                } finally { stagedObservation.restore() }
                val resumedObservation = ReplaySqlObservation.install(db)
                try {
                    assertEquals(totals, lifecycle.stageGenerationSnapshot(snapshot, source, "user-1", totals) {})
                    assertHotCounts(resumedObservation, 1, 4)
                    val workspace = resumedObservation.counts.filterKeys { it.startsWith("SELECT o.reserved_entries,o.reserved_bytes,") }.values.single()
                    assertEquals(1L, workspace.prepares)
                    assertEquals(4L, workspace.steps)
                    assertEquals(workspace.steps, workspace.resets)
                    assertEquals(workspace.resets, workspace.clears)
                    assertEquals(1L, workspace.closes)
                } finally { resumedObservation.restore() }
                // Mutation of the sealed operation is refused before another verification pass.
                db.execSQL("UPDATE _sync_operation_state SET snapshot_staged_row_count=0")
                assertFails { lifecycle.stageGenerationSnapshot(snapshot, source, "user-1", totals) {} }
            } finally { client.closeFromCoordinatedOwner() }
        }
    }

    private fun addRows(server: MockSyncServer, count: Int) {
        server.addRemoteBundleForTest((0 until count).map { index ->
            val id = "R$index"
            BundleRow("main", "nodes", mapOf("id" to id), "INSERT", 2,
                buildJsonObject { put("id", id); put("name", "row-$index"); put("parent_id", JsonNull) })
        })
    }

    private fun assertHotCounts(observation: ReplaySqlObservation, owners: Int, rows: Int) {
        val size = observation.counts.filterKeys { it.startsWith("SELECT charge,length(CAST(schema_name") }.values.single()
        val content = observation.counts.filterKeys { it.startsWith("SELECT schema_name,table_name,key_json,row_version,payload FROM _sync_snapshot_stage WHERE snapshot_id=? AND row_ordinal=?") }.values.single()
        for (count in listOf(size, content)) {
            assertEquals(owners.toLong(), count.prepares)
            assertEquals((owners * rows).toLong(), count.steps)
            assertEquals(count.steps, count.resets)
            assertEquals(count.resets, count.clears)
            assertEquals(count.prepares, count.closes)
        }
        // This is independent of log events and still observes every row/hash boundary.
        val totals = observation.counts.filterKeys { it.startsWith("SELECT count(*),COALESCE(sum(charge),0) FROM _sync_snapshot_stage WHERE snapshot_id=?") }.values.single()
        assertEquals(owners.toLong(), totals.steps)
    }
}
