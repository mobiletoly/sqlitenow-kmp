package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.CancellationException
import kotlin.test.*

internal class SnapshotRebuildResumeTest : SurvivorTestSupport() {
    @Test fun checkpointRebuildRetainsStageAcrossWorkYields() = runOwnedWebDatabaseTest {
        verifyResume(preserveIntent = false, invalidate = false)
    }

    @Test fun sourceRecoveryRetainsStageAndFrozenIntentAcrossWorkYields() = runOwnedWebDatabaseTest {
        verifyResume(preserveIntent = true, invalidate = false)
    }

    @Test fun changedBaselineRefetchesInsteadOfResumingStaleSnapshot() = runOwnedWebDatabaseTest {
        verifyResume(preserveIntent = false, invalidate = true)
    }

    private suspend fun verifyResume(preserveIntent: Boolean, invalidate: Boolean) {
        withSurvivorFixture { server, db, http, original ->
            if (preserveIntent) {
                db.execSQL("UPDATE nodes SET name='frozen L' WHERE id='P'")
                server.beforePushCommit = { throw CancellationException("retain intent") }
                assertFailsWith<CancellationException> { original.pushPending().getOrThrow() }
            }
            original.closeFromCoordinatedOwner()
            val client = newClient(db, http, syncTables = nodeTables, workUnits = 64)
            try {
                client.openAndConnect("user-1").getOrThrow()
                if (preserveIntent) markSourceRecoveryRequired(db, SourceRecoveryReason.HISTORY_PRUNED)
                val before = baselineQualificationVector(db, listOf("nodes"))
                val requests = server.snapshotSessionCreateRequestCount
                var yields = 0
                var previousRoot = -1L
                var stageId: String? = null
                while (true) {
                    val error = client.rebuild().exceptionOrNull() ?: break
                    assertIs<SyncWorkLimitException>(error)
                    assertTrue(++yields < 20, "preparation must make bounded progress")
                    val operation = OversqliteOperationStateStore(db).loadState()
                    assertTrue(operation.snapshotStageComplete)
                    assertEquals(4L, scalarLong(db, "SELECT count(*) FROM _sync_snapshot_stage"))
                    val root = scalarLong(db, "SELECT root_cursor FROM _sync_apply_plan")
                    if (stageId == operation.stagedSnapshotId) assertTrue(root > previousRoot,
                        "same-stage preparation must advance: $previousRoot -> $root")
                    previousRoot = root
                    stageId = operation.stagedSnapshotId
                    val after = baselineQualificationVector(db, listOf("nodes"))
                    for (table in listOf("nodes", "_sync_row_state", "_sync_relationships", "_sync_outbox_rows", "_sync_outbox_bundle"))
                        assertEquals(before.rows(table), after.rows(table), "yield leaves $table unchanged")
                    assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    if (invalidate && yields == 1) {
                        db.execSQL("UPDATE _sync_conflict_state SET baseline_revision=baseline_revision+1")
                    }
                }
                assertTrue(yields > 0)
                assertEquals(requests + if (invalidate) 2 else 1, server.snapshotSessionCreateRequestCount)
                assertEquals(if (preserveIntent) "frozen L" else "parent", scalarText(db, "SELECT name FROM nodes WHERE id='P'"))
                assertEquals("parent", scalarText(db, "SELECT json_extract(server_payload,'$.name.value') FROM _sync_row_state WHERE json_extract(key_json,'$.id')='P'"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                for (table in listOf("_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM $table"), table)
                assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+snapshot_stage_entries+snapshot_stage_bytes FROM _sync_operation_state"))
                assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(db).loadState())
                if (preserveIntent) {
                    val after = baselineQualificationVector(db, listOf("nodes"))
                    assertEquals(before.rows("_sync_outbox_rows").map { it - "source_bundle_id" },
                        after.rows("_sync_outbox_rows").map { it - "source_bundle_id" })
                    assertEquals(1L, scalarLong(db, "SELECT source_bundle_id FROM _sync_outbox_rows"))
                    assertEquals(1L, scalarLong(db, "SELECT source_bundle_id FROM _sync_outbox_bundle"))
                    client.closeFromCoordinatedOwner()
                    // The tiny target isolates snapshot preparation. Replay the
                    // retained intent with the ordinary public work target.
                    val replay = newClient(db, http, syncTables = nodeTables)
                    try {
                        replay.openAndConnect("user-1").getOrThrow()
                        replay.pushPending().getOrThrow()
                        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
                    } finally { replay.closeFromCoordinatedOwner() }
                }
            } finally { client.closeFromCoordinatedOwner() }
        }
    }
}
