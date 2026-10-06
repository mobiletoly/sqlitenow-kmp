package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.CancellationException
import kotlin.test.*

internal class SnapshotStagingResumeTest : SurvivorTestSupport() {
    @Test fun tinyTargetCommitsChunksAndEventuallyRebuilds() = runOwnedWebDatabaseTest { verify() }
    @Test fun partialSourceRecoveryReopensAndPreservesFrozenIntent() = runOwnedWebDatabaseTest { verify(preserve = true, reopen = true) }
    @Test fun changedGuardInvalidatesPartialStage() = runOwnedWebDatabaseTest { verify(invalidate = true) }
    @Test fun expiredPartialRecoveryUsesEffectiveSourceWithoutAnotherReplacement() = runOwnedWebDatabaseTest { verify(preserve = true, expire = true) }

    @Test fun interruptedExpiryRefreshRetainsRecoveryReceiptAcrossReopen() = runOwnedWebDatabaseTest {
        for (cancel in listOf(true, false)) {
            val path = createTempSqliteNowTestDbPath("snapshot-expiry-refresh")
            try { withSurvivorFixture(dbPath = path) { server, originalDb, http, seed ->
                originalDb.execSQL("UPDATE nodes SET name='frozen L' WHERE id='P'")
                server.beforePushCommit = { throw CancellationException("retain intent") }
                assertFailsWith<CancellationException> { seed.pushPending().getOrThrow() }
                server.beforePushCommit = null
                seed.closeFromCoordinatedOwner()
                var db = originalDb
                var client = newClient(db, http, syncTables = nodeTables, workUnits = 1, maxSnapshotChunkRows = 2)
                try {
                    client.openAndConnect("user-1").getOrThrow()
                    markSourceRecoveryRequired(db, SourceRecoveryReason.HISTORY_PRUNED)
                    val before = baselineQualificationVector(db, listOf("nodes"))
                    assertIs<SyncWorkLimitException>(client.rebuild().exceptionOrNull())
                    val retained = OversqliteOperationStateStore(db).loadState()
                    val replacements = server.requestedSnapshotSourceReplacements.size
                    assertEquals(2L, retained.snapshotStagedRowCount)
                    server.expireSnapshotSessionForTest(retained.stagedSnapshotId)
                    val fault = if (cancel) CancellationException("cancel refresh") else IllegalStateException("refresh transport failure")
                    server.beforeSnapshotCreateRequest = { throw fault }
                    assertNotNull(runCatching { client.rebuild().getOrThrow() }.exceptionOrNull())
                    assertEquals(retained, OversqliteOperationStateStore(db).loadState(), "refresh acquisition must retain durable recovery proof")
                    client.closeFromCoordinatedOwner(); db.close()
                    db = newFileBackedDb(path)
                    client = newClient(db, http, syncTables = nodeTables, workUnits = 1, maxSnapshotChunkRows = 2)
                    client.openAndConnect("user-1").getOrThrow()
                    assertEquals(retained.replacementSourceId, server.connectSourceIds.last())
                    var yields = 0
                    while (true) {
                        val failure = client.rebuild().exceptionOrNull() ?: break
                        assertIs<SyncWorkLimitException>(failure); assertTrue(++yields < 20)
                    }
                    assertEquals(replacements, server.requestedSnapshotSourceReplacements.size)
                    val after = baselineQualificationVector(db, listOf("nodes"))
                    assertEquals(before.rows("nodes"), after.rows("nodes"))
                    assertEquals(before.rows("_sync_outbox_rows").map { it - "source_bundle_id" },
                        after.rows("_sync_outbox_rows").map { it - "source_bundle_id" }, "complete frozen intent")
                    assertEquals(before.rows("_sync_outbox_bundle").map { it - "source_id" - "source_bundle_id" },
                        after.rows("_sync_outbox_bundle").map { it - "source_id" - "source_bundle_id" }, "frozen hash and bundle state")
                    assertEquals(1L, scalarLong(db, "SELECT source_bundle_id FROM _sync_outbox_bundle"))
                    assertEquals(retained.replacementSourceId, scalarText(db, "SELECT source_id FROM _sync_outbox_bundle"))
                    assertEquals(4L, scalarLong(db, "SELECT count(*) FROM _sync_row_state"))
                    assertEquals(retained.replacementSourceId, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
                    assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                    assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(db).loadState())
                } finally { client.closeFromCoordinatedOwner(); if (db !== originalDb) db.close() }
            } } finally { deleteTempSqliteNowTestDbArtifacts(path) }
        }
    }

    @Test fun failedSessionPublicationRetiresFreshSessionWithoutAdoptingLateGuard() = runOwnedWebDatabaseTest {
        for (refresh in listOf(false, true)) withSurvivorFixture { server, db, http, seed ->
            seed.closeFromCoordinatedOwner()
            val client = newClient(db, http, syncTables = nodeTables, workUnits = 1, maxSnapshotChunkRows = 2)
            try {
                client.openAndConnect("user-1").getOrThrow()
                if (refresh) {
                    markSourceRecoveryRequired(db, SourceRecoveryReason.HISTORY_PRUNED)
                    assertIs<SyncWorkLimitException>(client.rebuild().exceptionOrNull())
                    server.expireSnapshotSessionForTest(OversqliteOperationStateStore(db).loadState().stagedSnapshotId)
                }
                val retained = OversqliteOperationStateStore(db).loadState()
                val sessions = server.snapshotSessionIdsForTest()
                server.beforeSnapshotCreateResponse = { db.execSQL("UPDATE _sync_attachment_state SET attachment_epoch=attachment_epoch+1") }
                assertIs<SnapshotFinalApplyGateException>(client.rebuild().exceptionOrNull())
                assertEquals(sessions, server.snapshotSessionIdsForTest(), "unpublished session must be retired")
                if (refresh) assertEquals(retained, OversqliteOperationStateStore(db).loadState())
                assertEquals(4L, scalarLong(db, "SELECT count(*) FROM nodes"))
                assertEquals(4L, scalarLong(db, "SELECT count(*) FROM _sync_row_state"))
            } finally { client.closeFromCoordinatedOwner() }
        }
    }

    private suspend fun verify(preserve: Boolean = false, reopen: Boolean = false, invalidate: Boolean = false, expire: Boolean = false) {
        val path = if (reopen) createTempSqliteNowTestDbPath("snapshot-staging-reopen") else null
        try { withSurvivorFixture(dbPath = path) { server, originalDb, http, seed ->
            if (preserve) {
                originalDb.execSQL("UPDATE nodes SET name='frozen L' WHERE id='P'")
                server.beforePushCommit = { throw CancellationException("retain intent") }
                assertFailsWith<CancellationException> { seed.pushPending().getOrThrow() }
                server.beforePushCommit = null
            }
            seed.closeFromCoordinatedOwner()
            var db = originalDb
            var client = newClient(db, http, syncTables = nodeTables, workUnits = 1, maxSnapshotChunkRows = 2)
            try {
                client.openAndConnect("user-1").getOrThrow()
                if (preserve) markSourceRecoveryRequired(db, SourceRecoveryReason.HISTORY_PRUNED)
                val before = baselineQualificationVector(db, listOf("nodes"))
                val creates = server.snapshotSessionCreateRequestCount
                val requestStart = server.snapshotChunkRequests.size
                val replacements = server.requestedSnapshotSourceReplacements.size
                var attempts = 0
                var stageId = ""
                var cursor = -1L to -1L
                while (true) {
                    val failure = client.rebuild().exceptionOrNull() ?: break
                    assertIs<SyncWorkLimitException>(failure)
                    assertTrue(++attempts < 20, "each invocation must advance")
                    val state = OversqliteOperationStateStore(db).loadState()
                    val staged = scalarLong(db, "SELECT count(*) FROM _sync_snapshot_stage")
                    assertEquals(staged, state.snapshotStagedRowCount)
                    assertTrue(staged > 0, "a work yield must preserve a committed staging chunk")
                    assertTrue(state.snapshotStagedByteCount > 0)
                    val root = scalarLong(db, "SELECT COALESCE((SELECT root_cursor FROM _sync_apply_plan),-1)")
                    if (stageId == state.stagedSnapshotId) assertTrue(staged > cursor.first || root > cursor.second,
                        "same-stage durable progress must advance")
                    stageId = state.stagedSnapshotId; cursor = staged to root
                    val after = baselineQualificationVector(db, listOf("nodes"))
                    for (table in listOf("nodes", "_sync_row_state", "_sync_relationships", "_sync_outbox_rows", "_sync_outbox_bundle"))
                        assertEquals(before.rows(table), after.rows(table), table)
                    assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    if (attempts == 1 && invalidate) db.execSQL("UPDATE _sync_conflict_state SET baseline_revision=baseline_revision+1")
                    if (attempts == 1 && expire) server.expireSnapshotSessionForTest(stageId)
                    if (reopen) {
                        client.closeFromCoordinatedOwner(); db.close()
                        db = newFileBackedDb(checkNotNull(path))
                        client = newClient(db, http, syncTables = nodeTables, workUnits = 1, maxSnapshotChunkRows = 2)
                        client.openAndConnect("user-1").getOrThrow()
                    }
                }
                assertTrue(attempts > 0)
                assertEquals(creates + if (invalidate || expire) 2 else 1, server.snapshotSessionCreateRequestCount)
                if (preserve) assertEquals(replacements + 1, server.requestedSnapshotSourceReplacements.size)
                val requests = server.snapshotChunkRequests.drop(requestStart)
                assertEquals(requests.distinct(), requests, "no committed cursor is downloaded twice")
                assertEquals(before.rows("nodes"), baselineQualificationVector(db, listOf("nodes")).rows("nodes"))
                assertEquals(4L, scalarLong(db, "SELECT count(*) FROM _sync_row_state"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                for (table in listOf("_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM $table"))
                assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(db).loadState())
            } finally { client.closeFromCoordinatedOwner(); if (db !== originalDb) db.close() }
        } } finally { if (path != null) deleteTempSqliteNowTestDbArtifacts(path) }
    }

    @Test fun initialRemoteReplaceResumesPartialChunks() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, http, seed ->
            seed.detach().getOrThrow(); seed.closeFromCoordinatedOwner()
            val client = newClient(db, http, syncTables = nodeTables, workUnits = 1, maxSnapshotChunkRows = 2)
            try {
                client.open().getOrThrow()
                val creates = server.snapshotSessionCreateRequestCount
                var attempts = 0
                while (true) {
                    val failure = client.attach("user-1").exceptionOrNull() ?: break
                    assertIs<SyncWorkLimitException>(failure)
                    assertTrue(++attempts < 20)
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM nodes"))
                    assertTrue(scalarLong(db, "SELECT count(*) FROM _sync_snapshot_stage") > 0)
                }
                assertEquals(creates + 1, server.snapshotSessionCreateRequestCount)
                assertEquals(4L, scalarLong(db, "SELECT count(*) FROM nodes"))
                assertEquals(4L, scalarLong(db, "SELECT count(*) FROM _sync_row_state"))
                assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(db).loadState())
            } finally { client.closeFromCoordinatedOwner() }
        }
    }

    @Test fun stageCancellationAndCloseFailureRollBackCursorPublication() = runOwnedWebDatabaseTest {
        for (cancel in listOf(true, false)) withSurvivorFixture { server, db, http, seed ->
            seed.closeFromCoordinatedOwner()
            val client = newClient(db, http, syncTables = nodeTables, maxSnapshotChunkRows = 2)
            try {
                client.openAndConnect("user-1").getOrThrow()
                val before = baselineQualificationVector(db, listOf("nodes"))
                val fault = if (cancel) CancellationException("cancel stage close") else IllegalStateException("stage close failed")
                client.setSnapshotStageStatementCloseForTest { it.close(); throw fault }
                val failure = try { client.rebuild().getOrThrow(); error("fault did not fire") } catch (error: Throwable) { error }
                assertSame(fault, generateSequence(failure) { it.cause }.last(),
                    "primary stage fault survives coroutine stack recovery")
                client.setSnapshotStageStatementCloseForTest(null)
                val state = OversqliteOperationStateStore(db).loadState()
                assertEquals(0L, state.snapshotStagedRowCount); assertEquals(0L, state.snapshotStagedByteCount)
                assertFalse(state.snapshotStageComplete)
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_snapshot_stage"))
                val after = baselineQualificationVector(db, listOf("nodes"))
                for (table in listOf("nodes", "_sync_row_state", "_sync_outbox_rows", "_sync_outbox_bundle"))
                    assertEquals(before.rows(table), after.rows(table))
                val creates = server.snapshotSessionCreateRequestCount
                client.rebuild().getOrThrow()
                assertEquals(creates + if (cancel) 0 else 1, server.snapshotSessionCreateRequestCount)
            } finally { client.closeFromCoordinatedOwner() }
        }
    }

    @Test fun inconsistentRetainedCursorFailsClosedBeforeFetching() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, http, seed ->
            seed.closeFromCoordinatedOwner()
            val client = newClient(db, http, syncTables = nodeTables, workUnits = 1, maxSnapshotChunkRows = 2)
            try {
                client.openAndConnect("user-1").getOrThrow()
                assertIs<SyncWorkLimitException>(client.rebuild().exceptionOrNull())
                val requests = server.snapshotChunkRequests.size
                db.execSQL("UPDATE _sync_operation_state SET snapshot_staged_row_count=1")
                assertIs<IllegalArgumentException>(client.rebuild().exceptionOrNull())
                assertEquals(requests, server.snapshotChunkRequests.size)
                assertEquals(4L, scalarLong(db, "SELECT count(*) FROM nodes"))
            } finally { client.closeFromCoordinatedOwner() }
        }
    }

    @Test fun emptySnapshotCompletesAboveTinyTarget() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, http, seed ->
            seed.closeFromCoordinatedOwner()
            server.addRemoteBundleForTest(listOf("P", "Q", "C", "D").map { BundleRow("main", "nodes", mapOf("id" to it), "DELETE", 2, null) })
            val client = newClient(db, http, syncTables = nodeTables, workUnits = 1)
            try {
                client.openAndConnect("user-1").getOrThrow(); client.rebuild().getOrThrow()
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM nodes"))
                assertEquals(OversqliteOperationState(), OversqliteOperationStateStore(db).loadState())
            } finally { client.closeFromCoordinatedOwner() }
        }
    }
}
