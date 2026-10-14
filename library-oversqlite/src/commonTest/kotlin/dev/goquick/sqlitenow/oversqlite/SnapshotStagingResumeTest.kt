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
                    interruptAfterOneChunk(client)
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
                    client.rebuild().getOrThrow()
                    assertTrue(client.workSliceDiagnostics().workSlices > 0)
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
                    interruptAfterOneChunk(client)
                    server.expireSnapshotSessionForTest(OversqliteOperationStateStore(db).loadState().stagedSnapshotId)
                }
                val retained = OversqliteOperationStateStore(db).loadState()
                val sessions = server.snapshotSessionIdsForTest()
                server.beforeSnapshotCreateResponse = { db.execSQL("UPDATE _sync_attachment_state SET attachment_epoch=attachment_epoch+1") }
                // Retirement has a real cleanup deadline. Keep it off runTest's
                // virtual clock while MockEngine dispatches on a real worker.
                assertIs<SnapshotFinalApplyGateException>(kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.Default) {
                    client.rebuild().exceptionOrNull()
                })
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
                val interrupted = reopen || invalidate || expire
                if (interrupted) {
                    interruptAfterOneChunk(client)
                    val state = OversqliteOperationStateStore(db).loadState()
                    val staged = scalarLong(db, "SELECT count(*) FROM _sync_snapshot_stage")
                    assertEquals(staged, state.snapshotStagedRowCount)
                    assertTrue(staged > 0 && state.snapshotStagedByteCount > 0)
                    val after = baselineQualificationVector(db, listOf("nodes"))
                    for (table in listOf("nodes", "_sync_row_state", "_sync_relationships", "_sync_outbox_rows", "_sync_outbox_bundle"))
                        assertEquals(before.rows(table), after.rows(table), table)
                    assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    if (invalidate) db.execSQL("UPDATE _sync_conflict_state SET baseline_revision=baseline_revision+1")
                    if (expire) server.expireSnapshotSessionForTest(state.stagedSnapshotId)
                    if (reopen) {
                        client.closeFromCoordinatedOwner(); db.close()
                        db = newFileBackedDb(checkNotNull(path))
                        client = newClient(db, http, syncTables = nodeTables, workUnits = 1, maxSnapshotChunkRows = 2)
                        client.openAndConnect("user-1").getOrThrow()
                    }
                }
                client.rebuild().getOrThrow()
                assertTrue(client.workSliceDiagnostics().workSlices > 0, "tiny-target operation actually crosses slices")
                assertEquals(creates + if (invalidate || expire) 2 else 1, server.snapshotSessionCreateRequestCount)
                if (preserve) assertEquals(replacements + 1, server.requestedSnapshotSourceReplacements.size)
                val requests = server.snapshotChunkRequests.drop(requestStart)
                val committedRequests = if (interrupted) requests.filterIndexed { index, _ -> index != 1 } else requests
                assertEquals(committedRequests.distinct(), committedRequests, "only the explicitly cancelled, uncommitted chunk may repeat")
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
                client.attach("user-1").getOrThrow()
                assertTrue(client.workSliceDiagnostics().workSlices > 0)
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
                val failure = try {
                    kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.Default) { client.rebuild().getOrThrow() }
                    error("fault did not fire")
                } catch (error: Throwable) { error }
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
                kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.Default) { client.rebuild().getOrThrow() }
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
                interruptAfterOneChunk(client)
                val requests = server.snapshotChunkRequests.size
                db.execSQL("UPDATE _sync_operation_state SET snapshot_staged_row_count=1")
                assertIs<IllegalArgumentException>(client.rebuild().exceptionOrNull())
                assertEquals(requests, server.snapshotChunkRequests.size)
                assertEquals(4L, scalarLong(db, "SELECT count(*) FROM nodes"))
            } finally { client.closeFromCoordinatedOwner() }
        }
    }

    private suspend fun interruptAfterOneChunk(client: DefaultOversqliteClient) {
        var closes = 0
        client.setSnapshotStageStatementCloseForTest {
            it.close()
            if (++closes == 2) throw CancellationException("interrupt after one durable snapshot chunk")
        }
        try {
            assertFailsWith<CancellationException> { client.rebuild().getOrThrow() }
        } finally {
            client.setSnapshotStageStatementCloseForTest(null)
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
