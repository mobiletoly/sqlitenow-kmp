package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.TransactionMode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertSame

internal class WriterReservationBindingTest : SurvivorTestSupport() {
    @Test fun oneWriterReusesBindingAndDropsOnlyItsTemporaryCharge() = fixture { db, state, workspace, token ->
        db.transaction(TransactionMode.IMMEDIATE) {
            val writer = StatementCache(db)
            try {
                val retained = WorkspaceCharge(20, 8192)
                val charged = workspace.add(applyUnit("binding", 0, ""), retained, decisionReservationBindingCharge)
                val binding = assertNotNull(workspace.resizeInWriter(state, token, charged, writer = writer))
                assertSame(binding, workspace.resizeInWriter(state, token, charged, binding, writer))
                assertEquals(charged, workspace.reserved(token))
                workspace.finishDecisionBindingInWriter(token, retained, writer)
                assertEquals(retained, workspace.reserved(token))
                assertFails { workspace.finishDecisionBindingInWriter(token, charged, writer) }
                assertEquals(retained, workspace.reserved(token))
            } finally { writer.close() }
        }
    }

    @Test fun differentAndClosedWritersCannotReuseBinding() = fixture { db, state, workspace, token ->
        db.transaction(TransactionMode.IMMEDIATE) {
            val writer = StatementCache(db); val competing = StatementCache(db)
            try {
                val charge = WorkspaceCharge(20, 8192)
                val binding = assertNotNull(workspace.resizeInWriter(state, token, charge, writer = writer))
                val before = scalarText(db, "SELECT workspace_context FROM _sync_operation_state")
                assertFails { workspace.resizeInWriter(state, token, charge, binding, competing) }
                writer.close()
                assertFails { workspace.resizeInWriter(state, token, charge, binding, writer) }
                assertEquals(before, scalarText(db, "SELECT workspace_context FROM _sync_operation_state"))
            } finally { writer.close(); competing.close() }
        }
    }

    @Test fun aNewWriterReconstructsChangedGuardFields() = fixture { db, state, workspace, token ->
        suspend fun binding() = db.transaction(TransactionMode.IMMEDIATE) {
            val writer = StatementCache(db)
            try { assertNotNull(workspace.resizeInWriter(state, token, WorkspaceCharge(20, 8192), writer = writer)).context }
            finally { writer.close() }
        }
        val before = binding()
        db.execSQL("UPDATE _sync_conflict_state SET baseline_revision=baseline_revision+1")
        assertNotEquals(before, binding())
    }

    @Test fun optionalBindingCapacityRefusalFallsBackToOriginalAdmission() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, http, _ ->
            val config = OversqliteConfig(1, nodeTables, "fixture", "main")
            val generation = OversqliteContractStore(db).readGeneration(1)
            val validated = SyncRuntimeInitializer(config, TableInfoCache()).validateConfig(db).withGeneration(generation)
            val state = RuntimeState(validated, AdmittedOversqliteContract("v1", generation, testWireLimits(), false),
                "user-1", scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
            val workspace = OversqliteWorkspace(db, config)
            val unit = applyUnit("binding-fallback", 0, "")
            val token = workspace.reserve(state, unit, WorkspaceCharge())
            val used = workspace.usage(unit)
            val bounded = config.copy(maxSyncWorkspaceEntries = used.entries + 192)
            val resources = OversqliteClientResourceOwner(db, http)
            val operations = OversqliteClientOperationController(bounded, resources, SnapshotDiagnosticsState()) {}
            val local = OversqliteLocalStore(db, json) { validated }
            val survivors = OversqliteApplySurvivors(db, local, bounded, operations, json)
            try {
                db.transaction(TransactionMode.IMMEDIATE) {
                    val writer = StatementCache(db)
                    try {
                        val result = survivors.reserveDecisionCopies(state, token, WorkspaceCharge(), WorkspaceCharge(160),
                            "main", "nodes", "{\"id\":\"P\"}", "P", writer = writer, memoizeBinding = true)
                        assertEquals(192L, result.held.entries)
                        assertEquals(null, result.binding)
                        assertEquals(1L, operations.workSliceDiagnostics.reservationBindingCacheFallbacks)
                        assertEquals(192L, workspace.reserved(token).entries)
                    } finally { writer.close() }
                }
            } finally { workspace.release(token); resources.closeFromCoordinatedOwner() }
            assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
        }
    }

    private fun fixture(block: suspend (dev.goquick.sqlitenow.core.SafeSQLiteConnection, RuntimeState, OversqliteWorkspace, String) -> Unit) =
        runOwnedWebDatabaseTest {
            withSurvivorFixture { _, db, _, _ ->
                val config = OversqliteConfig(1, nodeTables, "fixture", "main")
                val generation = OversqliteContractStore(db).readGeneration(1)
                val validated = SyncRuntimeInitializer(config, TableInfoCache()).validateConfig(db).withGeneration(generation)
                val state = RuntimeState(validated, AdmittedOversqliteContract("v1", generation, testWireLimits(), false),
                    "user-1", scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
                val workspace = OversqliteWorkspace(db, config)
                val token = workspace.reserve(state, applyUnit("binding", 0, ""), WorkspaceCharge())
                try { block(db, state, workspace, token) }
                finally { workspace.release(token) }
                assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
            }
        }
}
