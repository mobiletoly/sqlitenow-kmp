package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import kotlin.test.*

internal class SnapshotBootstrapResourceTest : SurvivorTestSupport() {
    @Test fun ordinaryOperationPersistencePreservesItsCleanupCursor() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, _, _ ->
            db.execSQL("UPDATE _sync_operation_state SET snapshot_cleanup_ordinal=37")
            val store = OversqliteOperationStateStore(db)
            assertEquals(OversqliteOperationState(), store.loadState())
            store.persistState(store.loadState().copy(kind = operationKindRemoteReplace, targetUserId = "fixture-owner"))
            assertEquals(37L, db.scalarLong("SELECT snapshot_cleanup_ordinal FROM _sync_operation_state"))
            assertEquals("", db.withPreparedStatement("SELECT bootstrap_cleanup_chain FROM _sync_operation_state") { require(it.step()); it.getText(0) })
        }
    }

    @Test fun fixedOwnersHaveExplicitFormatAndReopenCensus() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, _, _ ->
            assertEquals("CURRENT", inspectRetainedRuntime(db))
            assertEquals(4, bootstrapRetainedTableDdl.size)
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_bootstrap_state WHERE format_identity='typed-bootstrap-v1' AND phase='inactive'"))
            for (table in listOf("_sync_bootstrap_tables", "_sync_bootstrap_wire", "_sync_bootstrap_pages"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM $table"))
            assertEquals(generalSnapshotEngine, OversqliteOperationStateStore(db).loadState().snapshotEngine)
            assertEquals(0L, scalarLong(db, "SELECT bootstrap_revision_low+bootstrap_revision_high FROM _sync_apply_state"))
        }
    }

    @Test fun previousApplyLayoutIsRejectedBeforeAnyMutation() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, _, _ ->
            db.execSQL("DROP TABLE _sync_apply_state")
            db.execSQL("CREATE TABLE _sync_apply_state(singleton_key INTEGER NOT NULL PRIMARY KEY CHECK(singleton_key=1),apply_mode INTEGER NOT NULL DEFAULT 0)")
            db.execSQL("INSERT INTO _sync_apply_state VALUES(1,0)")
            val before = baselineQualificationVector(db, listOf("nodes", "_sync_apply_state", "_sync_row_state"))
            val error = assertFails { inspectRetainedRuntime(db) }
            assertTrue(error.message.orEmpty().contains("recreate this development database explicitly"))
            assertEquals(before, baselineQualificationVector(db, listOf("nodes", "_sync_apply_state", "_sync_row_state")))
        }
    }

    @Test fun operationRowOwnsEngineAndReferenceWithIdleDefaults() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, _, _ ->
            val store = OversqliteOperationStateStore(db)
            val original = store.loadState()
            assertEquals(0L, original.bootstrapPageCount)
            val typed = original.copy(stagedSnapshotId = "typed-stage", snapshotBundleSeq = 1,
                snapshotStageGuard = "a".repeat(64), snapshotEngine = typedSnapshotEngine,
                bootstrapReference = "b".repeat(64))
            store.persistState(typed)
            assertEquals(typed, store.loadState())
            assertFails { inspectRetainedRuntime(db) }
            assertFails { store.persistState(original.copy(snapshotEngine = typedSnapshotEngine, bootstrapReference = "b".repeat(64))) }
            assertFails { store.persistState(typed.copy(bootstrapReference = "bad")) }
            assertFails { store.persistState(typed.copy(snapshotEngine = generalSnapshotEngine)) }
            assertFails { store.persistState(typed.copy(bootstrapPageCount = 1)) }
            assertFails { store.persistState(original.copy(bootstrapPageCount = 1)) }
            store.persistState(original)
            assertEquals("CURRENT", inspectRetainedRuntime(db))
        }
    }

    @Test fun bulkBaselineInsertKeepsReservedRevisionWithoutRowMaintenance() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, _, _ ->
            val base = scalarLong(db, "SELECT baseline_revision FROM _sync_conflict_state")
            enterBulkFixture(db, base + 1)
            insertBaseline(db, "bulk", base + 1)
            assertEquals(base, scalarLong(db, "SELECT baseline_revision FROM _sync_conflict_state"))
            assertEquals(base + 1, scalarLong(db, "SELECT server_revision FROM _sync_row_state WHERE key_json='{\"id\":\"bulk\"}'"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_relationships WHERE child_key='{\"id\":\"bulk\"}'"))
            assertFails { insertBaseline(db, "outside-range", base + 2) }
            assertFails { db.execSQL("UPDATE _sync_row_state SET row_version=2 WHERE key_json='{\"id\":\"bulk\"}'") }
            assertFails { db.execSQL("DELETE FROM _sync_row_state WHERE key_json='{\"id\":\"bulk\"}'") }
            db.execSQL("UPDATE _sync_conflict_state SET baseline_revision=${base + 1}")
            exitBulkFixture(db)
            db.execSQL("UPDATE _sync_row_state SET row_version=2 WHERE key_json='{\"id\":\"bulk\"}'")
            assertEquals(base + 2, scalarLong(db, "SELECT baseline_revision FROM _sync_conflict_state"))
            assertEquals(base + 2, scalarLong(db, "SELECT server_revision FROM _sync_row_state WHERE key_json='{\"id\":\"bulk\"}'"))
            assertEquals("CURRENT", inspectRetainedRuntime(db))
        }
    }

    @Test fun bulkOwnerMismatchAndRollbackPreserveOrdinaryState() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, _, _ ->
            val base = scalarLong(db, "SELECT baseline_revision FROM _sync_conflict_state")
            val before = baselineQualificationVector(db, listOf("nodes", "_sync_row_state", "_sync_relationships", "_sync_apply_state", "_sync_bootstrap_state"))
            assertFailsWith<kotlinx.coroutines.CancellationException> {
                db.transaction(TransactionMode.IMMEDIATE) {
                    enterBulkFixture(db, base + 1)
                    db.execSQL("UPDATE _sync_apply_state SET bootstrap_operation='different-owner'")
                    val failure = assertFails { insertBaseline(db, "wrong-owner", base + 1) }
                    assertTrue(generateSequence(failure) { it.cause }.any { it.message.orEmpty().contains("owned range or target context") })
                    throw kotlinx.coroutines.CancellationException("rollback owned mode")
                }
            }
            assertEquals(before, baselineQualificationVector(db, listOf("nodes", "_sync_row_state", "_sync_relationships", "_sync_apply_state", "_sync_bootstrap_state")))
            assertEquals("CURRENT", inspectRetainedRuntime(db))
        }
    }

    @Test fun openingRejectsUnownedBootstrapContentsAndLeftoverMode() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, _, _ ->
            db.execSQL("UPDATE _sync_bootstrap_state SET phase='receiving',snapshot_id='unowned'")
            assertFails { inspectRetainedRuntime(db) }
            db.execSQL("UPDATE _sync_bootstrap_state SET phase='inactive',snapshot_id=''")
            db.execSQL("UPDATE _sync_apply_state SET baseline_mode='bootstrap',bootstrap_operation='unowned',bootstrap_revision_low=1,bootstrap_revision_high=1")
            assertFails { inspectRetainedRuntime(db) }
            db.execSQL("UPDATE _sync_apply_state SET baseline_mode='row',bootstrap_operation='',bootstrap_revision_low=0,bootstrap_revision_high=0")
            assertEquals("CURRENT", inspectRetainedRuntime(db))
        }
    }

    @Test fun metadataQuotaCountersHaveIndependentUtf8AuditAndNoRepair() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, _, _ ->
            val config = OversqliteConfig(1, nodeTables, "fixture", "main")
            val workspace = OversqliteWorkspace(db, config)
            assertEquals(WorkspaceCharge(1, 512), measureBootstrapMetadataCharge(db))
            workspace.reconcileRetainedOwners()
            val before = baselineQualificationVector(db, listOf("nodes", "_sync_operation_state", "_sync_bootstrap_state"))
            for (mutation in listOf("bootstrap_metadata_entries=2", "bootstrap_metadata_bytes=513")) {
                assertFailsWith<kotlinx.coroutines.CancellationException> {
                    db.transaction(TransactionMode.IMMEDIATE) {
                        db.execSQL("UPDATE _sync_operation_state SET $mutation")
                        assertFails { inspectRetainedRuntime(db) }
                        assertFails { workspace.reconcileRetainedOwners() }
                        assertEquals(WorkspaceCharge(1, 512), measureBootstrapMetadataCharge(db))
                        throw kotlinx.coroutines.CancellationException("rollback isolated counter mutation")
                    }
                }
                assertEquals(before, baselineQualificationVector(db, listOf("nodes", "_sync_operation_state", "_sync_bootstrap_state")))
            }
            db.transaction(TransactionMode.IMMEDIATE) {
                // Accounting-only fixture, not a valid descriptor/receipt.
                db.execSQL("UPDATE _sync_bootstrap_state SET descriptor_json='Ω🧩'")
                val measured = measureBootstrapMetadataCharge(db)
                assertEquals(WorkspaceCharge(1, 512 + 6 * 6), measured)
                assertFails { workspace.reconcileRetainedOwners() }
                assertEquals(512L, scalarLong(db, "SELECT bootstrap_metadata_bytes FROM _sync_operation_state"))
                db.execSQL("UPDATE _sync_operation_state SET bootstrap_metadata_bytes=${measured.bytes}")
                workspace.reconcileRetainedOwners()
                db.execSQL("UPDATE _sync_bootstrap_state SET descriptor_json=''")
                db.execSQL("UPDATE _sync_operation_state SET bootstrap_metadata_bytes=512")
            }
            assertEquals(before, baselineQualificationVector(db, listOf("nodes", "_sync_operation_state", "_sync_bootstrap_state")))
        }
    }

    // This isolates trigger ownership/range semantics; it is not a receipt/seal proof.
    private suspend fun enterBulkFixture(db: SafeSQLiteConnection, revision: Long) {
        db.execSQL("UPDATE _sync_operation_state SET staged_snapshot_id='guard-stage',snapshot_bundle_seq=1,snapshot_stage_guard='${"a".repeat(64)}',snapshot_engine='typed_bootstrap',bootstrap_reference='${"b".repeat(64)}'")
        db.execSQL("UPDATE _sync_bootstrap_state SET phase='sealed',snapshot_id='guard-stage',expected_manifest='{}'")
        db.execSQL("UPDATE _sync_apply_state SET apply_mode=1,baseline_mode='bootstrap',bootstrap_operation='guard-stage',bootstrap_target_context=(SELECT active_context_id FROM _sync_provenance_state WHERE singleton_key=1),bootstrap_revision_low=$revision,bootstrap_revision_high=$revision")
    }

    private suspend fun exitBulkFixture(db: SafeSQLiteConnection) {
        db.execSQL("UPDATE _sync_apply_state SET apply_mode=0,baseline_mode='row',bootstrap_operation='',bootstrap_target_context=0,bootstrap_revision_low=0,bootstrap_revision_high=0")
        db.execSQL("UPDATE _sync_bootstrap_state SET phase='inactive',snapshot_id='',expected_manifest=''")
        OversqliteOperationStateStore(db).persistState(OversqliteOperationState())
    }

    private suspend fun insertBaseline(db: SafeSQLiteConnection, key: String, revision: Long) {
        db.withPreparedStatement("""INSERT INTO _sync_row_state(schema_name,table_name,key_json,row_version,
 server_kind,server_payload,server_provenance,relationships_known,server_revision)
 VALUES('main','nodes',?,1,'live',json_object('id',json_object('storage','text','value',?),
 'name',json_object('storage','text','value','bulk'),'parent_id',json_object('storage','text','value','P')),
 ${baselineProvenanceSql("'snapshot'", "1")},1,?)""") {
            it.bindText(1, "{\"id\":\"$key\"}"); it.bindText(2, key); it.bindLong(3, revision); it.step()
        }
    }
}
