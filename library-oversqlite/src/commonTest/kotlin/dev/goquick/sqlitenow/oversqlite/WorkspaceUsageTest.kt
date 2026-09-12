package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlin.test.*

internal class WorkspaceUsageTest : CrossTargetSyncTestSupport() {
    private val config = OversqliteConfig(1, emptyList(), "workspace-fixture", "main")
    private val unit = applyUnit("workspace-usage", 0, "")

    @Test fun absentAndPresentOwnersHaveExactCharges() = runOwnedWebDatabaseTest {
        withMetadata { db, workspace ->
            assertEquals(WorkspaceCharge(1, 536), workspace.usage(unit))
            insertApply(db)
            assertEquals(WorkspaceCharge(7, 1090), workspace.usage(unit))
            insertPush(db)
            assertEquals(WorkspaceCharge(12, 3202), workspace.usage(unit))
            db.execSQL("DELETE FROM _sync_apply_plan")
            assertEquals(WorkspaceCharge(6, 2648), workspace.usage(unit))
            db.execSQL("UPDATE _sync_push_plan SET entries=0,bytes=0")
            assertEquals(WorkspaceCharge(2, 2608), workspace.usage(unit), "present empty plan retains its owner charge")
            db.execSQL("DELETE FROM _sync_push_plan")
            insertApply(db)
            db.execSQL("UPDATE _sync_apply_plan SET entries=0,bytes=0,edge_entries=0,edge_bytes=0")
            assertEquals(WorkspaceCharge(2, 1060), workspace.usage(unit))
        }
    }

    @Test fun overridesBypassStoredOwnersIncludingOverflow() = runOwnedWebDatabaseTest {
        withMetadata { db, workspace ->
            insertApply(db)
            insertPush(db)
            assertEquals(WorkspaceCharge(8, 2698), workspace.usage(unit, apply = WorkspaceCharge(2, 50)))
            assertEquals(WorkspaceCharge(10, 1150), workspace.usage(unit, push = WorkspaceCharge(3, 60)))
            assertEquals(WorkspaceCharge(6, 646), workspace.usage(unit, WorkspaceCharge(2, 50), WorkspaceCharge(3, 60)))
            db.execSQL("UPDATE _sync_apply_plan SET entries=${Long.MAX_VALUE},bytes=${Long.MAX_VALUE}")
            assertEquals(WorkspaceCharge(6, 2648), workspace.usage(unit, apply = WorkspaceCharge()))
            db.execSQL("UPDATE _sync_push_plan SET entries=${Long.MAX_VALUE},bytes=${Long.MAX_VALUE}")
            assertEquals(WorkspaceCharge(1, 536), workspace.usage(unit, WorkspaceCharge(), WorkspaceCharge()))
            // A supplied charge must not even require the corresponding table.
            db.execSQL("DROP TABLE _sync_apply_plan")
            db.execSQL("DROP TABLE _sync_push_plan")
            assertEquals(WorkspaceCharge(6, 646), workspace.usage(unit, WorkspaceCharge(2, 50), WorkspaceCharge(3, 60)))
        }
    }

    @Test fun abandonedReservationsExcludeOnlyReservationAccounting() = runOwnedWebDatabaseTest {
        withMetadata { db, workspace ->
            db.execSQL("""UPDATE _sync_operation_state SET workspace_context='é',target_user_id='猫',reason='🙂',
                reserved_entries=2,reserved_bytes=11,snapshot_stage_entries=3,snapshot_stage_bytes=13""")
            assertEquals(WorkspaceCharge(6, 614), workspace.usage(unit))
            val abandoned = OversqliteWorkspace(db, config, abandonedReservations = true)
            assertEquals(WorkspaceCharge(4, 591), abandoned.usage(unit))
            insertApply(db)
            insertPush(db)
            assertEquals(WorkspaceCharge(15, 3257), abandoned.usage(unit))
        }
    }

    @Test fun currentUtf8MetadataAndNullableCursorAreMeasuredEachTime() = runOwnedWebDatabaseTest {
        withMetadata { db, workspace ->
            insertApply(db)
            insertPush(db)
            db.execSQL("UPDATE _sync_apply_plan SET unit='é',guard='猫',preserve_schema='🙂',preserve_table='t',preserve_key='k'")
            db.execSQL("UPDATE _sync_push_plan SET unit='é',guard='猫',state='🙂'")
            assertEquals(WorkspaceCharge(12, 3286), workspace.usage(unit))
            db.execSQL("UPDATE _sync_apply_plan SET row_cursor='é'")
            assertEquals(WorkspaceCharge(12, 3298), workspace.usage(unit))
            db.execSQL("UPDATE _sync_operation_state SET reserved_entries=1,reserved_bytes=7")
            assertEquals(WorkspaceCharge(13, 3305), workspace.usage(unit))
            db.execSQL("DELETE FROM _sync_apply_plan")
            db.execSQL("DELETE FROM _sync_push_plan")
            assertEquals(WorkspaceCharge(2, 543), workspace.usage(unit))
        }
    }

    @Test fun missingOperationOwnerFailsEvenWithOverrides() = runOwnedWebDatabaseTest {
        withMetadata { db, workspace ->
            insertApply(db)
            insertPush(db)
            db.execSQL("DELETE FROM _sync_operation_state")
            for (override in listOf(null, WorkspaceCharge())) {
                val failure = assertFailsWith<IllegalStateException> { workspace.usage(unit, override, override) }
                assertEquals("missing workspace operation owner", failure.message)
            }
        }
    }

    @Test fun limitsAndOverflowKeepDiagnosticsAndPrecedence() = runOwnedWebDatabaseTest {
        withMetadata { db, workspace ->
            insertApply(db)
            insertPush(db)
            for (delta in listOf(-1L, 0L, 1L)) {
                val entries = OversqliteWorkspace(db, config.copy(maxSyncWorkspaceEntries = 12 + delta))
                val bytes = OversqliteWorkspace(db, config.copy(maxSyncWorkspaceBytes = 3202 + delta))
                if (delta < 0) {
                    assertLimit(assertFailsWith { entries.check(unit) }, "entries", 11, 12, 0)
                    assertLimit(assertFailsWith { bytes.check(unit) }, "bytes", 3201, 3202, 0)
                } else {
                    entries.check(unit)
                    bytes.check(unit)
                }
            }
            db.execSQL("UPDATE _sync_operation_state SET reserved_entries=${Long.MAX_VALUE}")
            db.execSQL("UPDATE _sync_apply_plan SET entries=-1")
            assertLimit(assertFailsWith { workspace.usage(unit) }, "entries", config.maxSyncWorkspaceEntries, 1, Long.MAX_VALUE)
            db.execSQL("UPDATE _sync_operation_state SET reserved_entries=0")
            assertEquals("invalid negative workspace entries metadata", assertFailsWith<IllegalArgumentException> { workspace.usage(unit) }.message)
            db.execSQL("UPDATE _sync_apply_plan SET entries=${Long.MAX_VALUE}")
            assertLimit(assertFailsWith { workspace.usage(unit) }, "entries", config.maxSyncWorkspaceEntries, Long.MAX_VALUE, 3)
            db.execSQL("UPDATE _sync_apply_plan SET entries=0,bytes=${Long.MAX_VALUE}")
            assertLimit(assertFailsWith { workspace.usage(unit) }, "bytes", config.maxSyncWorkspaceBytes, Long.MAX_VALUE, 20)
            db.execSQL("DELETE FROM _sync_apply_plan")
            db.execSQL("UPDATE _sync_push_plan SET entries=${Long.MAX_VALUE}")
            assertLimit(assertFailsWith { workspace.usage(unit) }, "entries", config.maxSyncWorkspaceEntries, Long.MAX_VALUE, 1)
            db.execSQL("UPDATE _sync_push_plan SET entries=0,bytes=${Long.MAX_VALUE}")
            assertLimit(assertFailsWith { workspace.usage(unit) }, "bytes", config.maxSyncWorkspaceBytes, Long.MAX_VALUE, 2072)
            db.execSQL("DELETE FROM _sync_push_plan")
            assertEquals(WorkspaceCharge(1, 536), workspace.usage(unit), "connection remains usable after failures")
        }
    }

    private fun assertLimit(error: SyncWorkspaceLimitException, dimension: String, limit: Long, used: Long, reserved: Long) {
        assertEquals(unit, error.unit)
        assertEquals(dimension, error.dimension)
        assertEquals(limit, error.limit)
        assertEquals(used, error.used)
        assertEquals(reserved, error.reserved)
        assertFalse(error.resumable)
    }

    private suspend fun insertApply(db: SafeSQLiteConnection) = db.execSQL("""INSERT INTO _sync_apply_plan
        (singleton_key,unit,guard,phase,root_cursor,visit_cursor,table_cursor,fk_cursor,row_cursor,entries,bytes,edge_entries,edge_bytes)
        VALUES(1,'u','g','ready',0,0,0,0,NULL,2,10,3,20)""")

    private suspend fun insertPush(db: SafeSQLiteConnection) = db.execSQL("""INSERT INTO _sync_push_plan
        (singleton_key,unit,guard,phase,state,entries,bytes) VALUES(1,'u','g','ready','{}',4,40)""")

    private suspend fun withMetadata(block: suspend (SafeSQLiteConnection, OversqliteWorkspace) -> Unit) {
        val db = newDb()
        try {
            db.execSQL(oversqliteRetainedTableDdl.getValue("_sync_operation_state"))
            db.execSQL(applyPlanDdl)
            db.execSQL(pushPlanDdl)
            db.execSQL("INSERT INTO _sync_operation_state(singleton_key) VALUES(1)")
            block(db, OversqliteWorkspace(db, config))
        } finally { withContext(NonCancellable) { db.close() } }
    }
}
