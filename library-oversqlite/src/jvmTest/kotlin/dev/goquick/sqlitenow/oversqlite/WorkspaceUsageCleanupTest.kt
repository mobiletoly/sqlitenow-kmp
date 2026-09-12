package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlin.test.*

internal class WorkspaceUsageCleanupTest : CrossTargetSyncTestSupport() {
    private val config = OversqliteConfig(1, emptyList(), "workspace-cleanup", "main")
    private val unit = applyUnit("workspace-cleanup", 0, "")

    @Test fun applyOverflowRemainsPrimaryWhenCloseFails() = runBlocking {
        withCloseFailure("UPDATE _sync_apply_plan SET entries=${Long.MAX_VALUE},edge_entries=1") { observed, fault ->
            assertOverflow(observed, Long.MAX_VALUE, 1)
            assertCloseSuppressed(observed, fault)
        }
    }

    @Test fun invalidApplyMetadataRemainsPrimaryWhenCloseFails() = runBlocking {
        withCloseFailure("UPDATE _sync_apply_plan SET entries=-1") { observed, fault ->
            assertIs<IllegalArgumentException>(observed)
            assertEquals("invalid negative workspace entries metadata", observed.message)
            assertCloseSuppressed(observed, fault)
        }
    }

    @Test fun pushOverflowRemainsPrimaryWhenCloseFails() = runBlocking {
        withCloseFailure("UPDATE _sync_push_plan SET entries=${Long.MAX_VALUE}") { observed, fault ->
            assertOverflow(observed, Long.MAX_VALUE, 1)
            assertCloseSuppressed(observed, fault)
        }
    }

    @Test fun successfulAccountingStillReportsCloseFailure() = runBlocking {
        withCloseFailure(null) { observed, fault ->
            assertSame(fault.closeFailure, originalFailure(observed))
            assertTrue(observed.suppressedExceptions.isEmpty())
        }
    }

    private fun assertOverflow(observed: Throwable, used: Long, reserved: Long) {
        val error = assertIs<SyncWorkspaceLimitException>(observed)
        assertEquals(unit, error.unit)
        assertEquals("entries", error.dimension)
        assertEquals(config.maxSyncWorkspaceEntries, error.limit)
        assertEquals(used, error.used)
        assertEquals(reserved, error.reserved)
        assertFalse(error.resumable)
    }

    private fun assertCloseSuppressed(observed: Throwable, fault: JvmStatementCleanupFault) {
        assertSame(fault.closeFailure, originalFailure(originalFailure(observed).suppressedExceptions.single()))
    }

    private fun originalFailure(error: Throwable): Throwable = generateSequence(error) { it.cause }.last()

    private suspend fun withCloseFailure(
        mutation: String?,
        checkFailure: (Throwable, JvmStatementCleanupFault) -> Unit,
    ) {
        val db = newDb()
        var primaryFailure: Throwable? = null
        try {
            db.execSQL(oversqliteRetainedTableDdl.getValue("_sync_operation_state"))
            db.execSQL(applyPlanDdl)
            db.execSQL(pushPlanDdl)
            db.execSQL("INSERT INTO _sync_operation_state(singleton_key) VALUES(1)")
            db.execSQL("""INSERT INTO _sync_apply_plan
                (singleton_key,unit,guard,phase,root_cursor,visit_cursor,table_cursor,fk_cursor,entries,bytes)
                VALUES(1,'u','g','ready',0,0,0,0,0,0)""")
            db.execSQL("""INSERT INTO _sync_push_plan(singleton_key,unit,guard,phase,state,entries,bytes)
                VALUES(1,'u','g','ready','{}',0,0)""")
            mutation?.let { db.execSQL(it) }
            val fault = JvmStatementCleanupFault.install(db, JvmCleanupFailureMode.CLOSE_ONCE) { sql ->
                sql.startsWith("SELECT ") && sql.contains("FROM _sync_operation_state")
            }
            val observed = assertFailsWith<Throwable> { OversqliteWorkspace(db, config).usage(unit) }
            fault.assertFailureGraph(observed)
            assertEquals(1, fault.prepareCalls, "one joined workspace query")
            assertEquals(1, fault.rawConnectionCloseCalls, "fatal cleanup disposes the connection")
            assertEquals(0, fault.allStatementBindOrStepCallsAfterFailure)
            checkFailure(observed, fault)
        } catch (failure: Throwable) {
            primaryFailure = failure
            throw failure
        } finally {
            try { withContext(NonCancellable) { db.close() } }
            catch (cleanup: Throwable) {
                if (primaryFailure == null) throw cleanup
                attachSuppressedOnce(primaryFailure, cleanup)
            }
        }
    }
}
