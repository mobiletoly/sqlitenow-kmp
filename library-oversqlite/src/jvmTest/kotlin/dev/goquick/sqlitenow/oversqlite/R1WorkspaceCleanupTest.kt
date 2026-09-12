package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteStatement
import kotlinx.coroutines.*
import kotlin.test.*

/** Synchronous backend evidence, separate from actual worker cleanup faults. */
internal class R1WorkspaceCleanupTest : CrossTargetSyncTestSupport() {
    @Test fun successReportsSynchronousCleanupFailure() = cases(null)
    @Test fun missingOwnerStaysPrimaryWithSynchronousCleanupFailure() = cases("DELETE FROM _sync_operation_state")
    @Test fun negativeMetadataStaysPrimaryWithSynchronousCleanupFailure() = cases("UPDATE _sync_apply_plan SET entries=-1")
    @Test fun overflowStaysPrimaryWithSynchronousCleanupFailure() = cases("UPDATE _sync_operation_state SET reserved_entries=${Long.MAX_VALUE}")
    @Test fun callerCancellationStaysPrimaryWithSynchronousCleanupFailure() = cases(null, cancellation = true)

    private fun cases(mutation: String?, cancellation: Boolean = false) = runBlocking {
        for (mode in JvmCleanupFailureMode.entries) {
            val db = newDb()
            try {
                db.execSQL(oversqliteRetainedTableDdl.getValue("_sync_operation_state")); db.execSQL(applyPlanDdl); db.execSQL(pushPlanDdl)
                db.execSQL("INSERT INTO _sync_operation_state(singleton_key) VALUES(1)")
                db.execSQL("INSERT INTO _sync_apply_plan(singleton_key,unit,guard,phase,root_cursor,visit_cursor,table_cursor,fk_cursor,entries,bytes) VALUES(1,'u','g','ready',0,0,0,0,0,0)")
                mutation?.let { db.execSQL(it) }
                val fault = JvmStatementCleanupFault.install(db, mode) { it.startsWith("SELECT ") && it.contains("FROM _sync_operation_state o") }
                var failure: Throwable? = null
                supervisorScope {
                    launch {
                        val caller = currentCoroutineContext().job
                        val operations = StatementCacheOperations(prepare = { connection, sql ->
                            val statement = connection.prepare(sql)
                            if (!cancellation) statement else object : SQLiteStatement by statement {
                                override fun step(): Boolean {
                                    val row = statement.step()
                                    caller.cancel(CancellationException("r1-caller"))
                                    return row
                                }
                            }
                        })
                        try { db.withStatementCache(operations) { cache ->
                            OversqliteWorkspace(db, OversqliteConfig(1, emptyList(), "cleanup", "main")).usage("u", statementCache = cache)
                        } } catch (observed: Throwable) { failure = observed }
                    }.join()
                }
                val observed = assertNotNull(failure)
                if (cancellation) assertIs<CancellationException>(observed)
                fun graph(error: Throwable, seen: MutableList<Throwable> = mutableListOf()): List<Throwable> {
                    if (seen.any { it === error }) return emptyList()
                    seen += error; return listOf(error) + listOfNotNull(error.cause).flatMap { graph(it, seen) } + error.suppressedExceptions.flatMap { graph(it, seen) }
                }
                val all = graph(observed)
                if (mutation != null) {
                    when {
                        mutation.startsWith("DELETE") -> assertEquals("missing workspace operation owner", observed.message)
                        mutation.endsWith("=-1") -> assertEquals("invalid negative workspace entries metadata", observed.message)
                        else -> { val overflow = assertIs<SyncWorkspaceLimitException>(observed); assertEquals(1L, overflow.used); assertEquals(Long.MAX_VALUE, overflow.reserved) }
                    }
                }
                val first = if (mode == JvmCleanupFailureMode.CLOSE_ONCE) fault.closeFailure else if (mode == JvmCleanupFailureMode.CLEAR_ONLY) fault.clearFailure else fault.resetFailure
                assertTrue(all.any { it === first }); assertEquals(1, all.count { it === first })
                if (mode == JvmCleanupFailureMode.RESET_THEN_CLEAR) assertEquals(listOf(fault.clearFailure), fault.resetFailure.suppressedExceptions)
                all.forEach { error -> error.suppressedExceptions.forEachIndexed { i, item -> assertTrue(error.suppressedExceptions.take(i).none { it === item }) } }
                assertEquals(1, fault.prepareCalls); assertEquals(1, fault.rawConnectionCloseCalls)
                assertEquals(0, fault.allStatementBindOrStepCallsAfterFailure)
            } finally { db.close() }
        }
    }
}
