package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlin.test.*

internal class R1WorkspaceUsageTest : CrossTargetSyncTestSupport() {
    private val config = OversqliteConfig(1, emptyList(), "cached-workspace", "main")

    @Test fun eightShapesReadCurrentMetadataAndKeepValuesOutOfKeys() = runOwnedWebDatabaseTest {
        metadata { db ->
            val shapes = mutableListOf<String>()
            db.withStatementCache(StatementCacheOperations(prepare = { connection, sql -> shapes += sql; connection.prepare(sql) })) { cache ->
                suspend fun compare() {
                    for (abandoned in listOf(false, true)) for (apply in listOf(null, WorkspaceCharge(7, 11))) for (push in listOf(null, WorkspaceCharge(13, 17))) {
                        val workspace = OversqliteWorkspace(db, config, abandoned)
                        assertEquals(workspace.usage("uncached", apply, push), workspace.usage("cached", apply, push, cache))
                        assertEquals(workspace.usage("another-unit", apply?.copy(entries = 19), push?.copy(bytes = 29)),
                            workspace.usage("changed-unit", apply?.copy(entries = 19), push?.copy(bytes = 29), cache))
                    }
                    assertEquals(8, shapes.size); assertEquals(8, shapes.distinct().size)
                }
                compare()
                val workspace = OversqliteWorkspace(db, config)
                assertEquals(WorkspaceCharge(12, 3202), workspace.usage("u", statementCache = cache))
                db.execSQL("UPDATE _sync_apply_plan SET unit='é',guard='猫',preserve_schema='🙂',preserve_table='t',preserve_key='k'")
                db.execSQL("UPDATE _sync_push_plan SET unit='é',guard='猫',state='🙂'")
                assertEquals(WorkspaceCharge(12, 3286), workspace.usage("u", statementCache = cache))
                db.execSQL("UPDATE _sync_apply_plan SET row_cursor='é'")
                assertEquals(WorkspaceCharge(12, 3298), workspace.usage("u", statementCache = cache))
                db.execSQL("UPDATE _sync_operation_state SET reserved_entries=1,reserved_bytes=7,workspace_context='é'")
                compare()
                db.execSQL("DELETE FROM _sync_apply_plan"); db.execSQL("DELETE FROM _sync_push_plan")
                assertEquals(WorkspaceCharge(2, 555), workspace.usage("u", statementCache = cache))
                compare()
                db.execSQL("""INSERT INTO _sync_apply_plan(singleton_key,unit,guard,phase,root_cursor,visit_cursor,table_cursor,fk_cursor,entries,bytes,edge_entries,edge_bytes)
                    VALUES(1,'u','g','ready',0,0,0,0,2,10,3,20)""")
                db.execSQL("INSERT INTO _sync_push_plan(singleton_key,unit,guard,phase,state,entries,bytes) VALUES(1,'u','g','ready','{}',4,40)")
                assertEquals(WorkspaceCharge(13, 3221), workspace.usage("u", statementCache = cache))
                compare()
            }
        }
    }

    @Test fun cachedMissingNegativeAndOverflowKeepDiagnostics() = runOwnedWebDatabaseTest {
        metadata { db ->
            val workspace = OversqliteWorkspace(db, config)
            db.withStatementCache { cache ->
                assertEquals(WorkspaceCharge(12, 3202), workspace.usage("u", statementCache = cache))
                db.execSQL("UPDATE _sync_operation_state SET reserved_entries=${Long.MAX_VALUE}")
                db.execSQL("UPDATE _sync_apply_plan SET entries=-1")
                val overflow = assertFailsWith<SyncWorkspaceLimitException> { workspace.usage("u", statementCache = cache) }
                assertEquals("u", overflow.unit); assertEquals("entries", overflow.dimension)
                assertEquals(1L, overflow.used); assertEquals(Long.MAX_VALUE, overflow.reserved); assertFalse(overflow.resumable)
                db.execSQL("UPDATE _sync_operation_state SET reserved_entries=0")
                assertEquals("invalid negative workspace entries metadata", assertFailsWith<IllegalArgumentException> { workspace.usage("u", statementCache = cache) }.message)
                db.execSQL("UPDATE _sync_apply_plan SET entries=0,bytes=${Long.MAX_VALUE}")
                val bytes = assertFailsWith<SyncWorkspaceLimitException> { workspace.usage("u", statementCache = cache) }
                assertEquals("bytes", bytes.dimension); assertEquals(Long.MAX_VALUE, bytes.used); assertEquals(20L, bytes.reserved)
                db.execSQL("DELETE FROM _sync_operation_state")
                assertEquals("missing workspace operation owner", assertFailsWith<IllegalStateException> { workspace.usage("u", statementCache = cache) }.message)
                db.execSQL("INSERT INTO _sync_operation_state(singleton_key) VALUES(1)")
                db.execSQL("UPDATE _sync_apply_plan SET bytes=10")
                assertEquals(workspace.usage("u"), workspace.usage("u", statementCache = cache), "reusable after accounting errors")
            }
        }
    }

    @Test fun cacheOwnershipDoesNotCrossConnectionsOrWriterTransactions() = runOwnedWebDatabaseTest {
        repeat(2) {
            metadata { db ->
                var preparations = 0
                val operations = StatementCacheOperations(prepare = { connection, sql -> preparations++; connection.prepare(sql) })
                repeat(2) { invocation ->
                    db.transaction {
                        db.withStatementCache(operations) { cache ->
                            repeat(3) { assertEquals(WorkspaceCharge(12, 3202), OversqliteWorkspace(db, config).usage("u", statementCache = cache)) }
                        }
                    }
                    assertEquals(invocation + 1, preparations)
                }
            }
        }
    }

    private suspend fun metadata(block: suspend (SafeSQLiteConnection) -> Unit) {
        val db = newDb()
        try {
            db.execSQL(oversqliteRetainedTableDdl.getValue("_sync_operation_state")); db.execSQL(applyPlanDdl); db.execSQL(pushPlanDdl)
            db.execSQL("INSERT INTO _sync_operation_state(singleton_key) VALUES(1)")
            db.execSQL("""INSERT INTO _sync_apply_plan(singleton_key,unit,guard,phase,root_cursor,visit_cursor,table_cursor,fk_cursor,entries,bytes,edge_entries,edge_bytes)
                VALUES(1,'u','g','ready',0,0,0,0,2,10,3,20)""")
            db.execSQL("INSERT INTO _sync_push_plan(singleton_key,unit,guard,phase,state,entries,bytes) VALUES(1,'u','g','ready','{}',4,40)")
            block(db)
        } finally { withContext(NonCancellable) { db.close() } }
    }
}
