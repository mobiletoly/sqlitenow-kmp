package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.CancellationException
import kotlin.test.*

internal class SnapshotChunkStatementReuseTest : SurvivorTestSupport() {
    @Test fun workspaceHandlesCloseAcrossInternalChunkSlices() = runOwnedWebDatabaseTest { chunkCase() }
    @Test fun workspacePrepareFailureRollsBackChunk() = runOwnedWebDatabaseTest { chunkCase(Point.PREPARE) }
    @Test fun workspaceResetFailureRollsBackChunk() = runOwnedWebDatabaseTest { chunkCase(Point.RESET) }
    @Test fun workspaceClearFailureRollsBackChunk() = runOwnedWebDatabaseTest { chunkCase(Point.CLEAR) }
    @Test fun workspaceCloseFailureRollsBackChunk() = runOwnedWebDatabaseTest { chunkCase(Point.CLOSE) }
    @Test fun insertionCloseFailureRemainsPrimary() = runOwnedWebDatabaseTest { chunkCase(Point.CLOSE, primary = true) }
    @Test fun cancellationClosesWorkspaceBeforeReturning() = runOwnedWebDatabaseTest { chunkCase(primary = true, cancel = true) }

    private enum class Point { PREPARE, RESET, CLEAR, CLOSE }
    // Keep injected failures identifiable across coroutine stack recovery.
    private class ChunkStatementFailure(val point: String) : RuntimeException("$point failed")

    private suspend fun chunkCase(point: Point? = null, primary: Boolean = false, cancel: Boolean = false) {
        val path = createTempSqliteNowTestDbPath("snapshot-chunk-statements")
        try { withSurvivorFixture(dbPath = path) { _, db, http, seed ->
            seed.closeFromCoordinatedOwner()
            val client = newClient(db, http, syncTables = nodeTables, workUnits = 1, maxSnapshotChunkRows = 2)
            client.openAndConnect("user-1").getOrThrow()
            val before = baselineQualificationVector(db, listOf("nodes"))
            val fault = ChunkStatementFailure("workspace $point")
            val first = if (cancel) CancellationException("cancel insertion close") else ChunkStatementFailure("insertion close")
            val field = SafeSQLiteConnection::class.java.getDeclaredField("ref").also { it.isAccessible = true }
            val delegate = db.withExclusiveAccess { field.get(db) as SQLiteConnection }
            var injected = false
            var chunkInsertionPrepared = false
            var liveChunkInsertions = 0
            val resetCountsAtClose = mutableListOf<Int>()
            var liveWorkspace = 0
            val wrapper = object : SQLiteConnection by delegate {
                override fun prepare(sql: String): SQLiteStatement {
                    val insertion = sql.startsWith("INSERT INTO _sync_snapshot_stage (")
                    val workspace = liveChunkInsertions > 0 && sql.startsWith("SELECT o.reserved_entries") && sql.contains("LEFT JOIN _sync_apply_plan")
                    if (workspace && chunkInsertionPrepared && point == Point.PREPARE && !injected) { injected = true; throw fault }
                    val statement = delegate.prepare(sql)
                    if (insertion) {
                        chunkInsertionPrepared = true
                        liveChunkInsertions++
                        var closed = false
                        return object : SQLiteStatement by statement {
                            override fun close() {
                                statement.close()
                                if (!closed) { closed = true; liveChunkInsertions-- }
                            }
                        }
                    }
                    if (!workspace) return statement
                    var resets = 0
                    var closed = false
                    return object : SQLiteStatement by statement {
                        override fun reset() {
                            statement.reset()
                            if (resets++ == 0) liveWorkspace++
                            if (point == Point.RESET && !injected) { injected = true; throw fault }
                        }
                        override fun clearBindings() {
                            statement.clearBindings()
                            if (resets > 0 && point == Point.CLEAR && !injected) { injected = true; throw fault }
                        }
                        override fun close() {
                            statement.close()
                            if (resets > 0 && !closed) {
                                closed = true
                                liveWorkspace--
                                resetCountsAtClose += resets
                                if (point == Point.CLOSE && !injected) { injected = true; throw fault }
                            }
                        }
                    }
                }
            }
            db.withExclusiveAccess { field.set(db, wrapper) }
            try {
                if (primary) client.setSnapshotStageStatementCloseForTest { it.close(); throw first }
                val failure = try { client.rebuild().getOrThrow(); null } catch (error: Throwable) { error }
                client.setSnapshotStageStatementCloseForTest(null)
                assertTrue(chunkInsertionPrepared, "the fault must exercise chunk staging")
                assertEquals(0, liveChunkInsertions, "chunk insertion handle must close even when workspace preparation fails")
                assertEquals(0, liveWorkspace, "chunk workspace handles cannot cross return/yield")
                assertEquals(if (point == Point.PREPARE) 0 else if (point == null && !primary) 2 else 1, resetCountsAtClose.size,
                    "only successfully prepared workspace handles require cleanup")
                if (point == null && !primary) {
                    assertNull(failure)
                    assertEquals(listOf(2, 2), resetCountsAtClose, "each chunk releases its two-row workspace checks")
                    assertEquals(0L, OversqliteOperationStateStore(db).loadState().snapshotStagedRowCount)
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_snapshot_stage"))
                    assertEquals(4L, scalarLong(db, "SELECT count(*) FROM nodes"))
                } else {
                    assertNotNull(failure)
                    val expected = if (primary) first else fault
                    assertTrue(contains(failure, expected), "original failure remains in the cause graph")
                    if (point != null) {
                        assertTrue(injected, "workspace cleanup fault must be reached")
                        assertTrue(contains(failure, fault), "secondary fault must remain attached: ${failure.stackTraceToString()}")
                    }
                }
                // Raw-driver cleanup faults revoke the old connection. Prove durability
                // through a fresh physical connection rather than querying the poisoned owner.
                val reopened = newFileBackedDb(path)
                try {
                    if (point != null || primary) {
                        val retained = OversqliteOperationStateStore(reopened).loadState()
                        assertEquals(0L, retained.snapshotStagedRowCount)
                        assertEquals(0L, retained.snapshotStagedByteCount)
                        assertFalse(retained.snapshotStageComplete)
                        assertEquals(0L, scalarLong(reopened, "SELECT count(*) FROM _sync_snapshot_stage"))
                    }
                    val after = baselineQualificationVector(reopened, listOf("nodes"))
                    for (table in listOf("nodes", "_sync_outbox_rows", "_sync_outbox_bundle"))
                        assertEquals(before.rows(table), after.rows(table), table)
                    if (point == null && !primary) {
                        val replacementFields = setOf("server_provenance", "server_revision", "updated_at")
                        assertEquals(before.rows("_sync_row_state").map { it.filterKeys { key -> key !in replacementFields } },
                            after.rows("_sync_row_state").map { it.filterKeys { key -> key !in replacementFields } },
                            "completed replacement retains authoritative row contents and relationships")
                        assertEquals(4L, scalarLong(reopened, "SELECT count(*) FROM _sync_row_state WHERE json_extract(server_provenance,'$.origin')='snapshot'"))
                    } else assertEquals(before.rows("_sync_row_state"), after.rows("_sync_row_state"))
                } finally { reopened.close() }
            } finally {
                client.setSnapshotStageStatementCloseForTest(null)
                // The wrapper delegates to the same raw owner even when already disposed.
                assertSame(wrapper, field.get(db)); field.set(db, delegate)
                client.closeFromCoordinatedOwner()
            }
        } } finally { deleteTempSqliteNowTestDbArtifacts(path) }
    }

    private fun contains(root: Throwable, expected: Throwable, seen: MutableList<Throwable> = mutableListOf()): Boolean {
        if (root === expected) return true
        if (seen.any { it === root }) return false
        seen += root
        return root.cause?.let { contains(it, expected, seen) } == true ||
            root.suppressedExceptions.any { contains(it, expected, seen) }
    }
}
