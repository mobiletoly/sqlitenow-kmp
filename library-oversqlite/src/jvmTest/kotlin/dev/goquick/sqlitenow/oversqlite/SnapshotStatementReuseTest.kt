package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import java.util.IdentityHashMap
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.*
import kotlin.test.*

internal class SnapshotStatementReuseTest : SurvivorTestSupport() {
    @Test fun observesSqlAndCompleteState() = runOwnedWebDatabaseTest {
        for (rows in listOf(100, 1000)) snapshotCase(rows)
    }

    @Test fun preparationsAreBoundedByQueryShapes() = runOwnedWebDatabaseTest {
        val small = snapshotCase(100)
        val large = snapshotCase(1000)
        assertEquals(small.preparedSql.toSet(), large.preparedSql.toSet(), "row count does not grow cache shapes")
        assertEquals(small.preparedSql.size, large.preparedSql.size)
        assertEquals(large.preparedSql.size, large.preparedSql.toSet().size, "one physical cache handle per shape")
        for (observation in listOf(small, large)) {
            for (prefix in listOf("SELECT charge FROM _sync_snapshot_stage", "SELECT table_name,key_json,payload,row_version",
                "INSERT INTO _sync_apply_plan(", "SELECT bytes FROM _sync_apply_rows", "SELECT unit,guard,phase,root_cursor",
                "SELECT 1 FROM _sync_row_state WHERE schema_name=? AND table_name=? AND key_json=?")) {
                val sql = observation.physicalPrepares.keys.single { it.startsWith(prefix) }
                assertEquals(1L, observation.physicalPrepares.getValue(sql), prefix)
                assertTrue(observation.logicalSteps.getValue(sql) >= 100, "hot shape remains executed per row: $prefix")
            }
        }
    }

    @Test fun stageWorkspacePreparationsAreBoundedByChunks() = runOwnedWebDatabaseTest {
        for (rows in listOf(100, 1000)) {
            val observation = snapshotCase(rows)
            val counts = observation.physicalPrepares.filterKeys {
                it.startsWith("SELECT o.reserved_entries") && it.contains("LEFT JOIN _sync_apply_plan")
            }
            assertEquals(1, counts.size)
            // Allow fixed lifecycle queries plus one handle for each bounded chunk.
            assertTrue(counts.values.single() <= 8 + (rows + 255) / 256,
                "staging prepares depend on chunks rather than rows: $counts")
        }
    }

    @Test fun failedPreparationIsNotRetained() = runOwnedWebDatabaseTest { failureCase(setOf(Point.PREPARE)) }
    @Test fun resetFailureRollsBackAndClosesHandles() = runOwnedWebDatabaseTest { failureCase(setOf(Point.RESET)) }
    @Test fun clearFailureRollsBackAndClosesHandles() = runOwnedWebDatabaseTest { failureCase(setOf(Point.CLEAR)) }
    @Test fun closeFailureRollsBackAndClosesHandles() = runOwnedWebDatabaseTest { failureCase(setOf(Point.CLOSE)) }
    @Test fun bodyFailureKeepsPriorityThroughCleanup() = runOwnedWebDatabaseTest {
        failureCase(setOf(Point.STEP, Point.RESET, Point.CLEAR, Point.CLOSE))
    }
    @Test fun cancellationAfterMutationRollsBackAcrossReopen() = runOwnedWebDatabaseTest {
        failureCase(emptySet(), cancel = true)
    }
    @Test fun failedClosePreventsYieldProgressPublication() = runOwnedWebDatabaseTest {
        failureCase(setOf(Point.CLOSE), pending = true)
    }

    private suspend fun failureCase(points: Set<Point>, cancel: Boolean = false, pending: Boolean = false) {
        val path = createTempSqliteNowTestDbPath("snapshot-statement-cleanup")
        try {
            withSurvivorFixture(dbPath = path) { server, db, http, seed ->
                seed.closeFromCoordinatedOwner()
                server.addRemoteBundleForTest(listOf(BundleRow("main", "nodes", mapOf("id" to "P"), "UPDATE", 2,
                    buildJsonObject { put("id", "P"); put("name", "replacement"); put("parent_id", JsonNull) })))
                val client = newClient(db, http, syncTables = nodeTables, workUnits = if (pending) 64 else 1_000_000)
                client.openAndConnect("user-1").getOrThrow()
                val before = protectedState(db)
                val source = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
                val probe = FaultProbe(points, pending)
                var commitReached = false
                client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(
                    statementCacheOperations = probe.operations,
                    beforeCommit = { commitReached = true },
                    afterAppliedRow = if (cancel) ({ throw CancellationException("cancel applied snapshot row") }) else null,
                ))
                val failure = try { client.rebuild().getOrThrow(); fail("fault must interrupt rebuild") }
                    catch (error: Throwable) { error }
                client.setSnapshotApplyFaultInjectorForTest(null)
                if (cancel) assertIs<CancellationException>(failure)
                else {
                    val primary = probe.failures.getValue(if (Point.STEP in points) Point.STEP else points.single())
                    assertSame(primary, failure)
                    for (point in points - if (Point.STEP in points) Point.STEP else points.single())
                        assertTrue(containsFailure(failure, probe.failures.getValue(point)), "suppressed $point")
                    assertTrue(probe.reached.containsAll(points))
                }
                assertFalse(commitReached)
                probe.assertClosed()
                assertEquals(before, protectedState(db))
                assertEquals(source, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
                assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_apply_plan"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_apply_rows"))
                client.closeFromCoordinatedOwner(); db.close()
                val reopened = newFileBackedDb(path)
                try {
                    assertEquals(before, protectedState(reopened), "failure survives physical reopen")
                    val retry = newClient(reopened, http, syncTables = nodeTables)
                    try {
                        retry.openAndConnect("user-1").getOrThrow()
                        retry.rebuild().getOrThrow()
                        assertEquals("replacement", scalarText(reopened, "SELECT name FROM nodes WHERE id='P'"))
                        assertEquals(2L, scalarLong(reopened, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                        assertEquals(0L, scalarLong(reopened, "SELECT count(*) FROM _sync_snapshot_stage"))
                        assertEquals(0L, scalarLong(reopened, "SELECT count(*) FROM _sync_apply_rows"))
                    } finally { retry.closeFromCoordinatedOwner() }
                } finally { reopened.close() }
            }
        } finally { deleteTempSqliteNowTestDbArtifacts(path) }
    }

    private suspend fun protectedState(db: SafeSQLiteConnection) =
        baselineQualificationVector(db, listOf("nodes")).tables.filterKeys {
            it in setOf("nodes", "_sync_row_state", "_sync_relationships", "_sync_dirty_rows", "_sync_outbox_rows",
                "_sync_outbox_bundle", "_sync_source_state", "_sync_conflict_state", "_sync_apply_state")
        }

    private fun containsFailure(root: Throwable, expected: Throwable, visited: MutableList<Throwable> = mutableListOf()): Boolean {
        if (root === expected) return true
        if (visited.any { it === root }) return false
        visited += root
        return root.cause?.let { containsFailure(it, expected, visited) } == true ||
            root.suppressedExceptions.any { containsFailure(it, expected, visited) }
    }

    private enum class Point { PREPARE, STEP, RESET, CLEAR, CLOSE }
    private class InjectedFailure(val point: Point) : RuntimeException("injected snapshot statement $point")

    private class FaultProbe(private val points: Set<Point>, private val pending: Boolean) {
        val failures = Point.entries.associateWith { InjectedFailure(it) }
        val reached = mutableSetOf<Point>()
        private val live = IdentityHashMap<SQLiteStatement, String>()
        private var selected: SQLiteStatement? = null
        private fun matches(sql: String) = if (pending) sql.startsWith("INSERT INTO _sync_apply_plan(")
            else sql.startsWith("SELECT table_name,key_json,payload,row_version FROM _sync_snapshot_stage")
        private fun inject(point: Point) { if (point in points && reached.add(point)) throw failures.getValue(point) }
        val operations = StatementCacheOperations(
            prepare = { db, sql ->
                if (matches(sql)) inject(Point.PREPARE)
                val delegate = db.prepare(sql)
                val statement = object : SQLiteStatement by delegate {
                    override fun step(): Boolean {
                        if (matches(sql)) inject(Point.STEP)
                        return delegate.step()
                    }
                }
                live[statement] = sql
                if (matches(sql)) selected = statement
                statement
            },
            reset = { statement -> statement.reset(); if (statement === selected) inject(Point.RESET) },
            clearBindings = { statement -> statement.clearBindings(); if (statement === selected) inject(Point.CLEAR) },
            close = { statement -> statement.close(); live.remove(statement); if (statement === selected) inject(Point.CLOSE) },
        )
        fun assertClosed() { assertTrue(live.isEmpty(), "all successfully prepared handles were closed") }
    }

    private suspend fun snapshotCase(rows: Int): SnapshotStatementObservation {
        val observation = SnapshotStatementObservation()
        withSurvivorFixture { server, db, _, client ->
            try {
                db.execSQL("UPDATE nodes SET name='frozen L' WHERE id='P'")
                server.beforePushCommit = { throw CancellationException("retain intent") }
                assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
                val before = baselineQualificationVector(db, listOf("nodes"))
                val hash = scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle")
                server.addRemoteBundleForTest((0 until rows - 4).map { index ->
                    val id = "Z${index.toString().padStart(5, '0')}"
                    BundleRow("main", "nodes", mapOf("id" to id), "INSERT", 2, buildJsonObject {
                        put("id", id); put("name", "remote-$index"); put("parent_id", JsonNull)
                    })
                })
                val replacement = randomTestSourceId()
                markSourceRecoveryRequired(db, SourceRecoveryReason.HISTORY_PRUNED, replacement)
                observation.observe(db, client, "structural-$rows", sqlCounts = true) { client.rebuild().getOrThrow() }
                assertEquals(rows.toLong(), scalarLong(db, "SELECT count(*) FROM nodes"))
                assertEquals(rows.toLong(), scalarLong(db, "SELECT count(*) FROM _sync_row_state"))
                for (index in 0 until rows - 4) {
                    val id = "Z${index.toString().padStart(5, '0')}"
                    assertEquals("remote-$index", scalarText(db, "SELECT name FROM nodes WHERE id='$id'"))
                    assertEquals("remote-$index", scalarText(db, "SELECT json_extract(server_payload,'$.name.value') FROM _sync_row_state WHERE json_extract(key_json,'$.id')='$id'"))
                }
                assertEquals("frozen L", scalarText(db, "SELECT name FROM nodes WHERE id='P'"))
                assertEquals("parent", scalarText(db, "SELECT json_extract(server_payload,'$.name.value') FROM _sync_row_state WHERE json_extract(key_json,'$.id')='P'"))
                val after = baselineQualificationVector(db, listOf("nodes"))
                assertEquals(before.rows("_sync_outbox_rows").map { it - "source_bundle_id" }, after.rows("_sync_outbox_rows").map { it - "source_bundle_id" })
                assertEquals(hash, scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle"))
                assertEquals(1L, scalarLong(db, "SELECT source_bundle_id FROM _sync_outbox_bundle"))
                assertEquals(replacement, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
                assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
                for (table in listOf("_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_dirty_rows"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM $table"), table)
                assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+snapshot_stage_entries+snapshot_stage_bytes FROM _sync_operation_state"))
            } finally { client.closeFromCoordinatedOwner() }
        }
        return observation
    }
}
