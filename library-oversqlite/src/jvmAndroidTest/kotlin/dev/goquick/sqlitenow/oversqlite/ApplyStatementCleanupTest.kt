package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.DatabaseMigrations
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowDatabase
import java.util.IdentityHashMap
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.*
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

internal class ApplyStatementCleanupTest : ApplyStatementTestSupport() {
    @Test fun pullCleanupPageBoundaries() = pages("pull")
    @Test fun conflictCleanupPageBoundaries() = pages("conflict")
    @Test fun snapshotCleanupPageBoundaries() = pages("snapshot")
    @Test fun cleanupHandlesAreReused() = runOwnedWebDatabaseTest {
        for (operation in listOf("pull", "conflict", "snapshot")) cleanupCase(operation, 129, requireCache = true)
    }
    @Test fun traversalHandlesAreReused() = runOwnedWebDatabaseTest {
        for (operation in listOf("pull", "conflict")) {
            val observations = mutableListOf<ApplyStatementObservation>()
            for (rows in listOf(32, 129)) withApplyFixture(rows) { f ->
                observations += measure(f, operation, rows, 0)
            }
            val small = observations[0]; val large = observations[1]
            assertEquals(1, small.writers); assertEquals(1, large.writers)
            val shapes = small.steps.filter { (sql, steps) ->
                sql.startsWith("INSERT OR IGNORE INTO _sync_apply_edges") || sql == "SELECT changes()" ||
                    sql.startsWith("SELECT unit,guard,phase,root_cursor") ||
                    (steps >= 32 && (sql.startsWith("SELECT o.reserved_entries") || sql.contains("FROM \"nodes\"")))
            }.keys
            assertTrue(shapes.any { it.startsWith("SELECT o.reserved_entries") }, "workspace checks remain observed")
            assertTrue(shapes.any { it.contains("FROM \"nodes\"") }, "managed key/FK scans remain observed")
            for (observed in observations) observed.assertReused(shapes, cachedOnly = true)
            for (sql in shapes) {
                // Uncached wire sizing and dependency validation stay intact. Their fixed
                // root work must not conceal preparations proportional to the global scan.
                assertEquals(small.handles.count { it.sql == sql }, large.handles.count { it.sql == sql }, sql)
            }
        }
    }
    private fun pages(operation: String) = runOwnedWebDatabaseTest(timeout = 180.seconds) {
        for (population in listOf(0, 1, 127, 128, 129, 257)) cleanupCase(operation, population)
    }
    private suspend fun cleanupCase(operation: String, population: Int, requireCache: Boolean = false) {
        withApplyFixture(if (operation == "snapshot") 0 else population) { f ->
            val handle = if (operation == "conflict") conflict(f, "replacement", "Q") else null
            if (operation == "pull") f.server.addRemoteBundleForTest(listOf(rootEffect("replacement", "Q")))
            if (operation == "snapshot") {
                // Keep actual retained snapshot rows exactly at the requested page boundary.
                val rows = mutableListOf(BundleRow("main", "nodes", mapOf("id" to "Q"), "DELETE", 2, null),
                    BundleRow("main", "nodes", mapOf("id" to "W"), "DELETE", 2, null))
                repeat(population) { i -> rows += rootEffect("snapshot-$i", "S${i.toString().padStart(4, '0')}").copy(op = "INSERT") }
                f.server.addRemoteBundleForTest(rows)
            }
            val observed = ApplyStatementObservation.install(f.db)
            try {
                when (operation) {
                    "pull" -> f.client.pullToStable().getOrThrow()
                    "conflict" -> f.client.resolveConflict(checkNotNull(handle), MergeResult.AcceptServer).getOrThrow()
                    else -> f.client.rebuild().getOrThrow()
                }
            } finally { observed.restore() }
            observed.assertClosed()
            val edges = if (operation == "snapshot") 0 else population
            val rows = if (operation == "snapshot") population else population + 1
            fun probes(n: Int) = (n + 127L) / 128 + 1
            assertEquals(probes(edges), observed.steps[applyCleanupSql[0]], "edge pages including terminal probe")
            assertEquals(probes(rows), observed.steps[applyCleanupSql[1]], "row pages including terminal probe")
            assertEquals(1L, observed.steps[applyCleanupSql[3]])
            val indexChanges = if (operation == "snapshot") 0L else population.toLong()
            assertEquals(probes(edges) + probes(rows) + indexChanges, observed.steps[applyCleanupSql[2]])
            if (requireCache) observed.assertReused(applyCleanupSql, cachedOnly = true)
            assertApplyClean(f.db)
            if (operation == "snapshot") assertEquals(population.toLong(), scalarLong(f.db, "SELECT count(*) FROM nodes"))
            else {
                assertEquals("replacement", scalarText(f.db, "SELECT name FROM nodes WHERE id='Q'"))
                assertEquals(population.toLong(), scalarLong(f.db, "SELECT count(*) FROM nodes WHERE parent_id='Q' AND name='unrelated'"))
            }
            observed.publish("cleanup-$operation", population, 0, 0.0, 0.0, f.client.workSliceDiagnostics(), f.client.workspaceDiagnostics())
        }
    }

    @Test fun pullCleanupFailuresRollBackAcrossReopen() = faults("pull")
    @Test fun conflictCleanupFailuresRollBackAcrossReopen() = faults("conflict")
    @Test fun snapshotCleanupFailuresRollBackAcrossReopen() = faults("snapshot")
    @Test fun lateChangesFailureAfterDeleteRollsBackAcrossReopen() = runOwnedWebDatabaseTest(timeout = 180.seconds) {
        for (operation in listOf("pull", "conflict", "snapshot"))
            for (point in listOf(Point.STEP, Point.RESET, Point.CLEAR))
                failureCase(operation, "SELECT changes()", setOf(point), late = true)
    }
    @Test fun traversalFailuresRollBackAcrossReopen() = runOwnedWebDatabaseTest(timeout = 180.seconds) {
        for (operation in listOf("pull", "conflict")) for (point in Point.entries)
            failureCase(operation, "INSERT OR IGNORE INTO _sync_apply_edges", setOf(point), prefix = true)
    }
    @Test fun indexedChangesHandleFailsDuringCleanup() = runOwnedWebDatabaseTest {
        for (operation in listOf("pull", "conflict")) withApplyFixture(3) { f ->
            val handle = if (operation == "conflict") conflict(f, "replacement") else null
            if (operation == "pull") f.server.addRemoteBundleForTest(listOf(rootEffect("replacement")))
            val probe = Probe("SELECT changes()", setOf(Point.STEP), late = true, prefix = false, cancel = false)
            f.client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(statementCacheOperations = probe.operations))
            val failure = try { invoke(f.client, operation, handle); fail("late changes fault must execute") }
                catch (error: Throwable) { error } finally { f.client.setSnapshotApplyFaultInjectorForTest(null) }
            assertSame(probe.failures.getValue(Point.STEP), generateSequence(failure) { it.cause }.last())
            assertEquals(3, probe.changesBeforeDelete, "the writer-owned handle was used by indexing before cleanup")
            assertTrue(probe.deleteSteps > 0)
            assertTrue(probe.live.isEmpty())
            println("APPLY_INDEXED_CHANGES operation=$operation indexSteps=${probe.changesBeforeDelete} deleteSteps=${probe.deleteSteps} lateFailure=true closed=true")
        }
    }
    @Test fun pullCancellationReopensAtEveryWorkTarget() = runOwnedWebDatabaseTest(timeout = 180.seconds) {
        for (work in listOf(1L, 8L, 17L, 1_000_000L)) {
            val path = createTempSqliteNowTestDbPath("pull-cache-continuation")
            try {
                withApplyFixture(3, work, path) { f ->
                    f.server.addRemoteBundleForTest(listOf(rootEffect("replacement")))
                    var commits = 0
                    val observed = ApplyStatementObservation.install(f.db)
                    f.client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(beforeCommit = {
                        if (++commits == if (work == 1_000_000L) 1 else 2) throw CancellationException("cancel pull writer")
                    }))
                    try { assertFailsWith<CancellationException> { f.client.pullToStable().getOrThrow() } }
                    finally { f.client.setSnapshotApplyFaultInjectorForTest(null); observed.restore() }
                    observed.assertClosed()
                    val durable = baselineQualificationVector(f.db, listOf("nodes", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges"))
                    assertEquals("root", scalarText(f.db, "SELECT name FROM nodes WHERE id='W'"))
                    assertEquals(1L, scalarLong(f.db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    f.client.closeFromCoordinatedOwner(); f.db.close()
                    val reopened = newFileBackedDb(path)
                    try {
                        assertEquals(durable, baselineQualificationVector(reopened, listOf("nodes", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges")))
                        val retry = newApplyClient(reopened, f.http, work)
                        try {
                            retry.openAndConnect("user-1").getOrThrow()
                            val fresh = ApplyStatementObservation.install(reopened)
                            try { retry.pullToStable().getOrThrow() } finally { fresh.restore() }
                            fresh.assertClosed()
                            for (sql in fresh.steps.keys) {
                                val cached = fresh.handles.filter { it.sql == sql && it.resets > 0 }
                                cached.groupBy { it.writer }.values.forEach { assertEquals(1, it.size, sql) }
                            }
                            assertEquals("replacement", scalarText(reopened, "SELECT name FROM nodes WHERE id='W'"))
                            assertEquals(2L, scalarLong(reopened, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                            assertApplyClean(reopened)
                            println("APPLY_CONTINUATION target=$work cancelledWriters=${observed.writers} retryWriters=${fresh.writers} physicalReopen=true handlesClosed=true")
                        } finally { retry.closeFromCoordinatedOwner() }
                    } finally { reopened.close() }
                }
            } finally { deleteTempSqliteNowTestDbArtifacts(path) }
        }
    }
    @Test fun newerConflictIntentRetainsFrozenOwners() = runOwnedWebDatabaseTest {
        withApplyFixture(3) { f ->
            val oldHandle = conflict(f, "replacement")
            val frozen = baselineQualificationVector(f.db, listOf("nodes")).rows("_sync_outbox_rows")
            val request = scalarText(f.db, "SELECT canonical_request_hash FROM _sync_outbox_bundle")
            f.db.execSQL("UPDATE nodes SET name='newer pending',parent_id='Q' WHERE id='W'")
            assertIs<StaleConflictResolutionException>(f.client.resolveConflict(oldHandle, MergeResult.AcceptServer).exceptionOrNull())
            val handle = checkNotNull(f.client.pendingConflict().getOrThrow())
            val before = protectedState(f.db)
            f.client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(beforeCommit = { throw CancellationException("cancel newer conflict intent") }))
            try { assertFailsWith<CancellationException> { f.client.resolveConflict(handle, MergeResult.AcceptServer).getOrThrow() } }
            finally { f.client.setSnapshotApplyFaultInjectorForTest(null) }
            assertEquals(before, protectedState(f.db))
            assertEquals(frozen, baselineQualificationVector(f.db, listOf("nodes")).rows("_sync_outbox_rows"))
            assertEquals(request, scalarText(f.db, "SELECT canonical_request_hash FROM _sync_outbox_bundle"))
            f.client.resolveConflict(handle, MergeResult.AcceptServer).getOrThrow()
            assertEquals("newer pending", scalarText(f.db, "SELECT name FROM nodes WHERE id='W' AND parent_id='Q'"))
            assertEquals("replacement", scalarText(f.db, "SELECT json_extract(server_payload,'$.name.value') FROM _sync_row_state WHERE key_json='{\"id\":\"W\"}'"))
            assertEquals(1L, scalarLong(f.db, "SELECT count(*) FROM _sync_dirty_rows WHERE op='UPDATE' AND base_row_version=2"))
            assertEquals(0L, scalarLong(f.db, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(f.db, "SELECT reserved_entries+reserved_bytes FROM _sync_operation_state"))
        }
    }
    @Test fun pullStillRejectsUnknownParentEvidence() = runOwnedWebDatabaseTest {
        withApplyFixture(0) { f ->
            f.db.execSQL("UPDATE _sync_row_state SET server_kind='unknown',server_payload=NULL,row_version=0,relationships_known=0,server_provenance=json_set(server_provenance,'$.authoritative_version',0,'$.origin','explicit_conflict','$.bundle_sequence',NULL) WHERE key_json='{\"id\":\"Q\"}'")
            val row = rootEffect("replacement")
            f.server.addRemoteBundleForTest(listOf(row.copy(payload = JsonObject(row.payload!!.jsonObject + ("parent_id" to JsonPrimitive("Q"))))))
            assertIs<FKReconciliationRequiredException>(f.client.pullToStable().exceptionOrNull())
            assertEquals("root", scalarText(f.db, "SELECT name FROM nodes WHERE id='W'"))
            assertEquals(1L, scalarLong(f.db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        }
    }
    private fun faults(operation: String) = runOwnedWebDatabaseTest(timeout = 180.seconds) {
        for (sql in applyCleanupSql) {
            for (point in Point.entries) failureCase(operation, sql, setOf(point))
            failureCase(operation, sql, setOf(Point.STEP, Point.RESET, Point.CLEAR, Point.CLOSE))
            failureCase(operation, sql, setOf(Point.STEP, Point.RESET, Point.CLEAR, Point.CLOSE), cancel = true)
        }
    }
    private enum class Point { PREPARE, STEP, RESET, CLEAR, CLOSE }
    private class Fault(val point: Point) : RuntimeException("apply-statement-$point")
    private class Probe(val sql: String, val points: Set<Point>, val late: Boolean, val prefix: Boolean, val cancel: Boolean) {
        val failures = Point.entries.associateWith { if (it == Point.STEP && cancel) CancellationException("apply-statement-cancel") else Fault(it) }
        val reached = mutableSetOf<Point>()
        val live = IdentityHashMap<SQLiteStatement, String>()
        var deleteSteps = 0
        var changesBeforeDelete = 0
        private var active: SQLiteStatement? = null
        private fun matches(text: String) = if (prefix) text.startsWith(sql) else text == sql
        private fun armed() = !late || deleteSteps > 0
        private fun inject(point: Point) { if (point in points && reached.add(point)) throw failures.getValue(point) }
        val operations = StatementCacheOperations(
            prepare = { db, text ->
                if (matches(text) && armed()) inject(Point.PREPARE)
                val delegate = db.prepare(text)
                val wrapped = object : SQLiteStatement by delegate {
                    override fun step(): Boolean {
                        if (text in applyCleanupSql.take(2)) deleteSteps++
                        if (text == "SELECT changes()" && deleteSteps == 0) changesBeforeDelete++
                        if (matches(text) && armed()) { active = this; inject(Point.STEP) }
                        return delegate.step()
                    }
                }
                live[wrapped] = text
                wrapped
            },
            reset = { it.reset(); if (it === active || (active == null && matches(live[it].orEmpty()) && armed())) inject(Point.RESET) },
            clearBindings = { it.clearBindings(); if (it === active || (active == null && matches(live[it].orEmpty()) && armed())) inject(Point.CLEAR) },
            close = { val text = live.remove(it); it.close(); if (matches(text.orEmpty()) && armed()) inject(Point.CLOSE) },
        )
    }
    private suspend fun failureCase(operation: String, sql: String, points: Set<Point>, late: Boolean = false,
        prefix: Boolean = false, cancel: Boolean = false) {
        val path = createTempSqliteNowTestDbPath("apply-statement-rollback")
        try {
            withApplyFixture(3, path = path) { f ->
                val handle = if (operation == "conflict") conflict(f, "replacement") else null
                if (operation != "conflict") f.server.addRemoteBundleForTest(listOf(rootEffect("replacement")))
                val beforePublicCall = protectedState(f.db)
                // Explicit rebuild publishes recovery-required before staging the writer.
                // Writer rollback must retain this guard as well as every other protected field.
                val before = if (operation == "snapshot") beforePublicCall.mapValues { (table, rows) ->
                    if (table != "_sync_attachment_state") rows else rows.map { row ->
                        JsonObject(row + ("rebuild_required" to buildJsonObject {
                            put("storage", "integer"); put("value", "1")
                        }))
                    }
                } else beforePublicCall
                val probe = Probe(sql, points, late, prefix, cancel)
                var commit = false
                f.client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(statementCacheOperations = probe.operations,
                    beforeCommit = { commit = true }))
                val failure = try { invoke(f.client, operation, handle); fail("fault was not reached: $operation $sql $points") }
                    catch (e: Throwable) { e } finally { f.client.setSnapshotApplyFaultInjectorForTest(null) }
                val primary = if (Point.STEP in points) Point.STEP else points.single()
                assertSame(probe.failures.getValue(primary), generateSequence(failure) { it.cause }.last(), "body/cancellation retains primary-error precedence")
                for (point in points) assertTrue(containsFailure(failure, probe.failures.getValue(point)), "suppressed $point")
                assertEquals(points, probe.reached)
                assertTrue(probe.live.isEmpty(), "all successfully prepared cache handles close")
                assertFalse(commit)
                if (late) assertTrue(probe.deleteSteps > 0, "changes fault occurs after DELETE")
                assertEquals(before, protectedState(f.db))
                f.client.closeFromCoordinatedOwner(); f.db.close()
                val reopened = newFileBackedDb(path)
                try { assertEquals(before, protectedState(reopened), "physical reopen retains every protected value") }
                finally { reopened.close() }
                // Fatal release/close failures may retain an orphan reservation. Only coordinated opening owns reclamation.
                val database = SqliteNowDatabase(path, object : DatabaseMigrations {
                    override val targetVersion = 0
                    override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int) = 0
                }, syncManaged = true)
                val opening = database.openForSync(OversqliteConfig(1, applyNodeTables, "fixture", "main"), f.http)
                try {
                    assertEquals(OversqliteOpeningOutcome.READY, opening.advance().outcome)
                    val ready = assertNotNull(opening.readyOrNull())
                    ready.client.openAndConnect("user-1").getOrThrow()
                    invoke(ready.client, operation, handle)
                    val recovered = database.connection()
                    assertEquals("replacement", scalarText(recovered, "SELECT name FROM nodes WHERE id='W'"))
                    assertApplyClean(recovered)
                } finally { opening.close() }
                println("APPLY_STATEMENT_FAULT operation=$operation sql=$sql points=$points late=$late indexedChanges=${probe.changesBeforeDelete} deleteSteps=${probe.deleteSteps} cancellation=$cancel reopen=true retry=true")
            }
        } finally { deleteTempSqliteNowTestDbArtifacts(path) }
    }
    private suspend fun invoke(client: OversqliteClient, operation: String, handle: PendingConflict?) {
        when (operation) {
            "pull" -> client.pullToStable().getOrThrow()
            "conflict" -> client.resolveConflict(checkNotNull(handle), MergeResult.AcceptServer).getOrThrow()
            else -> client.rebuild().getOrThrow()
        }
    }
    private suspend fun protectedState(db: SafeSQLiteConnection) =
        baselineQualificationVector(db, listOf("nodes")).tables.filterKeys {
            it in setOf("nodes", "_sync_row_state", "_sync_relationships", "_sync_dirty_rows", "_sync_outbox_rows",
                "_sync_outbox_bundle", "_sync_source_state", "_sync_conflict_state", "_sync_attachment_state", "_sync_apply_state")
        }
    private fun containsFailure(root: Throwable, expected: Throwable): Boolean = root === expected ||
        root.cause?.let { containsFailure(it, expected) } == true || root.suppressedExceptions.any { containsFailure(it, expected) }
}
