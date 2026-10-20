package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import kotlinx.serialization.json.Json
import java.util.IdentityHashMap
import kotlin.test.*

/** Accounting reads are observed separately from dependency traversal and work. */
internal class ApplyIndexAccountingTest : ApplyStatementTestSupport() {
    private val indexUnit = applyUnit("index-accounting", 0, "fixture")
    private fun accounting(sql: String) = sql.startsWith("SELECT o.reserved_entries") && sql.contains("NULL,0,0,0,0,0")
    private fun eligibility(sql: String) = sql.contains("sqlite_temp_schema") && sql.contains("_sync_apply_edges")
    private fun reads(observed: ApplyStatementObservation) = observed.steps.filterKeys(::accounting).values.sum()

    @Test fun fixtureAdmissionMatchesDerivedCharges() = runOwnedWebDatabaseTest {
        // This population makes indexing admission exceed the fixed context-guard preflight.
        withApplyFixture(128) { f ->
            val rows = 128
            val base = OversqliteWorkspace(f.db, configuration(f)).usage(indexUnit, apply = WorkspaceCharge())
            val peak = indexingPeak(rows, base)
            for (dimension in listOf("entries", "bytes")) for (delta in listOf(-1L, 0L, 1L)) {
                val config = configuration(f).let {
                    if (dimension == "entries") it.copy(maxSyncWorkspaceEntries = peak.entries + delta)
                    else it.copy(maxSyncWorkspaceBytes = peak.bytes + delta)
                }
                val before = baselineQualificationVector(f.db, listOf("nodes", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges"))
                val failure = runCatching { prepareIndex(f, config, cached = false) }.exceptionOrNull()
                if (delta < 0) assertEquals(dimension, assertIs<SyncWorkspaceLimitException>(failure).dimension)
                else assertNull(failure)
                assertEquals(before, baselineQualificationVector(f.db, listOf("nodes", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges")))
            }
        }
    }

    @Test fun pullIndexingReadsAreBounded() = volume("pull")
    @Test fun conflictIndexingReadsAreBounded() = volume("conflict")
    private fun volume(operation: String) = runOwnedWebDatabaseTest {
        val counts = mutableListOf<Long>()
        for (rows in listOf(32, 129)) withApplyFixture(rows) { f ->
            val observed = measure(f, operation, rows, 0)
            counts += reads(observed)
            assertEquals(rows.toLong(), observed.steps.filterKeys { it.startsWith("INSERT OR IGNORE INTO _sync_apply_edges") }.values.sum())
            assertEquals(1, observed.writers)
            assertEquals(1L, observed.steps.filterKeys(::eligibility).values.sum())
        }
        assertEquals(counts[0], counts[1], "accounting SQL is independent of the related-row population")
    }

    @Test fun rawCallersKeepFreshReads() = runOwnedWebDatabaseTest {
        withApplyFixture(3) { f ->
            val observed = prepareIndex(f, configuration(f), cached = false)
            assertTrue(reads(observed) >= 15)
            assertEquals(0L, observed.steps.filterKeys(::eligibility).values.sum())
        }
    }

    @Test fun fallbackPushIndexingReadsAreBounded() = runOwnedWebDatabaseTest {
        val counts = mutableListOf<Long>()
        for (rows in listOf(32, 129)) withApplyFixture(rows) { f ->
            f.db.execSQL("CREATE TRIGGER force_survivor_apply AFTER UPDATE ON nodes BEGIN SELECT 1; END")
            f.db.execSQL("UPDATE nodes SET name='local pushed' WHERE id='W'")
            val requests = f.server.pushRequestCount
            val observed = ApplyStatementObservation.install(f.db)
            try { f.client.pushPending().getOrThrow() } finally { observed.restore() }
            observed.assertClosed()
            counts += reads(observed)
            assertEquals(rows.toLong(), observed.steps.filterKeys { it.startsWith("INSERT OR IGNORE INTO _sync_apply_edges") }.values.sum())
            assertEquals(1L, observed.steps.filterKeys(::eligibility).values.sum())
            assertEquals(requests + 1, f.server.pushRequestCount)
            assertEquals("local pushed", scalarText(f.db, "SELECT name FROM nodes WHERE id='W'"))
            assertEquals(rows.toLong(), scalarLong(f.db, "SELECT count(*) FROM nodes WHERE parent_id='Q' AND name='unrelated'"))
            assertApplyClean(f.db)
        }
        assertEquals(counts[0], counts[1])
    }

    @Test fun holderGrowthPreservesEntryBoundaries() = growth("entries")
    @Test fun holderGrowthPreservesByteBoundaries() = growth("bytes")
    private fun growth(dimension: String) = runOwnedWebDatabaseTest {
        withApplyFixture(128) { f ->
            val peak = indexingPeak(128, OversqliteWorkspace(f.db, configuration(f)).usage(indexUnit, apply = WorkspaceCharge()))
            val holder = if (dimension == "entries") 2L else 256L
            for (delta in listOf(-1L, 0L, holder - 1, holder, holder + 1)) {
                val config = configuration(f).let {
                    if (dimension == "entries") it.copy(maxSyncWorkspaceEntries = peak.entries + delta)
                    else it.copy(maxSyncWorkspaceBytes = peak.bytes + delta)
                }
                for (cached in listOf(false, true)) {
                    val probe = IndexProbe(128)
                    val before = baselineQualificationVector(f.db, listOf("nodes", "_sync_apply_plan", "_sync_apply_edges", "_sync_apply_rows"))
                    val failure = runCatching { prepareIndex(f, config, cached, probe = probe) }.exceptionOrNull()
                    if (delta < 0) assertEquals(dimension, assertIs<SyncWorkspaceLimitException>(failure).dimension)
                    else {
                        assertNull(failure)
                        if (cached && delta == holder - 1) assertTrue(127 in probe.metadataEdges, "holder evicted before the last FK allocation")
                        if (cached && delta >= holder) assertTrue(probe.metadataEdges.none { it in 1..127 }, "stable external total retained through indexing")
                    }
                    assertEquals(before, baselineQualificationVector(f.db, listOf("nodes", "_sync_apply_plan", "_sync_apply_edges", "_sync_apply_rows")))
                }
            }
            println("APPLY_INDEX_BOUNDARY dimension=$dimension originalPeak=$peak holder=$holder fallback=true")
        }
    }

    @Test fun initialHolderRefusalUsesFreshEntryAccounting() = initialRefusal("entries")
    @Test fun initialHolderRefusalUsesFreshByteAccounting() = initialRefusal("bytes")
    private fun initialRefusal(dimension: String) = runOwnedWebDatabaseTest {
        withApplyFixture(0) { f ->
            val base = OversqliteWorkspace(f.db, configuration(f)).usage(indexUnit, apply = WorkspaceCharge())
            val counts = mutableListOf<Long>()
            for (cached in listOf(false, true)) {
                // A valid owner grows after the guard/root preflight but before indexing.
                // This reaches initial optional refusal without bypassing setup admission.
                val probe = IndexProbe(0, afterIndexEntry = { raw ->
                    if (dimension == "entries") repeat(95) { i -> execute(raw,
                        "INSERT INTO _sync_snapshot_stage(snapshot_id,row_ordinal,schema_name,table_name,key_json,row_version,payload) VALUES('accounting',${i+1},'main','nodes','{\"id\":\"S$i\"}',1,'{}')") }
                    else execute(raw, "UPDATE _sync_operation_state SET reason='${"x".repeat(4096)}'")
                })
                val config = configuration(f).let {
                    if (dimension == "entries") it.copy(maxSyncWorkspaceEntries = base.entries + 95 + 1 + 3)
                    else it.copy(maxSyncWorkspaceBytes = base.bytes + 512 + 6 * 128 + 146 + 6 * 4096 + 255)
                }
                try {
                    val observed = prepareIndex(f, config, cached, probe = probe)
                    counts += reads(observed)
                    assertTrue(probe.mutated)
                    if (dimension == "entries") assertEquals(95L, scalarLong(f.db, "SELECT snapshot_stage_entries FROM _sync_operation_state"))
                    else assertEquals(4096L, scalarLong(f.db, "SELECT length(reason) FROM _sync_operation_state"))
                } finally {
                    f.db.execSQL("DELETE FROM _sync_snapshot_stage")
                    f.db.execSQL("UPDATE _sync_operation_state SET reason=''")
                }
            }
            assertEquals(counts[0], counts[1], "initial holder refusal retains all original accounting reads")
        }
    }

    @Test fun internalTriggersKeepFreshAccounting() = runOwnedWebDatabaseTest {
        for (temp in listOf(false, true)) for (table in listOf("_sync_apply_edges", "_sync_apply_plan")) withApplyFixture(3) { f ->
            val event = if (table == "_sync_apply_edges") "INSERT" else "UPDATE"
            f.db.execSQL("CREATE ${if (temp) "TEMP " else ""}TRIGGER mutate_index_owner AFTER $event ON $table BEGIN UPDATE _sync_operation_state SET reason=reason||'x'; END")
            val observed = prepareIndex(f, configuration(f), cached = true)
            assertTrue(reads(observed) >= 15)
            assertEquals(1L, observed.steps.filterKeys(::eligibility).values.sum())
            assertTrue(scalarLong(f.db, "SELECT length(reason) FROM _sync_operation_state") > 0)
            println("APPLY_INDEX_TRIGGER temp=$temp table=$table fresh=true")
        }
    }

    @Test fun snapshotAndAcknowledgementsDoNotRetainAccounting() = runOwnedWebDatabaseTest {
        withApplyFixture(3) { f ->
            for (kind in listOf("snapshot", "settlement")) {
                val observed = prepareIndex(f, configuration(f), cached = true, kind = kind, discover = kind == "snapshot")
                assertEquals(0L, observed.steps.filterKeys(::eligibility).values.sum())
                assertEquals(0L, observed.steps.filterKeys { it.startsWith("INSERT OR IGNORE INTO _sync_apply_edges") }.values.sum())
            }
            f.db.execSQL("UPDATE nodes SET name='acknowledged' WHERE id='W'")
            val observed = ApplyStatementObservation.install(f.db)
            try { f.client.pushPending().getOrThrow() } finally { observed.restore() }
            observed.assertClosed()
            assertEquals(0L, observed.steps.filterKeys(::eligibility).values.sum())
            assertEquals(0L, observed.steps.filterKeys { it.startsWith("INSERT OR IGNORE INTO _sync_apply_edges") }.values.sum())
            assertApplyClean(f.db)
        }
    }

    @Test fun constructionFailuresReopen() = failures(late = false)
    @Test fun fallbackReadFailuresReopen() = failures(late = true)

    @Test fun reopenedWriterReadsChangedAccounting() = runOwnedWebDatabaseTest {
        val path = createTempSqliteNowTestDbPath("index-accounting-new-writer")
        try {
            withApplyFixture(128, path = path) { f ->
                val peak = indexingPeak(128, OversqliteWorkspace(f.db, configuration(f)).usage(indexUnit, apply = WorkspaceCharge()))
                val changedPeak = peak.bytes + 6 * 4096
                val config = configuration(f).copy(maxSyncWorkspaceBytes = changedPeak - 1)
                prepareIndex(f, config, true)
                val state = field<OversqliteClientSessionState>(f.client, "sessionState").requireConnectedRuntimeState("index fixture")
                val workspace = OversqliteWorkspace(f.db, config)
                val guard = workspace.contextGuard(state, indexUnit)
                f.db.execSQL("UPDATE _sync_operation_state SET reason='${"x".repeat(4096)}'")
                assertEquals(guard, workspace.contextGuard(state, indexUnit))
                assertEquals("bytes", assertFailsWith<SyncWorkspaceLimitException> {
                    prepareIndex(f, config, true)
                }.dimension)
                f.db.execSQL("UPDATE _sync_operation_state SET reason=''")
                f.client.closeFromCoordinatedOwner(); f.db.close()
                val reopened = newFileBackedDb(path)
                try {
                    val retry = newApplyClient(reopened, f.http)
                    try {
                        retry.openAndConnect("user-1").getOrThrow()
                        val current = Fixture(f.server, reopened, f.http, retry)
                        reopened.execSQL("UPDATE _sync_operation_state SET reason='${"x".repeat(4096)}'")
                        assertEquals("bytes", assertFailsWith<SyncWorkspaceLimitException> {
                            prepareIndex(current, config, true)
                        }.dimension)
                        prepareIndex(current, config.copy(maxSyncWorkspaceBytes = changedPeak), true)
                        assertEquals(4096L, scalarLong(reopened, "SELECT length(reason) FROM _sync_operation_state"))
                        reopened.execSQL("UPDATE _sync_operation_state SET reason=''")
                        assertApplyClean(reopened)
                        println("APPLY_INDEX_REFRESH sameGuard=true consecutiveWriters=true physicalReopen=true ownerBytesAdded=${6*4096} originalRefusal=true retry=true")
                    } finally { retry.closeFromCoordinatedOwner() }
                } finally { reopened.close() }
            }
        } finally { deleteTempSqliteNowTestDbArtifacts(path) }
    }

    @Test fun cancellationReopensAtEveryWorkTarget() = runOwnedWebDatabaseTest {
        for (work in listOf(1L, 8L, 17L, 1_000_000L)) {
            val path = createTempSqliteNowTestDbPath("index-accounting-continuation")
            try {
                withApplyFixture(3, work, path) { f ->
                    f.server.addRemoteBundleForTest(listOf(rootEffect("replacement")))
                    val probe = IndexProbe(3, cancelAfterEdge = 1)
                    f.client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(statementCacheOperations = probe.operations))
                    try {
                        val cancelled = assertFailsWith<kotlinx.coroutines.CancellationException> { f.client.pullToStable().getOrThrow() }
                        assertSame(probe.edgeCancellation, generateSequence<Throwable>(cancelled) { it.cause }.last(), "coroutine stack recovery retains the original cancellation")
                    } finally { f.client.setSnapshotApplyFaultInjectorForTest(null) }
                    assertEquals(1, probe.edges)
                    assertTrue(probe.live.isEmpty())
                    val before = baselineQualificationVector(f.db, listOf("nodes", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges"))
                    val source = scalarText(f.db, "SELECT current_source_id FROM _sync_attachment_state")
                    val sequence = scalarLong(f.db, "SELECT next_source_bundle_id FROM _sync_source_state")
                    assertEquals("root", scalarText(f.db, "SELECT name FROM nodes WHERE id='W'"))
                    assertEquals(1L, scalarLong(f.db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    f.client.closeFromCoordinatedOwner(); f.db.close()
                    val reopened = newFileBackedDb(path)
                    try {
                        assertEquals(before, baselineQualificationVector(reopened, listOf("nodes", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges")))
                        val retry = newApplyClient(reopened, f.http, work)
                        try {
                            retry.openAndConnect("user-1").getOrThrow()
                            val observed = ApplyStatementObservation.install(reopened)
                            try { retry.pullToStable().getOrThrow() } finally { observed.restore() }
                            observed.assertClosed()
                            assertTrue(reads(observed) > 0)
                            assertTrue(observed.steps.filterKeys(::eligibility).values.sum() > 0)
                            assertEquals("replacement", scalarText(reopened, "SELECT name FROM nodes WHERE id='W'"))
                            assertEquals(2L, scalarLong(reopened, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                            assertEquals(source, scalarText(reopened, "SELECT current_source_id FROM _sync_attachment_state"))
                            assertEquals(sequence, scalarLong(reopened, "SELECT next_source_bundle_id FROM _sync_source_state"))
                            assertApplyClean(reopened)
                            println("APPLY_INDEX_CONTINUATION target=$work physicalReopen=true cancelledAfterEdge=true retryWriters=${observed.writers}")
                        } finally { retry.closeFromCoordinatedOwner() }
                    } finally { reopened.close() }
                }
            } finally { deleteTempSqliteNowTestDbArtifacts(path) }
        }
    }
    private fun failures(late: Boolean) = runOwnedWebDatabaseTest {
        // Fallback borrows the existing physical handle, so it cannot prepare it again.
        val singles = Point.entries.filter { !late || it != Point.PREPARE }.map(::setOf)
        val combined = setOf(Point.STEP, Point.RESET, Point.CLEAR, Point.CLOSE)
        for ((points, cancel) in singles.map { it to false } + listOf(combined to false, combined to true)) {
            val path = createTempSqliteNowTestDbPath("index-accounting-fault")
            try {
                withApplyFixture(128, path = path) { f ->
                    val peak = indexingPeak(128, OversqliteWorkspace(f.db, configuration(f)).usage(indexUnit, apply = WorkspaceCharge()))
                    val config = configuration(f).let { if (late) it.copy(maxSyncWorkspaceEntries = peak.entries + 1) else it }
                    val before = baselineQualificationVector(f.db, listOf("nodes", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges"))
                    val probe = IndexProbe(128, points = points, late = late, cancel = cancel)
                    val failure = runCatching { prepareIndex(f, config, true, probe = probe) }.exceptionOrNull()
                    assertNotNull(failure)
                    val primary = if (Point.STEP in points) Point.STEP else points.single()
                    assertSame(probe.failures.getValue(primary), generateSequence(failure) { it.cause }.last())
                    fun contains(root: Throwable, target: Throwable): Boolean = root === target ||
                        root.cause?.let { contains(it, target) } == true || root.suppressedExceptions.any { contains(it, target) }
                    points.forEach { assertTrue(contains(failure, probe.failures.getValue(it)), "failure retained: $it") }
                    assertEquals(points, probe.reached)
                    assertTrue(probe.live.isEmpty())
                    if (late) assertTrue(127 in probe.metadataEdges)
                    assertEquals(before, baselineQualificationVector(f.db, listOf("nodes", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges")))
                    f.client.closeFromCoordinatedOwner(); f.db.close()
                    val reopened = newFileBackedDb(path)
                    try {
                        assertEquals(before, baselineQualificationVector(reopened, listOf("nodes", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges")))
                        val retry = newApplyClient(reopened, f.http)
                        try {
                            retry.openAndConnect("user-1").getOrThrow()
                            val current = Fixture(f.server, reopened, f.http, retry)
                            prepareIndex(current, config, true)
                            assertApplyClean(reopened)
                        } finally { retry.closeFromCoordinatedOwner() }
                    } finally { reopened.close() }
                    println("APPLY_INDEX_FAULT late=$late points=$points cancellation=$cancel physicalReopen=true retry=true")
                }
            } finally { deleteTempSqliteNowTestDbArtifacts(path) }
        }
    }

    private enum class Point { PREPARE, STEP, RESET, CLEAR, CLOSE }
    private class Fault(val point: Point) : RuntimeException("index-accounting-$point")
    private inner class IndexProbe(val rows: Int, val afterIndexEntry: ((SQLiteConnection) -> Unit)? = null,
        val points: Set<Point> = emptySet(), val late: Boolean = false, val cancel: Boolean = false,
        val cancelAfterEdge: Int? = null) {
        lateinit var raw: SQLiteConnection
        var mutated = false
        var edges = 0
        var phase = ""
        val metadataEdges = mutableListOf<Int>()
        val edgeCancellation = kotlinx.coroutines.CancellationException("cancel after indexed edge")
        val reached = mutableSetOf<Point>()
        val failures = Point.entries.associateWith { if (it == Point.STEP && cancel) kotlinx.coroutines.CancellationException("index accounting cancelled") else Fault(it) }
        val live = IdentityHashMap<SQLiteStatement, String>()
        private var active: SQLiteStatement? = null
        private fun inject(point: Point) { if (point in points && reached.add(point)) throw failures.getValue(point) }
        val operations = StatementCacheOperations(
            prepare = { db, sql ->
                if (accounting(sql) && !late) inject(Point.PREPARE)
                val delegate = db.prepare(sql)
                var savedPhase = ""
                val wrapped = object : SQLiteStatement by delegate {
                    override fun bindText(index: Int, value: String) {
                        if (sql.startsWith("INSERT INTO _sync_apply_plan") && index == 3) savedPhase = value
                        delegate.bindText(index, value)
                    }
                    override fun step(): Boolean {
                        if (accounting(sql)) {
                            metadataEdges += edges
                            if (phase == "index" && (!late || edges in 1 until rows)) { active = this; inject(Point.STEP) }
                        }
                        val result = delegate.step()
                        if (sql.startsWith("INSERT OR IGNORE INTO _sync_apply_edges")) {
                            edges++
                            if (edges == cancelAfterEdge) throw edgeCancellation
                        }
                        if (savedPhase.isNotEmpty()) {
                            phase = savedPhase
                            if (phase == "index" && !mutated && afterIndexEntry != null) { mutated = true; afterIndexEntry.invoke(raw) }
                        }
                        return result
                    }
                }
                live[wrapped] = sql
                wrapped
            },
            reset = { it.reset(); if (it === active) inject(Point.RESET) },
            clearBindings = { it.clearBindings(); if (it === active) inject(Point.CLEAR) },
            close = { val sql = live.remove(it).orEmpty(); it.close(); if (accounting(sql) && (!late || active != null)) inject(Point.CLOSE) },
        )
    }
    private fun execute(raw: SQLiteConnection, sql: String) {
        val statement = raw.prepare(sql)
        try { statement.step() } finally { statement.close() }
    }

    /** Independent simulation of the existing preallocation checks, not observed output. */
    private fun indexingPeak(rows: Int, external: WorkspaceCharge): WorkspaceCharge {
        var cursor = ""
        var edges = 0L
        var edgeBytes = 0L
        var entries = 0L
        var bytes = 0L
        fun visit(extraEntries: Long = 0, extraBytes: Long = 0) {
            entries = maxOf(entries, external.entries + 1 + edges + extraEntries)
            bytes = maxOf(bytes, external.bytes + edgeBytes + 512 + 6 * (128 + utf8Size(cursor)) + extraBytes)
        }
        visit()
        for (key in listOf("Q", "W") + (0 until rows).map { "Z${it.toString().padStart(5, '0')}" }) {
            visit(3, 18 * utf8Size(key) + 128)
            cursor = key
            visit()
            if (key.startsWith("Z")) {
                val edge = 64 + 6 * (utf8Size("nodes") * 2 + utf8Size("Q") + utf8Size(key))
                visit(7, 6 * edge)
                edges++; edgeBytes += edge
            }
            visit()
        }
        cursor = ""; visit()
        return WorkspaceCharge(entries, bytes)
    }

    private fun configuration(f: Fixture): OversqliteConfig = field(f.client, "config")
    private inline fun <reified T> field(instance: Any, name: String): T =
        instance.javaClass.getDeclaredField(name).let { it.isAccessible = true; it.get(instance) as T }

    /** A private-plan-only writer; cleanup restores the entire fixture after each attempt. */
    private suspend fun prepareIndex(f: Fixture, config: OversqliteConfig, cached: Boolean,
        kind: String = "pull", discover: Boolean = true, probe: IndexProbe = IndexProbe(0)): ApplyStatementObservation {
        val state = field<OversqliteClientSessionState>(f.client, "sessionState").requireConnectedRuntimeState("index fixture")
        val owner = OversqliteClientOperationController(config, OversqliteClientResourceOwner(f.db, f.http), SnapshotDiagnosticsState()) {}
        val survivors = OversqliteApplySurvivors(f.db, field(f.client, "localStore"), config, owner, Json)
        probe.raw = field(f.db, "ref")
        val observed = ApplyStatementObservation.install(f.db)
        try {
            f.db.transaction(TransactionMode.IMMEDIATE) {
                f.db.execSQL("PRAGMA defer_foreign_keys = ON")
                val cache = StatementCache(f.db, probe.operations)
                var primary: Throwable? = null
                try {
                    survivors.prepare(state, indexUnit, 0, operationKind = kind,
                        workspaceStatementCache = if (cached) cache else null, replayStatementCache = cache,
                        sourceSnapshotId = if (kind == "snapshot") "fixture" else "", discoverDependents = discover) { error("no roots") }
                    survivors.finish(replayStatementCache = cache)
                } catch (error: Throwable) { primary = error; throw error }
                finally {
                    try { cache.close() } catch (cleanup: Throwable) {
                        if (primary == null) throw cleanup
                        attachSuppressedOnce(primary, cleanup)
                    }
                }
            }
        } finally { observed.restore(); observed.assertClosed() }
        return observed
    }
}
