package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.DatabaseMigrations
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowDatabase
import io.ktor.client.HttpClient
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

/** Actual settlement statements; synchronous operation faults are distinct from worker faults. */
internal class ReplayStatementCleanupTest : SurvivorTestSupport() {
    private enum class Target { PLAN_SAVE, CAPTURE_SIZE }
    private enum class Body { SUCCESS, FAILURE, CANCEL }
    private enum class Cleanup { NONE, RESET, CLEAR, RESET_AND_CLEAR, CLOSE, PREPARE }

    @Test fun planSaveReleaseFailuresPreserveBodyPriority() = releaseCases(Target.PLAN_SAVE)
    @Test fun captureSizeReleaseFailuresPreserveBodyPriority() = releaseCases(Target.CAPTURE_SIZE)
    @Test fun planSaveCloseFailurePreservesBodyPriorityAndRollsBackWrites() = closeCases(Target.PLAN_SAVE)
    @Test fun captureSizeCloseFailurePreservesBodyPriorityAndRollsBackWrites() = closeCases(Target.CAPTURE_SIZE)
    @Test fun planSaveFailedPreparationIsNotRetained() = failureCase(Target.PLAN_SAVE, Cleanup.PREPARE)
    @Test fun captureSizeFailedPreparationIsNotRetained() = failureCase(Target.CAPTURE_SIZE, Cleanup.PREPARE)

    @Test fun successfulCleanupClosesBothSelectedStatementFamilies() = runOwnedWebDatabaseTest(timeout = 180.seconds) {
        for (target in Target.entries) withLostReplay { server, db, _, client ->
            val probe = CleanupProbe(target, Cleanup.NONE, Body.SUCCESS)
            client.setSnapshotApplyFaultInjectorForTest(probe.injector())
            try { client.pushPending().getOrThrow() }
            finally { client.setSnapshotApplyFaultInjectorForTest(null) }
            probe.assertReached()
            assertTrue(probe.steps > 1, "the selected handle is reused within actual settlement: $target")
            assertEquals(probe.steps, probe.resets)
            assertEquals(probe.steps, probe.clears)
            assertEquals(1, probe.closes)
            assertTrue(probe.beforeCommit)
            assertSettled(server, db)
            println("REPLAY_CLEANUP reached target=$target cleanup=NONE body=SUCCESS")
        }
    }

    @Test fun naturalPendingPlanSaveClosesAndDurablyResumes() = pendingCase(Target.PLAN_SAVE, failClose = false)
    @Test fun naturalPendingCaptureSizeClosesAndDurablyResumes() = pendingCase(Target.CAPTURE_SIZE, failClose = false)
    @Test fun naturalPendingPlanSaveCloseFailurePreventsProgressCommit() = pendingCase(Target.PLAN_SAVE, failClose = true)
    @Test fun naturalPendingCaptureSizeCloseFailurePreventsProgressCommit() = pendingCase(Target.CAPTURE_SIZE, failClose = true)

    @Test fun actualPlanUpsertAbortKeepsExecutionFailurePrimaryAndRollsBackOnReopen() = runOwnedWebDatabaseTest(timeout = 180.seconds) {
        val path = createTempSqliteNowTestDbPath("replay-plan-abort")
        try {
            withLostReplay(path) { server, db, http, client ->
                val before = completeVector(db)
                db.execSQL("CREATE TEMP TRIGGER abort_replay_plan BEFORE INSERT ON _sync_apply_plan BEGIN SELECT RAISE(ABORT,'replay-plan-write-abort'); END")
                val probe = CleanupProbe(Target.PLAN_SAVE, Cleanup.NONE, Body.SUCCESS)
                val injector = probe.injector()
                val operations = assertNotNull(injector.statementCacheOperations)
                var beforeWriterMutation: BaselineQualificationVector? = null
                client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(
                    beforeCommit = injector.beforeCommit,
                    statementCacheOperations = StatementCacheOperations(
                        prepare = { connection, sql ->
                            // The first cached preparation precedes apply_mode and replay writes.
                            // Request/response buffer reservations have already committed separately.
                            if (beforeWriterMutation == null) beforeWriterMutation = completeVector(connection)
                            operations.prepare(connection, sql)
                        },
                        reset = operations.reset,
                        clearBindings = operations.clearBindings,
                        close = operations.close,
                    ),
                ))
                val failure = assertNotNull(invoke(client))
                client.setSnapshotApplyFaultInjectorForTest(null)
                assertTrue(failureGraph(failure).any { it.message.orEmpty().contains("replay-plan-write-abort") }, failure.toString())
                assertSame(originalFailure(assertNotNull(probe.executionFailure)), originalFailure(failure))
                assertEquals(1, probe.preparations)
                assertEquals(1, probe.steps)
                assertEquals(1, probe.resets, "reset is attempted after a real SQLite UPSERT abort")
                assertEquals(1, probe.clears, "clear is attempted even when native reset reports the execution error")
                assertEquals(1, probe.closes)
                assertEquals(0, probe.workAfterFailedRelease)
                assertFalse(probe.beforeCommit)
                recoverFatalReplay(path, server, db, http, client, before, assertNotNull(beforeWriterMutation))
                println("REPLAY_CLEANUP reached target=PLAN_SAVE actual_sqlite_abort=true durable_rollback=true")
            }
        } finally { deleteTempSqliteNowTestDbArtifacts(path) }
    }

    private suspend fun recoverFatalReplay(path: String, server: MockSyncServer, db: SafeSQLiteConnection,
        http: HttpClient, client: DefaultOversqliteClient, before: BaselineQualificationVector,
        beforeWriterMutation: BaselineQualificationVector) {
        val reservationFields = setOf("workspace_context", "reserved_entries", "reserved_bytes")
        fun protectedVector(vector: BaselineQualificationVector) = vector.tables.mapValues { (table, rows) ->
            if (table != "_sync_operation_state") rows else rows.map { row -> JsonObject(row.filterKeys { it !in reservationFields }) }
        }
        assertEquals(protectedVector(before), protectedVector(beforeWriterMutation),
            "the independently captured writer-entry preimage differs only by request buffer ownership")
        val owner = beforeWriterMutation.rows("_sync_operation_state").single()
        val ledgerText = assertNotNull(owner.text("workspace_context"))
        val ledger = assertIs<JsonArray>(json.parseToJsonElement(ledgerText))
        assertEquals(1, ledger.size, "this public replay invocation owns exactly one retained buffer reservation")
        val reservation = ledger.single().jsonObject
        assertEquals(setOf("token", "unit", "context", "entries", "bytes"), reservation.keys)
        for (field in listOf("token", "unit", "context")) {
            assertTrue(reservation.getValue(field).jsonPrimitive.isString)
            assertTrue(reservation.getValue(field).jsonPrimitive.content.matches(Regex("[0-9a-f]{64}")))
        }
        val source = assertNotNull(before.rows("_sync_attachment_state").single().text("current_source_id"))
        assertEquals(applyUnit("push-buffers", 0, source), reservation.getValue("unit").jsonPrimitive.content)
        assertFalse(reservation.getValue("entries").jsonPrimitive.isString)
        assertFalse(reservation.getValue("bytes").jsonPrimitive.isString)
        val entries = reservation.getValue("entries").jsonPrimitive.long
        val bytes = reservation.getValue("bytes").jsonPrimitive.long
        assertEquals(entries, assertNotNull(owner.text("reserved_entries")).toLong())
        assertEquals(bytes, assertNotNull(owner.text("reserved_bytes")).toLong())
        // reserve() adds its own one-entry/4096-byte ledger owner to the public 64/256KiB floor.
        assertTrue(entries >= 65L && bytes >= 256L * 1024 + 4096)
        assertEquals("0", before.rows("_sync_operation_state").single().text("reserved_entries"))
        assertEquals("0", before.rows("_sync_operation_state").single().text("reserved_bytes"))
        assertEquals("", before.rows("_sync_operation_state").single().text("workspace_context"))

        client.closeFromCoordinatedOwner()
        db.close()
        val ordinaryDb = newFileBackedDb(path)
        try {
            assertEquals(beforeWriterMutation, completeVector(ordinaryDb),
                "fatal cleanup rolls back all writer changes, retaining the exact prewriter reservation token/context/charges")
            val ordinary = newClient(ordinaryDb, http, syncTables = nodeTables)
            try {
                ordinary.openAndConnect("user-1").getOrThrow()
                assertEquals(beforeWriterMutation, completeVector(ordinaryDb),
                    "ordinary client opening is not proof that a reservation owner was abandoned")
            } finally { ordinary.closeFromCoordinatedOwner() }
        } finally { ordinaryDb.close() }

        // The existing coordinated-opening contract alone acquires the provider lifetime lease.
        val database = SqliteNowDatabase(path, object : DatabaseMigrations {
            override val targetVersion = 0
            override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int) = 0
        }, syncManaged = true)
        val opening = database.openForSync(OversqliteConfig(
            generationId = 1, databaseId = "fixture", serviceId = "main", syncTables = nodeTables,
        ), http)
        try {
            assertEquals(OversqliteOpeningOutcome.READY, opening.advance().outcome)
            val ready = assertNotNull(opening.readyOrNull())
            val recovered = database.connection()
            assertEquals(before, completeVector(recovered),
                "authorized lifetime recovery reclaims only the orphan reservation and restores the complete preinvocation vector")
            ready.client.openAndConnect("user-1").getOrThrow()
            ready.client.pushPending().getOrThrow()
            assertSettled(server, recovered)
        } finally { opening.close() }
    }

    private fun releaseCases(target: Target) = runOwnedWebDatabaseTest(timeout = 180.seconds) {
        for (cleanup in listOf(Cleanup.RESET, Cleanup.CLEAR, Cleanup.RESET_AND_CLEAR)) {
            for (body in Body.entries) failedSettlement(target, cleanup, body)
        }
    }

    private fun closeCases(target: Target) = runOwnedWebDatabaseTest(timeout = 180.seconds) {
        for (body in Body.entries) failedSettlement(target, Cleanup.CLOSE, body)
    }

    private fun failureCase(target: Target, cleanup: Cleanup) = runOwnedWebDatabaseTest(timeout = 180.seconds) {
        failedSettlement(target, cleanup, Body.SUCCESS)
    }

    private suspend fun failedSettlement(target: Target, cleanup: Cleanup, body: Body) {
        val path = createTempSqliteNowTestDbPath("replay-statement-cleanup")
        try {
            withLostReplay(path) { server, db, http, client ->
                val before = completeVector(db)
                val frozen = server.pushBodies.last()
                val probe = CleanupProbe(target, cleanup, body)
                client.setSnapshotApplyFaultInjectorForTest(probe.injector())
                val failure = assertNotNull(invoke(client))
                client.setSnapshotApplyFaultInjectorForTest(null)
                probe.assertFailure(failure)
                assertEquals(frozen, server.pushBodies.last(), "failed replay retains the exact frozen request")
                assertEquals(3, server.pushRequestCount, "seed, committed loss, failed replay")
                assertFalse(probe.beforeCommit, "cleanup/body failure cannot pass the settlement commit barrier")
                reopenAndRetry(path, server, db, http, client, before)
                println("REPLAY_CLEANUP reached target=$target cleanup=$cleanup body=$body durable_rollback=true")
            }
        } finally { deleteTempSqliteNowTestDbArtifacts(path) }
    }

    // The first bounded slice commits after release and close. A caller cancellation
    // before the next commit exposes its durable cursor without returning a work yield.
    private fun pendingCase(target: Target, failClose: Boolean) = runOwnedWebDatabaseTest(timeout = 180.seconds) {
        val path = createTempSqliteNowTestDbPath("replay-natural-progress")
        try {
            withLostReplay(path) { server, db, http, seed ->
                // Newer uploaded-row intent makes the root seed capture physical P before the long index walk.
                db.execSQL("UPDATE nodes SET name='newer parent' WHERE id='P'")
                db.execSQL("UPDATE _sync_apply_state SET apply_mode=1")
                repeat(128) { db.execSQL("INSERT INTO nodes VALUES('Z$it','unrelated',NULL)") }
                db.execSQL("UPDATE _sync_apply_state SET apply_mode=0")
                seed.closeFromCoordinatedOwner()
                val limited = newClient(db, http, syncTables = nodeTables, workUnits = 1800)
                try {
                    limited.openAndConnect("user-1").getOrThrow()
                    val before = completeVector(db)
                    val probe = CleanupProbe(target, if (failClose) Cleanup.CLOSE else Cleanup.NONE, Body.SUCCESS)
                    val injector = probe.injector()
                    var commits = 0
                    val operations = assertNotNull(injector.statementCacheOperations)
                    limited.setSnapshotApplyFaultInjectorForTest(if (failClose) injector else SnapshotApplyFaultInjector(
                        beforeCommit = {
                            if (++commits == 2) throw CancellationException("cancel after the first committed slice")
                            injector.beforeCommit?.invoke()
                        },
                        statementCacheOperations = StatementCacheOperations(
                            prepare = { connection, sql ->
                                if (commits == 0) operations.prepare(connection, sql) else connection.prepare(sql)
                            },
                            reset = operations.reset, clearBindings = operations.clearBindings, close = operations.close,
                        ),
                    ))
                    val failure = assertNotNull(invoke(limited))
                    limited.setSnapshotApplyFaultInjectorForTest(null)
                    probe.assertReached()
                    assertTrue(probe.steps > 0)
                    assertEquals(probe.steps, probe.resets)
                    assertEquals(probe.steps, probe.clears)
                    assertEquals(1, probe.closes)
                    assertEquals(0, probe.workAfterFailedRelease)
                    val durable = if (failClose) {
                        assertSame(probe.closeFailure, originalFailure(failure))
                        assertFalse(probe.beforeCommit)
                        assertEquals(before, completeVector(db), "failed close rolls back useful staged progress")
                        before
                    } else {
                        assertIs<CancellationException>(failure)
                        assertEquals(2, commits)
                        assertTrue(probe.beforeCommit, "ordinary pending staging passes the existing commit barrier")
                        assertTrue(scalarLong(db, "SELECT count(*) FROM _sync_apply_rows") > 0)
                        assertTrue(scalarLong(db, "SELECT root_cursor FROM _sync_apply_plan") > 0)
                        assertEquals("index", scalarText(db, "SELECT phase FROM _sync_apply_plan"))
                        val current = completeVector(db)
                        val progressTables = setOf("_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges")
                        assertEquals(before.tables.filterKeys { it !in progressTables }, current.tables.filterKeys { it !in progressTables },
                            "only useful apply staging changes before natural work exhaustion")
                        current
                    }
                    assertEquals(0L, scalarLong(db, "SELECT apply_mode FROM _sync_apply_state"))
                    limited.closeFromCoordinatedOwner()
                    db.close()
                    val reopened = newFileBackedDb(path)
                    try {
                        assertEquals(durable, completeVector(reopened))
                        val retry = newClient(reopened, http, syncTables = nodeTables)
                        try {
                            retry.openAndConnect("user-1").getOrThrow()
                            val fresh = CleanupProbe(target, Cleanup.NONE, Body.SUCCESS)
                            retry.setSnapshotApplyFaultInjectorForTest(fresh.injector())
                            retry.pushPending().getOrThrow()
                            fresh.assertReached()
                            assertEquals(1, fresh.closes)
                            assertNotSame(probe.selected, fresh.selected, "reopen uses a new transaction handle")
                            assertEquals("newer parent", scalarText(reopened, "SELECT name FROM nodes WHERE id='P'"))
                            assertEquals("Q", scalarText(reopened, "SELECT parent_id FROM nodes WHERE id='C'"))
                            assertEquals("C", scalarText(reopened, "SELECT parent_id FROM nodes WHERE id='D'"))
                            assertEquals(0L, scalarLong(reopened, "SELECT count(*) FROM _sync_apply_plan"))
                            assertEquals(0L, scalarLong(reopened, "SELECT count(*) FROM _sync_outbox_rows"))
                            assertEquals(2L, scalarLong(reopened, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                            assertEquals(server.pushBodies[1], server.pushBodies.last())
                        } finally { retry.closeFromCoordinatedOwner() }
                    } finally { reopened.close() }
                    println("REPLAY_CLEANUP reached target=$target natural_pending=true close_failure=$failClose durable_reopen=true")
                } finally { limited.closeFromCoordinatedOwner() }
            }
        } finally { deleteTempSqliteNowTestDbArtifacts(path) }
    }

    private suspend fun withLostReplay(
        path: String? = null,
        block: suspend (MockSyncServer, SafeSQLiteConnection, HttpClient, DefaultOversqliteClient) -> Unit,
    ) = withSurvivorFixture(dbPath = path) { server, db, http, client ->
        try {
            db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
            server.normalizePushEffects = { effects() }
            server.beforePushResponse = { throw CancellationException("lost committed response") }
            assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_apply_plan"))
            block(server, db, http, client)
        } finally { client.closeFromCoordinatedOwner() }
    }

    private suspend fun reopenAndRetry(path: String, server: MockSyncServer, db: SafeSQLiteConnection,
        http: HttpClient, client: DefaultOversqliteClient, expected: BaselineQualificationVector) {
        client.closeFromCoordinatedOwner()
        db.close()
        val reopened = newFileBackedDb(path)
        try {
            assertEquals(expected, completeVector(reopened), "all retained columns and business rows survive failed replay unchanged")
            val retry = newClient(reopened, http, syncTables = nodeTables)
            try {
                retry.openAndConnect("user-1").getOrThrow()
                retry.pushPending().getOrThrow()
                assertSettled(server, reopened)
            } finally { retry.closeFromCoordinatedOwner() }
        } finally { reopened.close() }
    }

    private suspend fun assertSettled(server: MockSyncServer, db: SafeSQLiteConnection) {
        requireSurvivors(db)
        assertEquals(server.pushBodies[1], server.pushBodies.last())
        assertEquals(3L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
        assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_apply_plan"))
        assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
    }

    private suspend fun completeVector(db: SafeSQLiteConnection): BaselineQualificationVector =
        baselineQualificationVector(db, listOf("nodes", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_apply_state"))

    private suspend fun invoke(client: DefaultOversqliteClient): Throwable? {
        var observed: Throwable? = null
        supervisorScope {
            launch {
                try { client.pushPending().getOrThrow() }
                catch (failure: Throwable) { observed = failure }
            }.join()
        }
        return observed
    }

    private inner class CleanupProbe(private val target: Target, private val cleanup: Cleanup, private val body: Body) {
        val bodyFailure = IllegalStateException("replay-body-failure")
        val cancellation = CancellationException("replay-caller-cancellation")
        val resetFailure = IllegalStateException("replay-reset-failure")
        val clearFailure = IllegalStateException("replay-clear-failure")
        val closeFailure = IllegalStateException("replay-close-after-native-failure")
        val prepareFailure = IllegalStateException("replay-prepare-failure")
        var selected: SQLiteStatement? = null
        var preparations = 0
        var steps = 0
        var resets = 0
        var clears = 0
        var closes = 0
        var beforeCommit = false
        var executionFailure: Throwable? = null
        var workAfterFailedRelease = 0
        private var failedRelease = false
        private val events = mutableListOf<String>()

        fun injector() = SnapshotApplyFaultInjector(
            beforeCommit = { beforeCommit = true },
            statementCacheOperations = StatementCacheOperations(
                prepare = { connection, sql ->
                    if (!selectedSql(target, sql)) connection.prepare(sql) else {
                        preparations++
                        events += "prepare"
                        if (cleanup == Cleanup.PREPARE) throw prepareFailure
                        val statement = connection.prepare(sql)
                        val caller = currentCoroutineContext().job
                        object : SQLiteStatement by statement {
                            private fun use() { if (failedRelease) workAfterFailedRelease++ }
                            override fun bindBlob(index: Int, value: ByteArray) { use(); statement.bindBlob(index, value) }
                            override fun bindDouble(index: Int, value: Double) { use(); statement.bindDouble(index, value) }
                            override fun bindLong(index: Int, value: Long) { use(); statement.bindLong(index, value) }
                            override fun bindText(index: Int, value: String) { use(); statement.bindText(index, value) }
                            override fun bindNull(index: Int) { use(); statement.bindNull(index) }
                            override fun step(): Boolean {
                                use(); steps++; events += "step"
                                val row = try { statement.step() } catch (failure: Throwable) {
                                    executionFailure = failure; throw failure
                                }
                                if (steps == 1) {
                                    assertEquals(target == Target.CAPTURE_SIZE, row, "real capture SELECT returns ROW; plan UPSERT returns DONE")
                                    when (body) {
                                        Body.SUCCESS -> Unit
                                        Body.FAILURE -> throw bodyFailure
                                        Body.CANCEL -> { caller.cancel(cancellation); throw cancellation }
                                    }
                                }
                                return row
                            }
                        }.also { selected = it }
                    }
                },
                reset = { statement ->
                    if (statement === selected) {
                        resets++; events += "reset"
                        try { statement.reset() } catch (failure: Throwable) { failedRelease = true; throw failure }
                        if (cleanup == Cleanup.RESET || cleanup == Cleanup.RESET_AND_CLEAR) {
                            failedRelease = true; throw resetFailure
                        }
                    } else statement.reset()
                },
                clearBindings = { statement ->
                    if (statement === selected) {
                        clears++; events += "clear"
                        statement.clearBindings()
                        if (cleanup == Cleanup.CLEAR || cleanup == Cleanup.RESET_AND_CLEAR) {
                            failedRelease = true; throw clearFailure
                        }
                    } else statement.clearBindings()
                },
                close = { statement ->
                    if (statement === selected) {
                        closes++; events += "close"
                        statement.close()
                        if (cleanup == Cleanup.CLOSE) throw closeFailure
                    } else statement.close()
                },
            ),
        )

        fun assertReached() {
            assertEquals(1, preparations, "one selected handle per owning settlement transaction: $target")
            assertNotNull(selected)
            assertTrue(steps > 0)
        }

        fun assertFailure(failure: Throwable) {
            assertEquals(1, preparations)
            if (cleanup == Cleanup.PREPARE) {
                assertSame(prepareFailure, originalFailure(failure))
                assertNull(selected)
                assertEquals(listOf("prepare"), events, "failed preparation never enters the owning cache")
                return
            }
            assertReached()
            assertEquals(1, closes)
            assertEquals(0, workAfterFailedRelease, "failed release cannot be followed by any selected bind or step")
            assertEquals("close", events.last())
            assertEquals(listOf("prepare", "step", "reset", "clear"), events.take(4))
            if (cleanup != Cleanup.CLOSE || body != Body.SUCCESS) {
                assertEquals(1, steps); assertEquals(1, resets); assertEquals(1, clears)
            } else {
                assertTrue(steps > 1, "successful body performs and reuses the real statement before close fails")
                assertEquals(steps, resets); assertEquals(steps, clears)
            }
            val cleanupFailure = when (cleanup) {
                Cleanup.RESET, Cleanup.RESET_AND_CLEAR -> resetFailure
                Cleanup.CLEAR -> clearFailure
                Cleanup.CLOSE -> closeFailure
                else -> error("not a cleanup failure")
            }
            val primary = when (body) { Body.SUCCESS -> cleanupFailure; Body.FAILURE -> bodyFailure; Body.CANCEL -> cancellation }
            assertSame(primary, originalFailure(failure))
            val graph = failureGraph(failure)
            assertEquals(1, graph.count { it === cleanupFailure }, "cleanup error occurs once in the failure graph")
            if (body != Body.SUCCESS) {
                assertEquals(listOf(cleanupFailure), primary.suppressedExceptions.map(::originalFailure))
            }
            if (cleanup == Cleanup.RESET_AND_CLEAR) {
                assertEquals(listOf(clearFailure), resetFailure.suppressedExceptions)
                assertEquals(1, graph.count { it === clearFailure })
            } else assertTrue(cleanupFailure.suppressedExceptions.isEmpty())
            graph.forEach { error ->
                error.suppressedExceptions.forEachIndexed { index, suppressed ->
                    assertTrue(error.suppressedExceptions.take(index).none { it === suppressed })
                }
            }
        }
    }

    private fun selectedSql(target: Target, sql: String): Boolean = when (target) {
        Target.PLAN_SAVE -> sql == planSaveSql
        Target.CAPTURE_SIZE -> sql == captureSizeSql
    }

    private fun originalFailure(error: Throwable): Throwable = generateSequence(error) { it.cause }.last()

    private fun failureGraph(error: Throwable, seen: MutableList<Throwable> = mutableListOf()): List<Throwable> {
        if (seen.any { it === error }) return emptyList()
        seen += error
        return listOf(error) + listOfNotNull(error.cause).flatMap { failureGraph(it, seen) } +
            error.suppressedExceptions.flatMap { failureGraph(it, seen) }
    }

    // Independent fixture SQL pins: do not derive these selectors from production SQL builders.
    private val planSaveSql = """INSERT INTO _sync_apply_plan(singleton_key,unit,guard,phase,root_cursor,visit_cursor,table_cursor,fk_cursor,row_cursor,entries,bytes,edge_entries,edge_bytes,operation_kind,preserve_schema,preserve_table,preserve_key,atomic_work,preservation_complete,source_snapshot_id)
        VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(singleton_key) DO UPDATE SET unit=excluded.unit,guard=excluded.guard,
        phase=excluded.phase,root_cursor=excluded.root_cursor,visit_cursor=excluded.visit_cursor,
        table_cursor=excluded.table_cursor,fk_cursor=excluded.fk_cursor,row_cursor=excluded.row_cursor,
        entries=excluded.entries,bytes=excluded.bytes,edge_entries=excluded.edge_entries,edge_bytes=excluded.edge_bytes,
        operation_kind=excluded.operation_kind,source_snapshot_id=excluded.source_snapshot_id,preserve_schema=excluded.preserve_schema,
        preserve_table=excluded.preserve_table,preserve_key=excluded.preserve_key,
        atomic_work=excluded.atomic_work,preservation_complete=excluded.preservation_complete"""

    private val captureSizeSql = "SELECT 2+92+CASE typeof(\"live_row\".\"id\") WHEN 'blob' THEN 2*length(\"live_row\".\"id\")+2 WHEN 'text' THEN 6*length(CAST(\"live_row\".\"id\" AS BLOB))+2 ELSE 32 END+104+CASE typeof(\"live_row\".\"name\") WHEN 'blob' THEN 2*length(\"live_row\".\"name\")+2 WHEN 'text' THEN 6*length(CAST(\"live_row\".\"name\" AS BLOB))+2 ELSE 32 END+134+CASE typeof(\"live_row\".\"parent_id\") WHEN 'blob' THEN 2*length(\"live_row\".\"parent_id\")+2 WHEN 'text' THEN 6*length(CAST(\"live_row\".\"parent_id\" AS BLOB))+2 ELSE 32 END FROM \"nodes\" live_row WHERE \"id\"=?"
}
