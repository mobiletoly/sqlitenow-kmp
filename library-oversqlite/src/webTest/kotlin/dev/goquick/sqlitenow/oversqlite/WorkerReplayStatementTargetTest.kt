package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteStatement
import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.worker.sqliteWorkerConnectionProvider
import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.*
import kotlin.random.Random
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

/** Proves test targeting on existing cache/executor ownership, before settlement activation. */
internal class WorkerReplayStatementTargetTest {
    @Test fun planSaveHandleSelection() = targetCase("plan-save", false)
    @Test fun captureSizeHandleSelection() = targetCase("capture-size", false)
    @Test fun planSaveDeferredCloseHasOwnershipWitness() = targetCase("plan-save", true)
    @Test fun captureSizeDeferredCloseHasOwnershipWitness() = targetCase("capture-size", true)

    private fun targetCase(family: String, closeFault: Boolean) = runTest(timeout = 180.seconds) {
        for (invalid in listOf("\"unrecognized\"", "null", "7")) {
            assertFails("unknown families must not silently select workspace") {
                w1Configure("""{"targetFamily":$invalid}""")
            }
        }
        w1Configure(buildJsonObject {
            put("targetFamily", family)
            if (closeFault) put("holdCommand", "closeStatement")
        }.toString())
        val name = "replay-target-${Random.nextLong().toString().replace('-', 'n')}"
        val db = sqliteWorkerConnectionProvider(w1WorkerModuleUrl(if (closeFault) "close-before" else "normal"))
            .openConnection(name, false, SqliteConnectionConfig())
        var failure: Throwable? = null
        var beforeClose: JsonObject? = null
        var afterClose: JsonObject? = null
        var initial: JsonObject? = null
        var expected: JsonObject? = null
        var durable: JsonObject? = null
        var preparations = 0
        var executions = 0
        try {
            db.execSQL("CREATE TABLE _sync_apply_state(singleton_key INTEGER PRIMARY KEY, apply_mode INTEGER NOT NULL)")
            db.execSQL("INSERT INTO _sync_apply_state VALUES(1,0)")
            db.execSQL(applyPlanDdl)
            db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id))")
            db.execSQL("INSERT INTO nodes VALUES('P','old',NULL)")
            val info = TableInfoCache().get(db, "nodes")
            val actualCaptureSql = ReplayTableSql("nodes", info, "id", mapOf("nodes" to info)).captureSize
            assertEquals(captureSizeSql, actualCaptureSql, "independent pinned capture SQL matches current owner")
            val sql = if (family == "plan-save") planSaveSql else actualCaptureSql
            initial = stateVector(db)
            val executor = OversqliteApplyExecutor(db, OversqliteApplyStateStore(db))
            executor.faultInjector = SnapshotApplyFaultInjector(
                afterStatementCacheClosed = { w1Mark("executorAfterClose") },
                beforeCommit = { w1Mark("executorBeforeCommit") },
                statementCacheOperations = StatementCacheOperations(prepare = { connection, text ->
                    if (text == sql) preparations++
                    connection.prepare(text)
                }),
            )
            w1Mark("armCommittedSettlement")
            suspend fun invoke() {
                try {
                    executor.inApplyModeTransaction(diagnosticState()) { cache ->
                        // A workspace lookalike must not steal either new target family.
                        db.withPreparedStatement("/* w1-workspace */ SELECT 1") { assertTrue(it.step()) }
                        var first: SQLiteStatement? = null
                        for (version in 1..2) {
                            db.execSQL("UPDATE nodes SET name='${if (version == 1) "first" else "changed"}' WHERE id='P'")
                            val statement = cache.get(sql)
                            if (version == 1) first = statement else assertSame(first, statement)
                            if (family == "plan-save") {
                                bindPlan(statement, version)
                                assertFalse(statement.step(), "actual UPSERT returns DONE, not a SELECT row")
                            } else {
                                statement.bindText(1, "P")
                                assertTrue(statement.step(), "actual capture-size SELECT returns a row")
                                // P is one UTF-8 byte, names are five/seven, parent_id is NULL.
                                val nameBytes = if (version == 1) 5 else 7
                                val expectedBound = 2L + 92 + (6 * 1 + 2) + 104 + (6 * nameBytes + 2) + 134 + 32
                                assertEquals(expectedBound, statement.getLong(0))
                                // Release a SELECT after its first row, before a second step reaches DONE.
                            }
                            executions++
                            cache.release(statement)
                            if (family == "plan-save") assertEquals(planValues(version), readPlan(db))
                        }
                        expected = stateVector(db).let { vector ->
                            buildJsonObject { vector.forEach { (key, value) -> put(key, if (key == "applyMode") JsonPrimitive(0) else value) } }
                        }
                        w1Mark("successfulTargetBody")
                    }
                    w1Mark("invocationSuccess")
                } catch (error: Throwable) {
                    failure = error
                    w1Mark("invocationFailure")
                }
            }
            if (closeFault) {
                val invocation = launch { invoke() }
                try {
                    w1AwaitHeld()
                    val heldEvents = events(snapshot())
                    assertFalse(invocation.isCompleted, "dispatch callback cannot complete the held native failure")
                    assertTrue(heldEvents.single { it.text("phase") == "finalizeBeforeFault" }
                        .getValue("selectedNativeAlive").jsonPrimitive.boolean)
                    assertTrue(heldEvents.any { it.text("phase") == "executorAfterClose" })
                    w1ReleaseHeld()
                    invocation.join()
                } finally { invocation.cancelAndJoin() }
            } else invoke()
            beforeClose = snapshot()
        } finally {
            withContext(NonCancellable) {
                try {
                    w1Mark("ownerShutdownEnter")
                    db.close()
                    afterClose = snapshot()
                    val reopened = sqliteWorkerConnectionProvider().openConnection(name, false, SqliteConnectionConfig())
                    try { durable = stateVector(reopened) } finally { reopened.close() }
                } finally { cleanupDirectWorkerTestDatabase(name) }
            }
        }
        val before = assertNotNull(beforeClose)
        val after = assertNotNull(afterClose)
        val trace = events(before)
        val selected = trace.single { it.text("phase") == "selected" }
        val selectedId = selected.getValue("statementId").jsonPrimitive.int
        fun phase(value: String) = trace.indexOfFirst { it.text("phase") == value }
        fun dispatch(command: String) = trace.indexOfFirst {
            it.text("phase") == "dispatch" && it.text("command") == command &&
                it["statementId"]?.jsonPrimitive?.intOrNull == selectedId
        }
        fun acknowledgement(command: String): Int {
            val request = trace[dispatch(command)].getValue("id").jsonPrimitive.int
            return trace.indexOfFirst { it.text("phase") == "dispatch" && it.text("kind") == "acknowledge" &&
                it["id"]?.jsonPrimitive?.intOrNull == request }
        }
        val receipt = buildJsonObject {
            put("family", family); put("closeFault", closeFault)
            put("preparations", preparations); put("executions", executions)
            put("beforeShutdown", before); put("afterShutdown", after)
            put("durable", durable ?: JsonNull)
            put("failure", failure?.toString()?.let(::JsonPrimitive) ?: JsonNull)
        }.toString().chunked(4000)
        receipt.forEachIndexed { index, chunk -> println("REPLAY_TARGET_CHUNK ${index + 1}/${receipt.size} $chunk") }

        assertEquals("browser-worker", before.text("runtime"), "this prerequisite requires durable browser evidence")
        assertEquals(1, preparations); assertEquals(2, executions)
        assertEquals(family, selected.text("targetFamily"))
        assertEquals(if (family == "plan-save") planSaveSql else captureSizeSql, selected.text("sql"))
        assertTrue(phase("settlementArmed") < phase("selected"))
        assertTrue(dispatch("reset") >= 0)
        assertTrue(dispatch("clearBindings") > dispatch("reset"))
        assertTrue(dispatch("closeStatement") > dispatch("clearBindings"))
        assertTrue(phase("executorAfterClose") > dispatch("closeStatement"))
        assertTrue(acknowledgement("closeStatement") > phase("executorAfterClose"), "native close ACK follows local close callback")
        assertEquals(1, after.getValue("diagnostics").jsonObject.getValue("terminationConfirmed").jsonPrimitive.int)
        if (closeFault) {
            assertNotNull(failure)
            assertTrue(failureGraph(failure!!).any { it.message.orEmpty().contains("finalize before native call") })
            assertTrue(phase("terminationConfirmed") in (phase("finalizeBeforeFault") + 1) until phase("invocationFailure"))
            assertTrue(phase("selectedResponseHeld") < phase("selectedResponseReleased"))
            assertEquals(0, trace.count { it.text("phase") == "nativeFinalizeReturn" })
            assertTrue(trace.none { it.text("phase") == "dispatch" &&
                it.text("sql")?.let { sql -> sql in setOf("COMMIT", "ROLLBACK") || sql.contains("pragma_foreign_key_check") } == true },
                "fenced owner must not dispatch FK/commit/rollback after failed close")
            assertTrue(phase("executorBeforeCommit") < 0)
            assertEquals(initial, durable, "uncommitted plan/business writes roll back on confirmed worker termination")
        } else {
            assertNull(failure)
            assertEquals(1, trace.count { it.text("phase") == "nativeFinalizeReturn" })
            assertTrue(phase("nativeFinalizeReturn") < acknowledgement("closeStatement"))
            assertTrue(phase("executorBeforeCommit") > acknowledgement("closeStatement"))
            assertTrue(trace.any { it.text("phase") == "dispatch" && it.text("sql") == "COMMIT" })
            assertEquals(expected, durable, "independent reopened plan/business vector matches committed values")
        }
    }

    private fun planValues(version: Int): JsonArray = Json.parseToJsonElement(
        """["unit-$version","guard","ready",$version,2,3,4,${if (version == 1) "null" else "\"cursor-2\""},5,6,7,8,"settlement","main","nodes","P",9,1]""",
    ).jsonArray

    private fun bindPlan(statement: SQLiteStatement, version: Int) {
        planValues(version).forEachIndexed { index, value ->
            when {
                value == JsonNull -> statement.bindNull(index + 1)
                value.jsonPrimitive.isString -> statement.bindText(index + 1, value.jsonPrimitive.content)
                else -> statement.bindLong(index + 1, value.jsonPrimitive.long)
            }
        }
    }

    private suspend fun readPlan(db: SafeSQLiteConnection): JsonElement = db.withPreparedStatement(
        "SELECT json_array(unit,guard,phase,root_cursor,visit_cursor,table_cursor,fk_cursor,row_cursor,entries,bytes,edge_entries,edge_bytes,operation_kind,preserve_schema,preserve_table,preserve_key,atomic_work,preservation_complete) FROM _sync_apply_plan WHERE singleton_key=1",
    ) { if (it.step()) Json.parseToJsonElement(it.getText(0)) else JsonNull }

    private suspend fun stateVector(db: SafeSQLiteConnection): JsonObject = buildJsonObject {
        put("plan", readPlan(db))
        put("nodes", db.withPreparedStatement("SELECT json_array(id,name,parent_id) FROM nodes ORDER BY id") {
            assertTrue(it.step()); Json.parseToJsonElement(it.getText(0))
        })
        put("applyMode", db.withPreparedStatement("SELECT apply_mode FROM _sync_apply_state WHERE singleton_key=1") {
            assertTrue(it.step()); it.getLong(0)
        })
    }

    private fun snapshot(): JsonObject = Json.parseToJsonElement(w1Snapshot()).jsonObject
    private fun events(snapshot: JsonObject) = snapshot.getValue("events").jsonArray.map { it.jsonObject }
    private fun JsonObject.text(key: String): String? = this[key]?.jsonPrimitive?.contentOrNull
    private fun failureGraph(error: Throwable): List<Throwable> {
        val found = mutableListOf<Throwable>()
        fun visit(item: Throwable) {
            if (found.any { it === item }) return
            found += item
            item.cause?.let(::visit)
            item.suppressedExceptions.forEach(::visit)
        }
        visit(error)
        return found
    }

    private fun diagnosticState(): RuntimeState {
        val generation = SyncGeneration("1", "replay-target-fixture", emptyList())
        val validated = ValidatedConfig(generation, emptyList(), emptyMap(), emptyMap(), emptyMap(), emptyMap())
        return RuntimeState(validated, AdmittedOversqliteContract("v1", generation, testWireLimits(), false), "target", "target")
    }

    // Independent pinned SQL: do not regenerate from candidate observations.
    private val planSaveSql = """INSERT INTO _sync_apply_plan(singleton_key,unit,guard,phase,root_cursor,visit_cursor,table_cursor,fk_cursor,row_cursor,entries,bytes,edge_entries,edge_bytes,operation_kind,preserve_schema,preserve_table,preserve_key,atomic_work,preservation_complete)
        VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(singleton_key) DO UPDATE SET unit=excluded.unit,guard=excluded.guard,
        phase=excluded.phase,root_cursor=excluded.root_cursor,visit_cursor=excluded.visit_cursor,
        table_cursor=excluded.table_cursor,fk_cursor=excluded.fk_cursor,row_cursor=excluded.row_cursor,
        entries=excluded.entries,bytes=excluded.bytes,edge_entries=excluded.edge_entries,edge_bytes=excluded.edge_bytes,
        operation_kind=excluded.operation_kind,preserve_schema=excluded.preserve_schema,
        preserve_table=excluded.preserve_table,preserve_key=excluded.preserve_key,
        atomic_work=excluded.atomic_work,preservation_complete=excluded.preservation_complete"""
    private val captureSizeSql = """SELECT 2+92+CASE typeof("live_row"."id") WHEN 'blob' THEN 2*length("live_row"."id")+2 WHEN 'text' THEN 6*length(CAST("live_row"."id" AS BLOB))+2 ELSE 32 END+104+CASE typeof("live_row"."name") WHEN 'blob' THEN 2*length("live_row"."name")+2 WHEN 'text' THEN 6*length(CAST("live_row"."name" AS BLOB))+2 ELSE 32 END+134+CASE typeof("live_row"."parent_id") WHEN 'blob' THEN 2*length("live_row"."parent_id")+2 WHEN 'text' THEN 6*length(CAST("live_row"."parent_id" AS BLOB))+2 ELSE 32 END FROM "nodes" live_row WHERE "id"=?"""
}
