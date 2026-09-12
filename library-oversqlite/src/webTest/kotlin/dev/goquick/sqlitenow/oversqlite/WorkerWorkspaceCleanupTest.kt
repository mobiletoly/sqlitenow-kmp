package dev.goquick.sqlitenow.oversqlite

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

/** Diagnostic cache/executor ownership composition, never the future activated R1 settlement path. */
class WorkerWorkspaceCleanupTest {
    private enum class Body { SUCCESS, FAILURE, PENDING, CANCEL, FLUSH_CANCEL, CLOSE_FLUSH_CANCEL }

    @Test fun closeBeforeNativeFinalizationMustDisposeSelectedWorkspaceHandle() = case("close-before", Body.SUCCESS)
    @Test fun resetWithSuccess() = case("reset", Body.SUCCESS)
    @Test fun clearWithSuccess() = case("clear", Body.SUCCESS)
    @Test fun closeWithSuccess() = case("close-before", Body.SUCCESS)
    @Test fun resetWithBodyFailure() = case("reset", Body.FAILURE)
    @Test fun clearWithBodyFailure() = case("clear", Body.FAILURE)
    @Test fun closeWithBodyFailure() = case("close-before", Body.FAILURE)
    @Test fun resetWithPendingProgress() = case("reset", Body.PENDING)
    @Test fun clearWithPendingProgress() = case("clear", Body.PENDING)
    @Test fun closeWithPendingProgress() = case("close-before", Body.PENDING)
    @Test fun resetWithCancellation() = case("reset", Body.CANCEL)
    @Test fun clearWithCancellation() = case("clear", Body.CANCEL)
    @Test fun closeWithCancellation() = case("close-before", Body.CANCEL)
    @Test fun cancellationDuringSelectedFlush() = case("reset", Body.FLUSH_CANCEL)
    @Test fun cancellationDuringSelectedCloseFlush() = case("close-before", Body.CLOSE_FLUSH_CANCEL)
    @Test fun ordinaryFailureThenTerminalBarrier() = case("reset-terminal", Body.SUCCESS)
    @Test fun ordinaryCleanupFailureWithDeniedRollbackRequiresOwnedShutdown() = case("reset-rollback-denied", Body.SUCCESS)
    @Test fun successfulCleanupCommits() = case("normal", Body.SUCCESS)
    @Test fun pendingProgressWithSuccessfulCleanupCommitsThenThrows() = case("normal", Body.PENDING)
    @Test fun closeAfterNativeFinalization() = case("close-after", Body.SUCCESS)

    private fun case(scenario: String, body: Body) = runTest(timeout = 180.seconds) {
        val name = "w1-workspace-${Random.nextLong().toString().replace('-', 'n')}"
        val held = body == Body.FLUSH_CANCEL || body == Body.CLOSE_FLUSH_CANCEL
        w1Configure(buildJsonObject {
            if (held) put("holdCommand", if (body == Body.FLUSH_CANCEL) "reset" else "closeStatement")
            put("dropClear", scenario == "reset-terminal")
        }.toString())
        val db = sqliteWorkerConnectionProvider(w1WorkerModuleUrl(scenario))
            .openConnection(name, false, SqliteConnectionConfig())
        val bodyFailure = IllegalStateException("w1-body")
        var observed: Throwable? = null
        var ownerShutdownFailure: Throwable? = null
        var probeFailure: Throwable? = null
        var beforeShutdown: JsonObject? = null
        var afterShutdown: JsonObject? = null
        var durableValue: Long? = null
        var durableApplyMode: Long? = null
        val entered = CompletableDeferred<Unit>()
        try {
            db.execSQL("CREATE TABLE _sync_apply_state(singleton_key INTEGER PRIMARY KEY, apply_mode INTEGER NOT NULL)")
            db.execSQL("INSERT INTO _sync_apply_state VALUES(1, 0)")
            db.execSQL("CREATE TABLE w1_workspace(value INTEGER NOT NULL)")
            db.execSQL("INSERT INTO w1_workspace VALUES(7)")
            val executor = OversqliteApplyExecutor(db, OversqliteApplyStateStore(db))
            executor.faultInjector = SnapshotApplyFaultInjector(
                afterStatementCacheClosed = { w1Mark("executorAfterClose") },
                beforeCommit = { w1Mark("executorBeforeCommit") },
            )
            suspend fun invoke() {
                try {
                    executor.inApplyModeTransaction(diagnosticState()) { cache ->
                        db.execSQL("UPDATE w1_workspace SET value=8")
                        val selected = cache.get("/* w1-workspace */ SELECT value FROM w1_workspace")
                        assertTrue(selected.step()); assertEquals(8L, selected.getLong(0)); assertFalse(selected.step())
                        w1Mark("armSelectedCleanup")
                        cache.release(selected)
                        // Keep the terminal-order witness outside the executor's equal 5000ms cleanup budget.
                        if (scenario == "reset-terminal") db.execSQL("/* w1-prevented */ SELECT 1")
                        when (body) {
                            Body.FAILURE -> { w1Mark("bodyFailure"); throw bodyFailure }
                            Body.PENDING -> { w1Mark("pendingProgress"); throw ApplyProgressPending(SyncWorkLimitException("w1", 1, 2)) }
                            Body.CANCEL -> { w1Mark("bodyCancellation"); entered.complete(Unit); awaitCancellation() }
                            Body.FLUSH_CANCEL -> db.execSQL("/* w1-prevented */ SELECT 1")
                            else -> w1Mark("successfulBody")
                        }
                    }
                    w1Mark("invocationSuccess")
                } catch (failure: Throwable) {
                    observed = failure; w1Mark("invocationFailure")
                }
            }
            if (body == Body.CANCEL || held) {
                val invocation = launch { invoke() }
                try {
                    if (held) w1AwaitHeld() else entered.await()
                    w1Mark("callerCancelled")
                    invocation.cancel(CancellationException("w1-caller"))
                    if (held) w1ReleaseHeld()
                    invocation.join()
                } finally { invocation.cancelAndJoin() }
            } else invoke()
            w1Mark("postInvocationProbe")
            try { assertEquals(1L, scalar(db, "/* w1-after-failure-probe */ SELECT 1")) }
            catch (failure: Throwable) { probeFailure = failure }
            beforeShutdown = snapshot()
        } finally {
            withContext(NonCancellable) {
                try {
                    w1Mark("ownerShutdownEnter")
                    try { db.close() } catch (failure: Throwable) { ownerShutdownFailure = failure }
                    afterShutdown = snapshot()
                    // Node's explicit runtime kind is transient; it cannot supply durable reopen evidence.
                    if (afterShutdown!!.text("runtime") == "browser-worker") {
                        val reopened = sqliteWorkerConnectionProvider().openConnection(name, false, SqliteConnectionConfig())
                        try {
                            durableValue = scalar(reopened, "SELECT value FROM w1_workspace")
                            durableApplyMode = scalar(reopened, "SELECT apply_mode FROM _sync_apply_state WHERE singleton_key=1")
                        } finally { reopened.close() }
                    }
                } finally { cleanupDirectWorkerTestDatabase(name) }
            }
        }
        val before = assertNotNull(beforeShutdown); val after = assertNotNull(afterShutdown)
        val events = before.getValue("events").jsonArray.map { it.jsonObject }
        fun phase(name: String) = events.indexOfFirst { it.text("phase") == name }
        fun sent(sql: String) = events.filter {
            it.text("phase") == "dispatch" && it.text("command") == "prepare" && it.text("sql") == sql
        }
        val selected = events.single { it.text("phase") == "selected" }
        val selectedId = selected.getValue("statementId").jsonPrimitive.int
        fun dispatch(command: String) = events.indexOfFirst {
            it.text("phase") == "dispatch" && it.text("command") == command && it["statementId"]?.jsonPrimitive?.int == selectedId
        }
        fun acknowledgement(command: String): Int {
            val id = events[dispatch(command)].getValue("id").jsonPrimitive.int
            return events.indexOfFirst { it.text("phase") == "dispatch" && it.text("kind") == "acknowledge" && it["id"]?.jsonPrimitive?.int == id }
        }
        val success = scenario == "normal"
        val terminal = scenario.startsWith("close") || scenario == "reset-terminal"
        val deniedRollback = scenario == "reset-rollback-denied"
        val graph = observed?.let(::failureGraph).orEmpty()
        println("W1_COMPOSITION_RECEIPT " + buildJsonObject {
            put("scenario", scenario); put("body", body.name); put("beforeShutdown", before); put("afterShutdown", after)
            observed?.let { put("failure", failureTree(it)) }
            probeFailure?.let { put("probeFailure", failureTree(it)) }
            ownerShutdownFailure?.let { put("ownerShutdownFailure", failureTree(it)) }
            put("durableValue", JsonPrimitive(durableValue)); put("durableApplyMode", JsonPrimitive(durableApplyMode))
            put("durableReopenClassification", if (before.text("runtime") == "browser-worker") "fresh" else "not-applicable-transient-node")
        })

        assertTrue(dispatch("reset") >= 0); assertTrue(dispatch("clearBindings") > dispatch("reset"))
        if (scenario == "reset-terminal") {
            assertTrue(dispatch("closeStatement") < 0, "already terminated owner prevents selected close dispatch")
            assertTrue(phase("executorAfterClose") > phase("terminationConfirmed"),
                "local close callback can complete on an already terminated owner without a remote close dispatch")
        } else {
            assertTrue(dispatch("closeStatement") > dispatch("clearBindings"))
            assertTrue(phase("executorAfterClose") > dispatch("closeStatement"))
        }
        // The synchronous callback witnesses dispatch only. A real later barrier owns the ACK.
        if (scenario != "reset-terminal") {
            assertTrue(acknowledgement("closeStatement") > phase("executorAfterClose"))
            assertTrue(acknowledgement("reset") > dispatch("reset"))
            assertTrue(acknowledgement("clearBindings") > dispatch("clearBindings"))
        }
        assertEquals(terminal, before.getValue("diagnostics").jsonObject.getValue("terminal").jsonPrimitive.boolean)
        assertEquals(if (terminal || deniedRollback) 1 else 0, before.getValue("diagnostics").jsonObject.getValue("terminationConfirmed").jsonPrimitive.int)
        assertEquals(1, after.getValue("diagnostics").jsonObject.getValue("terminationConfirmed").jsonPrimitive.int)
        assertEquals(terminal || deniedRollback, probeFailure != null)
        assertEquals(if (terminal || deniedRollback) 0 else 1, sent("/* w1-after-failure-probe */ SELECT 1").size)
        assertTrue(sent("/* w1-prevented */ SELECT 1").isEmpty(), "cancelled pre-dispatch barrier cannot dispatch the rejected request")

        if (success) {
            if (body == Body.PENDING) assertTrue(observed is SyncWorkLimitException) else assertNull(observed)
            assertEquals(1, sent("COMMIT").size); assertTrue(sent("ROLLBACK").isEmpty())
            assertTrue(phase("executorBeforeCommit") > acknowledgement("closeStatement"))
            assertTrue(events.any { it.text("sql")?.contains("pragma_foreign_key_check") == true })
        } else {
            val failure = assertNotNull(observed)
            assertFalse(failure is SyncWorkLimitException, "cleanup preempts deferred work-limit result")
            if (body == Body.FAILURE) assertTrue(graph.any { it === bodyFailure }, "body failure remains primary or original cause")
            if (body == Body.CANCEL || held) assertTrue(failure is CancellationException, "caller cancellation remains primary")
            val operation = if (scenario.startsWith("reset")) "reset" else if (scenario == "clear") "clear" else "finalize"
            assertTrue(graph.any { it.message.orEmpty().contains("W1 selected workspace $operation") })
            assertTrue(sent("COMMIT").isEmpty()); assertTrue(phase("executorBeforeCommit") < 0)
            assertTrue(events.none { it.text("sql")?.contains("pragma_foreign_key_check") == true }, "post-close FK SQL is prevented")
            val faultPhase = if (operation == "finalize") (if (scenario == "close-after") "finalizeAfterFault" else "finalizeBeforeFault") else operation + "BeforeFault"
            val fault = events.single { it.text("phase") == faultPhase }
            assertEquals(scenario != "close-after", fault.getValue("selectedNativeAlive").jsonPrimitive.boolean)
            if (terminal) {
                assertTrue(phase("terminationConfirmed") in (phase(faultPhase) + 1) until phase("invocationFailure"),
                    "selected handle must be disposed by confirmed owner termination before failure exit")
                assertTrue(sent("ROLLBACK").isEmpty())
            } else if (deniedRollback) {
                assertEquals(1, sent("ROLLBACK").size)
                val denial = events.single { it.text("phase") == "authorizerDeniedRollback" }
                assertFalse(denial.getValue("selectedNativeAlive").jsonPrimitive.boolean)
                assertTrue(graph.any { it.message.orEmpty().contains("not authorized") })
                assertTrue(events.none { it.text("phase") == "rollbackResult" })
            } else {
                assertEquals(1, sent("ROLLBACK").size)
                val rollback = events.single { it.text("phase") == "rollbackResult" }
                assertFalse(rollback.getValue("inTransaction").jsonPrimitive.boolean)
                assertFalse(rollback.getValue("selectedNativeAlive").jsonPrimitive.boolean)
                assertFalse(rollback.getValue("selectedLogicalAlive").jsonPrimitive.boolean)
                assertTrue(phase("nativeFinalizeReturn") < phase("rollbackResult"))
            }
            if (scenario == "reset-terminal") {
                assertTrue(failure.message.orEmpty().contains("W1 selected workspace reset"), "ordinary failure is first")
                assertTrue(graph.any { it.message.orEmpty().contains("cleanup deadline") }, "later terminal barrier failure is retained")
                assertTrue(events.any { it.text("phase") == "flushReturn" && it.getValue("barrierFailures").jsonArray.isNotEmpty() })
            }
        }
        val finalizations = events.count { it.text("phase") == "nativeFinalizeReturn" }
        assertEquals(if (scenario == "close-before" || scenario == "reset-terminal") 0 else 1, finalizations,
            "selected native disposal must be observed, not inferred from successful rollback")
        for (failure in graph) {
            val suppressed = failure.suppressedExceptions
            suppressed.forEachIndexed { index, item -> assertTrue(suppressed.take(index).none { it === item }, "no duplicate suppressed identities") }
        }
        if (held) {
            assertTrue(phase("selectedResponseHeld") < phase("callerCancelled"))
            assertTrue(phase("callerCancelled") < phase("selectedResponseReleased"))
            assertTrue(phase("selectedResponseReleased") < phase("invocationFailure"))
            assertTrue(assertNotNull(observed).suppressedExceptions.isNotEmpty(), "captured cancellation retains real queued failure")
        }
        if (before.text("runtime") == "browser-worker") {
            assertEquals(if (success) 8L else 7L, durableValue); assertEquals(0L, durableApplyMode)
        } else assertEquals("js-node-worker", before.text("runtime"))
    }

    private fun snapshot(): JsonObject = Json.parseToJsonElement(w1Snapshot()).jsonObject
    private fun JsonObject.text(key: String): String? = this[key]?.jsonPrimitive?.contentOrNull
    private suspend fun scalar(db: SafeSQLiteConnection, sql: String): Long {
        val statement = db.prepare(sql)
        try { assertTrue(statement.step()); return statement.getLong(0) }
        finally { statement.close() }
    }
    private fun diagnosticState(): RuntimeState {
        val generation = SyncGeneration("1", "w1-diagnostic", emptyList())
        val validated = ValidatedConfig(generation, emptyList(), emptyMap(), emptyMap(), emptyMap(), emptyMap())
        return RuntimeState(validated, AdmittedOversqliteContract("v1", generation, testWireLimits(), false), "w1", "w1")
    }
    private fun failureGraph(failure: Throwable): List<Throwable> {
        val seen = mutableListOf<Throwable>()
        fun visit(item: Throwable) {
            if (seen.any { it === item }) return
            seen += item; item.cause?.let(::visit); item.suppressedExceptions.forEach(::visit)
        }
        visit(failure); return seen
    }
    private fun failureTree(failure: Throwable, seen: MutableList<Throwable> = mutableListOf()): JsonObject = buildJsonObject {
        put("type", JsonPrimitive(failure::class.simpleName)); put("message", JsonPrimitive(failure.message))
        if (seen.any { it === failure }) { put("repeatedIdentity", true); return@buildJsonObject }
        seen += failure
        put("suppressed", JsonArray(failure.suppressedExceptions.map { failureTree(it, seen) }))
        failure.cause?.let { put("cause", failureTree(it, seen)) }
    }
}
