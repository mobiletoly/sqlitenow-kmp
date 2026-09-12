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

/** Actual cached replay statements; injected pending control flow is separate from natural work exhaustion. */
internal class WorkerReplayStatementCleanupTest : SurvivorTestSupport() {
    private enum class Body { SUCCESS, FAILURE, INJECTED_PENDING, CANCEL, FLUSH_CANCEL, CLOSE_FLUSH_CANCEL }

    @Test fun planSaveResetWithSuccess() = case("plan-save", "reset", Body.SUCCESS)
    @Test fun planSaveClearWithSuccess() = case("plan-save", "clear", Body.SUCCESS)
    @Test fun planSaveCloseWithSuccess() = case("plan-save", "close-before", Body.SUCCESS)
    @Test fun planSaveResetWithBodyFailure() = case("plan-save", "reset", Body.FAILURE)
    @Test fun planSaveClearWithBodyFailure() = case("plan-save", "clear", Body.FAILURE)
    @Test fun planSaveCloseWithBodyFailure() = case("plan-save", "close-before", Body.FAILURE)
    @Test fun planSaveResetWithInjectedPendingSignal() = case("plan-save", "reset", Body.INJECTED_PENDING)
    @Test fun planSaveClearWithInjectedPendingSignal() = case("plan-save", "clear", Body.INJECTED_PENDING)
    @Test fun planSaveCloseWithInjectedPendingSignal() = case("plan-save", "close-before", Body.INJECTED_PENDING)
    @Test fun planSaveResetWithCancellation() = case("plan-save", "reset", Body.CANCEL)
    @Test fun planSaveClearWithCancellation() = case("plan-save", "clear", Body.CANCEL)
    @Test fun planSaveCloseWithCancellation() = case("plan-save", "close-before", Body.CANCEL)
    @Test fun planSaveCancellationDuringSelectedFlush() = case("plan-save", "reset", Body.FLUSH_CANCEL)
    @Test fun planSaveCancellationDuringSelectedCloseFlush() = case("plan-save", "close-before", Body.CLOSE_FLUSH_CANCEL)
    @Test fun planSaveOrdinaryFailureThenTerminalBarrier() = case("plan-save", "reset-terminal", Body.SUCCESS)
    @Test fun planSaveOrdinaryCleanupFailureWithDeniedRollbackRequiresOwnedShutdown() = case("plan-save", "reset-rollback-denied", Body.SUCCESS)
    @Test fun planSaveSuccessfulCleanupSettlesCommittedEffects() = case("plan-save", "normal", Body.SUCCESS)
    @Test fun planSaveInjectedPendingSignalWithSuccessfulCleanupCommitsThenThrows() = case("plan-save", "normal", Body.INJECTED_PENDING)
    @Test fun planSaveCloseAfterNativeFinalization() = case("plan-save", "close-after", Body.SUCCESS)
    @Test fun captureSizeResetWithSuccess() = case("capture-size", "reset", Body.SUCCESS)
    @Test fun captureSizeClearWithSuccess() = case("capture-size", "clear", Body.SUCCESS)
    @Test fun captureSizeCloseWithSuccess() = case("capture-size", "close-before", Body.SUCCESS)
    @Test fun captureSizeResetWithBodyFailure() = case("capture-size", "reset", Body.FAILURE)
    @Test fun captureSizeClearWithBodyFailure() = case("capture-size", "clear", Body.FAILURE)
    @Test fun captureSizeCloseWithBodyFailure() = case("capture-size", "close-before", Body.FAILURE)
    @Test fun captureSizeResetWithInjectedPendingSignal() = case("capture-size", "reset", Body.INJECTED_PENDING)
    @Test fun captureSizeClearWithInjectedPendingSignal() = case("capture-size", "clear", Body.INJECTED_PENDING)
    @Test fun captureSizeCloseWithInjectedPendingSignal() = case("capture-size", "close-before", Body.INJECTED_PENDING)
    @Test fun captureSizeResetWithCancellation() = case("capture-size", "reset", Body.CANCEL)
    @Test fun captureSizeClearWithCancellation() = case("capture-size", "clear", Body.CANCEL)
    @Test fun captureSizeCloseWithCancellation() = case("capture-size", "close-before", Body.CANCEL)
    @Test fun captureSizeCancellationDuringSelectedFlush() = case("capture-size", "reset", Body.FLUSH_CANCEL)
    @Test fun captureSizeCancellationDuringSelectedCloseFlush() = case("capture-size", "close-before", Body.CLOSE_FLUSH_CANCEL)
    @Test fun captureSizeOrdinaryFailureThenTerminalBarrier() = case("capture-size", "reset-terminal", Body.SUCCESS)
    @Test fun captureSizeOrdinaryCleanupFailureWithDeniedRollbackRequiresOwnedShutdown() = case("capture-size", "reset-rollback-denied", Body.SUCCESS)
    @Test fun captureSizeSuccessfulCleanupSettlesCommittedEffects() = case("capture-size", "normal", Body.SUCCESS)
    @Test fun captureSizeInjectedPendingSignalWithSuccessfulCleanupCommitsThenThrows() = case("capture-size", "normal", Body.INJECTED_PENDING)
    @Test fun captureSizeCloseAfterNativeFinalization() = case("capture-size", "close-after", Body.SUCCESS)

    @Test fun naturalWorkLimitPersistsUsefulCheckpointAndReopensWithFreshStatements() = runTest(timeout = 180.seconds) {
        w1Configure("""{"targetFamily":"plan-save"}""")
        val name = "replay-progress-${Random.nextLong().toString().replace('-', 'n')}"
        val db = sqliteWorkerConnectionProvider(w1WorkerModuleUrl("normal")).openConnection(name, false, SqliteConnectionConfig())
        val server = MockSyncServer()
        val table = GenerationTable("main", "nodes", listOf("id"), listOf(
            GenerationColumn("id", "text", false), GenerationColumn("name", "text", false), GenerationColumn("parent_id", "text", true),
        ), listOf(GenerationForeignKey(listOf("parent_id"), "main", "nodes", listOf("id"), "cascade", "no_action")))
        server.generationOverride = SyncGeneration("1", "", listOf(table)).let { it.copy(fingerprint = it.computeFingerprint()) }
        val http = server.newHttpClient()
        val seed = newClient(db, http, syncTables = nodeTables)
        var limited: DefaultOversqliteClient? = null
        var resumed: DefaultOversqliteClient? = null
        var reopened: SafeSQLiteConnection? = null
        var firstHandle: SQLiteStatement? = null
        var limitedPreparations = 0
        var limitedExecutions = 0
        var resumedPreparations = 0
        var resumedExecutions = 0
        var primaryFailure: Throwable? = null
        try {
            db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
            seed.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')")
            seed.pushPending().getOrThrow()
            db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
            server.normalizePushEffects = { effects() }
            server.beforePushResponse = { throw CancellationException("lost committed response") }
            assertFailsWith<CancellationException> { seed.pushPending().getOrThrow() }
            val frozenRequest = server.pushBodies.last()
            val frozen = baselineQualificationVector(db, listOf("nodes") + derivedPlanTables)
            seed.closeFromCoordinatedOwner()

            // Each real preparation step reserves 16 units. This budget reaches roots/index
            // progress before the complete four-node preparation can finish; no fault throws it.
            val bounded = newClient(db, http, syncTables = nodeTables, workUnits = 256).also { limited = it }
            bounded.openAndConnect("user-1").getOrThrow()
            bounded.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(
                afterStatementCacheClosed = { w1Mark("executorAfterClose") },
                beforeCommit = { w1Mark("executorBeforeCommit") },
                statementCacheOperations = StatementCacheOperations(prepare = { connection, sql ->
                    val statement = connection.prepare(sql)
                    if (sql != planSaveSql) statement else {
                        limitedPreparations++
                        firstHandle = statement
                        object : SQLiteStatement by statement {
                            override suspend fun step(): Boolean {
                                val row = statement.step()
                                assertFalse(row, "complete plan UPSERT returns DONE")
                                limitedExecutions++
                                return row
                            }
                        }
                    }
                }),
            ))
            w1Mark("armCommittedSettlement")
            val pending = assertIs<SyncWorkLimitException>(bounded.pushPending().exceptionOrNull())
            assertEquals(256L, pending.limit)
            assertTrue(pending.used in 1L..256L)
            assertEquals(1, limitedPreparations)
            assertTrue(limitedExecutions > 1, "real progress saves more than the initial empty checkpoint")
            assertEquals(1L, scalar(db, "SELECT count(*) FROM _sync_apply_plan"))
            assertEquals("settlement", scalarText(db, "SELECT operation_kind FROM _sync_apply_plan"))
            assertTrue(scalar(db, "SELECT root_cursor FROM _sync_apply_plan") > 0L)
            assertTrue(scalar(db, "SELECT entries FROM _sync_apply_plan") > 0L)
            assertState(db, settled = false)
            assertFrozenState(db, frozen, allowDerivedProgress = true)
            assertEquals(frozenRequest, server.pushBodies.last())
            assertEquals(3, server.pushRequestCount)
            val saved = baselineQualificationVector(db, listOf("nodes") + derivedPlanTables)
            val evidence = snapshot()
            assertEquals("browser-worker", evidence.text("runtime"), "natural progress requires durable browser reopen")
            val events = evidence.getValue("events").jsonArray.map { it.jsonObject }
            assertEquals("plan-save", events.single { it.text("phase") == "selected" }.text("targetFamily"))
            assertEquals(1, events.count { it.text("phase") == "nativeFinalizeReturn" })
            assertTrue(events.indexOfFirst { it.text("phase") == "nativeFinalizeReturn" } <
                events.indexOfFirst { it.text("phase") == "executorBeforeCommit" })
            assertTrue(events.any { it.text("phase") == "dispatch" && it.text("sql") == "COMMIT" })

            bounded.closeFromCoordinatedOwner()
            db.close()
            val restored = sqliteWorkerConnectionProvider().openConnection(name, false, SqliteConnectionConfig()).also { reopened = it }
            assertEquals(saved, baselineQualificationVector(restored, listOf("nodes") + derivedPlanTables), "all committed progress survives physical reopen")
            val retry = newClient(restored, http, syncTables = nodeTables).also { resumed = it }
            retry.openAndConnect("user-1").getOrThrow()
            val admitted = baselineQualificationVector(restored, listOf("nodes") + derivedPlanTables)
            for (owner in derivedPlanTables) assertEquals(saved.rows(owner), admitted.rows(owner), "admission preserves the resumable $owner checkpoint")
            retry.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(
                statementCacheOperations = StatementCacheOperations(prepare = { connection, sql ->
                    val statement = connection.prepare(sql)
                    if (sql != planSaveSql) statement else {
                        resumedPreparations++
                        assertNotSame(firstHandle, statement, "reopen cannot reuse a handle from the completed transaction")
                        object : SQLiteStatement by statement {
                            override suspend fun step(): Boolean {
                                val row = statement.step(); resumedExecutions++
                                assertFalse(row)
                                return row
                            }
                        }
                    }
                }),
            ))
            assertEquals(PushOutcome.COMMITTED, retry.pushPending().getOrThrow().outcome)
            assertEquals(1, resumedPreparations)
            assertTrue(resumedExecutions > 0)
            assertState(restored, settled = true)
            for (owner in derivedPlanTables) assertEquals(0L, scalar(restored, "SELECT count(*) FROM $owner"))
            assertEquals(frozenRequest, server.pushBodies.last())
            assertEquals(4, server.pushRequestCount)
            val complete = baselineQualificationVector(restored, listOf("nodes") + derivedPlanTables)
            retry.closeFromCoordinatedOwner()
            restored.close()
            val readback = sqliteWorkerConnectionProvider().openConnection(name, false, SqliteConnectionConfig())
            try { assertEquals(complete, baselineQualificationVector(readback, listOf("nodes") + derivedPlanTables)) }
            finally { readback.close() }
            val receipt = buildJsonObject {
                put("limit", pending.limit); put("used", pending.used)
                put("limitedPreparations", limitedPreparations); put("limitedExecutions", limitedExecutions)
                put("resumedPreparations", resumedPreparations); put("resumedExecutions", resumedExecutions)
                put("checkpointReopened", true); put("settlementReopened", true)
                put("pendingCause", "natural-work-admission")
                put("savedPlan", JsonArray(saved.rows("_sync_apply_plan")))
                put("nativeOwnershipEvents", JsonArray(events.filter { event ->
                    event.text("phase") in setOf("settlementArmed", "selected", "nativeFinalizeEnter", "nativeFinalizeReturn", "executorAfterClose", "executorBeforeCommit")
                }))
                put("fullEventCount", events.size)
                put("fullEventSHA256", sha256Hex(evidence.getValue("events").toString().encodeToByteArray()))
            }.toString().chunked(4000)
            receipt.forEachIndexed { index, chunk -> println("WORKER_REPLAY_NATURAL_PROGRESS_CHUNK ${index + 1}/${receipt.size} $chunk") }
        } catch (failure: Throwable) {
            primaryFailure = failure
            throw failure
        } finally {
            withContext(NonCancellable) {
                val failures = listOfNotNull(
                    runCatching { resumed?.closeFromCoordinatedOwner() }.exceptionOrNull(),
                    runCatching { reopened?.close() }.exceptionOrNull(),
                    runCatching { limited?.closeFromCoordinatedOwner() }.exceptionOrNull(),
                    runCatching { seed.closeFromCoordinatedOwner() }.exceptionOrNull(),
                    runCatching { db.close() }.exceptionOrNull(),
                    runCatching { http.close() }.exceptionOrNull(),
                    runCatching { cleanupDirectWorkerTestDatabase(name) }.exceptionOrNull(),
                )
                val primary = primaryFailure ?: failures.firstOrNull()
                failures.filter { it !== primary }.forEach { primary?.addSuppressed(it) }
                if (primaryFailure == null && primary != null) throw primary
            }
        }
    }

    private fun case(family: String, scenario: String, body: Body) = runTest(timeout = 180.seconds) {
        val name = "replay-statement-$family-${Random.nextLong().toString().replace('-', 'n')}"
        val held = body == Body.FLUSH_CANCEL || body == Body.CLOSE_FLUSH_CANCEL
        w1Configure(buildJsonObject {
            put("targetFamily", family)
            if (held) put("holdCommand", if (body == Body.FLUSH_CANCEL) "reset" else "closeStatement")
            put("dropClear", scenario == "reset-terminal")
        }.toString())
        val db = sqliteWorkerConnectionProvider(w1WorkerModuleUrl(scenario)).openConnection(name, false, SqliteConnectionConfig())
        val server = MockSyncServer()
        val table = GenerationTable("main", "nodes", listOf("id"), listOf(
            GenerationColumn("id", "text", false), GenerationColumn("name", "text", false), GenerationColumn("parent_id", "text", true),
        ), listOf(GenerationForeignKey(listOf("parent_id"), "main", "nodes", listOf("id"), "cascade", "no_action")))
        server.generationOverride = SyncGeneration("1", "", listOf(table)).let { it.copy(fingerprint = it.computeFingerprint()) }
        val http = server.newHttpClient()
        val client = newClient(db, http, syncTables = nodeTables)
        val bodyFailure = IllegalStateException("replay-statement-body")
        var observed: Throwable? = null
        var ownerShutdownFailure: Throwable? = null
        var probeFailure: Throwable? = null
        var beforeShutdown: JsonObject? = null
        var afterShutdown: JsonObject? = null
        var durable: JsonObject? = null
        var frozenVector: BaselineQualificationVector? = null
        val entered = CompletableDeferred<Unit>()
        var writerEntered = false
        var selectedPreparations = 0
        var selectedReads = 0
        val settled = scenario == "normal" && body == Body.SUCCESS
        val retainedInjectedProgress = scenario == "normal" && body == Body.INJECTED_PENDING
        var retainedDerived: BaselineQualificationVector? = null
        try {
            db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')")
            client.pushPending().getOrThrow()
            db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
            server.normalizePushEffects = { effects() }
            server.beforePushResponse = { throw CancellationException("lost committed response") }
            assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
            val frozen = server.pushBodies.last()
            frozenVector = baselineQualificationVector(db, listOf("nodes") + derivedPlanTables)
            assertEquals(0L, scalar(db, "SELECT count(*) FROM _sync_dirty_rows"), "freeze transfers the pending row into the outbox owner")
            client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(
                afterStatementCacheClosed = { w1Mark("executorAfterClose") },
                beforeCommit = { w1Mark("executorBeforeCommit") },
                statementCacheOperations = StatementCacheOperations(prepare = { connection, sql ->
                    if (!writerEntered) { writerEntered = true; w1Mark("writerTransactionEnter") }
                    val statement = connection.prepare(sql)
                    if (!selected(family, sql)) statement else {
                        selectedPreparations++
                        object : SQLiteStatement by statement {
                            override suspend fun step(): Boolean {
                                val row = statement.step(); selectedReads++
                                if (selectedReads == 1) {
                                    assertEquals(family == "capture-size", row, "actual plan UPSERT returns DONE; capture-size returns its live row")
                                    w1Mark("armSelectedCleanup")
                                    when (body) {
                                        Body.FAILURE -> { w1Mark("bodyFailure"); throw bodyFailure }
                                        Body.INJECTED_PENDING -> { w1Mark("pendingProgress"); throw SyncWorkLimitException("injected-replay-control-flow", 1, 2) }
                                        Body.CANCEL -> { w1Mark("bodyCancellation"); entered.complete(Unit); awaitCancellation() }
                                        else -> w1Mark("successfulReplayStatementBody")
                                    }
                                }
                                return row
                            }
                        }
                    }
                }),
            ))
            w1Mark("armCommittedSettlement")
            suspend fun invoke() {
                try { client.pushPending().getOrThrow(); w1Mark("invocationSuccess") }
                catch (failure: Throwable) { observed = failure; w1Mark("invocationFailure") }
            }
            if (body == Body.CANCEL || held) {
                val invocation = launch { invoke() }
                try {
                    if (held) w1AwaitHeld() else entered.await()
                    w1Mark("callerCancelled"); invocation.cancel(CancellationException("replay-statement-caller"))
                    if (held) w1ReleaseHeld()
                    invocation.join()
                } finally { invocation.cancelAndJoin() }
            } else invoke()
            assertEquals(1, selectedPreparations, "one actual cached replay statement handle in this settlement transaction")
            assertTrue(selectedReads > 0)
            assertEquals(frozen, server.pushBodies.last(), "exact frozen committed replay request")
            assertEquals(3, server.pushRequestCount, "seed, committed loss, replay")
            w1Mark("postInvocationProbe")
            try { assertEquals(1L, scalar(db, "/* replay-after-failure-probe */ SELECT 1")) }
            catch (failure: Throwable) { probeFailure = failure }
            beforeShutdown = snapshot()
            if (probeFailure == null) {
                assertState(db, settled)
                if (!settled) assertFrozenState(db, assertNotNull(frozenVector), retainedInjectedProgress)
                if (retainedInjectedProgress) {
                    assertEquals(1L, scalar(db, "SELECT count(*) FROM _sync_apply_plan"))
                    assertEquals("settlement", scalarText(db, "SELECT operation_kind FROM _sync_apply_plan"))
                    retainedDerived = baselineQualificationVector(db, listOf("nodes") + derivedPlanTables)
                }
            }
        } finally {
            withContext(NonCancellable) {
                try {
                    w1Mark("ownerShutdownEnter")
                    try { db.close() } catch (failure: Throwable) { ownerShutdownFailure = failure }
                    afterShutdown = snapshot()
                    try { client.closeFromCoordinatedOwner() } catch (_: Throwable) { /* Owned DB state is recorded separately. */ }
                    if (afterShutdown!!.text("runtime") == "browser-worker") {
                        val reopened = sqliteWorkerConnectionProvider().openConnection(name, false, SqliteConnectionConfig())
                        try {
                            assertState(reopened, settled)
                            if (!settled) assertFrozenState(reopened, assertNotNull(frozenVector), retainedInjectedProgress)
                            retainedDerived?.let { saved ->
                                val restored = baselineQualificationVector(reopened, listOf("nodes") + derivedPlanTables)
                                for (table in derivedPlanTables) assertEquals(saved.rows(table), restored.rows(table), "durable injected checkpoint $table")
                            }
                            durable = buildJsonObject {
                                put("checkpoint", scalar(reopened, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                                put("nextSource", scalar(reopened, "SELECT next_source_bundle_id FROM _sync_source_state"))
                                put("outbox", scalar(reopened, "SELECT count(*) FROM _sync_outbox_rows"))
                                put("applyMode", scalar(reopened, "SELECT apply_mode FROM _sync_apply_state"))
                                put("reservations", scalar(reopened, "SELECT reserved_entries FROM _sync_operation_state"))
                            }
                        } finally { reopened.close() }
                    }
                } finally { http.close(); cleanupDirectWorkerTestDatabase(name) }
            }
        }
        val before = assertNotNull(beforeShutdown); val after = assertNotNull(afterShutdown)
        val events = before.getValue("events").jsonArray.map { it.jsonObject }
        fun phase(name: String) = events.indexOfFirst { it.text("phase") == name }
        val writerStart = events.indexOfFirst { it.text("phase") == "writerTransactionEnter" }
        assertTrue(writerStart >= 0)
        val boundary = events.withIndex().firstOrNull { (index, event) -> index > writerStart &&
            event.text("phase") == "dispatch" && event.text("command") == "prepare" && event.text("sql") in setOf("COMMIT", "ROLLBACK") }
        // The public operation may commit separate reservation/release transactions. The first boundary
        // after the cache owner enters belongs to the settlement writer; retain full telemetry separately.
        fun sent(sql: String) = events.withIndex().filter { (index, event) ->
            (sql !in setOf("COMMIT", "ROLLBACK") || index == boundary?.index) &&
            event.text("phase") == "dispatch" && event.text("command") == "prepare" && event.text("sql") == sql
        }.map { it.value }
        val selectedEvent = events.single { it.text("phase") == "selected" }
        assertEquals(family, selectedEvent.text("targetFamily"))
        assertEquals(if (family == "plan-save") planSaveSql else captureSizeSql, selectedEvent.text("sql"))
        assertTrue(phase("settlementArmed") in 0 until phase("selected"), "private worker selection follows its explicit arm marker")
        val selectedId = selectedEvent.getValue("statementId").jsonPrimitive.int
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
        val selectedRequestIds = events.filter { it.text("phase") == "dispatch" &&
            (it["statementId"]?.jsonPrimitive?.intOrNull == selectedId || it.text("sql")?.let { sql -> selected(family, sql) } == true) }
            .mapNotNull { it["id"]?.jsonPrimitive?.intOrNull }.toSet()
        val afterFailedFlush = events.indices.filter { events[it].text("phase") == "flushReturn" &&
            (events[it]["errors"]?.jsonArray?.isNotEmpty() == true || events[it]["barrierFailures"]?.jsonArray?.isNotEmpty() == true) }
            .mapNotNull { start -> events.indices.firstOrNull { it > start && events[it].text("phase") == "dispatch" && events[it].text("command") == "prepare" } }.toSet()
        val evidenceEvents = events.mapIndexedNotNull { index, event ->
            val ids = (event["ids"] ?: event["envelopeIds"])?.jsonArray?.map { it.jsonPrimitive.int }.orEmpty()
            val keep = when (event.text("phase")) {
                "dispatch" -> event["id"]?.jsonPrimitive?.intOrNull in selectedRequestIds || index in afterFailedFlush ||
                    event.text("sql")?.let { it in setOf("COMMIT", "ROLLBACK", "/* replay-after-failure-probe */ SELECT 1") || it.contains("pragma_foreign_key_check") } == true
                "response" -> event["id"]?.jsonPrimitive?.intOrNull in selectedRequestIds
                "flushEnter", "flushReturn" -> ids.any { it in selectedRequestIds } || event["errors"]?.jsonArray?.isNotEmpty() == true || event["barrierFailures"]?.jsonArray?.isNotEmpty() == true
                else -> true
            }
            if (!keep) null else buildJsonObject {
                put("ordinal", index)
                for ((key, value) in event) {
                    if (key in setOf("ids", "envelopeIds")) put(key, JsonArray(ids.filter { it in selectedRequestIds }.map(::JsonPrimitive)))
                    else if (key != "sql" || event.text("phase") == "selected" || event.text("command") == "prepare") put(key, value)
                }
            }
        }
        val receipt = buildJsonObject {
            put("family", family); put("scenario", scenario); put("body", body.name)
            put("pendingEvidence", if (body == Body.INJECTED_PENDING) "injected-control-flow-only" else "not-injected")
            put("derivedCheckpointRetained", retainedInjectedProgress)
            put("beforeShutdown", buildJsonObject {
                for ((key, value) in before) if (key != "events") put(key, value)
                put("events", JsonArray(evidenceEvents)); put("fullEventCount", events.size)
                put("fullEventSHA256", sha256Hex(before.getValue("events").toString().encodeToByteArray()))
            })
            put("afterShutdown", buildJsonObject { for ((key, value) in after) if (key != "events") put(key, value) })
            observed?.let { put("failure", failureTree(it)) }
            probeFailure?.let { put("probeFailure", failureTree(it)) }
            ownerShutdownFailure?.let { put("ownerShutdownFailure", failureTree(it)) }
            put("durableState", durable ?: JsonNull); put("selectedPreparations", selectedPreparations); put("selectedReads", selectedReads)
            put("durableReopenClassification", if (before.text("runtime") == "browser-worker") "fresh" else "not-applicable-transient-node")
        }.toString()
        val chunks = receipt.chunked(4000)
        chunks.forEachIndexed { index, chunk -> println("WORKER_REPLAY_CLEANUP_CHUNK ${index + 1}/${chunks.size} $chunk") }

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
        assertEquals(if (terminal || deniedRollback) 0 else 1, sent("/* replay-after-failure-probe */ SELECT 1").size)
        val rejectedFlush = events.indexOfFirst { it.text("phase") == "flushReturn" &&
            (it["errors"]?.jsonArray?.isNotEmpty() == true || it["barrierFailures"]?.jsonArray?.isNotEmpty() == true) }
        if (!success) {
            assertTrue(rejectedFlush >= 0, "actual worker cleanup failure reaches the existing barrier")
            val nextDispatch = events.drop(rejectedFlush + 1).firstOrNull { it.text("phase") == "dispatch" && it.text("command") == "prepare" }
            assertTrue(nextDispatch == null || nextDispatch.text("sql") == "ROLLBACK", "failed barrier fences the triggering SQL; rollback is the next permitted dispatch")
        }

        if (success) {
            if (body == Body.INJECTED_PENDING) assertTrue(observed is SyncWorkLimitException) else assertNull(observed)
            assertEquals(1, sent("COMMIT").size); assertTrue(sent("ROLLBACK").isEmpty())
            assertTrue(phase("executorBeforeCommit") > acknowledgement("closeStatement"))
            assertTrue(events.any { it.text("sql")?.contains("pragma_foreign_key_check") == true })
        } else {
            val failure = assertNotNull(observed)
            assertFalse(failure is SyncWorkLimitException, "cleanup preempts the injected deferred pending signal; this is not natural-progress evidence")
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
        if (before.text("runtime") == "browser-worker") assertNotNull(durable)
        else assertEquals("js-node-worker", before.text("runtime"))
    }

    private fun snapshot(): JsonObject = Json.parseToJsonElement(w1Snapshot()).jsonObject
    private fun JsonObject.text(key: String): String? = this[key]?.jsonPrimitive?.contentOrNull
    private suspend fun scalar(db: SafeSQLiteConnection, sql: String): Long {
        val statement = db.prepare(sql)
        try { assertTrue(statement.step()); return statement.getLong(0) }
        finally { statement.close() }
    }
    private fun selected(family: String, sql: String) = sql == if (family == "plan-save") planSaveSql else captureSizeSql
    private suspend fun assertState(db: SafeSQLiteConnection, settled: Boolean) {
        if (settled) requireSurvivors(db) else {
            assertEquals("changed parent", scalarText(db, "SELECT name FROM nodes WHERE id='P'"))
            assertEquals("P", scalarText(db, "SELECT parent_id FROM nodes WHERE id='C'"))
            assertEquals("C", scalarText(db, "SELECT parent_id FROM nodes WHERE id='D'"))
            assertEquals(0L, scalar(db, "SELECT count(*) FROM pragma_foreign_key_check"))
        }
        assertEquals(if (settled) 2L else 1L, scalar(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        assertEquals(if (settled) 3L else 2L, scalar(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
        assertEquals(if (settled) 0L else 1L, scalar(db, "SELECT count(*) FROM _sync_outbox_rows"))
        assertEquals(0L, scalar(db, "SELECT count(*) FROM _sync_dirty_rows"), "frozen pending intent belongs to outbox until settlement")
        assertEquals(0L, scalar(db, "SELECT apply_mode FROM _sync_apply_state"))
    }
    private suspend fun assertFrozenState(db: SafeSQLiteConnection, frozen: BaselineQualificationVector, allowDerivedProgress: Boolean = false) {
        val current = baselineQualificationVector(db, listOf("nodes") + derivedPlanTables)
        // Reservation/staging operation metadata has its own lifetime. Every business/baseline/intent,
        // relationship, source and checkpoint column remains exactly frozen. Only a successful
        // pending-control-flow commit may retain separately checked derived apply-plan state.
        for (table in frozen.tables.keys.filter { it != "_sync_operation_state" && (!allowDerivedProgress || it !in derivedPlanTables) }) {
            assertEquals(frozen.rows(table), current.rows(table), "complete frozen $table vector")
        }
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
    private val derivedPlanTables = setOf("_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges")

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
