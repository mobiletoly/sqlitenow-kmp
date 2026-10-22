package dev.goquick.sqlitenow.oversqlite.platform

import dev.goquick.sqlitenow.oversqlite.*
import dev.goquick.sqlitenow.core.TransactionMode
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import androidx.sqlite.async.step
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.encodeToString
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import io.ktor.client.HttpClient
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.assertFailsWith

private val recoveryTables = listOf("users", "posts")
private val recoveryConfig = OversqliteConfig(
    generationId = 1, databaseId = "platform-sync", serviceId = "platform-mock",
    syncTables = listOf(SyncTable("users", syncKeyColumnName = "id"),
        SyncTable("posts", syncKeyColumnName = "id")),
)

// Preserve every existing owner vector and the complete typed upload journal.
private suspend fun processDeathStateVector(
    db: SafeSQLiteConnection, includeDurableStages: Boolean = true,
): Map<String, List<String>> {
    val primary = databaseStateVector(db, recoveryTables, ignoreDerivedCharges = true,
        includeDurableStages = includeDurableStages)
    val journal = db.prepare("SELECT json_array(singleton_key,binding,typeof(request_body)," +
        "lower(hex(request_body)),request_digest,typeof(outcome_body),lower(hex(outcome_body))," +
        "outcome_hash,phase) FROM _sync_upload_journal ORDER BY singleton_key").use { statement ->
        buildList { while (statement.step()) add(statement.getText(0)) }
    }
    return primary + ("_sync_upload_journal" to journal)
}

internal class ProcessDeathRecoveryJvmTest : PlatformCrossTargetTestSupport() {
    @Test fun reservationSurvivesProcessDeath() = checkKilledOwner("reserved")
    @Test fun stageCloseSurvivesProcessDeath() = checkKilledOwner("stage_close")
    @Test fun pageReadySurvivesProcessDeath() = checkKilledOwner("page_ready")
    @Test fun appliedRowSurvivesProcessDeath() = checkKilledOwner("applied_row")
    @Test fun cacheClosedSurvivesProcessDeath() = checkKilledOwner("cache_closed")
    @Test fun beforeCommitSurvivesProcessDeath() = checkKilledOwner("before_commit")
    @Test fun afterCommitSurvivesProcessDeath() = checkKilledOwner("after_commit")
    @Test fun plannerRootsSurvivesProcessDeath() = checkKilledOwner("plan_roots")
    @Test fun plannerIndexSurvivesProcessDeath() = checkKilledOwner("plan_index")
    @Test fun plannerWalkSurvivesProcessDeath() = checkKilledOwner("plan_walk")
    @Test fun plannerReadySurvivesProcessDeath() = checkKilledOwner("plan_ready")
    @Test fun plannerClearSurvivesProcessDeath() = checkKilledOwner("plan_clear")

    private fun checkKilledOwner(cut: String) = runBlocking {
        if (!platformSuiteEnabled()) {
            println("PROCESS_RECOVERY_NOT_EXECUTED $cut opt_in_disabled")
            return@runBlocking
        }
        println("PROCESS_RECOVERY_ENTER $cut")
        withOwnerProcess(cut) { root, child ->
            val databasePath = root.resolve("durable.sqlite")
            val expectedPath = root.resolve("expected.json")
            awaitMarker(root, child, cut, 30_000)
            child.destroyForcibly()
            assertTrue(child.waitFor(5, TimeUnit.SECONDS), "$cut SIGKILL deadline")
            assertEquals(137, child.exitValue(), "$cut parent-owned SIGKILL")

            val expected = Json.decodeFromString<Map<String, List<String>>>(Files.readString(expectedPath))
            val inspection = newFileBackedDb(databasePath.toString())
            try {
                assertEquals(expected, processDeathStateVector(inspection),
                    "$cut physical reopen has one complete old-or-new state")
            } finally { inspection.close() }

            // A new coordinated lifetime may reclaim only orphaned charge metadata.
            val successorDb = newCoordinatedFileBackedDb(databasePath.toString())
            val successorHttp = MockSyncServer().newHttpClient()
            try {
                successorDb.openClient(recoveryConfig, successorHttp, ServerWinsResolver)
                assertEquals(expected,
                    processDeathStateVector(successorDb.connection()),
                    "$cut new lifetime preserved the complete state")
                if (cut.startsWith("plan_")) {
                    assertEquals(cut.removePrefix("plan_"), scalarText(successorDb,
                        "SELECT phase FROM _sync_apply_plan"), "$cut durable planner phase")
                    val primaryBefore = processDeathStateVector(successorDb.connection(), includeDurableStages = false)
                    resumePlannerPhase(successorDb, successorHttp,
                        if (cut == "plan_clear") applyUnit("snapshot", 3, "stale-plan")
                        else applyUnit("snapshot", 2, "phase-death-plan"))
                    assertEquals(0L, scalarLong(successorDb, "SELECT count(*) FROM _sync_apply_plan"))
                    assertEquals(0L, scalarLong(successorDb, "SELECT count(*) FROM _sync_apply_rows"))
                    assertEquals(0L, scalarLong(successorDb, "SELECT count(*) FROM _sync_apply_edges"))
                    assertEquals(primaryBefore, processDeathStateVector(successorDb.connection(), includeDurableStages = false),
                        "$cut successor completed the retained plan without changing primary state")
                }
                assertEquals(0L, scalarLong(successorDb,
                    "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"),
                    "$cut orphan reservations reclaimed")
                assertEquals(0L, scalarLong(successorDb, "SELECT count(*) FROM pragma_foreign_key_check"))
            } finally { successorHttp.close(); successorDb.close() }
            println("PROCESS_RECOVERY_COMPLETE cut=$cut pid=${child.pid()} signal=SIGKILL exit=${child.exitValue()} physicalReopen=PASS")
        }
    }

    @Test fun missingArrivalTerminatesOwnedProcess() = runBlocking {
        if (!platformSuiteEnabled()) {
            println("PROCESS_RECOVERY_NOT_EXECUTED missing_arrival opt_in_disabled")
            return@runBlocking
        }
        var owned: Process? = null
        var directory: Path? = null
        val started = System.nanoTime()
        val failure = assertFailsWith<AssertionError> {
            withOwnerProcess("missing_marker") { root, child ->
                owned = child
                directory = root
                awaitMarker(root, child, "missing_marker", 100)
            }
        }
        assertTrue(failure.message.orEmpty().contains("missing missing_marker marker"))
        assertTrue(owned != null && !owned.isAlive, "missing arrival releases the owned process")
        assertTrue(directory != null && !Files.exists(directory), "owned files removed")
        assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(10), "bounded arrival and termination")
        println("PROCESS_RECOVERY_COMPLETE missing_arrival terminated=true files_removed=true")
    }

    private fun awaitMarker(root: Path, child: Process, cut: String, timeoutMillis: Long) {
        val marker = root.resolve("child.ready")
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
        while (!Files.exists(marker) && child.isAlive && System.nanoTime() < deadline) Thread.sleep(10)
        assertTrue(Files.exists(marker), "missing $cut marker: ${Files.readString(root.resolve("child.log"))}")
        assertTrue(child.isAlive, "$cut owner exited before parent termination")
    }

    private suspend fun withOwnerProcess(cut: String, block: suspend (Path, Process) -> Unit) {
        val root = Files.createTempDirectory("process-recovery-$cut-")
        var child: Process? = null
        var primary: Throwable? = null
        try {
            // Existing JVM runner interface; no new build variant or watchdog.
            val started = ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", checkNotNull(System.getProperty("a3.platform.jvmTest.classpath")),
                ProcessDeathRecoveryOwner::class.java.name,
                cut, root.resolve("durable.sqlite").toString(),
                root.resolve("expected.json").toString(), root.resolve("child.ready").toString(),
            ).redirectErrorStream(true).redirectOutput(root.resolve("child.log").toFile()).start()
            child = started
            block(root, started)
        } catch (failure: Throwable) {
            primary = failure
            throw failure
        } finally {
            val failures = listOfNotNull(
                runCatching {
                    child?.let {
                        if (it.isAlive) it.destroyForcibly()
                        check(it.waitFor(5, TimeUnit.SECONDS)) { "$cut child termination deadline" }
                    }
                }.exceptionOrNull(),
                runCatching { child?.inputStream?.close() }.exceptionOrNull(),
                runCatching { child?.errorStream?.close() }.exceptionOrNull(),
                runCatching { child?.outputStream?.close() }.exceptionOrNull(),
                runCatching {
                    check(child?.isAlive != true) { "$cut cannot remove files of a live owner" }
                    check(root.toFile().deleteRecursively()) { "$cut owned file cleanup failed" }
                }.exceptionOrNull(),
            )
            val first = primary ?: failures.firstOrNull()
            failures.filter { it !== first }.forEach { first?.addSuppressed(it) }
            if (primary == null && first != null) throw first
        }
    }

    private suspend fun resumePlannerPhase(
        database: PlatformTestDatabase, http: HttpClient, unit: String,
    ) {
        val connection = database.connection()
        val generation = OversqliteContractStore(connection).readGeneration(1)
        val validated = SyncRuntimeInitializer(recoveryConfig, TableInfoCache())
            .validateConfig(connection).withGeneration(generation)
        val state = RuntimeState(
            validated,
            AdmittedOversqliteContract("v1", generation, testWireLimits().let {
                OversqliteWireLimits(it.maxPushRows, it.maxPushDecodedBytes, it.maxPullBundles,
                    it.maxPullDecodedBytes, it.maxSnapshotRowsPerChunk, it.maxSnapshotDecodedBytesPerChunk,
                    it.maxSnapshotRows, it.maxSnapshotLogicalBytes, it.maxRowDecodedBytes,
                    it.maxPushOutcomeDecodedBytes)
            }, false),
            "user-1", scalarText(database, "SELECT current_source_id FROM _sync_attachment_state"),
        )
        val resources = OversqliteClientResourceOwner(connection, http)
        val operations = OversqliteClientOperationController(recoveryConfig, resources,
            SnapshotDiagnosticsState()) {}
        val local = OversqliteLocalStore(connection, Json) { state.validated }
        val owner = OversqliteApplySurvivors(connection, local, recoveryConfig, operations, Json)
        connection.transaction(TransactionMode.IMMEDIATE) {
            owner.prepare(state, unit, 2, "snapshot", 2) { index ->
                if (index == 0) ApplySurvivor("users", "P", null)
                else ApplySurvivor("posts", "C", null)
            }
            owner.finish()
        }
        resources.closeFromCoordinatedOwner()
    }
}

// Fresh process holds the public coordinated lifetime until its parent kills it.
// No finally, close, or release path is allowed after publishing the marker.
internal object ProcessDeathRecoveryOwner : PlatformCrossTargetTestSupport() {
    @JvmStatic fun main(args: Array<String>) = runBlocking {
        val cut = args[0]
        if (cut == "missing_marker") while (true) delay(50)
        val database = newCoordinatedFileBackedDb(args[1])
        val expected = Path.of(args[2])
        val marker = Path.of(args[3])
        val server = MockSyncServer()
        val http = server.newHttpClient()
        createUsersAndPostsTables(database)
        val client = database.openClient(recoveryConfig, http, ServerWinsResolver)
        client.attach("user-1").getOrThrow()
        insertUser(database, "P", "parent")
        insertPost(database, "C", "P", "clean child")
        client.pushPending().getOrThrow()
        if (cut.startsWith("plan_")) {
            holdPlannerPhase(cut, client, database, args[1], expected, marker)
        }
        suspend fun park() {
            Files.writeString(marker, cut)
            while (true) delay(50)
        }
        if (cut in setOf("stage_close", "page_ready", "applied_row")) {
            suspend fun witnessDurableStage() {
                val observer = newFileBackedDb(args[1])
                try {
                    val durable = processDeathStateVector(observer)
                    Files.writeString(expected, Json.encodeToString(durable))
                } finally { observer.close() }
                park()
            }
            when (cut) {
                "stage_close" -> client.setSnapshotStageStatementCloseForTest {
                    runBlocking { witnessDurableStage() }
                }
                "page_ready" -> client.setSnapshotApplyFaultInjectorForTest(
                    SnapshotApplyFaultInjector(afterApplyPageLoaded = { witnessDurableStage() }),
                )
                "applied_row" -> client.setSnapshotApplyFaultInjectorForTest(
                    SnapshotApplyFaultInjector(afterAppliedRow = { witnessDurableStage() }),
                )
            }
            client.rebuild().getOrThrow()
            error("$cut hook did not hold the owner")
        }
        updateUserName(database, "P", "local intent")
        val loseOriginalResponse: suspend () -> Unit = {
            server.beforePushCommit = null
            server.beforePushResponse = null
            throw CancellationException("retain original upload at $cut")
        }
        if (cut == "reserved") server.beforePushCommit = loseOriginalResponse
        else server.beforePushResponse = loseOriginalResponse
        assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
        val old = processDeathStateVector(database.connection())
        val oldPrimary = databaseStateVector(database.connection(), recoveryTables, ignoreDerivedCharges = true, includeDurableStages = false)
        val frozen = checkNotNull(loadUploadJournal(database.connection()))
        val originalRequestHex = scalarText(database,
            "SELECT lower(hex(request_body)) FROM _sync_upload_journal")
        assertEquals(2L, frozen.binding.operation.sourceBundleId)
        assertEquals("frozen", frozen.phase)
        assertEquals(0L, frozen.outcomeBytes)
        Files.writeString(expected, Json.encodeToString(old))
        if (cut == "reserved") {
            val session = client.javaClass.getDeclaredField("sessionState")
                .apply { isAccessible = true }.get(client) as OversqliteClientSessionState
            val state = session.requireConnectedRuntimeState("phase-death")
            val reservation = OversqliteWorkspace(database.connection(), recoveryConfig)
                .reserve(state, applyUnit("target-death-reservation", 0, ""),
                    WorkspaceCharge(3, 100))
            check(reservation.isNotEmpty())
            val held = ByteArray(100)
            Files.writeString(marker, cut)
            while (true) {
                delay(50)
                held[0] = (held[0] + 1).toByte()
            }
        }
        suspend fun witnessCommitBoundary() {
            val observer = newFileBackedDb(args[1])
            try {
                assertEquals(oldPrimary, databaseStateVector(observer, recoveryTables, ignoreDerivedCharges = true, includeDurableStages = false),
                    "$cut original primary intent remains before settlement commit")
                val observed = checkNotNull(loadUploadJournal(observer))
                assertEquals(frozen.binding, observed.binding)
                assertEquals(frozen.requestDigest, observed.requestDigest)
                assertEquals(frozen.requestBytes, observed.requestBytes)
                assertEquals(originalRequestHex, scalarText(observer,
                    "SELECT lower(hex(request_body)) FROM _sync_upload_journal"))
                assertEquals("frozen", observed.phase)
                assertTrue(observed.outcomeBytes > 0)
                val reply = Json { ignoreUnknownKeys = true }.decodeFromString<TerminalPushOutcome>(
                    scalarText(observer, "SELECT CAST(outcome_body AS TEXT) FROM _sync_upload_journal"))
                val operation = frozen.binding.operation
                assertEquals("committed", reply.status)
                assertEquals(operation.sourceId, reply.sourceId)
                assertEquals(operation.generationId, reply.generationId)
                assertEquals(operation.generationFingerprint, reply.generationFingerprint)
                assertEquals(operation.sourceBundleId, reply.sourceBundleId)
                assertEquals(operation.canonicalRequestHash, reply.canonicalRequestHash)
                assertEquals(operation.initializationId, reply.initializationId)
                assertEquals(observed.outcomeHash, reply.outcomeHash)
                assertEquals(2L, reply.committed!!.bundleSeq)
                assertEquals(1L, reply.committed!!.rowCount)
                assertEquals(2, server.pushRequestCount, "retry resolves original C without resending F")
                assertEquals(1, server.requestCounts["/sync/push-outcomes/resolve"])
                assertEquals(1, server.requestCounts["/sync/push-outcomes/ack"], "only initial seed C is acknowledged")
                Files.writeString(expected, Json.encodeToString(processDeathStateVector(observer)))
            } finally { observer.close() }
            park()
        }
        when (cut) {
            "cache_closed" -> client.setSnapshotApplyFaultInjectorForTest(
                SnapshotApplyFaultInjector(afterStatementCacheClosed = { witnessCommitBoundary() }),
            )
            "before_commit" -> client.setSnapshotApplyFaultInjectorForTest(
                SnapshotApplyFaultInjector(beforeCommit = { witnessCommitBoundary() }),
            )
            "after_commit" -> Unit
            else -> error("unknown cut $cut")
        }
        val completed = client.pushPending().getOrThrow()
        check(cut == "after_commit") { "$cut hook did not hold the owner" }
        assertEquals(PushOutcome.COMMITTED, completed.outcome)
        assertEquals("local intent", scalarText(database, "SELECT name FROM users WHERE id='P'"))
        assertEquals(2L, scalarLong(database, "SELECT row_version FROM _sync_row_state WHERE table_name='users'"))
        assertEquals(0L, scalarLong(database, "SELECT count(*) FROM _sync_upload_journal"))
        assertEquals(0L, scalarLong(database, "SELECT count(*) FROM _sync_dirty_rows"))
        assertEquals("none", scalarText(database, "SELECT state FROM _sync_outbox_bundle"))
        val settled = processDeathStateVector(database.connection())
        Files.writeString(expected, Json.encodeToString(settled))
        park()
    }

    private suspend fun holdPlannerPhase(
        cut: String, client: DefaultOversqliteClient, database: PlatformTestDatabase,
        databasePath: String, expected: Path, marker: Path,
    ) {
        val session = client.javaClass.getDeclaredField("sessionState")
            .apply { isAccessible = true }.get(client) as OversqliteClientSessionState
        val state = session.requireConnectedRuntimeState("phase-death-plan")
        val connection = database.connection()
        val limitedConfig = recoveryConfig.copy(maxSyncWorkUnitsPerInvocation =
            if (cut == "plan_ready") 100_000 else 16)
        val unit = applyUnit("snapshot", 2, "phase-death-plan")
        val targetPhase = cut.removePrefix("plan_")
        var observed = ""
        repeat(128) { attempt ->
            val probeHttp = MockSyncServer().newHttpClient()
            val resources = OversqliteClientResourceOwner(connection, probeHttp)
            val operations = OversqliteClientOperationController(limitedConfig, resources,
                SnapshotDiagnosticsState()) {}
            val local = OversqliteLocalStore(connection, Json) { state.validated }
            val owner = OversqliteApplySurvivors(connection, local, limitedConfig, operations, Json)
            connection.transaction(TransactionMode.IMMEDIATE) {
                try {
                    owner.prepare(state,
                        if (cut == "plan_clear" && attempt > 1) applyUnit("snapshot", 3, "stale-plan") else unit,
                        2, "snapshot", 2) { index ->
                        if (index == 0) ApplySurvivor("users", "P", null)
                        else ApplySurvivor("posts", "C", null)
                    }
                } catch (_: ApplyProgressPending) {
                    // A charged work boundary leaves the last saved phase durable.
                }
            }
            resources.closeFromCoordinatedOwner()
            probeHttp.close()
            observed = scalarText(database, "SELECT phase FROM _sync_apply_plan")
            if (observed == targetPhase) {
                val observer = newFileBackedDb(databasePath)
                try {
                    Files.writeString(expected, Json.encodeToString(
                        processDeathStateVector(observer)))
                } finally { observer.close() }
                Files.writeString(marker, cut)
                while (true) delay(50)
            }
            check(observed in setOf("roots", "index", "walk", "ready")) {
                "unexpected planner phase $observed while targeting $targetPhase"
            }
        }
        error("planner phase $targetPhase not reached after 128 bounded steps; last=$observed")
    }
}
