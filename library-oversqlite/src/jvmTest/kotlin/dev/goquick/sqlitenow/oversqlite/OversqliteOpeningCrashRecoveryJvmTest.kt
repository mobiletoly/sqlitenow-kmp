package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import androidx.sqlite.driver.bundled.BundledSQLiteDriver
import androidx.sqlite.driver.bundled.SQLITE_OPEN_READWRITE
import dev.goquick.sqlitenow.core.DatabaseMigrations
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowDatabase
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import io.ktor.serialization.kotlinx.json.json
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.TimeUnit
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

private const val crashExitCode = 86
private const val childClasspathProperty = "coordinatedopening.jvmTest.classpath"
private const val childEventPrefix = "COORDINATED_OPENING_PROCESS_EVENT "

class OversqliteOpeningCrashRecoveryJvmTest {
    private data class Observation(
        val pid: Long,
        val phase: String,
        val outcome: String,
        val revision: Long,
        val drainComplete: Boolean,
        val migrationComplete: Boolean,
        val replacementState: String,
        val stagedRows: Long,
        val stagedBytes: Long,
        val writesAvailable: Boolean,
        val targetReadsAvailable: Boolean,
        val integrityHash: String,
        val preservationDigest: String,
        val pendingFrozenCount: Long,
        val pendingRequestHash: String,
        val pendingIntentRevision: Long,
        val oldSourceId: String,
        val newSourceId: String,
        val initialOldSourceId: String,
        val snapshotId: String,
        val snapshotBundleSeq: Long,
        val oldSourceRecoveryReceipt: String,
        val stagedTableRows: Long,
        val stagedMaxOrdinal: Long,
        val installedGenerationId: Long,
        val installedLocalVersion: Long,
        val userVersion: Long,
        val planDigest: String,
        val requestedGenerationIds: String,
        val migrationWitnesses: Long,
        val targetGraphTables: Long,
        val targetForeignKeys: Long,
        val foreignKeyViolations: Long,
        val draftRows: Long,
        val draftIndexWitnesses: Long,
        val draftTriggerWitnesses: Long,
    )

    private data class ScenarioResult(
        val cutPid: Long,
        val beforeResume: Observation,
        val afterResume: Observation,
    )

    private data class LocalObservation(
        val pid: Long,
        val mode: String,
        val phase: String,
        val outcome: String,
        val revision: Long,
        val userVersion: Long,
        val installedGenerationId: Long,
        val installedLocalVersion: Long,
        val preservationDigest: String,
        val writesAvailable: Boolean,
    )

    private data class LocalScenarioResult(
        val cutPid: Long,
        val beforeResume: LocalObservation,
        val afterResume: LocalObservation,
    )

    private data class Stage(
        val number: Int,
        val phase: String,
        val drainComplete: Boolean = false,
        val migrationComplete: Boolean = false,
        val replacementState: String = "none",
        val stagedRows: Long = 0,
        val writesAvailable: Boolean = false,
        val targetReadsAvailable: Boolean = false,
        val installedGenerationId: Long = 1,
        val hasNewSource: Boolean = false,
        val hasSnapshot: Boolean = false,
    )

    @Test
    fun additiveAndSparseProcessesRecoverBeforeAndAfterEveryU0ThroughU8Commit() {
        val stages = listOf(
            Stage(0, "upgrade_started"),
            Stage(1, "resolving_old_intent"),
            Stage(2, "resolving_old_intent", drainComplete = true),
            Stage(3, "migrating_local", drainComplete = true),
            Stage(
                4,
                "local_migrated",
                drainComplete = true,
                migrationComplete = true,
                installedGenerationId = 2,
            ),
            Stage(
                5,
                "reattach_pending",
                drainComplete = true,
                migrationComplete = true,
                replacementState = "reserved",
                installedGenerationId = 2,
                hasNewSource = true,
            ),
            Stage(
                6,
                "rebuild_running",
                drainComplete = true,
                migrationComplete = true,
                replacementState = "confirmed",
                installedGenerationId = 2,
                hasNewSource = true,
                hasSnapshot = true,
            ),
            Stage(
                7,
                "rebuild_running",
                drainComplete = true,
                migrationComplete = true,
                replacementState = "confirmed",
                stagedRows = 1,
                installedGenerationId = 2,
                hasNewSource = true,
                hasSnapshot = true,
            ),
            Stage(
                8,
                "ready",
                drainComplete = true,
                migrationComplete = true,
                replacementState = "confirmed",
                stagedRows = 1,
                writesAvailable = true,
                targetReadsAvailable = true,
                installedGenerationId = 2,
                hasNewSource = true,
                hasSnapshot = true,
            ),
        )

        for (targetGeneration in listOf(2L, 3L)) {
          for (stage in stages) {
            val label = "g1-g$targetGeneration U${stage.number}"
            val before = runScenario(targetGeneration, "before_u${stage.number}")
            val after = runScenario(targetGeneration, "after_u${stage.number}")

            assertEquals(before.beforeResume.revision + 1L, after.beforeResume.revision, label)
            assertEquals(stage.phase, after.beforeResume.phase, label)
            assertEquals(if (stage.number == 8) "ready" else "", after.beforeResume.outcome, label)
            assertEquals(stage.drainComplete, after.beforeResume.drainComplete, label)
            assertEquals(stage.migrationComplete, after.beforeResume.migrationComplete, label)
            assertEquals(stage.replacementState, after.beforeResume.replacementState, label)
            assertEquals(stage.stagedRows, after.beforeResume.stagedRows, label)
            assertEquals(stage.writesAvailable, after.beforeResume.writesAvailable, label)
            assertEquals(stage.targetReadsAvailable, after.beforeResume.targetReadsAvailable, label)
            assertEquals(if (stage.number >= 4) targetGeneration else 1L, after.beforeResume.installedGenerationId, label)
            assertEquals(if (stage.number >= 4) 4L else 3L, after.beforeResume.installedLocalVersion, label)
            assertEquals(if (stage.number >= 4) 4L else 3L, after.beforeResume.userVersion, label)
            assertTrue(after.beforeResume.integrityHash.isNotBlank(), label)
            assertTrue(after.beforeResume.preservationDigest.isNotBlank(), label)
            assertTrue(after.beforeResume.planDigest.isNotBlank(), label)
            assertTrue(after.beforeResume.oldSourceId.isNotBlank(), label)
            assertEquals(after.beforeResume.oldSourceId, after.beforeResume.initialOldSourceId, label)
            assertEquals(0L, after.beforeResume.pendingFrozenCount, label)
            assertEquals("", after.beforeResume.pendingRequestHash, label)
            assertEquals(0L, after.beforeResume.pendingIntentRevision, label)
            assertEquals("", after.beforeResume.oldSourceRecoveryReceipt, label)
            assertEquals(stage.hasNewSource, after.beforeResume.newSourceId.isNotBlank(), label)
            assertEquals(stage.hasSnapshot, after.beforeResume.snapshotId.isNotBlank(), label)
            assertEquals(if (stage.hasSnapshot) 1L else 0L, after.beforeResume.snapshotBundleSeq, label)
            assertEquals(if (stage.number >= 4) 1L else 0L, after.beforeResume.migrationWitnesses, label)
            assertEquals(if (stage.number >= 4) 4L else 2L, after.beforeResume.targetGraphTables, label)
            assertEquals(if (stage.number >= 4) 2L else 0L, after.beforeResume.targetForeignKeys, label)
            assertEquals(0L, after.beforeResume.foreignKeyViolations, label)
            assertEquals(1L, after.beforeResume.draftRows, label)
            assertEquals(1L, after.beforeResume.draftIndexWitnesses, label)
            assertEquals(1L, after.beforeResume.draftTriggerWitnesses, label)
            if (stage.number == 7) {
                assertEquals(1L, after.beforeResume.stagedTableRows)
                assertEquals(1L, after.beforeResume.stagedMaxOrdinal)
            }

            for (scenario in listOf(before, after)) {
                assertNotEquals(scenario.cutPid, scenario.beforeResume.pid, "fresh observer for U${stage.number}")
                assertEquals(scenario.beforeResume.pid, scenario.afterResume.pid, "one observer owns resume")
                assertEquals("ready", scenario.afterResume.phase, "resume U${stage.number}")
                assertEquals("ready", scenario.afterResume.outcome, "resume U${stage.number}")
                assertTrue(scenario.afterResume.writesAvailable, "resume U${stage.number}")
                assertTrue(scenario.afterResume.targetReadsAvailable, "resume U${stage.number}")
                assertEquals(1L, scenario.afterResume.stagedRows, "resume U${stage.number}")
                assertEquals(targetGeneration, scenario.afterResume.installedGenerationId, label)
                assertEquals(4L, scenario.afterResume.installedLocalVersion, label)
                assertEquals(4L, scenario.afterResume.userVersion, label)
                assertTrue(scenario.afterResume.integrityHash.isNotBlank(), "resume U${stage.number}")
                assertTrue(scenario.afterResume.preservationDigest.isNotBlank(), "resume U${stage.number}")
                assertEquals(1L, scenario.afterResume.migrationWitnesses, label)
                assertEquals(4L, scenario.afterResume.targetGraphTables, label)
                assertEquals(2L, scenario.afterResume.targetForeignKeys, label)
                assertEquals(0L, scenario.afterResume.foreignKeyViolations, label)
                assertEquals(1L, scenario.afterResume.draftRows, label)
                assertEquals(1L, scenario.afterResume.draftIndexWitnesses, label)
                assertEquals(1L, scenario.afterResume.draftTriggerWitnesses, label)
                assertTrue(targetGeneration.toString() in scenario.afterResume.requestedGenerationIds.split(','), label)
                if (targetGeneration == 3L) {
                    assertTrue("2" !in scenario.afterResume.requestedGenerationIds.split(','), label)
                }
            }
          }
        }
    }

    @Test
    fun localOnlyReadyProcessDiesBeforeGenerationOpeningAndNextProcessCompletesEdge() {
        val root = createTempDirectory("schemaevolution-local-ready-boundary-")
        val database = root.resolve("client.sqlite3")
        try {
            assertEquals(0, runChild(root, "schemaevolution-seed", database.toString()).exitCode)
            val first = runChild(root, "schemaevolution-local-ready-exit", database.toString())
            assertEquals(crashExitCode, first.exitCode, first.output)
            val firstReady = localObservation(event(first.output, "local_ready"))
            assertEquals("ready", firstReady.phase)
            assertEquals("ready", firstReady.outcome)
            assertEquals(1L, firstReady.installedGenerationId)
            assertEquals(3L, firstReady.installedLocalVersion)
            assertTrue(firstReady.writesAvailable)

            val second = runChild(root, "schemaevolution-observe-local-additive", database.toString())
            assertEquals(0, second.exitCode, second.output)
            val before = localObservation(event(second.output, "before_generation"))
            val after = observation(event(second.output, "after_resume"))
            assertEquals("ready", before.phase)
            assertEquals(1L, before.installedGenerationId)
            assertEquals(3L, before.installedLocalVersion)
            assertEquals("ready", after.phase)
            assertEquals(2L, after.installedGenerationId)
            assertEquals(4L, after.installedLocalVersion)
            assertEquals(1L, after.migrationWitnesses)
            assertEquals(4L, after.targetGraphTables)
            assertEquals(2L, after.targetForeignKeys)
            assertEquals(0L, after.foreignKeyViolations)
            assertEquals(1L, after.draftRows)
            assertEquals(1L, after.draftIndexWitnesses)
            assertEquals(1L, after.draftTriggerWitnesses)
        } finally {
            root.toFile().deleteRecursively()
        }
    }

    @Test
    fun freshProcessesRecoverBeforeAndAfterFreshAndLocalOnlyCommits() {
        for (mode in listOf("fresh", "local_only")) {
            val targetVersion = if (mode == "fresh") 2L else 3L
            for (commit in listOf("journal", "ready")) {
                val before = runLocalScenario(mode, "before_${mode}_$commit")
                val after = runLocalScenario(mode, "after_${mode}_$commit")
                assertEquals(before.beforeResume.revision + 1L, after.beforeResume.revision, "$mode $commit")
                val expectedPhase = if (commit == "journal") "migrating_local" else "ready"
                assertEquals(mode, after.beforeResume.mode)
                assertEquals(expectedPhase, after.beforeResume.phase)
                assertEquals(if (commit == "ready") "ready" else "", after.beforeResume.outcome)
                assertEquals(if (commit == "ready") targetVersion else if (mode == "fresh") 0L else 2L, after.beforeResume.userVersion)
                assertEquals(if (commit == "ready" || mode == "local_only") 1L else 0L, after.beforeResume.installedGenerationId)
                assertEquals(
                    if (commit == "ready") targetVersion else if (mode == "fresh") -1L else 2L,
                    after.beforeResume.installedLocalVersion,
                )
                assertEquals(commit == "ready", after.beforeResume.writesAvailable)
                assertEquals(mode == "local_only", after.beforeResume.preservationDigest.isNotBlank())

                for (scenario in listOf(before, after)) {
                    assertNotEquals(scenario.cutPid, scenario.beforeResume.pid)
                    assertEquals(scenario.beforeResume.pid, scenario.afterResume.pid)
                    assertEquals(mode, scenario.afterResume.mode)
                    assertEquals("ready", scenario.afterResume.phase)
                    assertEquals("ready", scenario.afterResume.outcome)
                    assertEquals(targetVersion, scenario.afterResume.userVersion)
                    assertEquals(1L, scenario.afterResume.installedGenerationId)
                    assertEquals(targetVersion, scenario.afterResume.installedLocalVersion)
                    assertTrue(scenario.afterResume.writesAvailable)
                }
            }
        }
    }

    private fun runScenario(targetGeneration: Long, cut: String): ScenarioResult {
        val root = createTempDirectory("schemaevolution-g1-g$targetGeneration-crash-$cut-")
        val database = root.resolve("client.sqlite3")
        try {
            assertEquals(0, runChild(root, "schemaevolution-seed-local", database.toString()).exitCode, "seed $cut")
            val crashed = runChild(root, "schemaevolution-cut-g$targetGeneration", database.toString(), cut)
            assertEquals(crashExitCode, crashed.exitCode, "cut $cut\n${crashed.output}")
            val cutEvent = event(crashed.output, "cut")

            val observer = runChild(root, "schemaevolution-observe-resume-g$targetGeneration", database.toString())
            assertEquals(0, observer.exitCode, "observer $cut\n${observer.output}")
            return ScenarioResult(
                cutPid = cutEvent.getValue("pid").jsonPrimitive.content.toLong(),
                beforeResume = observation(event(observer.output, "before_resume")),
                afterResume = observation(event(observer.output, "after_resume")),
            )
        } finally {
            root.toFile().deleteRecursively()
        }
    }

    private fun runLocalScenario(mode: String, cut: String): LocalScenarioResult {
        val root = createTempDirectory("coordinatedopening-opening-$mode-$cut-")
        val database = root.resolve("client.sqlite3")
        try {
            if (mode == "local_only") {
                assertEquals(0, runChild(root, "seed", database.toString()).exitCode, "seed $cut")
            }
            val crashed = runChild(root, "cut-$mode", database.toString(), cut)
            assertEquals(crashExitCode, crashed.exitCode, "cut $cut\n${crashed.output}")
            val cutEvent = event(crashed.output, "cut")

            val observer = runChild(root, "observe-resume-$mode", database.toString())
            assertEquals(0, observer.exitCode, "observer $cut\n${observer.output}")
            return LocalScenarioResult(
                cutPid = cutEvent.getValue("pid").jsonPrimitive.content.toLong(),
                beforeResume = localObservation(event(observer.output, "before_resume")),
                afterResume = localObservation(event(observer.output, "after_resume")),
            )
        } finally {
            root.toFile().deleteRecursively()
        }
    }

    private data class ChildResult(val exitCode: Int, val output: String)

    private fun runChild(root: Path, vararg arguments: String): ChildResult {
        val classpath = checkNotNull(System.getProperty(childClasspathProperty)) {
            "$childClasspathProperty is not configured"
        }
        val log = Files.createTempFile(root, "child-", ".log")
        val process = ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-cp",
            classpath,
            OversqliteOpeningCrashProcess::class.java.name,
            *arguments,
        ).redirectErrorStream(true).redirectOutput(log.toFile()).start()
        if (!process.waitFor(90, TimeUnit.SECONDS)) {
            process.destroyForcibly()
            process.waitFor()
            error("child timed out: ${arguments.joinToString(" ")}")
        }
        return ChildResult(process.exitValue(), Files.readString(log))
    }

    private fun event(output: String, name: String) = output.lineSequence()
        .filter { it.startsWith(childEventPrefix) }
        .map { Json.parseToJsonElement(it.removePrefix(childEventPrefix)).jsonObject }
        .single { it.getValue("event").jsonPrimitive.content == name }

    private fun observation(value: kotlinx.serialization.json.JsonObject) = Observation(
        pid = value.getValue("pid").jsonPrimitive.content.toLong(),
        phase = value.getValue("phase").jsonPrimitive.content,
        outcome = value.getValue("outcome").jsonPrimitive.content,
        revision = value.getValue("revision").jsonPrimitive.content.toLong(),
        drainComplete = value.getValue("drain_complete").jsonPrimitive.content.toBooleanStrict(),
        migrationComplete = value.getValue("migration_complete").jsonPrimitive.content.toBooleanStrict(),
        replacementState = value.getValue("replacement_state").jsonPrimitive.content,
        stagedRows = value.getValue("staged_rows").jsonPrimitive.content.toLong(),
        stagedBytes = value.getValue("staged_bytes").jsonPrimitive.content.toLong(),
        writesAvailable = value.getValue("writes_available").jsonPrimitive.content.toBooleanStrict(),
        targetReadsAvailable = value.getValue("target_reads_available").jsonPrimitive.content.toBooleanStrict(),
        integrityHash = value.getValue("integrity_hash").jsonPrimitive.content,
        preservationDigest = value.getValue("preservation_digest").jsonPrimitive.content,
        pendingFrozenCount = value.getValue("pending_frozen_count").jsonPrimitive.content.toLong(),
        pendingRequestHash = value.getValue("pending_request_hash").jsonPrimitive.content,
        pendingIntentRevision = value.getValue("pending_intent_revision").jsonPrimitive.content.toLong(),
        oldSourceId = value.getValue("old_source_id").jsonPrimitive.content,
        newSourceId = value.getValue("new_source_id").jsonPrimitive.content,
        initialOldSourceId = value.getValue("initial_old_source_id").jsonPrimitive.content,
        snapshotId = value.getValue("snapshot_id").jsonPrimitive.content,
        snapshotBundleSeq = value.getValue("snapshot_bundle_seq").jsonPrimitive.content.toLong(),
        oldSourceRecoveryReceipt = value.getValue("old_source_recovery_receipt").jsonPrimitive.content,
        stagedTableRows = value.getValue("staged_table_rows").jsonPrimitive.content.toLong(),
        stagedMaxOrdinal = value.getValue("staged_max_ordinal").jsonPrimitive.content.toLong(),
        installedGenerationId = value.getValue("installed_generation_id").jsonPrimitive.content.toLong(),
        installedLocalVersion = value.getValue("installed_local_version").jsonPrimitive.content.toLong(),
        userVersion = value.getValue("user_version").jsonPrimitive.content.toLong(),
        planDigest = value.getValue("plan_digest").jsonPrimitive.content,
        requestedGenerationIds = value.getValue("requested_generation_ids").jsonPrimitive.content,
        migrationWitnesses = value.getValue("migration_witnesses").jsonPrimitive.content.toLong(),
        targetGraphTables = value.getValue("target_graph_tables").jsonPrimitive.content.toLong(),
        targetForeignKeys = value.getValue("target_foreign_keys").jsonPrimitive.content.toLong(),
        foreignKeyViolations = value.getValue("foreign_key_violations").jsonPrimitive.content.toLong(),
        draftRows = value.getValue("draft_rows").jsonPrimitive.content.toLong(),
        draftIndexWitnesses = value.getValue("draft_index_witnesses").jsonPrimitive.content.toLong(),
        draftTriggerWitnesses = value.getValue("draft_trigger_witnesses").jsonPrimitive.content.toLong(),
    )

    private fun localObservation(value: kotlinx.serialization.json.JsonObject) = LocalObservation(
        pid = value.getValue("pid").jsonPrimitive.content.toLong(),
        mode = value.getValue("mode").jsonPrimitive.content,
        phase = value.getValue("phase").jsonPrimitive.content,
        outcome = value.getValue("outcome").jsonPrimitive.content,
        revision = value.getValue("revision").jsonPrimitive.content.toLong(),
        userVersion = value.getValue("user_version").jsonPrimitive.content.toLong(),
        installedGenerationId = value.getValue("installed_generation_id").jsonPrimitive.content.toLong(),
        installedLocalVersion = value.getValue("installed_local_version").jsonPrimitive.content.toLong(),
        preservationDigest = value.getValue("preservation_digest").jsonPrimitive.content,
        writesAvailable = value.getValue("writes_available").jsonPrimitive.content.toBooleanStrict(),
    )
}

internal object OversqliteOpeningCrashProcess {
    @JvmStatic
    fun main(args: Array<String>) = runBlocking {
        require(args.size in 2..3) { "expected mode database [cut]" }
        when (args[0]) {
            "seed" -> seed(args[1])
            "cut" -> crashAt(args[1], args.getOrNull(2) ?: error("cut is required"))
            "observe-resume" -> observeAndResume(args[1])
            "cut-fresh" -> crashAtLocalMode(args[1], args.getOrNull(2) ?: error("cut is required"), false)
            "cut-local_only" -> crashAtLocalMode(args[1], args.getOrNull(2) ?: error("cut is required"), true)
            "observe-resume-fresh" -> observeAndResumeLocalMode(args[1], false)
            "observe-resume-local_only" -> observeAndResumeLocalMode(args[1], true)
            "schemaevolution-seed" -> schemaevolutionSeed(args[1])
            "schemaevolution-seed-local" -> schemaevolutionSeedLocal(args[1])
            "schemaevolution-cut-g2" -> schemaevolutionCrashAt(args[1], args.getOrNull(2) ?: error("cut is required"), 2)
            "schemaevolution-cut-g3" -> schemaevolutionCrashAt(args[1], args.getOrNull(2) ?: error("cut is required"), 3)
            "schemaevolution-observe-resume-g2" -> schemaevolutionObserveAndResume(args[1], 2, 4, 3)
            "schemaevolution-observe-resume-g3" -> schemaevolutionObserveAndResume(args[1], 3, 4, 3)
            "schemaevolution-local-ready-exit" -> schemaevolutionLocalReadyExit(args[1])
            "schemaevolution-observe-local-additive" -> schemaevolutionObserveLocalThenAdditive(args[1])
            else -> error("unknown mode ${args[0]}")
        }
    }

    private suspend fun schemaevolutionSeed(path: String) {
        val database = schemaevolutionCrashDatabase(path, 2)
        val http = schemaevolutionCrashGenerationHttp(schemaevolutionCrashGeneration(1), path, includeConnect = true)
        val opening = database.openForSync(schemaevolutionCrashConfig(1), http)
        try {
            opening.advance()
            val ready = checkNotNull(opening.readyOrNull())
            ready.database.connection().execSQL(
                "INSERT INTO drafts(id,label,payload) VALUES('draft-a','mixed',X'000102FF')",
            )
            ready.client.attach("crash-user").getOrThrow()
        } finally {
            opening.close()
            http.close()
        }
    }

    private suspend fun schemaevolutionSeedLocal(path: String) {
        schemaevolutionSeed(path)
        val database = schemaevolutionCrashDatabase(path, 3, includeTargetGraph = false)
        val http = crashOfflineHttp()
        val opening = database.openForSync(
            schemaevolutionCrashConfig(1),
            http,
            upgradePlan = schemaevolutionLocalOnlyPlan(),
        )
        try {
            opening.advance()
            checkNotNull(opening.readyOrNull())
            check(database.connection().scalarLong("SELECT count(*) FROM drafts WHERE normalized='MIXED'") == 1L)
        } finally {
            opening.close()
            http.close()
        }
    }

    private suspend fun schemaevolutionCrashAt(path: String, cut: String, targetGeneration: Long) {
        openingDurableCommitForTest = { observed, _ ->
            if (observed == cut) {
                emitProcessEvent("cut", mapOf("pid" to ProcessHandle.current().pid(), "cut" to cut))
                System.out.flush()
                Runtime.getRuntime().halt(crashExitCode)
            }
        }
        val database = schemaevolutionCrashDatabase(path, 4)
        val http = schemaevolutionCrashGenerationHttp(schemaevolutionCrashGeneration(targetGeneration), path)
        val opening = database.openForSync(
            schemaevolutionCrashConfig(targetGeneration),
            http,
            upgradePlan = schemaevolutionCrashPlan(targetGeneration, 3, 4),
        )
        opening.advance()
        error("durable cut was not reached: $cut")
    }

    private suspend fun schemaevolutionObserveAndResume(
        path: String,
        targetGeneration: Long,
        targetLocalVersion: Int,
        fromLocalVersion: Int,
    ) {
        emitObservation("before_resume", path)
        val database = schemaevolutionCrashDatabase(path, targetLocalVersion)
        val http = schemaevolutionCrashGenerationHttp(schemaevolutionCrashGeneration(targetGeneration), path)
        val opening = database.openForSync(
            schemaevolutionCrashConfig(targetGeneration),
            http,
            upgradePlan = schemaevolutionCrashPlan(targetGeneration, fromLocalVersion, targetLocalVersion),
        )
        try {
            repeat(4) {
                if (opening.readyOrNull() == null) opening.advance()
            }
            checkNotNull(opening.readyOrNull()) { "schemaevolution opening did not publish after recovery" }
            requireSchemaEvolutionTargetGraph(database)
            emitObservation("after_resume", path)
        } finally {
            opening.close()
            http.close()
        }
    }

    private suspend fun schemaevolutionLocalReadyExit(path: String) {
        val database = schemaevolutionCrashDatabase(path, 3, includeTargetGraph = false)
        val http = crashOfflineHttp()
        val opening = database.openForSync(
            schemaevolutionCrashConfig(1),
            http,
            upgradePlan = schemaevolutionLocalOnlyPlan(),
        )
        opening.advance()
        checkNotNull(opening.readyOrNull())
        check(database.connection().scalarLong("SELECT count(*) FROM drafts WHERE normalized='MIXED'") == 1L)
        emitLocalObservation("local_ready", path)
        System.out.flush()
        Runtime.getRuntime().halt(crashExitCode)
    }

    private suspend fun schemaevolutionObserveLocalThenAdditive(path: String) {
        emitLocalObservation("before_generation", path)
        schemaevolutionObserveAndResume(path, targetGeneration = 2, targetLocalVersion = 4, fromLocalVersion = 3)
    }

    private suspend fun requireSchemaEvolutionTargetGraph(database: SqliteNowDatabase) {
        val connection = database.connection()
        check(connection.scalarLong("SELECT count(*) FROM schemaevolution_migration_witness") == 1L)
        check(connection.scalarLong("SELECT count(*) FROM pragma_foreign_key_check") == 0L)
        check(connection.scalarLong("SELECT count(*) FROM drafts WHERE label='mixed' AND hex(payload)='000102FF'") == 1L)
        check(connection.scalarLong(
            "SELECT count(*) FROM sqlite_schema WHERE name IN ('drafts_normalized','drafts_touch')",
        ) == 2L)
    }

    private suspend fun seed(path: String) {
        val database = crashDatabase(path)
        val http = crashGenerationHttp(crashGeneration(1), includeConnect = true)
        val opening = database.openForSync(crashConfig(1), http)
        try {
            opening.advance()
            opening.readyOrNull()!!.client.attach("crash-user").getOrThrow()
            database.connection().execSQL("CREATE TABLE drafts(label TEXT,payload BLOB)")
            database.connection().execSQL("INSERT INTO drafts VALUES('kept',X'000102FF')")
        } finally {
            opening.close()
            http.close()
        }
    }

    private suspend fun crashAt(path: String, cut: String) {
        openingDurableCommitForTest = { observed, _ ->
            if (observed == cut) {
                emitProcessEvent("cut", mapOf("pid" to ProcessHandle.current().pid(), "cut" to cut))
                System.out.flush()
                Runtime.getRuntime().halt(crashExitCode)
            }
        }
        val database = crashDatabase(path)
        val http = crashGenerationHttp(crashGeneration(2))
        val opening = database.openForSync(crashConfig(2), http, upgradePlan = crashPlan())
        opening.advance()
        error("durable cut was not reached: $cut")
    }

    private suspend fun crashAtLocalMode(path: String, cut: String, localOnly: Boolean) {
        openingDurableCommitForTest = { observed, _ ->
            if (observed == cut) {
                emitProcessEvent("cut", mapOf("pid" to ProcessHandle.current().pid(), "cut" to cut))
                System.out.flush()
                Runtime.getRuntime().halt(crashExitCode)
            }
        }
        val database = crashDatabase(path, if (localOnly) 3 else 2)
        val http = crashOfflineHttp()
        val opening = database.openForSync(
            crashConfig(1),
            http,
            upgradePlan = if (localOnly) crashLocalOnlyPlan() else null,
        )
        opening.advance()
        error("durable cut was not reached: $cut")
    }

    private suspend fun observeAndResume(path: String) {
        emitObservation("before_resume", path)
        val database = crashDatabase(path)
        val http = crashGenerationHttp(crashGeneration(2))
        val opening = database.openForSync(crashConfig(2), http, upgradePlan = crashPlan())
        try {
            repeat(4) {
                if (opening.readyOrNull() == null) opening.advance()
            }
            checkNotNull(opening.readyOrNull()) { "opening did not publish after recovery" }
            check(database.connection().scalarLong("SELECT count(*) FROM drafts WHERE label='kept'") == 1L)
            check(database.connection().scalarLong("SELECT count(*) FROM notes WHERE title='recovered'") == 1L)
            emitObservation("after_resume", path)
        } finally {
            opening.close()
            http.close()
        }
    }

    private suspend fun observeAndResumeLocalMode(path: String, localOnly: Boolean) {
        emitLocalObservation("before_resume", path)
        val database = crashDatabase(path, if (localOnly) 3 else 2)
        val http = crashOfflineHttp()
        val opening = database.openForSync(
            crashConfig(1),
            http,
            upgradePlan = if (localOnly) crashLocalOnlyPlan() else null,
        )
        try {
            opening.advance()
            checkNotNull(opening.readyOrNull()) { "local opening did not publish after recovery" }
            if (localOnly) {
                check(database.connection().scalarLong("SELECT count(*) FROM drafts WHERE label='kept' AND category='migrated'") == 1L)
            } else {
                check(database.connection().scalarLong("SELECT count(*) FROM notes") == 0L)
            }
            emitLocalObservation("after_resume", path)
        } finally {
            opening.close()
            http.close()
        }
    }

    private fun emitObservation(event: String, path: String) {
        BundledSQLiteDriver().open(path, SQLITE_OPEN_READWRITE).use { connection ->
            connection.prepare(
                """
                SELECT phase,outcome,revision,drain_complete,migration_complete,replacement_state,
                       COALESCE(staged_row_count,0),COALESCE(staged_byte_count,0),
                       writes_available,target_reads_available,integrity_hash,preservation_digest,
                       pending_frozen_count,pending_request_hash,pending_intent_revision,
                       old_source_id,new_source_id,initial_old_source_id,snapshot_id,
                       snapshot_bundle_seq,old_source_recovery_receipt,plan_digest
                FROM _sync_opening_state WHERE singleton_key=1
                """.trimIndent(),
            ).use { statement ->
                check(statement.step()) { "opening observation is missing" }
                emitProcessEvent(
                    event,
                    mapOf(
                        "pid" to ProcessHandle.current().pid(),
                        "phase" to statement.getText(0),
                        "outcome" to statement.getText(1),
                        "revision" to statement.getLong(2),
                        "drain_complete" to (statement.getLong(3) == 1L),
                        "migration_complete" to (statement.getLong(4) == 1L),
                        "replacement_state" to statement.getText(5),
                        "staged_rows" to statement.getLong(6),
                        "staged_bytes" to statement.getLong(7),
                        "writes_available" to (statement.getLong(8) == 1L),
                        "target_reads_available" to (statement.getLong(9) == 1L),
                        "integrity_hash" to statement.getText(10),
                        "preservation_digest" to statement.getText(11),
                        "pending_frozen_count" to statement.getLong(12),
                        "pending_request_hash" to statement.getText(13),
                        "pending_intent_revision" to statement.getLong(14),
                        "old_source_id" to statement.getText(15),
                        "new_source_id" to statement.getText(16),
                        "initial_old_source_id" to statement.getText(17),
                        "snapshot_id" to statement.getText(18),
                        "snapshot_bundle_seq" to statement.getLong(19),
                        "old_source_recovery_receipt" to statement.getText(20),
                        "plan_digest" to statement.getText(21),
                        "staged_table_rows" to connection.scalarLongForProcess(
                            "SELECT count(*) FROM _sync_snapshot_stage",
                        ),
                        "staged_max_ordinal" to connection.scalarLongForProcess(
                            "SELECT COALESCE(max(row_ordinal),0) FROM _sync_snapshot_stage",
                        ),
                        "installed_generation_id" to connection.scalarLongForProcess(
                            "SELECT generation_id FROM _sync_contract_identity WHERE singleton_key=1",
                        ),
                        "installed_local_version" to connection.scalarLongForProcess(
                            "SELECT local_version FROM _sync_contract_identity WHERE singleton_key=1",
                        ),
                        "user_version" to connection.scalarLongForProcess("PRAGMA user_version"),
                        "requested_generation_ids" to schemaevolutionRequestedGenerationIds(path),
                        "migration_witnesses" to connection.optionalScalarLongForProcess(
                            "schemaevolution_migration_witness",
                            "SELECT count(*) FROM schemaevolution_migration_witness",
                        ),
                        "target_graph_tables" to listOf("people", "notes", "projects", "attachments").sumOf { table ->
                            connection.scalarLongForProcess(
                                "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='$table'",
                            )
                        },
                        "target_foreign_keys" to listOf("people", "attachments").sumOf { table ->
                            connection.scalarLongForProcess("SELECT count(*) FROM pragma_foreign_key_list('$table')")
                        },
                        "foreign_key_violations" to connection.scalarLongForProcess(
                            "SELECT count(*) FROM pragma_foreign_key_check",
                        ),
                        "draft_rows" to connection.scalarLongForProcess(
                            "SELECT count(*) FROM drafts WHERE label='mixed' AND hex(payload)='000102FF'",
                        ),
                        "draft_index_witnesses" to connection.scalarLongForProcess(
                            "SELECT count(*) FROM sqlite_schema WHERE type='index' AND name='drafts_normalized'",
                        ),
                        "draft_trigger_witnesses" to connection.scalarLongForProcess(
                            "SELECT count(*) FROM sqlite_schema WHERE type='trigger' AND name='drafts_touch'",
                        ),
                    ),
                )
            }
        }
    }

    private fun emitLocalObservation(event: String, path: String) {
        BundledSQLiteDriver().open(path, SQLITE_OPEN_READWRITE).use { connection ->
            val hasJournal = connection.scalarLongForProcess(
                "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='_sync_opening_state'",
            ) == 1L && connection.scalarLongForProcess(
                "SELECT count(*) FROM _sync_opening_state WHERE singleton_key=1",
            ) == 1L
            val hasIdentity = connection.scalarLongForProcess(
                "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='_sync_contract_identity'",
            ) == 1L && connection.scalarLongForProcess(
                "SELECT count(*) FROM _sync_contract_identity WHERE singleton_key=1",
            ) == 1L
            var mode = ""
            var phase = ""
            var outcome = ""
            var revision = 0L
            var preservationDigest = ""
            var writesAvailable = false
            if (hasJournal) {
                connection.prepare(
                    "SELECT mode,phase,outcome,revision,preservation_digest,writes_available " +
                        "FROM _sync_opening_state WHERE singleton_key=1",
                ).use { statement ->
                    check(statement.step())
                    mode = statement.getText(0)
                    phase = statement.getText(1)
                    outcome = statement.getText(2)
                    revision = statement.getLong(3)
                    preservationDigest = statement.getText(4)
                    writesAvailable = statement.getLong(5) == 1L
                }
            }
            emitProcessEvent(
                event,
                mapOf(
                    "pid" to ProcessHandle.current().pid(),
                    "mode" to mode,
                    "phase" to phase,
                    "outcome" to outcome,
                    "revision" to revision,
                    "user_version" to connection.scalarLongForProcess("PRAGMA user_version"),
                    "installed_generation_id" to if (hasIdentity) connection.scalarLongForProcess(
                        "SELECT generation_id FROM _sync_contract_identity WHERE singleton_key=1",
                    ) else 0L,
                    "installed_local_version" to if (hasIdentity) connection.scalarLongForProcess(
                        "SELECT local_version FROM _sync_contract_identity WHERE singleton_key=1",
                    ) else -1L,
                    "preservation_digest" to preservationDigest,
                    "writes_available" to writesAvailable,
                ),
            )
        }
    }
}

private fun androidx.sqlite.SQLiteConnection.scalarLongForProcess(sql: String): Long =
    prepare(sql).use { statement ->
        check(statement.step()) { "scalar observation is missing: $sql" }
        statement.getLong(0)
    }

private fun androidx.sqlite.SQLiteConnection.optionalScalarLongForProcess(
    table: String,
    sql: String,
): Long = if (scalarLongForProcess(
        "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='$table'",
    ) == 0L
) 0L else scalarLongForProcess(sql)

private fun schemaevolutionRequestedGenerationIds(path: String): String {
    val requestLog = Path.of("$path.requests")
    if (!Files.exists(requestLog)) return ""
    return Files.readAllLines(requestLog).filter(String::isNotBlank).distinct().joinToString(",")
}

private fun emitProcessEvent(event: String, values: Map<String, Any>) {
    val json = buildJsonObject {
        put("event", event)
        for ((name, value) in values) {
            when (value) {
                is Boolean -> put(name, value)
                is Int -> put(name, value)
                is Long -> put(name, value)
                is String -> put(name, value)
                else -> error("unsupported event value $name")
            }
        }
    }
    println(childEventPrefix + json)
}

private fun crashDatabase(path: String, targetLocalVersion: Int = 2) = SqliteNowDatabase(
    dbName = path,
    migration = object : DatabaseMigrations {
        override val targetVersion = targetLocalVersion

        override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
            if (currentVersion == -1) {
                conn.execSQL("CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL,title TEXT NOT NULL)")
                return 2
            }
            if (currentVersion == 2 && targetLocalVersion == 3) {
                conn.execSQL("ALTER TABLE drafts ADD COLUMN category TEXT NOT NULL DEFAULT 'migrated'")
                return 3
            }
            return currentVersion
        }
    },
    syncManaged = true,
)

private fun schemaevolutionCrashDatabase(
    path: String,
    targetLocalVersion: Int,
    includeTargetGraph: Boolean = true,
) = SqliteNowDatabase(
    dbName = path,
    migration = object : DatabaseMigrations {
        override val targetVersion = targetLocalVersion

        override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
            if (currentVersion == -1) {
                conn.execSQL("CREATE TABLE people(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)")
                conn.execSQL("CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL,body TEXT NOT NULL)")
                conn.execSQL(
                    "CREATE TABLE drafts(id TEXT PRIMARY KEY NOT NULL,label TEXT NOT NULL,payload BLOB NOT NULL)",
                )
                return 2
            }
            if (currentVersion == 2 && targetLocalVersion >= 3) {
                conn.execSQL("ALTER TABLE drafts ADD COLUMN normalized TEXT")
                if (!includeTargetGraph) {
                    conn.execSQL("UPDATE drafts SET normalized=upper(label)")
                }
                conn.execSQL("CREATE INDEX drafts_normalized ON drafts(normalized)")
                conn.execSQL(
                    "CREATE TRIGGER drafts_touch AFTER UPDATE OF label ON drafts " +
                        "BEGIN UPDATE drafts SET normalized=upper(new.label) WHERE id=new.id; END",
                )
                if (targetLocalVersion == 3 && includeTargetGraph) {
                    createSchemaEvolutionTargetGraph(conn)
                    return 3
                }
                return 3
            }
            if (currentVersion == 3 && targetLocalVersion == 4) {
                createSchemaEvolutionTargetGraph(conn)
                return 4
            }
            return currentVersion
        }
    },
    syncManaged = true,
)

private suspend fun createSchemaEvolutionTargetGraph(conn: SafeSQLiteConnection) {
    conn.execSQL("CREATE TABLE projects(id TEXT PRIMARY KEY NOT NULL,title TEXT NOT NULL)")
    conn.execSQL("ALTER TABLE people ADD COLUMN details TEXT")
    conn.execSQL(
        "ALTER TABLE people ADD COLUMN project_id TEXT REFERENCES projects(id) ON DELETE SET NULL",
    )
    conn.execSQL(
        "CREATE TABLE attachments(id TEXT PRIMARY KEY NOT NULL,person_id TEXT " +
            "REFERENCES people(id) ON DELETE CASCADE,filename TEXT NOT NULL)",
    )
    conn.execSQL("CREATE TABLE schemaevolution_migration_witness(id INTEGER PRIMARY KEY CHECK(id=1))")
    conn.execSQL("INSERT INTO schemaevolution_migration_witness(id) VALUES(1)")
}

private fun schemaevolutionCrashConfig(generationId: Long) = OversqliteConfig(
    generationId = generationId,
    databaseId = "schemaevolution-opening-crash-fixture",
    serviceId = "schemaevolution-opening-crash-service",
    syncTables = if (generationId == 1L) {
        listOf(SyncTable("people", "id"), SyncTable("notes", "id"))
    } else {
        listOf(
            SyncTable("people", "id"),
            SyncTable("notes", "id"),
            SyncTable("projects", "id"),
            SyncTable("attachments", "id"),
        )
    },
)

private fun schemaevolutionCrashPlan(
    targetGeneration: Long,
    fromLocalVersion: Int,
    toLocalVersion: Int,
) = OversqliteUpgradePlan(
    migrationId = "schemaevolution-crash-g1-g$targetGeneration-v$fromLocalVersion-v$toLocalVersion",
    fromGenerationId = 1,
    toGenerationId = targetGeneration,
    fromLocalVersion = fromLocalVersion,
    toLocalVersion = toLocalVersion,
    preservedLocalTables = setOf("drafts"),
)

private fun schemaevolutionLocalOnlyPlan() = OversqliteUpgradePlan(
    migrationId = "schemaevolution-crash-local-v2-v3",
    fromGenerationId = 1,
    toGenerationId = 1,
    fromLocalVersion = 2,
    toLocalVersion = 3,
    preservedLocalTables = setOf("drafts"),
)

private fun schemaevolutionCrashGeneration(id: Long): SyncGeneration {
    val people = GenerationTable(
        schema = "business",
        table = "people",
        syncKeyColumns = listOf("id"),
        columns = buildList {
            add(GenerationColumn("id", "uuid", false))
            add(GenerationColumn("name", "text", false))
            if (id != 1L) {
                add(GenerationColumn("details", "text", true))
                add(GenerationColumn("project_id", "uuid", true))
            }
        },
        foreignKeys = if (id == 1L) emptyList() else listOf(
            GenerationForeignKey(
                columns = listOf("project_id"),
                referencedSchema = "business",
                referencedTable = "projects",
                referencedColumns = listOf("id"),
                onDelete = "set_null",
                onUpdate = "no_action",
            ),
        ),
    )
    val notes = GenerationTable(
        schema = "business",
        table = "notes",
        syncKeyColumns = listOf("id"),
        columns = listOf(GenerationColumn("id", "uuid", false), GenerationColumn("body", "text", false)),
        foreignKeys = emptyList(),
    )
    val projects = GenerationTable(
        "business", "projects", listOf("id"),
        listOf(GenerationColumn("id", "uuid", false), GenerationColumn("title", "text", false)), emptyList(),
    )
    val attachments = GenerationTable(
        "business", "attachments", listOf("id"),
        listOf(
            GenerationColumn("id", "uuid", false),
            GenerationColumn("person_id", "uuid", true),
            GenerationColumn("filename", "text", false),
        ),
        listOf(
            GenerationForeignKey(
                columns = listOf("person_id"),
                referencedSchema = "business",
                referencedTable = "people",
                referencedColumns = listOf("id"),
                onDelete = "cascade",
                onUpdate = "no_action",
            ),
        ),
    )
    val provisional = SyncGeneration(
        id = id.toString(),
        fingerprint = "0".repeat(64),
        tables = if (id == 1L) listOf(people, notes) else listOf(people, notes, projects, attachments),
    )
    return provisional.copy(fingerprint = provisional.computeFingerprint())
}

private fun schemaevolutionCrashGenerationHttp(
    generation: SyncGeneration,
    databasePath: String,
    includeConnect: Boolean = false,
): HttpClient {
    val discovery = Json.encodeToString(GenerationDiscovery("v1", generation, testWireLimits(), false))
    val installedDiscovery = Json.encodeToString(
        GenerationDiscovery("v1", schemaevolutionCrashGeneration(1), testWireLimits(), false),
    )
    val row = SnapshotRow(
        schema = "business",
        table = "notes",
        key = mapOf("id" to "33333333-3333-4333-8333-333333333333"),
        rowVersion = 1,
        payload = buildJsonObject {
            put("id", "33333333-3333-4333-8333-333333333333")
            put("body", "recovered")
        },
    )
    val snapshotRows = listOf(BundleRow(row.schema, row.table, row.key, "INSERT", row.rowVersion, row.payload))
    val byteCount = OversqliteCompactCodec.snapshotLogicalByteCount(generation, snapshotRows)
    return HttpClient(MockEngine { request ->
        if (generation.id != "1" && request.url.encodedPath == "/sync/generations/1") {
            return@MockEngine respond(
                installedDiscovery,
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }
        when (request.url.encodedPath) {
            "/sync/generations/${generation.id}" -> {
                Files.writeString(
                    Path.of("$databasePath.requests"),
                    "${generation.id}\n",
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND,
                )
                respond(discovery, headers = headersOf(HttpHeaders.ContentType, "application/json"))
            }
            "/sync/connect" -> {
                respond(
                    Json.encodeToString(
                        ConnectResponse(
                            resolution = if (includeConnect) "initialize_empty" else "remote_authoritative",
                        ),
                    ),
                    headers = headersOf(HttpHeaders.ContentType, "application/json"),
                )
            }
            "/sync/snapshot-sessions" -> respond(
                Json.encodeToString(
                    SnapshotSession(
                        snapshotId = "schemaevolution-snapshot-g${generation.id}",
                        snapshotBundleSeq = 1,
                        rowCount = 1,
                        logicalByteCount = byteCount,
                        expiresAt = "2099-01-01T00:00:00Z",
                    ),
                ),
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
            "/sync/snapshot-sessions/schemaevolution-snapshot-g${generation.id}" -> when (request.method.value) {
                "GET" -> {
                    val afterRowOrdinal = request.url.parameters["after_row_ordinal"]?.toLongOrNull() ?: 0L
                    respond(
                        compactCrashSnapshotChunk(generation, snapshotRows, afterRowOrdinal, byteCount),
                        headers = headersOf(HttpHeaders.ContentType, "application/json"),
                    )
                }
                "DELETE" -> respond("", status = HttpStatusCode.NoContent)
                else -> error("unexpected snapshot method ${request.method.value}")
            }
            else -> error("unexpected schemaevolution crash request ${request.method.value} ${request.url.encodedPath}")
        }
    }) {
        install(ContentNegotiation) { json() }
        defaultRequest { url("https://sync.test/") }
    }
}

private fun crashConfig(generationId: Long) = OversqliteConfig(
    generationId = generationId,
    databaseId = "opening-crash-fixture",
    serviceId = "opening-crash-service",
    syncTables = listOf(SyncTable("notes", syncKeyColumnName = "id")),
)

private fun crashPlan() = OversqliteUpgradePlan(
    migrationId = "opening-crash-g1-g2",
    fromGenerationId = 1,
    toGenerationId = 2,
    fromLocalVersion = 2,
    toLocalVersion = 2,
    preservedLocalTables = setOf("drafts"),
)

private fun crashLocalOnlyPlan() = OversqliteUpgradePlan(
    migrationId = "opening-crash-local-2-3",
    fromGenerationId = 1,
    toGenerationId = 1,
    fromLocalVersion = 2,
    toLocalVersion = 3,
    preservedLocalTables = setOf("drafts"),
)

private fun crashGeneration(id: Long): SyncGeneration {
    val provisional = SyncGeneration(
        id = id.toString(),
        fingerprint = "0".repeat(64),
        tables = listOf(
            GenerationTable(
                schema = "business",
                table = "notes",
                syncKeyColumns = listOf("id"),
                columns = listOf(
                    GenerationColumn("id", "uuid", false),
                    GenerationColumn("title", "text", false),
                ),
                foreignKeys = emptyList(),
            ),
        ),
    )
    return provisional.copy(fingerprint = provisional.computeFingerprint())
}

private fun crashGenerationHttp(generation: SyncGeneration, includeConnect: Boolean = false): HttpClient {
    val discovery = Json.encodeToString(GenerationDiscovery("v1", generation, testWireLimits(), false))
    val installedDiscovery = Json.encodeToString(
        GenerationDiscovery("v1", crashGeneration(1), testWireLimits(), false),
    )
    val row = SnapshotRow(
        schema = "business",
        table = "notes",
        key = mapOf("id" to "33333333-3333-4333-8333-333333333333"),
        rowVersion = 1,
        payload = buildJsonObject {
            put("id", "33333333-3333-4333-8333-333333333333")
            put("title", "recovered")
        },
    )
    val snapshotRows = listOf(BundleRow(row.schema, row.table, row.key, "INSERT", row.rowVersion, row.payload))
    val byteCount = OversqliteCompactCodec.snapshotLogicalByteCount(generation, snapshotRows)
    return HttpClient(MockEngine { request ->
        if (generation.id != "1" && request.url.encodedPath == "/sync/generations/1") {
            return@MockEngine respond(
                installedDiscovery,
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
        }
        when (request.url.encodedPath) {
            "/sync/generations/${generation.id}" -> respond(
                discovery,
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
            "/sync/connect" -> {
                respond(
                    Json.encodeToString(
                        ConnectResponse(
                            resolution = if (includeConnect) "initialize_empty" else "remote_authoritative",
                        ),
                    ),
                    headers = headersOf(HttpHeaders.ContentType, "application/json"),
                )
            }
            "/sync/snapshot-sessions" -> respond(
                Json.encodeToString(
                    SnapshotSession(
                        snapshotId = "snapshot-g${generation.id}",
                        snapshotBundleSeq = 1,
                        rowCount = 1,
                        logicalByteCount = byteCount,
                        expiresAt = "2099-01-01T00:00:00Z",
                    ),
                ),
                headers = headersOf(HttpHeaders.ContentType, "application/json"),
            )
            "/sync/snapshot-sessions/snapshot-g${generation.id}" -> when (request.method.value) {
                "GET" -> {
                    val afterRowOrdinal = request.url.parameters["after_row_ordinal"]?.toLongOrNull() ?: 0L
                    respond(
                        compactCrashSnapshotChunk(generation, snapshotRows, afterRowOrdinal, byteCount),
                        headers = headersOf(HttpHeaders.ContentType, "application/json"),
                    )
                }
                "DELETE" -> respond("", status = HttpStatusCode.NoContent)
                else -> error("unexpected snapshot method ${request.method.value}")
            }
            else -> error("unexpected crash-fixture request ${request.method.value} ${request.url.encodedPath}")
        }
    }) {
        install(ContentNegotiation) { json() }
        defaultRequest { url("https://sync.test/") }
    }
}

private fun compactCrashSnapshotChunk(
    generation: SyncGeneration,
    rows: List<BundleRow>,
    afterRowOrdinal: Long,
    logicalByteCount: Long,
): String {
    val selected = if (afterRowOrdinal == 0L) rows else emptyList()
    return buildJsonObject {
        put("rows", OversqliteCompactCodec.encodeBundleRows(
            generation,
            selected,
            firstOrdinal = afterRowOrdinal + 1L,
            snapshot = true,
        ))
        put("next_row_ordinal", if (selected.isEmpty()) afterRowOrdinal else afterRowOrdinal + selected.size)
        put("has_more", false)
        put("logical_byte_count", if (selected.isEmpty()) 0 else logicalByteCount)
    }.toString()
}

private fun crashOfflineHttp(): HttpClient = HttpClient(MockEngine { request ->
    error("offline local mode used network: ${request.method.value} ${request.url.encodedPath}")
}) {
    install(ContentNegotiation) { json() }
    defaultRequest { url("https://sync.test/") }
}
