package dev.goquick.sqlitenow.oversqlite

import com.sun.management.ThreadMXBean
import com.sun.management.UnixOperatingSystemMXBean
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.serialization.kotlinx.json.json
import java.io.BufferedReader
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.lang.management.ManagementFactory
import java.net.InetSocketAddress
import java.net.Socket
import java.nio.file.Files
import java.nio.file.Path
import java.util.Comparator
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.job
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout

private const val MEASURED_CLIENT_RSS_CEILING_MIB = 300L
private const val MEASURED_CLIENT_RSS_CEILING_BYTES =
    MEASURED_CLIENT_RSS_CEILING_MIB * 1024L * 1024L
private const val EXPECTED_CLIENT_MAX_HEAP_ARGUMENT = "-Xmx128m"

internal enum class SnapshotMemoryCleanupPhase {
    CLIENT,
    HTTP_CLOSE,
    HTTP_JOIN,
    DATABASE,
    DIRECTORY,
    SERVER_SAMPLER,
    FIXTURE,
    POST_FIXTURE_DIAGNOSTIC,
    CLIENT_SAMPLER,
}

internal class SnapshotMemoryCleanupPlan {
    private data class Cleanup(
        val phase: SnapshotMemoryCleanupPhase,
        val sequence: Int,
        val name: String,
        val action: suspend () -> Unit,
    )

    private val cleanups = mutableListOf<Cleanup>()

    fun defer(
        phase: SnapshotMemoryCleanupPhase,
        name: String,
        action: suspend () -> Unit,
    ) {
        cleanups += Cleanup(phase, cleanups.size, name, action)
    }

    suspend fun cleanup(): Throwable? {
        var firstFailure: Throwable? = null
        cleanups.sortedWith(
            compareBy<Cleanup> { it.phase }.thenByDescending { it.sequence },
        ).forEach { cleanup ->
            try {
                cleanup.action()
            } catch (error: Throwable) {
                val contextual = IllegalStateException("snapshot memory cleanup failed: ${cleanup.name}", error)
                val existing = firstFailure
                if (existing == null) {
                    firstFailure = contextual
                } else {
                    existing.addSuppressed(contextual)
                }
            }
        }
        cleanups.clear()
        return firstFailure
    }
}

internal suspend fun <T> withSnapshotMemoryCleanup(
    block: suspend SnapshotMemoryCleanupPlan.() -> T,
): T {
    val cleanupPlan = SnapshotMemoryCleanupPlan()
    var primaryFailure: Throwable? = null
    try {
        return cleanupPlan.block()
    } catch (error: Throwable) {
        primaryFailure = error
        throw error
    } finally {
        val cleanupFailure = withContext(NonCancellable) { cleanupPlan.cleanup() }
        if (cleanupFailure != null) {
            val primary = primaryFailure
            if (primary == null) {
                throw cleanupFailure
            }
            primary.addSuppressed(cleanupFailure)
        }
    }
}

internal class SnapshotMemoryProfileDirectory private constructor(
    val path: Path,
    private val ownershipMarker: Path,
) {
    fun deleteRecursively() {
        check(Files.isRegularFile(ownershipMarker)) {
            "refusing to delete a snapshot memory directory without its ownership marker"
        }
        var failure: Throwable? = null
        val ownedEntries = Files.walk(path).use { paths ->
            paths.filter { it != path && it != ownershipMarker }
                .sorted(Comparator.reverseOrder())
                .toList()
        }
        ownedEntries.forEach { entry ->
            try {
                Files.deleteIfExists(entry)
            } catch (error: Throwable) {
                failure = appendFailure(failure, error)
            }
        }
        if (failure == null) {
            Files.delete(ownershipMarker)
            try {
                Files.delete(path)
            } catch (error: Throwable) {
                try {
                    Files.createFile(ownershipMarker)
                } catch (markerError: Throwable) {
                    error.addSuppressed(markerError)
                }
                throw error
            }
        }
        failure?.let { throw it }
    }

    companion object {
        private const val markerName = ".oversqlite-memory-profile-owned"

        fun create(parent: Path? = null): SnapshotMemoryProfileDirectory {
            val path = if (parent == null) {
                Files.createTempDirectory("oversqlite-kmp-memory-baseline-")
            } else {
                Files.createTempDirectory(parent, "oversqlite-kmp-memory-baseline-")
            }
            val marker = path.resolve(markerName)
            return try {
                Files.createFile(marker)
                SnapshotMemoryProfileDirectory(path, marker)
            } catch (error: Throwable) {
                try {
                    Files.deleteIfExists(marker)
                } catch (cleanupError: Throwable) {
                    error.addSuppressed(cleanupError)
                }
                try {
                    Files.deleteIfExists(path)
                } catch (cleanupError: Throwable) {
                    error.addSuppressed(cleanupError)
                }
                throw error
            }
        }
    }
}

class SnapshotMemoryBaselineTest : BundleClientContractTestSupport() {
    private companion object {
        val passMarker = Regex("snapshot_memory_result_verifier status=pass")
        val measured100k256Labels = setOf(
            "measured-100k-256-1",
            "measured-100k-256-2",
            "measured-100k-256-3",
        )
    }

    private val allowedRowCounts = setOf(10_000, 100_000, 1_000_000)
    private val allowedTargetRowBytes = setOf(256, 1024)

    @Test
    fun snapshotMemorySamplerOnlyControl() = runBlocking<Unit> {
        val rawEnabled = System.getenv("OVERSQLITE_MEMORY_SAMPLER_CONTROL") ?: return@runBlocking
        check(System.getenv("GITHUB_ACTIONS") != "true") {
            "snapshot memory sampler control is local-only and must not run with GITHUB_ACTIONS=true"
        }
        require(rawEnabled == "true") {
            "OVERSQLITE_MEMORY_SAMPLER_CONTROL must be true when present"
        }
        check(System.getenv("OVERSQLITE_MEMORY_BASELINE_ROWS") == null) {
            "snapshot memory sampler control must not configure a snapshot fixture"
        }

        val requestedDurationMillis = 5_000L
        val clientPid = ProcessHandle.current().pid()
        val clientCommandLine = ProcessHandle.current().info().commandLine().orElse("unavailable")
        val clientInputArguments = ManagementFactory.getRuntimeMXBean().inputArguments.joinToString(" ")
        lateinit var metrics: JvmMemoryMetrics
        var observationDurationNanos = 0L
        withSnapshotMemoryCleanup {
            val sampler = JvmMemorySampler.start()
            defer(SnapshotMemoryCleanupPhase.CLIENT_SAMPLER, "client sampler") {
                metrics = sampler.stop()
            }
            val observationStartedNanos = System.nanoTime()
            TimeUnit.MILLISECONDS.sleep(requestedDurationMillis)
            observationDurationNanos = System.nanoTime() - observationStartedNanos
        }
        val samplesPerSecond = ratePerSecond(metrics.sampleCount, metrics.durationNanos)
        val allocatedBytesPerSecond =
            ratePerSecond(metrics.sampledThreadAllocatedBytes, metrics.durationNanos)

        println(
            "snapshot_memory_sampler_control_process pid=$clientPid " +
                "command_line=${evidenceValue(clientCommandLine)} " +
                "jvm_input_arguments=${evidenceValue(clientInputArguments)}",
        )
        println(
            "snapshot_memory_sampler_control runtime=kmp-jvm requested_duration_ms=$requestedDurationMillis " +
                "observation_duration_nanos=$observationDurationNanos " +
                "sampler_duration_nanos=${metrics.durationNanos} sample_count=${metrics.sampleCount} " +
                "samples_per_second=${"%.2f".format(samplesPerSecond)} " +
                "baseline_heap_bytes=${metrics.baselineHeapBytes} " +
                "peak_heap_bytes=${metrics.peakHeapBytes} " +
                "adjusted_heap_bytes=${metrics.adjustedHeapBytes} " +
                "baseline_rss_bytes=${metrics.baselineRssBytes} " +
                "peak_rss_bytes=${metrics.peakRssBytes} " +
                "adjusted_rss_bytes=${metrics.adjustedRssBytes} " +
                "sampled_thread_allocated_bytes=${metrics.sampledThreadAllocatedBytes} " +
                "sampled_thread_allocated_bytes_per_second=${"%.2f".format(allocatedBytesPerSecond)}",
        )
    }

    @Test
    fun snapshotMemoryBaseline() = runBlocking<Unit> {
        val rawRows = System.getenv("OVERSQLITE_MEMORY_BASELINE_ROWS") ?: return@runBlocking
        check(System.getenv("GITHUB_ACTIONS") != "true") {
            "snapshot memory baseline is local-heavy and must not run with GITHUB_ACTIONS=true"
        }
        val rowCount = rawRows.toIntOrNull()
            ?: error("OVERSQLITE_MEMORY_BASELINE_ROWS must be numeric")
        require(rowCount in allowedRowCounts) {
            "OVERSQLITE_MEMORY_BASELINE_ROWS must be 10000, 100000, or 1000000"
        }
        val targetRowBytes = (System.getenv("OVERSQLITE_MEMORY_ROW_BYTES") ?: "256").toIntOrNull()
            ?: error("OVERSQLITE_MEMORY_ROW_BYTES must be numeric")
        require(targetRowBytes in allowedTargetRowBytes) {
            "OVERSQLITE_MEMORY_ROW_BYTES must be 256 or 1024"
        }
        val runLabel = System.getenv("OVERSQLITE_MEMORY_RUN_LABEL") ?: "manual"
        val lifecycleDiagnosticEnabled = when (
            System.getenv("OVERSQLITE_MEMORY_LIFECYCLE_DIAGNOSTIC")
        ) {
            null, "", "false" -> false
            "true" -> true
            else -> error("OVERSQLITE_MEMORY_LIFECYCLE_DIAGNOSTIC must be true or false")
        }
        val postRestoreGcDiagnosticEnabled = when (
            System.getenv("OVERSQLITE_MEMORY_POST_RESTORE_GC_DIAGNOSTIC")
        ) {
            null, "", "false" -> false
            "true" -> true
            else -> error("OVERSQLITE_MEMORY_POST_RESTORE_GC_DIAGNOSTIC must be true or false")
        }
        if (postRestoreGcDiagnosticEnabled) {
            require(rowCount == 100_000 && targetRowBytes == 256) {
                "post-restore GC diagnostics require exactly 100000 rows of 256 bytes"
            }
            require(runLabel == "diagnostic-100k-256-post-gc") {
                "post-restore GC diagnostics require the diagnostic-100k-256-post-gc label"
            }
        }
        if (lifecycleDiagnosticEnabled) {
            require(!postRestoreGcDiagnosticEnabled) {
                "lifecycle diagnostics cannot run with post-restore explicit-GC diagnostics"
            }
            require(rowCount == 100_000 && targetRowBytes == 256) {
                "lifecycle diagnostics require exactly 100000 rows of 256 bytes"
            }
            require(runLabel == "diagnostic-100k-256-peak-lifecycle") {
                "lifecycle diagnostics require the diagnostic-100k-256-peak-lifecycle label"
            }
        }

        val clientPid = ProcessHandle.current().pid()
        val clientCommandLine = ProcessHandle.current().info().commandLine().orElse("unavailable")
        val clientInputArgumentList = ManagementFactory.getRuntimeMXBean().inputArguments
        assertEquals(
            listOf(EXPECTED_CLIENT_MAX_HEAP_ARGUMENT),
            clientInputArgumentList.filter { it.startsWith("-Xmx") },
            "snapshot memory profile worker must have exactly one -Xmx128m argument",
        )
        val clientInputArguments = clientInputArgumentList.joinToString(" ")
        val probeRow = encodedMemorySnapshotRow(1, targetRowBytes)
        assertEquals(targetRowBytes, probeRow.encodeToByteArray().size)
        val declaredTotalBytes = Math.multiplyExact(rowCount.toLong(), targetRowBytes.toLong())

        var diagnostics = SnapshotRestoreDiagnostics()
        var appliedRows = 0L
        var sqliteBytes = 0L
        var walBytes = 0L
        val startedNanos = System.nanoTime()
        val restoreTimer = SnapshotRestoreTimer()
        var cleanupStartedNanos: Long? = null
        var diagnosticCleanupNanos = 0L
        var restoreSucceeded = false
        lateinit var serverFinal: SnapshotMemoryServerFinal
        lateinit var serverRss: ProcessRssMetrics
        lateinit var clientMetrics: JvmMemoryMetrics
        var afterRestoreCheckpoint: JvmMemoryCheckpoint? = null
        var postRestoreGcDiagnostics: JvmPostRestoreGcDiagnostics? = null
        var fixtureStopped = false
        var restoreCompleted = false
        lateinit var fixture: SnapshotMemoryFixtureProcess
        fun recordLifecycleCheckpoint(phase: String) {
            if (!lifecycleDiagnosticEnabled) return
            val checkpoint = currentJvmMemoryCheckpoint(clientPid)
            println(
                "snapshot_memory_lifecycle_checkpoint pid=$clientPid label=$runLabel phase=$phase " +
                    "elapsed_nanos=${System.nanoTime() - startedNanos} " +
                    checkpoint.asEvidenceFields("checkpoint"),
            )
        }

        try {
            withSnapshotMemoryCleanup {
                fixture = SnapshotMemoryFixtureProcess.start(rowCount, targetRowBytes)
                defer(SnapshotMemoryCleanupPhase.FIXTURE, "fixture process") {
                    try {
                        serverFinal = fixture.stop()
                    } finally {
                        fixtureStopped = !fixture.process.isAlive
                    }
                    recordLifecycleCheckpoint("fixture_stopped")
                }

                val serverRssSampler = ProcessRssSampler.start(fixture.pid)
                defer(SnapshotMemoryCleanupPhase.SERVER_SAMPLER, "server RSS sampler") {
                    serverRss = serverRssSampler.stop()
                    recordLifecycleCheckpoint("server_sampler_stopped")
                }

                val clientSampler = JvmMemorySampler.start()
                defer(SnapshotMemoryCleanupPhase.CLIENT_SAMPLER, "client memory sampler") {
                    clientMetrics = clientSampler.stop()
                }
                defer(SnapshotMemoryCleanupPhase.POST_FIXTURE_DIAGNOSTIC, "post-fixture diagnostic") {
                    val diagnosticStartedNanos = System.nanoTime()
                    try {
                        if (restoreCompleted && lifecycleDiagnosticEnabled) {
                            Thread.sleep(5_000L)
                            recordLifecycleCheckpoint("natural_idle_5s_completed")
                        }
                        val completedRestoreCheckpoint = afterRestoreCheckpoint
                        if (
                            restoreCompleted &&
                            postRestoreGcDiagnosticEnabled &&
                            fixtureStopped &&
                            completedRestoreCheckpoint != null
                        ) {
                            postRestoreGcDiagnostics = runPostRestoreGcDiagnostic(
                                pid = clientPid,
                                afterRestore = completedRestoreCheckpoint,
                            )
                        }
                    } finally {
                        diagnosticCleanupNanos = Math.subtractExact(
                            System.nanoTime(),
                            diagnosticStartedNanos,
                        )
                    }
                }
                recordLifecycleCheckpoint("sampler_started")

                val directory = SnapshotMemoryProfileDirectory.create()
                defer(SnapshotMemoryCleanupPhase.DIRECTORY, "profile directory") {
                    directory.deleteRecursively()
                }
                val databasePath = directory.path.resolve("oversqlite.sqlite")
                val db = BundledSqliteConnectionProvider.openConnection(databasePath.toString(), debug = false)
                defer(SnapshotMemoryCleanupPhase.DATABASE, "database") {
                    db.close()
                    recordLifecycleCheckpoint("database_closed")
                }
                recordLifecycleCheckpoint("database_opened")

                createUsersTable(db)
                db.execSQL("PRAGMA journal_mode = WAL")
                val http = HttpClient(CIO) {
                    install(ContentNegotiation) {
                        json()
                    }
                    defaultRequest {
                        url("http://127.0.0.1:${fixture.port}")
                    }
                }
                defer(SnapshotMemoryCleanupPhase.HTTP_CLOSE, "caller HTTP client close") {
                    http.close()
                }
                defer(SnapshotMemoryCleanupPhase.HTTP_JOIN, "caller HTTP client job") {
                    withTimeout(20_000L) { http.coroutineContext.job.join() }
                    recordLifecycleCheckpoint("caller_http_client_closed")
                }
                recordLifecycleCheckpoint("caller_http_client_created")

                val client = newClient(
                    db = db,
                    http = http,
                    syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
                    snapshotChunkRows = 1_000,
                    snapshotChunkBytes = 4L * 1024L * 1024L,
                    snapshotApplyBatchRows = 256,
                    snapshotApplyBatchBytes = 4L * 1024L * 1024L,
                    transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
                )
                defer(SnapshotMemoryCleanupPhase.CLIENT, "oversqlite client") {
                    client.close()
                    recordLifecycleCheckpoint("oversqlite_client_closed")
                }
                defer(SnapshotMemoryCleanupPhase.CLIENT, "cleanup timing boundary") {
                    cleanupStartedNanos = System.nanoTime()
                }
                recordLifecycleCheckpoint("oversqlite_client_created")

                client.openAndConnect("user-1").getOrThrow()
                recordLifecycleCheckpoint("connected")
                restoreTimer.measure { client.rebuild() }.getOrThrow()
                restoreSucceeded = true
                appliedRows = scalarLong(db, "SELECT COUNT(*) FROM users")
                assertEquals(rowCount.toLong(), appliedRows)
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
                diagnostics = client.snapshotRestoreDiagnosticsForTest()
                assertEquals(rowCount.toLong(), diagnostics.finalStagedRows)
                assertEquals(rowCount.toLong(), diagnostics.finalAppliedRows)
                assertTrue(diagnostics.maxLiveApplyPageRows <= 256)
                assertTrue(diagnostics.maxLiveApplyPageStagedTextBytes <= 4L * 1024L * 1024L)
                assertEquals((rowCount.toLong() + 255L) / 256L, diagnostics.applyPages)
                sqliteBytes = fileSize(databasePath)
                walBytes = fileSize(databasePath.resolveSibling(databasePath.fileName.toString() + "-wal"))
                if (postRestoreGcDiagnosticEnabled) {
                    afterRestoreCheckpoint = currentJvmMemoryCheckpoint(clientPid)
                }
                recordLifecycleCheckpoint("restore_completed")
                restoreCompleted = true
            }
        } finally {
            val completedNanos = System.nanoTime()
            val restoreStartedNanos = restoreTimer.startedNanos
            val restoreCompletedNanos = restoreTimer.completedNanos
            if (restoreStartedNanos != null && restoreCompletedNanos != null) {
                val cleanupStarted = cleanupStartedNanos ?: completedNanos
                val postRestoreDiagnosticsNanos = Math.subtractExact(
                    cleanupStarted,
                    restoreCompletedNanos,
                ).coerceAtLeast(0L)
                val totalCleanupNanos = Math.subtractExact(
                    completedNanos,
                    cleanupStarted,
                ).coerceAtLeast(0L)
                println(
                    "snapshot_memory_timing runtime=kmp-jvm label=$runLabel " +
                        "restore_status=${if (restoreSucceeded) "pass" else "fail"} " +
                        "setup_nanos=${Math.subtractExact(restoreStartedNanos, startedNanos)} " +
                        "restore_nanos=${Math.subtractExact(restoreCompletedNanos, restoreStartedNanos)} " +
                        "diagnostics_nanos=${Math.addExact(postRestoreDiagnosticsNanos, diagnosticCleanupNanos)} " +
                        "cleanup_nanos=${Math.subtractExact(totalCleanupNanos, diagnosticCleanupNanos)}",
                )
            }
        }

        val elapsedNanos = checkNotNull(restoreTimer.elapsedNanos)
        val elapsedMillis = elapsedNanos / 1_000_000
        val throughput = if (elapsedNanos == 0L) 0.0 else rowCount * 1_000_000_000.0 / elapsedNanos
        assertEquals(serverFinal.chunkCount, diagnostics.fetchedChunks)
        println(
            "snapshot_memory_client_process pid=$clientPid " +
                "command_line=${evidenceValue(clientCommandLine)} " +
                "jvm_input_arguments=${evidenceValue(clientInputArguments)}",
        )
        println(
            "snapshot_memory_server_process pid=${fixture.pid} " +
                "command_line=${evidenceValue(fixture.commandLine)}",
        )
        println(
            "snapshot_memory_phase3 runtime=kmp-jvm label=$runLabel rows=$rowCount target_row_bytes=$targetRowBytes " +
                "declared_total_bytes=$declaredTotalBytes applied_rows=$appliedRows chunks=${serverFinal.chunkCount} " +
                "apply_pages=${diagnostics.applyPages} max_chunk_rows=${diagnostics.maxValidatedChunkRows} " +
                "max_chunk_bytes=${diagnostics.maxDeclaredChunkBytes} " +
                "max_decoded_body_bytes=${diagnostics.maxCompletelyDecodedChunkBodyBytes} " +
                "max_apply_page_rows=${diagnostics.maxLiveApplyPageRows} " +
                "max_apply_page_staged_text_bytes=${diagnostics.maxLiveApplyPageStagedTextBytes} " +
                "server_max_chunk_rows=${serverFinal.maxChunkRows} " +
                "server_max_chunk_bytes=${serverFinal.maxChunkBytes} " +
                "elapsed_ms=$elapsedMillis throughput_rows_per_second=${"%.2f".format(throughput)} " +
                "baseline_heap_bytes=${clientMetrics.baselineHeapBytes} " +
                "peak_heap_bytes=${clientMetrics.peakHeapBytes} " +
                "adjusted_heap_bytes=${clientMetrics.adjustedHeapBytes} " +
                "baseline_rss_bytes=${clientMetrics.baselineRssBytes} " +
                "peak_rss_bytes=${clientMetrics.peakRssBytes} " +
                "adjusted_rss_bytes=${clientMetrics.adjustedRssBytes} " +
                "server_baseline_rss_bytes=${serverRss.baselineRssBytes} " +
                "server_peak_rss_bytes=${serverRss.peakRssBytes} " +
                "server_adjusted_rss_bytes=${serverRss.adjustedRssBytes} " +
                "sampler_duration_nanos=${clientMetrics.durationNanos} " +
                "sample_count=${clientMetrics.sampleCount} " +
                "sampled_thread_allocated_bytes=${clientMetrics.sampledThreadAllocatedBytes} " +
                "sampled_thread_allocated_bytes_per_second=${
                    "%.2f".format(
                        ratePerSecond(
                            clientMetrics.sampledThreadAllocatedBytes,
                            clientMetrics.durationNanos,
                        ),
                    )
                } " +
                "sqlite_bytes=$sqliteBytes sqlite_wal_bytes=$walBytes",
        )
        postRestoreGcDiagnostics?.let { diagnostic ->
            println(
                "snapshot_memory_post_restore_gc_diagnostic pid=$clientPid label=$runLabel " +
                    "explicit_gc_observed=${diagnostic.explicitGcObserved} " +
                    "idle_wait_ms=${diagnostic.idleWaitMillis} " +
                    "explicit_gc_confirmation_wait_ms=${diagnostic.explicitGcConfirmationWaitMillis} " +
                    "post_gc_reclamation_wait_ms=${diagnostic.postGcReclamationWaitMillis} " +
                    diagnostic.afterRestore.asEvidenceFields("after_restore") + " " +
                    diagnostic.afterResourceClosure.asEvidenceFields("after_resource_close") + " " +
                    diagnostic.afterIdle.asEvidenceFields("after_idle") + " " +
                    diagnostic.afterExplicitGc.asEvidenceFields("after_explicit_gc"),
            )
        }
        if (runLabel.startsWith("measured-")) {
            assertTrue(
                clientMetrics.peakRssBytes < MEASURED_CLIENT_RSS_CEILING_BYTES,
                "measured client PID reached the $MEASURED_CLIENT_RSS_CEILING_MIB MiB " +
                    "RSS stop ceiling: " +
                    "${clientMetrics.peakRssBytes} bytes",
            )
        }
        System.getenv("OVERSQLITE_MEMORY_RESULT_FILE")?.let { resultPath ->
            SnapshotMemoryResult(
                runLabel = runLabel,
                rowCount = rowCount,
                targetRowBytes = targetRowBytes,
                baselineHeapBytes = clientMetrics.baselineHeapBytes,
                peakHeapBytes = clientMetrics.peakHeapBytes,
                adjustedHeapBytes = clientMetrics.adjustedHeapBytes,
                baselineRssBytes = clientMetrics.baselineRssBytes,
                peakRssBytes = clientMetrics.peakRssBytes,
                adjustedRssBytes = clientMetrics.adjustedRssBytes,
            ).write(Path.of(resultPath))
        }
    }

    @Test
    fun githubActionsGuardRunsBeforeFixtureSetup() {
        if (System.getenv("OVERSQLITE_MEMORY_BASELINE_ROWS") == null) return
        if (System.getenv("GITHUB_ACTIONS") != "true") return
        error("snapshot memory guard test must have failed before test fixture setup")
    }

    @Test
    fun snapshotMemoryResultVerifierEnforcesCompletenessCeilingAndScaling() {
        val accepted = acceptedSnapshotMemoryResults()
        val acceptedRecords = acceptedResultRecords(accepted)
        assertVerifierAccepts("internally-consistent", acceptedRecords)
        assertVerifierAccepts(
            "rss-one-byte-below-ceiling",
            acceptedResultRecords(
                accepted.replaceLabel("measured-1m-256") {
                    it.copy(
                        baselineRssBytes = 200L * 1024L * 1024L,
                        peakRssBytes = MEASURED_CLIENT_RSS_CEILING_BYTES - 1L,
                        adjustedRssBytes = 100L * 1024L * 1024L - 1L,
                    )
                },
            ),
        )

        val rejectionCases = linkedMapOf(
            "missing-profile" to acceptedRecords.dropLast(1),
            "extra-profile" to acceptedRecords +
                ("extra.properties" to snapshotMemoryResultText(accepted.first().copy(runLabel = "extra"))),
            "duplicate-label" to acceptedResultRecords(accepted.dropLast(1) + accepted.first()),
            "duplicate-field" to acceptedRecords.replaceFirstRecord { it + "row_count=10000\n" },
            "unknown-field" to acceptedRecords.replaceFirstRecord { it + "unknown_field=1\n" },
            "missing-field" to acceptedRecords.replaceFirstRecord {
                it.lineSequence().filterNot { line -> line.startsWith("baseline_heap_bytes=") }
                    .joinToString("\n", postfix = "\n")
            },
            "malformed-line" to acceptedRecords.replaceFirstRecord { it + "malformed-line\n" },
            "malformed-number" to acceptedRecords.replaceFirstRecord {
                it.replace("baseline_heap_bytes=33554432", "baseline_heap_bytes=not-a-number")
            },
            "narrowing-overflow" to acceptedRecords.replaceFirstRecord {
                it.replace("row_count=10000", "row_count=4294977296")
            },
            "negative-metric" to acceptedResultRecords(
                accepted.replaceFirst { it.copy(baselineHeapBytes = -1L) },
            ),
            "wrong-shape" to acceptedResultRecords(
                accepted.replaceFirst { it.copy(rowCount = 10_001) },
            ),
            "peak-below-baseline" to acceptedResultRecords(
                accepted.replaceFirst {
                    it.copy(
                        baselineHeapBytes = 64L * 1024L * 1024L,
                        peakHeapBytes = 32L * 1024L * 1024L,
                        adjustedHeapBytes = 0L,
                    )
                },
            ),
            "adjusted-inconsistent" to acceptedResultRecords(
                accepted.replaceFirst { it.copy(adjustedHeapBytes = 31L * 1024L * 1024L) },
            ),
            "rss-ceiling-equality" to acceptedResultRecords(
                accepted.replaceLabel("measured-1m-256") {
                    it.copy(
                        baselineRssBytes = 200L * 1024L * 1024L,
                        peakRssBytes = MEASURED_CLIENT_RSS_CEILING_BYTES,
                        adjustedRssBytes = 100L * 1024L * 1024L,
                    )
                },
            ),
            "heap-scaling" to acceptedResultRecords(
                accepted.replaceLabel("measured-1m-256") {
                    it.copy(
                        peakHeapBytes = 200L * 1024L * 1024L,
                        adjustedHeapBytes = 168L * 1024L * 1024L,
                    )
                },
            ),
            "rss-scaling" to acceptedResultRecords(
                accepted.replaceLabel("measured-1m-256") {
                    it.copy(
                        baselineRssBytes = 16L * 1024L * 1024L,
                        peakRssBytes = 240L * 1024L * 1024L,
                        adjustedRssBytes = 224L * 1024L * 1024L,
                    )
                },
            ),
            "scaling-overflow" to acceptedResultRecords(
                accepted.map {
                    if (it.runLabel in measured100k256Labels) {
                        it.copy(
                            baselineHeapBytes = 1L,
                            peakHeapBytes = Long.MAX_VALUE,
                            adjustedHeapBytes = Long.MAX_VALUE - 1L,
                        )
                    } else {
                        it
                    }
                },
            ),
        )
        val unexpectedlyAccepted = rejectionCases.mapNotNull { (name, records) ->
            name.takeUnless { verifierRejects(name, records) }
        }
        assertEquals(emptyList(), unexpectedlyAccepted, "verifier false acceptances")
    }

    @Test
    fun rssSamplingRejectsUnavailableProcessInsteadOfReturningZero() {
        assertFailsWith<IllegalStateException> { rssBytes(Long.MAX_VALUE) }
    }

    private fun fileSize(path: Path): Long = if (Files.exists(path)) Files.size(path) else 0L

    private fun evidenceValue(value: String): String = "\"${value.replace('"', '\'')}\""

    private fun ratePerSecond(value: Long, durationNanos: Long): Double =
        if (durationNanos == 0L) 0.0 else value * 1_000_000_000.0 / durationNanos

    private fun acceptedSnapshotMemoryResults(): List<SnapshotMemoryResult> =
        SnapshotMemoryResultVerifier.expectedProfiles.map { (label, shape) ->
            SnapshotMemoryResult(
                runLabel = label,
                rowCount = shape.first,
                targetRowBytes = shape.second,
                baselineHeapBytes = 32L * 1024L * 1024L,
                peakHeapBytes = if (label == "measured-1m-256") {
                    80L * 1024L * 1024L
                } else {
                    64L * 1024L * 1024L
                },
                adjustedHeapBytes = if (label == "measured-1m-256") {
                    48L * 1024L * 1024L
                } else {
                    32L * 1024L * 1024L
                },
                baselineRssBytes = 128L * 1024L * 1024L,
                peakRssBytes = if (label == "measured-1m-256") {
                    224L * 1024L * 1024L
                } else {
                    192L * 1024L * 1024L
                },
                adjustedRssBytes = if (label == "measured-1m-256") {
                    96L * 1024L * 1024L
                } else {
                    64L * 1024L * 1024L
                },
            )
        }

    private fun acceptedResultRecords(
        results: List<SnapshotMemoryResult>,
    ): List<Pair<String, String>> = results.mapIndexed { index, result ->
        "${index.toString().padStart(2, '0')}-${result.runLabel}.properties" to
            snapshotMemoryResultText(result)
    }

    private fun snapshotMemoryResultText(result: SnapshotMemoryResult): String =
        """
        run_label=${result.runLabel}
        row_count=${result.rowCount}
        target_row_bytes=${result.targetRowBytes}
        baseline_heap_bytes=${result.baselineHeapBytes}
        peak_heap_bytes=${result.peakHeapBytes}
        adjusted_heap_bytes=${result.adjustedHeapBytes}
        baseline_rss_bytes=${result.baselineRssBytes}
        peak_rss_bytes=${result.peakRssBytes}
        adjusted_rss_bytes=${result.adjustedRssBytes}
        """.trimIndent() + "\n"

    private fun assertVerifierAccepts(
        name: String,
        records: List<Pair<String, String>>,
    ) {
        val invocation = invokeSnapshotMemoryResultVerifier(records)
        assertNull(invocation.failure, name)
        assertEquals(1, passMarker.findAll(invocation.output).count(), name)
        println("snapshot_memory_verifier_case name=$name status=accepted")
    }

    private fun verifierRejects(
        name: String,
        records: List<Pair<String, String>>,
    ): Boolean {
        val invocation = invokeSnapshotMemoryResultVerifier(records)
        val rejected = invocation.failure != null && !passMarker.containsMatchIn(invocation.output)
        println(
            "snapshot_memory_verifier_case name=$name status=${
                if (rejected) "rejected" else "unexpectedly-accepted"
            }",
        )
        return rejected
    }

    private fun invokeSnapshotMemoryResultVerifier(
        records: List<Pair<String, String>>,
    ): VerifierInvocation {
        val directory = SnapshotMemoryProfileDirectory.create()
        val originalOut = System.out
        val captured = ByteArrayOutputStream()
        var failure: Throwable? = null
        try {
            records.forEach { (fileName, contents) ->
                Files.writeString(directory.path.resolve(fileName), contents)
            }
            System.setOut(PrintStream(captured, true, Charsets.UTF_8))
            try {
                SnapshotMemoryResultVerifier.main(arrayOf(directory.path.toString()))
            } catch (error: Throwable) {
                failure = error
            }
        } finally {
            System.setOut(originalOut)
            directory.deleteRecursively()
        }
        return VerifierInvocation(failure, captured.toString(Charsets.UTF_8))
    }

    private fun List<Pair<String, String>>.replaceFirstRecord(
        replacement: (String) -> String,
    ): List<Pair<String, String>> = mapIndexed { index, record ->
        if (index == 0) record.first to replacement(record.second) else record
    }

    private fun List<SnapshotMemoryResult>.replaceFirst(
        replacement: (SnapshotMemoryResult) -> SnapshotMemoryResult,
    ): List<SnapshotMemoryResult> = mapIndexed { index, result ->
        if (index == 0) replacement(result) else result
    }

    private fun List<SnapshotMemoryResult>.replaceLabel(
        label: String,
        replacement: (SnapshotMemoryResult) -> SnapshotMemoryResult,
    ): List<SnapshotMemoryResult> = map { result ->
        if (result.runLabel == label) replacement(result) else result
    }

    private data class VerifierInvocation(
        val failure: Throwable?,
        val output: String,
    )

}

internal data class SnapshotMemoryResult(
    val runLabel: String,
    val rowCount: Int,
    val targetRowBytes: Int,
    val baselineHeapBytes: Long,
    val peakHeapBytes: Long,
    val adjustedHeapBytes: Long,
    val baselineRssBytes: Long,
    val peakRssBytes: Long,
    val adjustedRssBytes: Long,
) {
    fun write(path: Path) {
        Files.createDirectories(path.parent)
        Files.writeString(
            path,
            """
            run_label=$runLabel
            row_count=$rowCount
            target_row_bytes=$targetRowBytes
            baseline_heap_bytes=$baselineHeapBytes
            peak_heap_bytes=$peakHeapBytes
            adjusted_heap_bytes=$adjustedHeapBytes
            baseline_rss_bytes=$baselineRssBytes
            peak_rss_bytes=$peakRssBytes
            adjusted_rss_bytes=$adjustedRssBytes
            """.trimIndent() + "\n",
        )
    }
}

internal object SnapshotMemoryResultVerifier {
    private const val heapScalingAllowanceBytes = 32L * 1024L * 1024L
    private const val rssScalingAllowanceBytes = 64L * 1024L * 1024L
    private val decimalLongPattern = Regex("(?:0|[1-9][0-9]*)")
    private val requiredFields = linkedSetOf(
        "run_label",
        "row_count",
        "target_row_bytes",
        "baseline_heap_bytes",
        "peak_heap_bytes",
        "adjusted_heap_bytes",
        "baseline_rss_bytes",
        "peak_rss_bytes",
        "adjusted_rss_bytes",
    )

    val expectedProfiles: Map<String, Pair<Int, Int>> = linkedMapOf(
        "representative-10k-256" to (10_000 to 256),
        "representative-10k-1k" to (10_000 to 1024),
        "warmup-100k-256" to (100_000 to 256),
        "measured-100k-256-1" to (100_000 to 256),
        "measured-100k-256-2" to (100_000 to 256),
        "measured-100k-256-3" to (100_000 to 256),
        "measured-100k-1k" to (100_000 to 1024),
        "measured-1m-256" to (1_000_000 to 256),
    )

    @JvmStatic
    fun main(args: Array<String>) {
        require(args.size == 1) { "expected the snapshot memory result directory" }
        val directory = Path.of(args.single())
        require(Files.isDirectory(directory)) { "snapshot memory result directory is missing" }
        val results = Files.list(directory).use { paths ->
            paths.filter { Files.isRegularFile(it) && it.fileName.toString().endsWith(".properties") }
                .sorted()
                .map(::read)
                .toList()
        }
        verify(results)
        println("snapshot_memory_result_verifier status=pass profiles=${results.size}")
    }

    fun verify(results: List<SnapshotMemoryResult>) {
        require(results.size == expectedProfiles.size) {
            "expected ${expectedProfiles.size} snapshot memory results, found ${results.size}"
        }
        val byLabel = results.groupBy(SnapshotMemoryResult::runLabel)
        require(byLabel.keys == expectedProfiles.keys && byLabel.values.all { it.size == 1 }) {
            "snapshot memory results must contain each expected profile exactly once"
        }
        for ((label, expectedShape) in expectedProfiles) {
            val result = byLabel.getValue(label).single()
            require(result.rowCount == expectedShape.first && result.targetRowBytes == expectedShape.second) {
                "snapshot memory result $label has an unexpected fixture shape"
            }
            require(
                result.baselineHeapBytes > 0L && result.peakHeapBytes > 0L &&
                    result.baselineRssBytes > 0L && result.peakRssBytes > 0L &&
                    result.adjustedHeapBytes >= 0L && result.adjustedRssBytes >= 0L
            ) { "snapshot memory result $label contains invalid memory metrics" }
            require(result.peakHeapBytes >= result.baselineHeapBytes) {
                "snapshot memory result $label has peak heap below baseline"
            }
            require(result.peakRssBytes >= result.baselineRssBytes) {
                "snapshot memory result $label has peak RSS below baseline"
            }
            val recomputedAdjustedHeap = Math.max(
                Math.subtractExact(result.peakHeapBytes, result.baselineHeapBytes),
                0L,
            )
            val recomputedAdjustedRss = Math.max(
                Math.subtractExact(result.peakRssBytes, result.baselineRssBytes),
                0L,
            )
            require(result.adjustedHeapBytes == recomputedAdjustedHeap) {
                "snapshot memory result $label has inconsistent adjusted heap"
            }
            require(result.adjustedRssBytes == recomputedAdjustedRss) {
                "snapshot memory result $label has inconsistent adjusted RSS"
            }
        }

        val million = byLabel.getValue("measured-1m-256").single()
        require(million.peakRssBytes < MEASURED_CLIENT_RSS_CEILING_BYTES) {
            "measured-1m-256 reached the $MEASURED_CLIENT_RSS_CEILING_MIB MiB client RSS ceiling"
        }
        val measured100k = listOf(
            "measured-100k-256-1",
            "measured-100k-256-2",
            "measured-100k-256-3",
        ).map { byLabel.getValue(it).single() }
        val conservativeHeap = measured100k.minOf(SnapshotMemoryResult::adjustedHeapBytes)
        val conservativeRss = measured100k.minOf(SnapshotMemoryResult::adjustedRssBytes)
        val heapLimit = Math.addExact(Math.multiplyExact(conservativeHeap, 2L), heapScalingAllowanceBytes)
        val rssLimit = Math.addExact(Math.multiplyExact(conservativeRss, 2L), rssScalingAllowanceBytes)
        require(million.adjustedHeapBytes <= heapLimit) {
            "measured-1m-256 adjusted heap exceeds the 2x + 32 MiB scaling gate"
        }
        require(million.adjustedRssBytes <= rssLimit) {
            "measured-1m-256 adjusted RSS exceeds the 2x + 64 MiB scaling gate"
        }
    }

    private fun read(path: Path): SnapshotMemoryResult {
        val fields = linkedMapOf<String, String>()
        Files.readAllLines(path).filter(String::isNotBlank).forEach { line ->
            val separator = line.indexOf('=')
            require(separator > 0) { "invalid snapshot memory result line in ${path.fileName}" }
            val name = line.substring(0, separator)
            require(name in requiredFields) {
                "snapshot memory result ${path.fileName} has unknown field $name"
            }
            require(fields.put(name, line.substring(separator + 1)) == null) {
                "snapshot memory result ${path.fileName} has duplicate field $name"
            }
        }
        require(fields.keys == requiredFields) {
            val missing = requiredFields - fields.keys
            "snapshot memory result ${path.fileName} has an incomplete field set; missing=$missing"
        }
        fun required(name: String): String = fields[name]
            ?: error("snapshot memory result ${path.fileName} is missing $name")
        fun requiredLong(name: String): Long {
            val value = required(name)
            require(decimalLongPattern.matches(value)) {
                "snapshot memory result ${path.fileName} has invalid $name"
            }
            return value.toLongOrNull()
                ?: error("snapshot memory result ${path.fileName} has out-of-range $name")
        }
        fun requiredInt(name: String): Int = try {
            Math.toIntExact(requiredLong(name))
        } catch (error: ArithmeticException) {
            throw IllegalArgumentException(
                "snapshot memory result ${path.fileName} has out-of-range $name",
                error,
            )
        }
        return SnapshotMemoryResult(
            runLabel = required("run_label"),
            rowCount = requiredInt("row_count"),
            targetRowBytes = requiredInt("target_row_bytes"),
            baselineHeapBytes = requiredLong("baseline_heap_bytes"),
            peakHeapBytes = requiredLong("peak_heap_bytes"),
            adjustedHeapBytes = requiredLong("adjusted_heap_bytes"),
            baselineRssBytes = requiredLong("baseline_rss_bytes"),
            peakRssBytes = requiredLong("peak_rss_bytes"),
            adjustedRssBytes = requiredLong("adjusted_rss_bytes"),
        )
    }
}

internal data class SnapshotMemoryServerFinal(
    val chunkCount: Long,
    val maxChunkRows: Long,
    val maxChunkBytes: Long,
)

internal class SnapshotMemoryFixtureProcess private constructor(
    val process: Process,
    private val output: BufferedReader,
    val port: Int,
    val pid: Long,
    val commandLine: String,
    private val gracefulShutdownTimeoutMillis: Long,
    private val forceShutdownTimeoutMillis: Long,
    private val shutdownRequester: (Int) -> Unit,
) {
    @Volatile
    internal var outputClosed = false
        private set

    fun stop(): SnapshotMemoryServerFinal {
        var failure: Throwable? = null
        var gracefulShutdownRequested = !process.isAlive
        if (process.isAlive) {
            try {
                shutdownRequester(port)
                gracefulShutdownRequested = true
            } catch (error: Throwable) {
                failure = appendFailure(failure, error)
            }
        }
        if (process.isAlive && gracefulShutdownRequested) {
            try {
                if (!waitForProcess(process, gracefulShutdownTimeoutMillis)) {
                    failure = appendFailure(
                        failure,
                        IllegalStateException("snapshot memory fixture server did not stop gracefully"),
                    )
                }
            } catch (error: Throwable) {
                failure = appendFailure(failure, error)
            }
        }
        if (process.isAlive) {
            failure = forceStopProcess(process, forceShutdownTimeoutMillis, failure)
        }

        var finalLine: String? = null
        if (!process.isAlive) {
            try {
                while (true) {
                    val line = output.readLine() ?: break
                    if (line.startsWith("snapshot_memory_server_final ")) finalLine = line
                }
            } catch (error: Throwable) {
                failure = appendFailure(failure, error)
            }
            try {
                check(process.exitValue() == 0) {
                    "snapshot memory fixture server failed with exit ${process.exitValue()}"
                }
            } catch (error: Throwable) {
                failure = appendFailure(failure, error)
            }
        } else {
            failure = appendFailure(
                failure,
                IllegalStateException("snapshot memory fixture server remained alive after forced shutdown"),
            )
        }
        try {
            output.close()
        } catch (error: Throwable) {
            failure = appendFailure(failure, error)
        } finally {
            outputClosed = true
        }
        failure?.let { throw it }

        val completedLine = finalLine
            ?: error("snapshot memory fixture server did not emit final metrics")
        val fields = completedLine.substringAfter(' ').split(' ').associate { field ->
            field.substringBefore('=') to field.substringAfter('=')
        }
        return SnapshotMemoryServerFinal(
            chunkCount = fields.getValue("chunks").toLong(),
            maxChunkRows = fields.getValue("server_max_chunk_rows").toLong(),
            maxChunkBytes = fields.getValue("server_max_chunk_bytes").toLong(),
        )
    }

    companion object {
        private const val defaultReadinessTimeoutMillis = 20_000L
        private const val defaultGracefulShutdownTimeoutMillis = 20_000L
        private const val defaultForceShutdownTimeoutMillis = 5_000L
        private val readyPattern = Regex("snapshot_memory_server_ready port=(\\d+) pid=(\\d+)")

        fun start(rowCount: Int, targetRowBytes: Int): SnapshotMemoryFixtureProcess {
            val classpath = System.getenv("OVERSQLITE_MEMORY_SERVER_CLASSPATH")
                ?.takeIf { it.isNotBlank() }
                ?: error("OVERSQLITE_MEMORY_SERVER_CLASSPATH is required")
            val javaExecutable = Path.of(System.getProperty("java.home"), "bin", "java").toString()
            val command = listOf(
                javaExecutable,
                "-cp",
                classpath,
                "dev.goquick.sqlitenow.oversqlite.SnapshotMemoryFixtureServerKt",
                rowCount.toString(),
                targetRowBytes.toString(),
            )
            val process = ProcessBuilder(command).redirectErrorStream(true).start()
            return fromStartedProcessForTest(
                process = process,
                readinessTimeoutMillis = defaultReadinessTimeoutMillis,
                gracefulShutdownTimeoutMillis = defaultGracefulShutdownTimeoutMillis,
                forceShutdownTimeoutMillis = defaultForceShutdownTimeoutMillis,
                fallbackCommandLine = command.joinToString(" "),
            )
        }

        internal fun fromStartedProcessForTest(
            process: Process,
            readinessTimeoutMillis: Long,
            gracefulShutdownTimeoutMillis: Long = defaultGracefulShutdownTimeoutMillis,
            forceShutdownTimeoutMillis: Long = defaultForceShutdownTimeoutMillis,
            fallbackCommandLine: String = "test fixture process",
            shutdownRequester: (Int) -> Unit = ::requestSnapshotMemoryFixtureShutdown,
        ): SnapshotMemoryFixtureProcess {
            val cleanupTimeoutMillis = forceShutdownTimeoutMillis.takeIf { it > 0L }
                ?: defaultForceShutdownTimeoutMillis
            var output: BufferedReader? = null
            var readerExecutor: java.util.concurrent.ExecutorService? = null
            try {
                require(readinessTimeoutMillis > 0L)
                require(gracefulShutdownTimeoutMillis > 0L)
                require(forceShutdownTimeoutMillis > 0L)
                output = process.inputStream.bufferedReader()
                readerExecutor = Executors.newSingleThreadExecutor { runnable ->
                    Thread(runnable, "snapshot-memory-fixture-readiness").apply { isDaemon = true }
                }
                val readyLine = readerExecutor.submit<String?> { output.readLine() }
                    .get(readinessTimeoutMillis, TimeUnit.MILLISECONDS)
                    ?: error("snapshot memory fixture server exited before readiness")
                val match = readyPattern.matchEntire(readyLine)
                    ?: error("unexpected snapshot memory fixture readiness line")
                val port = match.groupValues[1].toInt()
                require(port in 1..65_535) { "snapshot memory fixture server reported an invalid port" }
                val pid = match.groupValues[2].toLong()
                check(process.pid() == pid) { "snapshot memory fixture server reported the wrong PID" }
                return SnapshotMemoryFixtureProcess(
                    process = process,
                    output = output,
                    port = port,
                    pid = pid,
                    commandLine = process.info().commandLine().orElse(fallbackCommandLine),
                    gracefulShutdownTimeoutMillis = gracefulShutdownTimeoutMillis,
                    forceShutdownTimeoutMillis = forceShutdownTimeoutMillis,
                    shutdownRequester = shutdownRequester,
                )
            } catch (error: Throwable) {
                forceStopProcess(process, cleanupTimeoutMillis, error)
                try {
                    output?.close()
                } catch (cleanupError: Throwable) {
                    error.addSuppressed(cleanupError)
                }
                if (error is InterruptedException) Thread.currentThread().interrupt()
                throw error
            } finally {
                readerExecutor?.shutdownNow()
            }
        }
    }
}

private fun requestSnapshotMemoryFixtureShutdown(port: Int) {
    Socket().use { socket ->
        socket.connect(InetSocketAddress("127.0.0.1", port), 5_000)
        socket.soTimeout = 5_000
        val writer = socket.getOutputStream().bufferedWriter(Charsets.US_ASCII)
        writer.write("POST /__shutdown HTTP/1.1\r\n")
        writer.write("Host: 127.0.0.1:$port\r\n")
        writer.write("Content-Length: 0\r\n")
        writer.write("Connection: close\r\n\r\n")
        writer.flush()
        socket.getInputStream().read()
    }
}

private fun appendFailure(first: Throwable?, next: Throwable): Throwable {
    if (first == null) return next
    if (first !== next) first.addSuppressed(next)
    return first
}

private fun forceStopProcess(
    process: Process,
    timeoutMillis: Long,
    initialFailure: Throwable? = null,
): Throwable? {
    var failure = initialFailure
    try {
        if (process.isAlive) process.destroyForcibly()
    } catch (error: Throwable) {
        failure = appendFailure(failure, error)
    }
    try {
        if (!waitForProcess(process, timeoutMillis)) {
            failure = appendFailure(
                failure,
                IllegalStateException("snapshot memory fixture process did not exit after forced shutdown"),
            )
        }
    } catch (error: Throwable) {
        failure = appendFailure(failure, error)
    }
    return failure
}

private fun waitForProcess(process: Process, timeoutMillis: Long): Boolean {
    val deadlineNanos = Math.addExact(System.nanoTime(), TimeUnit.MILLISECONDS.toNanos(timeoutMillis))
    var interruption: InterruptedException? = null
    while (process.isAlive) {
        val remainingNanos = deadlineNanos - System.nanoTime()
        if (remainingNanos <= 0L) break
        try {
            if (process.waitFor(remainingNanos, TimeUnit.NANOSECONDS)) break
        } catch (error: InterruptedException) {
            interruption = interruption?.also { it.addSuppressed(error) } ?: error
        }
    }
    val interrupted = interruption
    if (interrupted != null) {
        Thread.currentThread().interrupt()
        throw interrupted
    }
    return !process.isAlive
}

internal data class JvmMemoryMetrics(
    val durationNanos: Long,
    val sampleCount: Long,
    val baselineHeapBytes: Long,
    val peakHeapBytes: Long,
    val adjustedHeapBytes: Long,
    val baselineRssBytes: Long,
    val peakRssBytes: Long,
    val adjustedRssBytes: Long,
    val sampledThreadAllocatedBytes: Long,
)

private data class JvmMemoryCheckpoint(
    val heapUsedBytes: Long,
    val heapCommittedBytes: Long,
    val heapMaxBytes: Long,
    val rssBytes: Long,
    val gcCollectionCount: Long,
    val gcCollectionTimeMillis: Long,
    val liveThreadCount: Int,
    val daemonThreadCount: Int,
    val openFileDescriptorCount: Long,
) {
    fun asEvidenceFields(prefix: String): String =
        "${prefix}_heap_used_bytes=$heapUsedBytes " +
            "${prefix}_heap_committed_bytes=$heapCommittedBytes " +
            "${prefix}_heap_max_bytes=$heapMaxBytes " +
            "${prefix}_rss_bytes=$rssBytes " +
            "${prefix}_gc_count=$gcCollectionCount " +
            "${prefix}_gc_time_ms=$gcCollectionTimeMillis " +
            "${prefix}_live_thread_count=$liveThreadCount " +
            "${prefix}_daemon_thread_count=$daemonThreadCount " +
            "${prefix}_open_fd_count=$openFileDescriptorCount"
}

private data class JvmPostRestoreGcDiagnostics(
    val afterRestore: JvmMemoryCheckpoint,
    val afterResourceClosure: JvmMemoryCheckpoint,
    val afterIdle: JvmMemoryCheckpoint,
    val afterExplicitGc: JvmMemoryCheckpoint,
    val explicitGcObserved: Boolean,
    val idleWaitMillis: Long,
    val explicitGcConfirmationWaitMillis: Long,
    val postGcReclamationWaitMillis: Long,
)

internal class JvmMemorySampler private constructor(
    private val pid: Long,
    private val baselineHeapBytes: Long,
    private val baselineRssBytes: Long,
    private val baselineThreadAllocatedBytes: Long,
    private val peakHeapBytes: AtomicLong,
    private val peakRssBytes: AtomicLong,
    private val peakThreadAllocatedBytes: AtomicLong,
    private val startedNanos: Long,
    private val sampleCount: AtomicLong,
    private val stop: AtomicBoolean,
    private val failure: AtomicReference<Throwable?>,
    private val worker: Thread,
) {
    internal val workerAliveForTest: Boolean
        get() = worker.isAlive

    fun stop(): JvmMemoryMetrics {
        stop.set(true)
        stopSamplerWorker(worker)
        failure.get()?.let { throw IllegalStateException("client RSS sampling failed", it) }
        peakHeapBytes.accumulateAndGet(currentHeapBytes(), ::maxOf)
        peakRssBytes.accumulateAndGet(rssBytes(pid), ::maxOf)
        peakThreadAllocatedBytes.accumulateAndGet(currentLiveThreadAllocatedBytes(), ::maxOf)
        sampleCount.incrementAndGet()
        return JvmMemoryMetrics(
            durationNanos = (System.nanoTime() - startedNanos).coerceAtLeast(1L),
            sampleCount = sampleCount.get(),
            baselineHeapBytes = baselineHeapBytes,
            peakHeapBytes = peakHeapBytes.get(),
            adjustedHeapBytes = (peakHeapBytes.get() - baselineHeapBytes).coerceAtLeast(0),
            baselineRssBytes = baselineRssBytes,
            peakRssBytes = peakRssBytes.get(),
            adjustedRssBytes = (peakRssBytes.get() - baselineRssBytes).coerceAtLeast(0),
            sampledThreadAllocatedBytes =
                (peakThreadAllocatedBytes.get() - baselineThreadAllocatedBytes).coerceAtLeast(0),
        )
    }

    companion object {
        fun start(): JvmMemorySampler {
            val pid = ProcessHandle.current().pid()
            val baselineHeap = currentHeapBytes()
            val baselineRss = rssBytes(pid)
            val baselineAllocated = currentLiveThreadAllocatedBytes()
            val peakHeap = AtomicLong(baselineHeap)
            val peakRss = AtomicLong(baselineRss)
            val peakAllocated = AtomicLong(baselineAllocated)
            val sampleCount = AtomicLong(1L)
            val stop = AtomicBoolean(false)
            val failure = AtomicReference<Throwable?>(null)
            val startedNanos = System.nanoTime()
            val worker = thread(start = false, isDaemon = true, name = "snapshot-client-memory-sampler") {
                try {
                    while (!stop.get()) {
                        peakHeap.accumulateAndGet(currentHeapBytes(), ::maxOf)
                        peakRss.accumulateAndGet(rssBytes(pid), ::maxOf)
                        peakAllocated.accumulateAndGet(currentLiveThreadAllocatedBytes(), ::maxOf)
                        sampleCount.incrementAndGet()
                        Thread.sleep(50)
                    }
                } catch (error: Throwable) {
                    failure.compareAndSet(null, error)
                    stop.set(true)
                }
            }
            worker.start()
            return JvmMemorySampler(
                pid = pid,
                baselineHeapBytes = baselineHeap,
                baselineRssBytes = baselineRss,
                baselineThreadAllocatedBytes = baselineAllocated,
                peakHeapBytes = peakHeap,
                peakRssBytes = peakRss,
                peakThreadAllocatedBytes = peakAllocated,
                startedNanos = startedNanos,
                sampleCount = sampleCount,
                stop = stop,
                failure = failure,
                worker = worker,
            )
        }
    }
}

internal data class ProcessRssMetrics(
    val baselineRssBytes: Long,
    val peakRssBytes: Long,
    val adjustedRssBytes: Long,
)

internal class ProcessRssSampler private constructor(
    private val pid: Long,
    private val baselineRssBytes: Long,
    private val peakRssBytes: AtomicLong,
    private val stop: AtomicBoolean,
    private val failure: AtomicReference<Throwable?>,
    private val worker: Thread,
) {
    internal val workerAliveForTest: Boolean
        get() = worker.isAlive

    fun stop(): ProcessRssMetrics {
        stop.set(true)
        stopSamplerWorker(worker)
        failure.get()?.let { throw IllegalStateException("server RSS sampling failed", it) }
        if (ProcessHandle.of(pid).map { it.isAlive }.orElse(false)) {
            peakRssBytes.accumulateAndGet(rssBytes(pid), ::maxOf)
        }
        return ProcessRssMetrics(
            baselineRssBytes = baselineRssBytes,
            peakRssBytes = peakRssBytes.get(),
            adjustedRssBytes = (peakRssBytes.get() - baselineRssBytes).coerceAtLeast(0),
        )
    }

    companion object {
        fun start(pid: Long): ProcessRssSampler {
            val baselineRss = rssBytes(pid)
            val peakRss = AtomicLong(baselineRss)
            val stop = AtomicBoolean(false)
            val failure = AtomicReference<Throwable?>(null)
            val worker = thread(start = false, isDaemon = true, name = "snapshot-server-rss-sampler") {
                try {
                    while (!stop.get()) {
                        peakRss.accumulateAndGet(rssBytes(pid), ::maxOf)
                        Thread.sleep(50)
                    }
                } catch (error: Throwable) {
                    failure.compareAndSet(null, error)
                    stop.set(true)
                }
            }
            worker.start()
            return ProcessRssSampler(pid, baselineRss, peakRss, stop, failure, worker)
        }
    }
}

private fun stopSamplerWorker(worker: Thread) {
    var failure: Throwable? = null
    try {
        worker.join(5_000L)
    } catch (error: InterruptedException) {
        failure = error
    }
    if (worker.isAlive) {
        worker.interrupt()
        try {
            worker.join(1_000L)
        } catch (error: InterruptedException) {
            failure = appendFailure(failure, error)
        }
    }
    if (worker.isAlive) {
        failure = appendFailure(
            failure,
            IllegalStateException("snapshot memory sampler thread did not stop"),
        )
    }
    val completedFailure = failure
    if (completedFailure != null) {
        if (completedFailure is InterruptedException) Thread.currentThread().interrupt()
        throw completedFailure
    }
}

private fun currentHeapBytes(): Long = ManagementFactory.getMemoryMXBean().heapMemoryUsage.used

private fun currentJvmMemoryCheckpoint(pid: Long): JvmMemoryCheckpoint {
    val heap = ManagementFactory.getMemoryMXBean().heapMemoryUsage
    val garbageCollectors = ManagementFactory.getGarbageCollectorMXBeans()
    val threads = ManagementFactory.getThreadMXBean()
    val openFileDescriptorCount =
        (ManagementFactory.getOperatingSystemMXBean() as? UnixOperatingSystemMXBean)
            ?.openFileDescriptorCount
            ?: -1L
    return JvmMemoryCheckpoint(
        heapUsedBytes = heap.used,
        heapCommittedBytes = heap.committed,
        heapMaxBytes = heap.max,
        rssBytes = rssBytes(pid),
        gcCollectionCount = garbageCollectors.sumOf { it.collectionCount.coerceAtLeast(0L) },
        gcCollectionTimeMillis = garbageCollectors.sumOf { it.collectionTime.coerceAtLeast(0L) },
        liveThreadCount = threads.threadCount,
        daemonThreadCount = threads.daemonThreadCount,
        openFileDescriptorCount = openFileDescriptorCount,
    )
}

private fun runPostRestoreGcDiagnostic(
    pid: Long,
    afterRestore: JvmMemoryCheckpoint,
): JvmPostRestoreGcDiagnostics {
    val idleWaitMillis = 5_000L
    val gcConfirmationTimeoutMillis = 5_000L
    val postGcReclamationWaitMillis = 5_000L

    val afterResourceClosure = currentJvmMemoryCheckpoint(pid)
    Thread.sleep(idleWaitMillis)
    val afterIdle = currentJvmMemoryCheckpoint(pid)

    val gcRequestedNanos = System.nanoTime()
    System.gc()
    val gcDeadlineNanos = Math.addExact(
        gcRequestedNanos,
        TimeUnit.MILLISECONDS.toNanos(gcConfirmationTimeoutMillis),
    )
    var afterGcRequest = currentJvmMemoryCheckpoint(pid)
    while (
        afterGcRequest.gcCollectionCount <= afterIdle.gcCollectionCount &&
        System.nanoTime() < gcDeadlineNanos
    ) {
        Thread.sleep(50L)
        afterGcRequest = currentJvmMemoryCheckpoint(pid)
    }
    val explicitGcObserved = afterGcRequest.gcCollectionCount > afterIdle.gcCollectionCount
    check(explicitGcObserved) {
        "explicit GC was not observed within $gcConfirmationTimeoutMillis ms"
    }
    val explicitGcConfirmationWaitMillis =
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - gcRequestedNanos)

    Thread.sleep(postGcReclamationWaitMillis)
    val afterExplicitGc = currentJvmMemoryCheckpoint(pid)
    return JvmPostRestoreGcDiagnostics(
        afterRestore = afterRestore,
        afterResourceClosure = afterResourceClosure,
        afterIdle = afterIdle,
        afterExplicitGc = afterExplicitGc,
        explicitGcObserved = explicitGcObserved,
        idleWaitMillis = idleWaitMillis,
        explicitGcConfirmationWaitMillis = explicitGcConfirmationWaitMillis,
        postGcReclamationWaitMillis = postGcReclamationWaitMillis,
    )
}

private fun currentLiveThreadAllocatedBytes(): Long {
    val bean = ManagementFactory.getThreadMXBean() as? ThreadMXBean ?: return 0L
    if (!bean.isThreadAllocatedMemorySupported) return 0L
    if (!bean.isThreadAllocatedMemoryEnabled) bean.isThreadAllocatedMemoryEnabled = true
    return bean.getThreadAllocatedBytes(bean.allThreadIds).filter { it > 0L }.sum()
}

private fun rssBytes(pid: Long): Long {
    val process = ProcessBuilder("/bin/ps", "-o", "rss=", "-p", pid.toString()).start()
    var primaryFailure: Throwable? = null
    try {
        check(waitForProcess(process, 2_000L)) { "RSS sampler timed out for pid $pid" }
        val output = process.inputStream.bufferedReader().use { it.readText().trim() }
        process.errorStream.bufferedReader().use { it.readText() }
        val exitCode = process.exitValue()
        check(exitCode == 0) {
            "RSS sampler failed for pid $pid with exit=$exitCode"
        }
        val kibibytes = output.toLongOrNull()
            ?: error("RSS sampler returned no numeric value for pid $pid")
        check(kibibytes > 0L) { "RSS sampler returned a non-positive value for pid $pid" }
        return Math.multiplyExact(kibibytes, 1024L)
    } catch (error: Throwable) {
        primaryFailure = error
        throw error
    } finally {
        var cleanupFailure: Throwable? = null
        if (process.isAlive) {
            cleanupFailure = forceStopProcess(process, 2_000L)
        }
        listOf<java.io.Closeable>(
            process.inputStream,
            process.errorStream,
            process.outputStream,
        ).forEach { stream ->
            try {
                stream.close()
            } catch (error: Throwable) {
                cleanupFailure = appendFailure(cleanupFailure, error)
            }
        }
        val completedCleanupFailure = cleanupFailure
        if (completedCleanupFailure != null) {
            val primary = primaryFailure
            if (primary == null) {
                throw completedCleanupFailure
            }
            primary.addSuppressed(completedCleanupFailure)
        }
    }
}
