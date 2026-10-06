package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.defaultRequest
import java.lang.management.ManagementFactory
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.job
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** Local-only, fresh-JVM compact-protocol memory profiles. */
class SnapshotMemoryBaselineTest {
    private fun requireFreshWorker() {
        check(System.getenv("GITHUB_ACTIONS") != "true") { "memory profile is local-only" }
        assertEquals(listOf("-Xmx128m"), ManagementFactory.getRuntimeMXBean().inputArguments
            .filter { it.startsWith("-Xmx") })
    }

    @Test
    fun snapshotMemorySamplerOnlyControl() {
        if (System.getenv("OVERSQLITE_MEMORY_SAMPLER_CONTROL") == null) return
        requireFreshWorker()
        val sampler = JvmMemorySampler.start()
        TimeUnit.SECONDS.sleep(5)
        val metrics = sampler.stop()
        assertTrue(metrics.sampleCount > 1)
        println("snapshot_memory_sampler_control status=pass samples=${metrics.sampleCount} " +
            "peak_rss_bytes=${metrics.peakRssBytes}")
    }

    @Test
    fun snapshotMemoryBaseline() = runBlocking {
        val rawRows = System.getenv("OVERSQLITE_MEMORY_BASELINE_ROWS") ?: return@runBlocking
        requireFreshWorker()
        val rows = rawRows.toInt()
        val rowBytes = System.getenv("OVERSQLITE_MEMORY_ROW_BYTES")!!.toInt()
        require(rows in setOf(10_000, 50_000, 100_000, 1_000_000))
        require(rowBytes in setOf(256, 1024))
        val label = System.getenv("OVERSQLITE_MEMORY_RUN_LABEL") ?: "manual"
        lateinit var metrics: JvmMemoryMetrics
        lateinit var fixtureFinal: SnapshotMemoryServerFinal
        var appliedRows = 0L
        var maximumBodyBytes = 0L
        withSnapshotMemoryCleanup {
            val fixture = SnapshotMemoryFixtureProcess.start(rows, rowBytes)
            defer(SnapshotMemoryCleanupPhase.FIXTURE, "compact fixture") { fixtureFinal = fixture.stop() }
            val sampler = JvmMemorySampler.start()
            defer(SnapshotMemoryCleanupPhase.CLIENT_SAMPLER, "client sampler") { metrics = sampler.stop() }
            val directory = SnapshotMemoryProfileDirectory.create()
            defer(SnapshotMemoryCleanupPhase.DIRECTORY, "profile directory") { directory.deleteRecursively() }
            val db = BundledSqliteConnectionProvider.openConnection(
                directory.path.resolve("oversqlite.sqlite").toString(), debug = false)
            defer(SnapshotMemoryCleanupPhase.DATABASE, "database") { db.close() }
            db.execSQL("CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
            db.execSQL("PRAGMA journal_mode = WAL")
            val http = HttpClient(CIO) { defaultRequest { url("http://127.0.0.1:${fixture.port}/") } }
            defer(SnapshotMemoryCleanupPhase.HTTP_CLOSE, "HTTP client") { http.close() }
            defer(SnapshotMemoryCleanupPhase.HTTP_JOIN, "HTTP job") {
                withTimeout(20_000L) { http.coroutineContext.job.join() }
            }
            val client = DefaultOversqliteClient.forTesting(
                db = db,
                http = http,
                config = OversqliteConfig(
                    generationId = 1, databaseId = "memory", serviceId = "fixture",
                    syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
                    transferTargets = OversqliteTransferTargets(
                        maxSnapshotChunkRows = 1_000,
                        maxSnapshotChunkDecodedBytes = 4L * 1024L * 1024L,
                    ),
                    transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
                ),
            )
            defer(SnapshotMemoryCleanupPhase.CLIENT, "oversqlite client") { client.close() }
            var incomplete = 0
            var durableProgress = -1L
            while (true) {
                val failure = client.openAndConnect("user-1").exceptionOrNull() ?: break
                assertTrue(failure is SyncWorkLimitException, "unexpected snapshot failure: $failure")
                incomplete++
                assertTrue(incomplete <= 64, "snapshot did not complete after 64 useful yields")
                val staged = db.scalarLong("SELECT count(*) FROM _sync_snapshot_stage")
                val prepared = db.scalarLong("SELECT COALESCE((SELECT root_cursor FROM _sync_apply_plan WHERE singleton_key=1),0)")
                assertTrue(staged + prepared > durableProgress, "work yield must retain useful stage or plan progress")
                durableProgress = staged + prepared
                assertEquals(0L, db.scalarLong("SELECT count(*) FROM users"), "business state changed before atomic install")
                assertEquals(0L, db.scalarLong("SELECT count(*) FROM _sync_row_state"), "A changed before atomic install")
                assertEquals(0L, db.scalarLong("SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                maximumBodyBytes = maxOf(maximumBodyBytes,
                    client.snapshotRestoreDiagnosticsForTest().maxCompletelyDecodedChunkBodyBytes)
            }
            appliedRows = db.scalarLong("SELECT COUNT(*) FROM users")
            assertEquals(rows.toLong(), appliedRows)
            assertEquals(0L, db.scalarLong("SELECT COUNT(*) FROM _sync_snapshot_stage"))
            val diagnostics = client.snapshotRestoreDiagnosticsForTest()
            assertEquals(rows.toLong(), diagnostics.finalStagedRows)
            assertEquals(rows.toLong(), diagnostics.finalAppliedRows)
            maximumBodyBytes = maxOf(maximumBodyBytes, diagnostics.maxCompletelyDecodedChunkBodyBytes)
            assertTrue(maximumBodyBytes in 1..(4L * 1024L * 1024L))
        }
        assertTrue(fixtureFinal.chunkCount > 1L)
        recordResult(label, rows, rowBytes, metrics)
        println("snapshot_memory_mapping protocol=compact rows=$rows target_row_bytes=$rowBytes " +
            "applied_rows=$appliedRows chunks=${fixtureFinal.chunkCount} " +
            "max_decoded_body_bytes=$maximumBodyBytes peak_heap_bytes=${metrics.peakHeapBytes} " +
            "peak_rss_bytes=${metrics.peakRssBytes}")
    }

    @Test
    fun pullMemoryBaseline() = runBlocking {
        val rawRows = System.getenv("OVERSQLITE_PULL_MEMORY_ROWS") ?: return@runBlocking
        requireFreshWorker()
        val rows = rawRows.toInt()
        val rowBytes = System.getenv("OVERSQLITE_PULL_MEMORY_ROW_BYTES")!!.toInt()
        val label = System.getenv("OVERSQLITE_PULL_MEMORY_RUN_LABEL") ?: "pull-$rows-$rowBytes"
        require(rows == 25_000 && rowBytes == 1024)
        lateinit var metrics: JvmMemoryMetrics
        lateinit var fixtureFinal: SnapshotMemoryServerFinal
        withSnapshotMemoryCleanup {
            val fixture = SnapshotMemoryFixtureProcess.start(rows, rowBytes, mode = "pull")
            defer(SnapshotMemoryCleanupPhase.FIXTURE, "compact pull fixture") { fixtureFinal = fixture.stop() }
            val sampler = JvmMemorySampler.start()
            defer(SnapshotMemoryCleanupPhase.CLIENT_SAMPLER, "client sampler") { metrics = sampler.stop() }
            val http = HttpClient(CIO) { defaultRequest { url("http://127.0.0.1:${fixture.port}/") } }
            defer(SnapshotMemoryCleanupPhase.HTTP_CLOSE, "HTTP client") { http.close() }
            defer(SnapshotMemoryCleanupPhase.HTTP_JOIN, "HTTP job") {
                withTimeout(20_000L) { http.coroutineContext.job.join() }
            }
            val api = OversqliteRemoteApi(http, Json, generation = { testGeneration() }, log = { println(it()) })
            val response = api.sendPullRequest(0, 1, 0, "memory-source", testWireLimits().maxPullDecodedBytes)
            validatePullResponse(response, 0)
            assertEquals(rows, response.bundles.single().rows.size)
        }
        recordResult(label, rows, rowBytes, metrics, "OVERSQLITE_PULL_MEMORY_RESULT_FILE")
        assertTrue(fixtureFinal.pullBodyBytes > 16L * 1024L * 1024L)
        assertTrue(fixtureFinal.pullBodyBytes <= testWireLimits().maxPullDecodedBytes)
        assertTrue(metrics.peakRssBytes < 300L * 1024L * 1024L)
        println("pull_memory_mapping protocol=compact rows=$rows target_row_bytes=$rowBytes " +
            "decoded_body_bytes=${fixtureFinal.pullBodyBytes} " +
            "peak_heap_bytes=${metrics.peakHeapBytes} peak_rss_bytes=${metrics.peakRssBytes}")
    }

    private fun recordResult(
        label: String,
        rows: Int,
        rowBytes: Int,
        metrics: JvmMemoryMetrics,
        environmentName: String = "OVERSQLITE_MEMORY_RESULT_FILE",
    ) {
        val path = System.getenv(environmentName) ?: return
        SnapshotMemoryResult(
            runLabel = label, rowCount = rows, targetRowBytes = rowBytes,
            baselineHeapBytes = metrics.baselineHeapBytes, peakHeapBytes = metrics.peakHeapBytes,
            adjustedHeapBytes = metrics.adjustedHeapBytes,
            baselineRssBytes = metrics.baselineRssBytes, peakRssBytes = metrics.peakRssBytes,
            adjustedRssBytes = metrics.adjustedRssBytes,
        ).write(Path.of(path))
    }
}
