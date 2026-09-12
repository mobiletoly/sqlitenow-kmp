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

internal enum class SnapshotMemoryProfileSet(val wireName: String) {
    CLOSURE("closure"),
    EXTENDED("extended"),
    ;

    companion object {
        fun parse(value: String): SnapshotMemoryProfileSet =
            entries.singleOrNull { it.wireName == value }
                ?: error("unknown snapshot memory profile set: $value")
    }
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

    private val closureProfiles: Map<String, Pair<Int, Int>> = linkedMapOf(
        "closure-10k-256" to (10_000 to 256),
        "closure-10k-1k" to (10_000 to 1024),
        "closure-100k-256" to (100_000 to 256),
        "closure-100k-1k" to (100_000 to 1024),
    )
    private val extendedProfiles: Map<String, Pair<Int, Int>> = linkedMapOf(
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
        require(args.size in 1..2) {
            "expected the snapshot memory result directory and optional profile set"
        }
        val directory = Path.of(args.first())
        val profileSet = args.getOrNull(1)?.let(SnapshotMemoryProfileSet::parse)
            ?: SnapshotMemoryProfileSet.EXTENDED
        require(Files.isDirectory(directory)) { "snapshot memory result directory is missing" }
        val results = Files.list(directory).use { paths ->
            paths.filter { Files.isRegularFile(it) && it.fileName.toString().endsWith(".properties") }
                .sorted()
                .map(::read)
                .toList()
        }
        verify(results, profileSet)
        println(
            "snapshot_memory_result_verifier status=pass profile_set=${profileSet.wireName} " +
                "profiles=${results.size}",
        )
    }

    fun expectedProfiles(profileSet: SnapshotMemoryProfileSet): Map<String, Pair<Int, Int>> =
        when (profileSet) {
            SnapshotMemoryProfileSet.CLOSURE -> closureProfiles
            SnapshotMemoryProfileSet.EXTENDED -> extendedProfiles
        }

    fun verify(
        results: List<SnapshotMemoryResult>,
        profileSet: SnapshotMemoryProfileSet = SnapshotMemoryProfileSet.EXTENDED,
    ) {
        val expectedProfiles = expectedProfiles(profileSet)
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
            require(result.peakRssBytes < MEASURED_CLIENT_RSS_CEILING_BYTES) {
                "$label reached the $MEASURED_CLIENT_RSS_CEILING_MIB MiB client RSS ceiling"
            }
        }

        if (profileSet == SnapshotMemoryProfileSet.CLOSURE) return

        val million = byLabel.getValue("measured-1m-256").single()
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
    val pullBodyBytes: Long,
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
            pullBodyBytes = fields.getValue("pull_body_bytes").toLong(),
        )
    }

    companion object {
        private const val defaultReadinessTimeoutMillis = 20_000L
        private const val defaultGracefulShutdownTimeoutMillis = 20_000L
        private const val defaultForceShutdownTimeoutMillis = 5_000L
        private val readyPattern = Regex("snapshot_memory_server_ready port=(\\d+) pid=(\\d+)")

        fun start(rowCount: Int, targetRowBytes: Int, mode: String = "snapshot"): SnapshotMemoryFixtureProcess {
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
                mode,
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
