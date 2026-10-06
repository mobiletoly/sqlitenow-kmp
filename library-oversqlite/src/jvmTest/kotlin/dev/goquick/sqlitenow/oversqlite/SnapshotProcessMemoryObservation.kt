package dev.goquick.sqlitenow.oversqlite

import com.sun.management.UnixOperatingSystemMXBean
import java.lang.management.ManagementFactory
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.delay
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject

/** Opt-in process diagnostics; deliberately excluded from throughput measurements. */
internal class SnapshotProcessMemoryObservation(private val directory: Path, private val closureOnly: Boolean = false) {
    private val sqlite = SnapshotNativeAllocationObservation.fromEnvironment()
    private val pid = ProcessHandle.current().pid()
    private val observed = mutableSetOf<String>()

    @Synchronized
    fun capture(phase: String, httpClosed: Boolean? = null) {
        require(phase.matches(Regex("[a-z_]+")))
        if (closureOnly && phase !in setOf("after_final_close", "after_idle", "after_gc")) return
        if (!observed.add(phase)) return
        Files.createDirectories(directory)
        val name = "${observed.size.toString().padStart(2, '0')}-$phase"
        val heap = ManagementFactory.getMemoryMXBean().heapMemoryUsage
        val nonHeap = ManagementFactory.getMemoryMXBean().nonHeapMemoryUsage
        val threads = ManagementFactory.getThreadMXBean()
        val rssPath = directory.resolve("$name-rss.txt")
        val rss = runMemoryObservationCommand(listOf("/bin/ps", "-o", "rss=", "-p", pid.toString()), rssPath)
        check(!rss.timedOut && rss.exit == 0) { "RSS observation failed: $rss" }
        val rssBytes = Math.multiplyExact(Files.readString(rssPath).trim().toLong(), 1024L)
        val nmtPath = directory.resolve("$name-nmt.txt")
        val nmt = runMemoryObservationCommand(listOf(Path.of(System.getProperty("java.home"), "bin", "jcmd").toString(),
            pid.toString(), "VM.native_memory", "summary", "scale=KB"), nmtPath)
        val mapPath = directory.resolve("$name-vmmap.txt")
        val map = runMemoryObservationCommand(listOf("/usr/bin/vmmap", "-summary", pid.toString()), mapPath)
        val evidence = buildJsonObject {
            put("phase", JsonPrimitive(phase)); put("pid", JsonPrimitive(pid))
            put("epochMillis", JsonPrimitive(System.currentTimeMillis()))
            put("javaHome", JsonPrimitive(System.getProperty("java.home")))
            put("heapUsedBytes", JsonPrimitive(heap.used)); put("heapCommittedBytes", JsonPrimitive(heap.committed))
            put("heapMaxBytes", JsonPrimitive(heap.max)); put("nonHeapUsedBytes", JsonPrimitive(nonHeap.used))
            put("rssBytes", JsonPrimitive(rssBytes)); put("gcCount", JsonPrimitive(gcCount()))
            put("threads", JsonPrimitive(threads.threadCount)); put("peakThreads", JsonPrimitive(threads.peakThreadCount))
            put("loadedClasses", JsonPrimitive(ManagementFactory.getClassLoadingMXBean().loadedClassCount))
            put("openFileDescriptors", JsonPrimitive((ManagementFactory.getOperatingSystemMXBean() as? UnixOperatingSystemMXBean)?.openFileDescriptorCount ?: -1L))
            put("httpClosed", httpClosed?.let(::JsonPrimitive) ?: JsonNull)
            sqlite?.let { put("sqliteAllocation", it.capture()) }
            put("nmtExit", JsonPrimitive(nmt.exit)); put("nmtTimedOut", JsonPrimitive(nmt.timedOut))
            put("vmmapExit", JsonPrimitive(map.exit)); put("vmmapTimedOut", JsonPrimitive(map.timedOut))
        }
        Files.writeString(directory.resolve("$name.json"), evidence.toString())
        println("capacity_memory_observation $evidence")
        check(!nmt.timedOut && nmt.exit == 0 && hasEnabledNativeMemoryTracking(Files.readString(nmtPath))) {
            "native-memory observation unavailable: $nmt"
        }
        check(!map.timedOut && map.exit == 0 && Files.readString(mapPath).contains("Physical footprint:")) {
            "resident memory observation unavailable: $map"
        }
    }

    suspend fun afterClosure() {
        delay(5_000)
        capture("after_idle")
        val before = gcCount()
        System.gc()
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (gcCount() <= before && System.nanoTime() < deadline) delay(50)
        val observed = gcCount() > before
        delay(5_000)
        capture("after_gc")
        Files.writeString(directory.resolve("gc-observation.json"), buildJsonObject {
            put("observed", JsonPrimitive(observed)); put("countBefore", JsonPrimitive(before))
            put("countAfter", JsonPrimitive(gcCount()))
        }.toString())
        check(observed) { "diagnostic GC was not observed" }
    }

    private fun gcCount(): Long = ManagementFactory.getGarbageCollectorMXBeans().sumOf { it.collectionCount.coerceAtLeast(0) }

    companion object {
        fun fromEnvironment(): SnapshotProcessMemoryObservation? =
            System.getenv("OVERSQLITE_CAPACITY_MEMORY_DIAGNOSTICS_DIR")?.let {
                val mode = System.getenv("OVERSQLITE_CAPACITY_MEMORY_CAPTURE") ?: "all"
                require(mode == "all" || mode == "closure") { "unsupported memory capture mode" }
                SnapshotProcessMemoryObservation(Path.of(it), closureOnly = mode == "closure")
            }
    }
}

internal data class MemoryObservationCommandResult(val pid: Long, val exit: Int, val timedOut: Boolean)

/** Redirect before waiting so diagnostic output cannot deadlock a child; reap every launched child. */
internal fun runMemoryObservationCommand(args: List<String>, output: Path, timeoutMillis: Long = 5_000): MemoryObservationCommandResult {
    val process = ProcessBuilder(args).redirectErrorStream(true).redirectOutput(output.toFile()).start()
    try {
        process.outputStream.close()
        val timedOut = !process.waitFor(timeoutMillis, TimeUnit.MILLISECONDS)
        if (timedOut) {
            process.destroyForcibly()
            check(process.waitFor(2, TimeUnit.SECONDS)) { "diagnostic child ${process.pid()} could not be reaped" }
        }
        val result = MemoryObservationCommandResult(process.pid(), process.exitValue(), timedOut)
        process.inputStream.close()
        process.errorStream.close()
        return result
    } catch (error: Throwable) {
        try {
            if (process.isAlive) process.destroyForcibly()
            check(process.waitFor(2, TimeUnit.SECONDS)) { "diagnostic child ${process.pid()} could not be reaped" }
        } catch (cleanup: Throwable) {
            if (cleanup !== error) error.addSuppressed(cleanup)
        }
        for (stream in listOf(process.inputStream, process.errorStream)) {
            try { stream.close() } catch (cleanup: Throwable) {
                if (cleanup !== error) error.addSuppressed(cleanup)
            }
        }
        throw error
    }
}

internal fun hasEnabledNativeMemoryTracking(text: String): Boolean =
    text.contains("Native Memory Tracking:") && text.contains("Total: reserved=") &&
        !text.contains("Native memory tracking is not enabled")
