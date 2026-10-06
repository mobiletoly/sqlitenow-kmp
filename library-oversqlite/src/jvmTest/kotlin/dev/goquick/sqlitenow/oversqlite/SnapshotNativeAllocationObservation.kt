package dev.goquick.sqlitenow.oversqlite

import java.nio.file.Files
import java.nio.file.Path
import kotlinx.serialization.json.*

/** Opt-in macOS JVM instrumentation, never part of normal library binaries. */
internal class SnapshotNativeAllocationObservation private constructor(library: Path) {
    init {
        require(System.getProperty("os.name").contains("Mac")) { "native allocation bridge requires macOS" }
        require(Files.isRegularFile(library)) { "native allocation bridge is missing" }
        System.load(library.toAbsolutePath().toString())
        // Initialize the driver's own one-time loader, without opening a connection.
        Class.forName("androidx.sqlite.driver.bundled.BundledSQLiteDriver\$NativeLibraryObject", true,
            androidx.sqlite.driver.bundled.BundledSQLiteDriver::class.java.classLoader)
        check(enableNativeMetrics() == 0) { "SQLite memory diagnostics must start before engine initialization" }
    }

    private external fun enableNativeMetrics(): Int
    private external fun readNativeMetrics(): LongArray?

    fun capture(): JsonObject = decodeNativeAllocationMetrics(checkNotNull(readNativeMetrics()) {
        "configured SQLite engine allocation identity disappeared"
    })

    companion object {
        fun fromEnvironment(): SnapshotNativeAllocationObservation? =
            System.getenv("OVERSQLITE_CAPACITY_SQLITE_NATIVE_LIBRARY")?.let { SnapshotNativeAllocationObservation(Path.of(it)) }
    }
}

internal fun decodeNativeAllocationMetrics(values: LongArray): JsonObject {
    require(values.size == 9 && values.all { it >= 0 }) { "invalid SQLite allocation metric vector" }
    for (index in listOf(0, 2, 4, 6)) require(values[index + 1] >= values[index]) { "SQLite highwater is below current" }
    require(values[8] >= 3_000_000) { "invalid SQLite engine version" }
    val names = listOf("currentBytes", "highwaterBytes", "pageCacheOverflowBytes", "pageCacheOverflowHighwaterBytes",
        "configuredPageCacheUsedPages", "configuredPageCacheHighwaterPages", "allocationCount", "allocationHighwaterCount", "versionNumber")
    return buildJsonObject {
        put("diagnosticMemstatusEnabled", true)
        put("highwaterReset", false)
        names.forEachIndexed { index, name -> put(name, values[index]) }
    }
}
