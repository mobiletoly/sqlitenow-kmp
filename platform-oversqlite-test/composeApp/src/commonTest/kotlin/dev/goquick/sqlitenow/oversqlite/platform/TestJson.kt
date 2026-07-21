package dev.goquick.sqlitenow.oversqlite.platform

import dev.goquick.sqlitenow.oversqlite.BundleCapabilitiesLimits
import dev.goquick.sqlitenow.oversqlite.RegisteredTableSpec
import kotlinx.serialization.json.Json

internal val testJson = Json { ignoreUnknownKeys = true }

internal fun testBundleCapabilitiesLimits(): BundleCapabilitiesLimits = BundleCapabilitiesLimits(
    maxRowsPerBundle = 100_000,
    maxBytesPerBundle = 64 * 1024 * 1024,
    maxBundlesPerPull = 1000,
    defaultRowsPerPushChunk = 1000,
    maxRowsPerPushChunk = 10_000,
    pushSessionTtlSeconds = 900,
    defaultRowsPerCommittedBundleChunk = 1000,
    maxRowsPerCommittedBundleChunk = 10_000,
    defaultRowsPerSnapshotChunk = 1000,
    maxRowsPerSnapshotChunk = 10_000,
    snapshotSessionTtlSeconds = 900,
    maxRowsPerSnapshotSession = 10_000_000,
    maxBytesPerSnapshotSession = 2L * 1024L * 1024L * 1024L,
    defaultBytesPerSnapshotChunk = 4L * 1024L * 1024L,
    maxBytesPerSnapshotChunk = 16L * 1024L * 1024L,
    maxBytesPerSnapshotRow = 4L * 1024L * 1024L,
    snapshotMaterializationBatchRows = 1000,
    snapshotMaterializationBatchBytes = 4L * 1024L * 1024L,
    maxConcurrentSnapshotBuilds = 2,
    maxConcurrentSnapshotChunkRequests = 8,
    initializationLeaseTtlSeconds = 900,
)

internal fun testRegisteredTableSpecs(vararg tables: String): List<RegisteredTableSpec> =
    tables.map { table ->
        RegisteredTableSpec(schema = "main", table = table, syncKeyColumns = listOf("id"))
    }
