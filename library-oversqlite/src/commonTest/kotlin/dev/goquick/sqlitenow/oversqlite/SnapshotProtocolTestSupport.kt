/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.Json

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
        RegisteredTableSpec(
            schema = "main",
            table = table,
            syncKeyColumns = listOf("id"),
        )
    }

internal fun snapshotRowWireBytes(json: Json, row: SnapshotRow): Long =
    json.encodeToString(SnapshotRow.serializer(), row).encodeToByteArray().size.toLong()

internal fun snapshotRowsWireBytes(json: Json, rows: List<SnapshotRow>): Long =
    rows.fold(0L) { total, row -> total + snapshotRowWireBytes(json, row) }
