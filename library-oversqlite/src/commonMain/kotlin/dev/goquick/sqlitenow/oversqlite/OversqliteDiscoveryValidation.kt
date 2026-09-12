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

internal fun validateAdmittedWireLimits(limits: OversqliteWireLimits) = with(limits) {
    require(maxPushRows in 1..100_000) { "max_push_rows exceeds protocol range" }
    require(maxPullBundles in 1..5_000) { "max_pull_bundles exceeds protocol range" }
    require(maxSnapshotRowsPerChunk in 1..100_000) { "max_snapshot_rows_per_chunk exceeds protocol range" }
    require(maxSnapshotRows > 0L) { "max_snapshot_rows must be positive" }
    require(maxPushDecodedBytes > 0L && maxPullDecodedBytes > 0L &&
        maxSnapshotDecodedBytesPerChunk > 0L && maxSnapshotLogicalBytes > 0L &&
        maxRowDecodedBytes > 0L) { "decoded and logical byte limits must be positive" }
    require(maxRowDecodedBytes < maxPushDecodedBytes) {
        "max_push_decoded_bytes must include a complete one-row envelope"
    }
    require(maxRowDecodedBytes < maxPullDecodedBytes) {
        "max_pull_decoded_bytes must include a complete one-row envelope"
    }
    require(maxRowDecodedBytes < maxSnapshotDecodedBytesPerChunk) {
        "max_snapshot_decoded_bytes_per_chunk must include a complete one-row envelope"
    }
}
