/*
 * Copyright 2025 Anatoliy Pochkin
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

data class SyncRun(
    val upload: UploadSummary,
    val downloaded: Int,
)

/**
 * Convenience one-shot sync helper that performs an upload, then downloads
 * changes in pages until a non-full page is applied.
 *
 * When to prefer this
 * - Highly bi-directional, chatty flows (e.g., multiple devices frequently
 *   editing the same data). In these cases, pairing an upload with a bounded
 *   download helps converge local state with minimal ceremony.
 *
 * When to stay explicit
 * - Mostly upload-centric scenarios (one primary writer). Calling [uploadOnce]
 *   on its own reduces unnecessary round-trips. You can then schedule periodic
 *   or event-driven [downloadOnce] separately (e.g., on app resume).
 *
 * This helper preserves the recommended order (upload → download). It relies on
 * the client’s internal post-upload lookback to avoid resurrecting deletes.
 */
suspend fun OversqliteClient.syncOnce(
    limit: Int = 1000,
    includeSelf: Boolean = false
): Result<SyncRun> {
    // 1) Upload pending changes
    val up = uploadOnce()
    if (up.isFailure) return Result.failure(up.exceptionOrNull()!!)

    // 2) Download until applied < limit
    var total = 0
    var more = true
    while (more) {
        val dn = downloadOnce(limit = limit, includeSelf = includeSelf)
        if (dn.isFailure) return Result.failure(dn.exceptionOrNull()!!)
        val (applied, _) = dn.getOrNull() ?: (0 to 0L)
        total += applied
        more = applied == limit && limit > 0
        if (applied == 0) break
    }

    return Result.success(SyncRun(upload = up.getOrThrow(), downloaded = total))
}
