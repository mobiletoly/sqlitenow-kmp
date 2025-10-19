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

import kotlinx.serialization.json.JsonElement

/**
 * Resolver defines how to resolve optimistic concurrency conflicts reported by the server.
 *
 * On conflict, the server returns the current server row (including server_version and payload).
 * Implementations can either:
 *  - return MergeResult.AcceptServer to accept server state and drop local pending change, or
 *  - return MergeResult.KeepLocal(mergedPayload) to keep local intent by rewriting the row
 *    and re‑enqueuing an UPDATE with base_version set to the server’s version.
 */
fun interface Resolver {
    fun merge(
        table: String,
        pk: String,
        serverRow: JsonElement?,
        localPayload: JsonElement?
    ): MergeResult
}

/** Outcome of conflict resolution. */
sealed class MergeResult {
    /** Accept server’s version and drop local pending change. */
    data object AcceptServer : MergeResult()

    /** Keep local by writing merged payload locally and retrying as UPDATE. */
    data class KeepLocal(val mergedPayload: JsonElement) : MergeResult()
}

/** Default resolver: server wins. */
object ServerWinsResolver : Resolver {
    override fun merge(
        table: String,
        pk: String,
        serverRow: JsonElement?,
        localPayload: JsonElement?
    ): MergeResult = MergeResult.AcceptServer
}

