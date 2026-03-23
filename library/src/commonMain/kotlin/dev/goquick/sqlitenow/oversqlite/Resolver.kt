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
 * On push conflict, the server returns structured conflict data for the first conflicting row:
 * schema/table/key/op, the client base version, the authoritative server row version, an explicit
 * deleted flag, and the current server row payload when one exists.
 *
 * Implementations can either:
 *  - return [MergeResult.AcceptServer] to accept server state and drop local pending change
 *  - return [MergeResult.KeepLocal] to retry the original local intent where that is valid
 *  - return [MergeResult.KeepMerged] to retry an explicit merged row payload where that is valid
 *
 * Current runtime behavior:
 *  - [ServerWinsResolver] is handled automatically from the structured conflict payload.
 *  - [ClientWinsResolver] is a convenience resolver that always returns [MergeResult.KeepLocal].
 *  - [MergeResult.KeepLocal] and [MergeResult.KeepMerged] are applied automatically only when the
 *    conflict shape makes that result valid for the local operation.
 *  - Invalid automatic outcomes surface [InvalidConflictResolutionException].
 *  - Repeated auto-resolution conflicts are bounded; exhausting the retry budget surfaces
 *    [PushConflictRetryExhaustedException].
 */
fun interface Resolver {
    fun resolve(conflict: ConflictContext): MergeResult
}

/** Outcome of conflict resolution. */
sealed class MergeResult {
    /** Accept server’s version and drop local pending change. */
    data object AcceptServer : MergeResult()

    /** Keep the original local intent where the runtime deems that operation valid. */
    data object KeepLocal : MergeResult()

    /** Retry an explicit merged row payload where the runtime deems that operation valid. */
    data class KeepMerged(val mergedPayload: JsonElement) : MergeResult()
}

/**
 * Structured conflict context passed to resolvers.
 *
 * This is the canonical resolver input for bundle-era oversqlite. It exposes enough information for
 * operation-aware conflict decisions without forcing resolvers to infer semantics from transport
 * errors or human-readable messages.
 */
data class ConflictContext(
    val schema: String,
    val table: String,
    val key: SyncKey,
    val localOp: String,
    val localPayload: JsonElement?,
    val baseRowVersion: Long,
    val serverRowVersion: Long,
    val serverRowDeleted: Boolean,
    val serverRow: JsonElement?,
)

/** Default resolver: server wins. */
object ServerWinsResolver : Resolver {
    override fun resolve(conflict: ConflictContext): MergeResult = MergeResult.AcceptServer
}

/**
 * Convenience resolver for client-wins semantics.
 *
 * This only declares intent. Whether [MergeResult.KeepLocal] is a valid automatic outcome for the
 * specific conflict shape is determined by runtime conflict-resolution rules.
 */
object ClientWinsResolver : Resolver {
    override fun resolve(conflict: ConflictContext): MergeResult = MergeResult.KeepLocal
}
