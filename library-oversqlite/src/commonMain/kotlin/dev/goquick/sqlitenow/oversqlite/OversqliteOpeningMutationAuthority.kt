/*
 * Copyright 2026 Toly Pochkin
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

internal enum class OversqliteOpeningMutationKind(val diagnosticName: String) {
    FREEZE_OUTBOUND("freeze_outbound"),
    RECORD_CONFLICT("record_conflict"),
    RETRY_CONFLICT("clear_conflict"),
    RESOLVE_CONFLICT("clear_conflict"),
    RESTORE_INVALID_RESOLUTION("clear_conflict"),
    APPLY_PUSH_RESPONSE("apply_push_response"),
}

/** Opening-private authority used only by the old-generation client during U1-U2. */
internal interface OversqliteOpeningMutationAuthority {
    suspend fun beforeMutation(kind: OversqliteOpeningMutationKind): OversqliteOpeningRecord

    suspend fun recordConflict(
        expected: OversqliteOpeningRecord,
        pending: PendingConflict,
    )

    suspend fun completeConflictDecision(expected: OversqliteOpeningRecord)
}
