package dev.goquick.sqlitenow.oversqlite.e2e

import dev.goquick.sqlitenow.oversqlite.AttachResult
import dev.goquick.sqlitenow.oversqlite.OversqliteClient

suspend fun OversqliteClient.openAndAttach(
    userId: String,
    sourceId: String,
): Result<AttachResult> {
    open(sourceId).getOrThrow()
    return attach(userId)
}
