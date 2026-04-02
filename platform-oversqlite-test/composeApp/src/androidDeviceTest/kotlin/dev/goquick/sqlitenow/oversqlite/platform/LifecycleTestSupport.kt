package dev.goquick.sqlitenow.oversqlite.platform

import dev.goquick.sqlitenow.oversqlite.AttachResult
import dev.goquick.sqlitenow.oversqlite.OversqliteClient

suspend fun OversqliteClient.openAndAttach(
    userId: String,
): Result<AttachResult> {
    open().getOrThrow()
    return attach(userId)
}
