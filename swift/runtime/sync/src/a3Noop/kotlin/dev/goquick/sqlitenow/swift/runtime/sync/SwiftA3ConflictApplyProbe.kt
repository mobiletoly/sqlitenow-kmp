package dev.goquick.sqlitenow.swift.runtime.sync

import dev.goquick.sqlitenow.oversqlite.OversqliteClient

internal object SwiftA3ConflictApplyProbe {
    suspend fun <T> aroundResolution(
        requestHash: String,
        client: OversqliteClient,
        operation: suspend () -> T,
    ): T = operation()
}
