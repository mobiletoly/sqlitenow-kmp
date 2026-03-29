package dev.goquick.sqlitenow.oversqlite

import kotlin.random.Random

internal const val DefaultTestSourceId = "test-source"

suspend fun OversqliteClient.open(): Result<OpenState> = open(DefaultTestSourceId)

suspend fun OversqliteClient.openAndConnect(
    userId: String,
    sourceId: String = randomTestSourceId(),
): Result<AttachResult> {
    open(sourceId).getOrThrow()
    return attach(userId)
}

suspend fun OversqliteClient.rotateSource(): Result<SourceRotationResult> {
    return rotateSource(randomTestSourceId("rotated-source"))
}

suspend fun OversqliteClient.rebuild(mode: RebuildMode): Result<RemoteSyncReport> {
    return when (mode) {
        RebuildMode.KEEP_SOURCE -> rebuild(mode = mode, newSourceId = null)
        RebuildMode.ROTATE_SOURCE -> rebuild(
            mode = mode,
            newSourceId = randomTestSourceId("rebuild-source"),
        )
    }
}

internal fun randomTestSourceId(prefix: String = "test-source"): String {
    return "$prefix-${Random.nextInt().toString().removePrefix("-")}"
}
