package dev.goquick.sqlitenow.oversqlite

suspend fun OversqliteClient.openAndAttach(
    userId: String,
    sourceId: String,
): Result<AttachResult> {
    open(sourceId).getOrThrow()
    return attach(userId)
}

@Deprecated("Use openAndAttach(userId, sourceId)")
suspend fun OversqliteClient.openAndConnect(
    userId: String,
    sourceId: String,
): Result<AttachResult> = openAndAttach(userId, sourceId)

@Deprecated("Use rebuild(RebuildMode.KEEP_SOURCE)")
suspend fun OversqliteClient.hydrate(): Result<RemoteSyncReport> = rebuild(RebuildMode.KEEP_SOURCE)
