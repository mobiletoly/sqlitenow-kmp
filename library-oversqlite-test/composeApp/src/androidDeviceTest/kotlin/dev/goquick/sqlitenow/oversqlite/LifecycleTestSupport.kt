package dev.goquick.sqlitenow.oversqlite

suspend fun OversqliteClient.openAndAttach(
    userId: String,
): Result<AttachResult> {
    open().getOrThrow()
    return attach(userId)
}
