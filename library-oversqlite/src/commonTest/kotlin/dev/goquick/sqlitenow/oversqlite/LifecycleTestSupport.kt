package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

suspend fun OversqliteClient.openAndConnect(
    userId: String): Result<AttachResult> {
    open().getOrThrow()
    return attach(userId)
}

@OptIn(ExperimentalUuidApi::class)
internal fun randomTestSourceId(prefix: String = "test-source"): String {
    return Uuid.random().toString()
}

suspend fun markSourceRecoveryRequired(
    db: SafeSQLiteConnection,
    reason: SourceRecoveryReason = SourceRecoveryReason.SOURCE_SEQUENCE_CHANGED,
    replacementSourceId: String = randomTestSourceId("reserved-rotated"),
) {
    OversqliteSourceStateStore(db).ensureSource(replacementSourceId)
    OversqliteAttachmentStateStore(db).setRebuildRequired(true)
    OversqliteOperationStateStore(db).persistState(
        OversqliteOperationState(
            kind = operationKindSourceRecovery,
            reason = reason.toPersistedOperationReason(),
            replacementSourceId = replacementSourceId,
        ),
    )
}
