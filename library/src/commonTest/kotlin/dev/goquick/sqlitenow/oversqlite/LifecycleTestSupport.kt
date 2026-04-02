package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import kotlin.random.Random

suspend fun OversqliteClient.openAndConnect(
    userId: String): Result<AttachResult> {
    open().getOrThrow()
    return attach(userId)
}

internal fun randomTestSourceId(prefix: String = "test-source"): String {
    return "$prefix-${Random.nextInt().toString().removePrefix("-")}"
}

suspend fun markSourceRecoveryRequired(
    db: SafeSQLiteConnection,
    reason: SourceRecoveryReason = SourceRecoveryReason.SOURCE_SEQUENCE_CHANGED,
    sourceBundleId: Long = 0,
    intentState: String = sourceRecoveryIntentStateNone,
) {
    val sourceId = db.prepare(
        "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1",
    ).use { st ->
        check(st.step())
        st.getText(0)
    }
    OversqliteOperationStateStore(db).persistState(
        OversqliteOperationState(
            kind = operationKindSourceRecovery,
            sourceRecoveryReason = reason.toPersistedOperationReason(),
            sourceRecoverySourceId = sourceId,
            sourceRecoverySourceBundleId = sourceBundleId,
            sourceRecoveryIntentState = intentState,
        ),
    )
}
