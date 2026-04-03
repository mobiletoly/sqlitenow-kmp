package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection

suspend fun markSourceRecoveryRequired(
    db: SafeSQLiteConnection,
    reason: SourceRecoveryReason = SourceRecoveryReason.SOURCE_SEQUENCE_CHANGED,
    replacementSourceId: String,
) {
    db.execSQL(
        """
        INSERT INTO _sync_source_state(source_id, next_source_bundle_id, replaced_by_source_id)
        VALUES('$replacementSourceId', 1, '')
        ON CONFLICT(source_id) DO NOTHING
        """.trimIndent(),
    )
    db.execSQL("UPDATE _sync_attachment_state SET rebuild_required = 1 WHERE singleton_key = 1")
    db.execSQL(
        """
        UPDATE _sync_operation_state
        SET kind = 'source_recovery',
            reason = '${reason.persistedValue()}',
            replacement_source_id = '$replacementSourceId'
        WHERE singleton_key = 1
        """.trimIndent(),
    )
}

private fun SourceRecoveryReason.persistedValue(): String = when (this) {
    SourceRecoveryReason.HISTORY_PRUNED -> "history_pruned"
    SourceRecoveryReason.SOURCE_SEQUENCE_OUT_OF_ORDER -> "source_sequence_out_of_order"
    SourceRecoveryReason.SOURCE_SEQUENCE_CHANGED -> "source_sequence_changed"
    SourceRecoveryReason.SOURCE_RETIRED -> "source_retired"
}
