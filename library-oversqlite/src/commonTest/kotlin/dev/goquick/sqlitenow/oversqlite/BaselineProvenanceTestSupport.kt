package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection

/** Corruption/history setup for the current immutable-context format; test builds only. */
internal suspend fun replaceHistoricalContextForTest(
    db: SafeSQLiteConnection,
    owner: String,
    overrides: Map<String, Any>,
): Long {
    require(owner in setOf("_sync_dirty_rows", "_sync_outbox_rows"))
    val columns = listOf("database_id", "service_id", "account_binding", "account_id", "generation_id",
        "generation_fingerprint", "admission", "local_version", "physical_shape", "origin_source_id")
    require(overrides.isNotEmpty() && overrides.keys.all { it in columns })
    val original = db.scalarLong("SELECT base_context_id FROM $owner LIMIT 1")
    val parameters = columns.filter { it in overrides }
    val sql = "INSERT INTO _sync_provenance_contexts(${columns.joinToString()}) SELECT " +
        columns.joinToString { if (it in overrides) "?" else "c.$it" } +
        " FROM _sync_provenance_contexts c WHERE c.context_id=?"
    db.withPreparedStatement(sql) { statement ->
        parameters.forEachIndexed { index, name -> when (val value = overrides.getValue(name)) {
            is String -> statement.bindText(index + 1, value)
            is Long -> statement.bindLong(index + 1, value)
            else -> error("unsupported context fixture value")
        } }
        statement.bindLong(parameters.size + 1, original); statement.step()
    }
    val replacement = db.scalarLong("SELECT last_insert_rowid()")
    check(replacement > 0 && replacement != original)
    db.withPreparedStatement("UPDATE $owner SET base_provenance=json_set(base_provenance,'$.context_id',?) WHERE base_context_id=?") {
        it.bindLong(1, replacement); it.bindLong(2, original); it.step()
    }
    return replacement
}
