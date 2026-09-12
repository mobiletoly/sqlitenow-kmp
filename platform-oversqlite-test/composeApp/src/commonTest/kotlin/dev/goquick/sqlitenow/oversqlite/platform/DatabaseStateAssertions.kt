package dev.goquick.sqlitenow.oversqlite.platform

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.oversqlite.platformsupport.sha256Hex
import dev.goquick.sqlitenow.oversqlite.webRuntimeKind
import kotlin.test.assertTrue

// Read-only opener census, including incomplete/old layouts that cannot satisfy
// databaseStateVector's current-format owner roster. It preserves storage classes,
// every schema object, every table column/index/FK and every stored row.
// SQLite's journal mode is observed; this is not a physical WAL-byte witness.
internal suspend fun retainedDatabaseVector(db: SafeSQLiteConnection): Map<String, List<String>> {
    fun identifier(value: String) = "\"${value.replace("\"", "\"\"")}\""
    fun literal(value: String) = "'${value.replace("'", "''")}'"
    fun projection(columns: List<String>) = columns.joinToString(",") { column ->
        val quoted = identifier(column)
        "${literal(column)},json_object('storage',typeof($quoted),'value',CASE WHEN typeof($quoted)='blob' THEN lower(hex($quoted)) WHEN $quoted IS NULL THEN NULL ELSE CAST($quoted AS TEXT) END)"
    }
    suspend fun rows(sql: String): List<String> = db.withExclusiveAccess {
        db.prepare(sql).use { statement ->
            buildList { while (statement.step()) add(sha256Hex(statement.getText(0).encodeToByteArray())) }
        }
    }
    val result = linkedMapOf<String, List<String>>()
    val schemaColumns = listOf("type", "name", "tbl_name", "rootpage", "sql")
    result["database/schema"] = rows("SELECT json_object(${projection(schemaColumns)}) FROM sqlite_schema ORDER BY type,name")
    for (pragma in listOf("user_version", "schema_version", "journal_mode")) {
        result["database/$pragma"] = db.withExclusiveAccess {
            db.prepare("PRAGMA $pragma").use { statement ->
                check(statement.step()) { "retained census requires $pragma" }
                listOf(statement.getText(0))
            }
        }
    }
    val tables = db.withExclusiveAccess {
        db.prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name").use { statement ->
            buildList { while (statement.step()) add(statement.getText(0)) }
        }
    }
    for (table in tables) {
        val columns = db.withExclusiveAccess {
            db.prepare("PRAGMA table_xinfo(${identifier(table)})").use { statement ->
                buildList { while (statement.step()) add(statement.getText(1)) }
            }
        }
        assertTrue(columns.isNotEmpty(), "complete retained census requires columns for $table")
        result["table/$table/columns"] = rows("SELECT json_object(${projection(listOf("cid", "name", "type", "notnull", "dflt_value", "pk", "hidden"))}) FROM pragma_table_xinfo(${literal(table)}) ORDER BY cid")
        result["table/$table/indexes"] = rows("SELECT json_object(${projection(listOf("seq", "name", "unique", "origin", "partial"))}) FROM pragma_index_list(${literal(table)}) ORDER BY seq")
        val indexes = db.withExclusiveAccess {
            db.prepare("SELECT name FROM pragma_index_list(${literal(table)}) ORDER BY seq").use { statement ->
                buildList { while (statement.step()) add(statement.getText(0)) }
            }
        }
        for (index in indexes) {
            result["table/$table/index/$index/columns"] = rows("SELECT json_object(${projection(listOf("seqno", "cid", "name", "desc", "coll", "key"))}) FROM pragma_index_xinfo(${literal(index)}) ORDER BY seqno")
        }
        result["table/$table/foreign-keys"] = rows("SELECT json_object(${projection(listOf("id", "seq", "table", "from", "to", "on_update", "on_delete", "match"))}) FROM pragma_foreign_key_list(${literal(table)}) ORDER BY id,seq")
        result["table/$table/rows"] = rows("SELECT json_object(${projection(columns)}) FROM ${identifier(table)} ORDER BY ${columns.joinToString(",", transform = ::identifier)}")
    }
    return result
}

// Preparation/verification only: hash each complete typed row separately so the
// witness never retains a second copy of every backlog payload.
internal suspend fun databaseStateVector(
    db: SafeSQLiteConnection,
    business: List<String>,
    predicates: Map<String, String> = emptyMap(),
    ignoreDerivedCharges: Boolean = false,
    includeDurableStages: Boolean = true,
): Map<String, List<String>> {
    val useBlob = webRuntimeKind() in setOf("jvm", "android", "native")
    val primary = business + listOf("_sync_row_state", "_sync_dirty_rows", "_sync_relationships",
        "_sync_outbox_rows", "_sync_outbox_bundle", "_sync_source_state",
        "_sync_attachment_state", "_sync_operation_state", "_sync_conflict_state",
        "_sync_opening_state", "_sync_contract_identity", "_sync_admitted_contracts")
    val durableStages = listOf("_sync_snapshot_stage", "_sync_apply_state", "_sync_apply_plan", "_sync_apply_rows",
        "_sync_apply_edges", "_sync_push_plan", "_sync_push_nodes", "_sync_push_edges",
        "_sync_managed_tables")
    val tables = primary + if (includeDurableStages) durableStages else emptyList()
    return tables.associateWith { table ->
        val columns = db.withExclusiveAccess { db.prepare("PRAGMA table_info(\"$table\")").use { statement ->
            buildList { while (statement.step()) add(statement.getText(1)) }
        }}
        assertTrue(columns.isNotEmpty(), "complete vector requires $table")
        val derived = setOf("workspace_context", "reserved_entries", "reserved_bytes", "snapshot_stage_entries",
            "snapshot_stage_bytes", "snapshot_cleanup_ordinal", "push_stage_entries", "push_stage_bytes", "push_cleanup_ordinal")
        val compared = if (ignoreDerivedCharges && table == "_sync_operation_state") columns.filter { it !in derived } else columns
        val projection = compared.joinToString(",") { column ->
            "'$column',json_object('storage',typeof(\"$column\"),'value',CASE WHEN typeof(\"$column\")='blob' THEN lower(hex(\"$column\")) WHEN \"$column\" IS NULL THEN NULL ELSE CAST(\"$column\" AS TEXT) END)"
        }
        val where = predicates[table]?.let { " WHERE $it" }.orEmpty()
        val rowProjection = if (useBlob) "CAST(json_object($projection) AS BLOB)" else "json_object($projection)"
        db.withExclusiveAccess { db.prepare("SELECT $rowProjection FROM \"$table\"$where ORDER BY ${columns.joinToString(",") { "\"$it\"" }}").use { statement ->
            buildList { while (statement.step()) add(sha256Hex(if (useBlob) statement.getBlob(0) else statement.getText(0).encodeToByteArray())) }
        }}
    }
}
