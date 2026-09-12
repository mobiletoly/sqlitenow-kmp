/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection

internal val oversqliteRetainedTables = oversqliteRetainedTableDdl.keys

internal val oversqliteManagedIndexes = baselineIndexDdl.keys + pushIndexDdl.keys + setOf("_sync_apply_rows_pending_idx", "_sync_apply_rows_role_ordinal_idx")
internal val oversqliteStaticTriggers = setOf(
    "_sync_conflict_revision_insert", "_sync_conflict_revision_update", "_sync_conflict_revision_delete",
 "_sync_baseline_revision_insert", "_sync_baseline_revision_update", "_sync_baseline_revision_delete",
) + snapshotStageAccountingDdl.keys

private enum class RetainedRuntimeCensus { FRESH, CURRENT }

internal suspend fun rejectPartialRetainedRuntimeBeforeConfiguration(db: SafeSQLiteConnection) {
    inspectRetainedRuntime(db)
}

internal suspend fun inspectRetainedRuntime(db: SafeSQLiteConnection): String {
    val objects = db.queryList(
        "SELECT type, lower(type), name, lower(name), tbl_name, lower(tbl_name), rootpage, " +
            "COALESCE(sql, '') FROM sqlite_schema ORDER BY type, name",
    ) {
        RetainedObject(
            rawType = it.getText(0), type = it.getText(1), rawName = it.getText(2),
            name = it.getText(3), rawTableName = it.getText(4), tableName = it.getText(5),
            rootPage = it.getLong(6), sql = it.getText(7),
        )
    }
    requireExactReservedObjects(objects)
    val identityExists = objects.any { it.type == "table" && it.name == "_sync_contract_identity" }
    val openingExists = objects.any { it.type == "table" && it.name == oversqliteOpeningTable }
    val firstOwned = objects.firstOrNull { objectRow ->
        objectRow.name in oversqliteRetainedTables || objectRow.name == oversqliteOpeningTable ||
            (objectRow.type == "index" && objectRow.name in oversqliteManagedIndexes) ||
            (objectRow.type == "trigger" && triggerReferencesSyncTable(objectRow.sql))
    }
    if (!identityExists) {
        if (openingExists && firstOwned?.type == "table" && firstOwned.name == oversqliteOpeningTable) {
            val otherOwned = objects.any { objectRow ->
                objectRow.name in oversqliteRetainedTables ||
                    (objectRow.type == "index" && objectRow.name in oversqliteManagedIndexes) ||
                    (objectRow.type == "trigger" && triggerReferencesSyncTable(objectRow.sql))
            }
            require(!otherOwned) {
                "incompatible partial oversqlite runtime; recreate this development database explicitly"
            }
            inspectOversqliteOpeningExtension(db, tableExists = true)
            return RetainedRuntimeCensus.FRESH.name
        }
        require(firstOwned == null) {
            "incompatible partial oversqlite runtime; recreate this development database explicitly"
        }
        return RetainedRuntimeCensus.FRESH.name
    }
    val tables = objects.filter { it.type == "table" }.map { it.name }.toSet()
    val indexes = objects.filter { it.type == "index" }.map { it.name }.toSet()
    val triggers = objects.filter { it.type == "trigger" }.map { it.name }.toSet()
    require(tables.containsAll(oversqliteRetainedTables) &&
        indexes.containsAll(oversqliteManagedIndexes) && triggers.containsAll(oversqliteStaticTriggers)
    ) { "incompatible partial oversqlite runtime; recreate this development database explicitly" }
    for (table in oversqliteRetainedTables) {
        val actual = objects.single { it.name == table }.sql
        require(retainedSqlSignature(actual) == retainedSqlSignature(oversqliteRetainedTableDdl.getValue(table))) {
            "malformed retained table $table; recreate this development database explicitly"
        }
    }
    require(retainedSqlSignature(objects.single { it.name == "_sync_apply_rows_pending_idx" }.sql) ==
        retainedSqlSignature(applyPendingIndexDdl)) { "malformed retained apply frontier index" }
    for (table in listOf("_sync_contract_identity", "_sync_apply_state", "_sync_conflict_state",
        "_sync_attachment_state", "_sync_operation_state", "_sync_outbox_bundle")) {
        val keys = db.queryList("SELECT singleton_key FROM $table") { it.getLong(0) }
        require(keys == listOf(1L)) { "malformed or missing retained singleton row in $table" }
    }
    require(db.scalarLong("SELECT count(*) FROM _sync_apply_state WHERE typeof(apply_mode)='integer' AND apply_mode IN (0,1)") == 1L) {
        "malformed retained apply state"
    }
    require(objects.none { it.name=="idx_sync_dirty_rows_dirty_ordinal" }) { "superseded dirty index must be absent" }
    for ((name, ddl) in baselineIndexDdl + pushIndexDdl + mapOf("_sync_apply_rows_role_ordinal_idx" to applyRoleIndexDdl)) {
        require(retainedSqlSignature(objects.single { it.name==name }.sql)==retainedSqlSignature(ddl)) { "malformed retained index $name" }
    }
    for ((name,ddl) in snapshotStageAccountingDdl) {
        require(retainedSqlSignature(objects.single { it.name==name }.sql)==retainedSqlSignature(ddl)) { "malformed retained stage trigger $name" }
    }
    for (operation in listOf("insert","update","delete")) {
        for ((name,ddl) in listOf("_sync_conflict_revision_$operation" to expectedConflictRevisionTriggerSql(operation), "_sync_baseline_revision_$operation" to expectedBaselineRevisionTriggerSql(operation))) {
            require(retainedSqlSignature(objects.single { it.name==name }.sql)==retainedSqlSignature(ddl)) { "malformed retained trigger $name" }
        }
    }
    validateRetainedCaptureTriggers(db)
    inspectOversqliteOpeningExtension(db, openingExists)
    return RetainedRuntimeCensus.CURRENT.name
}

private data class RetainedObject(
    val rawType: String,
    val type: String,
    val rawName: String,
    val name: String,
    val rawTableName: String,
    val tableName: String,
    val rootPage: Long,
    val sql: String,
)

private fun requireExactReservedObjects(objects: List<RetainedObject>) {
    val expectedTypes = buildMap {
        oversqliteRetainedTables.forEach { put(it, "table") }
        put(oversqliteOpeningTable, "table")
        oversqliteManagedIndexes.forEach { put(it, "index") }
        oversqliteStaticTriggers.forEach { put(it, "trigger") }
    }
    for ((name, expectedType) in expectedTypes) {
        val matches = objects.filter { it.name == name }
        require(matches.size <= 1) {
            "reserved oversqlite object $name has multiple schema entries; recreate this development database explicitly"
        }
        val objectRow = matches.singleOrNull() ?: continue
        val expectedOwner = when (expectedType) {
            "table" -> name
            else -> when {
 name in setOf("_sync_apply_rows_pending_idx","_sync_apply_rows_role_ordinal_idx") -> "_sync_apply_rows"
 name in snapshotStageAccountingDdl -> "_sync_snapshot_stage"
 name.startsWith("_sync_push_nodes_") -> "_sync_push_nodes"
 name.startsWith("_sync_push_edges_") -> "_sync_push_edges"
 name=="_sync_relationships_parent_idx" -> "_sync_relationships"
 name=="_sync_row_state_unknown_idx" || name.startsWith("_sync_baseline_revision_") -> "_sync_row_state"
 else -> "_sync_dirty_rows"
 }
        }
        require(
            objectRow.rawName == name && objectRow.rawType == expectedType &&
                objectRow.rawTableName == expectedOwner && objectRow.tableName == expectedOwner &&
                objectRow.sql.isNotBlank() &&
                (if (expectedType == "trigger") objectRow.rootPage == 0L else objectRow.rootPage > 0L),
        ) {
            "reserved oversqlite object $name has incompatible type, case, owner, or definition; " +
                "recreate this development database explicitly"
        }
    }
}

private data class RetainedIndexColumn(
    val sequence: Long,
    val columnId: Long,
    val name: String?,
    val descending: Boolean,
    val collation: String?,
    val key: Boolean,
)

// This is a bounded DDL signature for SQLiteNow-owned static triggers, not a SQL parser.
// It preserves every literal, operator and punctuation token while normalizing identifier case
// and quoting plus the optional CREATE TRIGGER IF NOT EXISTS spelling.
internal fun retainedSqlSignature(value: String): List<String> {
    val tokens = mutableListOf<String>()
    var i = 0
    while (i < value.length) {
        val ch = value[i]
        when {
            ch.isWhitespace() -> i++
            value.startsWith("--", i) -> { while (i < value.length && value[i] != '\n') i++ }
            value.startsWith("/*", i) -> {
                val end = value.indexOf("*/", i + 2)
                i = if (end < 0) value.length else end + 2
            }
            ch == '\'' -> {
                val literal = StringBuilder("'")
                i++
                while (i < value.length) {
                    literal.append(value[i])
                    if (value[i++] == '\'') {
                        if (i < value.length && value[i] == '\'') {
                            literal.append(value[i++])
                        } else break
                    }
                }
                tokens += "string:${literal}"
            }
            ch == '"' || ch == '`' || ch == '[' -> {
                val end = if (ch == '[') ']' else ch
                val identifier = StringBuilder()
                i++
                while (i < value.length) {
                    if (value[i] == end) {
                        i++
                        if (ch != '[' && i < value.length && value[i] == end) {
                            identifier.append(end)
                            i++
                            continue
                        }
                        break
                    }
                    identifier.append(value[i++])
                }
                tokens += identifier.toString().lowercase()
            }
            ch.isLetterOrDigit() || ch == '_' || ch == '$' -> {
                val start = i++
                while (i < value.length && (value[i].isLetterOrDigit() || value[i] == '_' || value[i] == '$')) i++
                tokens += value.substring(start, i).lowercase()
            }
            else -> {
                val operator = listOf("->>", "||", "==", "!=", "<>", "<=", ">=", "<<", ">>", "->")
                    .firstOrNull { value.startsWith(it, i) }
                if (operator != null) {
                    tokens += operator
                    i += operator.length
                } else {
                    tokens += ch.toString()
                    i++
                }
            }
        }
    }
    val trigger = if (tokens.firstOrNull() == "create" && tokens.getOrNull(1) in listOf("trigger", "table", "index")) 1 else -1
    if (trigger >= 0 && tokens.getOrNull(trigger + 1) == "if" &&
        tokens.getOrNull(trigger + 2) == "not" && tokens.getOrNull(trigger + 3) == "exists"
    ) {
        repeat(3) { tokens.removeAt(trigger + 1) }
    }
    if (tokens.lastOrNull() == ";") tokens.removeAt(tokens.lastIndex)
    return tokens
}

internal suspend fun firstRetainedSyncObject(db: SafeSQLiteConnection): String? =
    db.queryList("SELECT type, name, COALESCE(sql, '') FROM sqlite_schema ORDER BY type, name") { row ->
        Triple(row.getText(0), row.getText(1), row.getText(2))
    }.firstOrNull { (type, name, sql) ->
        name.lowercase() in oversqliteRetainedTables || name.lowercase() == oversqliteOpeningTable ||
            (type.equals("index", ignoreCase = true) &&
                name.lowercase() in oversqliteManagedIndexes) ||
            (type == "trigger" && triggerReferencesSyncTable(sql))
    }?.let { (type, name, _) -> "$type $name" }

// This lexer only recognizes table references for the missing-identity guard. SQLite
// metadata remains the source of schema/affinity information; no wire contract is parsed here.
private data class SyncSqlToken(val text: String, val quoted: Boolean = false)

private fun syncSqlTokens(sql: String): List<SyncSqlToken> {
    val tokens = mutableListOf<SyncSqlToken>()
    var i = 0
    while (i < sql.length) {
        val ch = sql[i]
        when {
            ch.isWhitespace() -> i++
            sql.startsWith("--", i) -> { while (i < sql.length && sql[i] != '\n') i++ }
            sql.startsWith("/*", i) -> {
                val end = sql.indexOf("*/", i + 2)
                i = if (end < 0) sql.length else end + 2
            }
            ch == '\'' || ch == '"' || ch == '`' || ch == '[' -> {
                val end = if (ch == '[') ']' else ch
                val text = StringBuilder()
                i++
                while (i < sql.length) {
                    if (sql[i] == end) {
                        i++
                        if (ch != '[' && i < sql.length && sql[i] == end) { text.append(end); i++; continue }
                        break
                    }
                    text.append(sql[i++])
                }
                // SQLite also permits single-quoted names in table-reference positions.
                tokens += SyncSqlToken(text.toString().lowercase(), quoted = true)
            }
            ch.isLetterOrDigit() || ch == '_' || ch == '$' -> {
                val start = i++
                while (i < sql.length && (sql[i].isLetterOrDigit() || sql[i] == '_' || sql[i] == '$')) i++
                tokens += SyncSqlToken(sql.substring(start, i).lowercase())
            }
            else -> { tokens += SyncSqlToken(ch.toString()); i++ }
        }
    }
    return tokens
}

internal fun triggerReferencesSyncTable(sql: String): Boolean {
    val tokens = syncSqlTokens(sql)
    var depth = 0
    var inTriggerHeader = true
    var triggerTargetSeen = false
    val fromDepths = mutableSetOf<Int>()
    for ((index, token) in tokens.withIndex()) {
        if (token.quoted) continue
        when (token.text) {
            "(" -> depth++
            ")" -> { fromDepths.remove(depth); depth-- }
            ";" -> fromDepths.clear()
            "begin" -> { inTriggerHeader = false; fromDepths.clear() }
            "where", "group", "order", "limit", "having", "union", "except", "intersect", "returning", "window", "end" -> fromDepths.remove(depth)
        }
        if (token.text == "from") fromDepths += depth
        val triggerTarget = token.text == "on" && inTriggerHeader && !triggerTargetSeen
        if (triggerTarget) triggerTargetSeen = true
        val startsReference = triggerTarget || token.text in setOf("from", "join", "into", "update", "in") ||
            (token.text == "," && depth in fromDepths)
        if (!startsReference) continue
        var next = index + 1
        if (token.text == "update" && tokens.getOrNull(next)?.text == "or") next += 2
        if (tokens.getOrNull(next + 1)?.text == ".") next += 2
        if (tokens.getOrNull(next)?.text in oversqliteRetainedTables) return true
    }
    return false
}

internal suspend fun checkRetainedApplyWorkspace(db: SafeSQLiteConnection, config: OversqliteConfig, abandonedReservations: Boolean = false) {
    val workspace=OversqliteWorkspace(db, config, abandonedReservations)
    workspace.reconcileRetainedOwners()
    workspace.check(applyUnit("retained",0,""))
}
