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

private const val preservationChunkBytes = 64 * 1024
private const val preservationMultisetTable = "_sqlitenow_preservation_multiset"
private const val preservationGuardPrefix = "_sqlitenow_preserve_"
private const val preservationChainDigestDomain = "sqlitenow-preservation-chain-v1"
private const val preservationRowDigestDomain = "sqlitenow-preservation-row-v1"

internal fun preservedLocalTableGuardNames(tables: Set<String>): Set<String> = buildSet {
    for (table in tables) {
        val stem = preservationGuardPrefix + sha256Hex(table.encodeToByteArray()).take(16)
        add("${stem}_i")
        add("${stem}_u")
        add("${stem}_d")
    }
}

internal suspend fun installPreservedLocalTableGuards(
    db: SafeSQLiteConnection,
    tables: Set<String>,
) {
    for (table in tables.sorted()) {
        val stem = preservationGuardPrefix + sha256Hex(table.encodeToByteArray()).take(16)
        for ((suffix, operation) in listOf("i" to "INSERT", "u" to "UPDATE", "d" to "DELETE")) {
            val name = "${stem}_$suffix"
            val sql = preservedLocalTableGuardSql(name, table, operation)
            val existing = db.queryList(
                "SELECT type,lower(tbl_name),sql FROM sqlite_schema WHERE lower(name)=?",
                bind = { it.bindText(1, name) },
            ) { row -> Triple(row.getText(0), row.getText(1), row.getText(2)) }
            if (existing.isNotEmpty()) {
                require(existing.single() == Triple("trigger", table, sql)) {
                    "preservation guard name $name is owned by an application object or was modified"
                }
                continue
            }
            db.execSQL(sql)
        }
    }
}

internal suspend fun requirePreservedLocalTableGuards(db: SafeSQLiteConnection, tables: Set<String>) {
    for (table in tables.sorted()) {
        val stem = preservationGuardPrefix + sha256Hex(table.encodeToByteArray()).take(16)
        for ((suffix, operation) in listOf("i" to "INSERT", "u" to "UPDATE", "d" to "DELETE")) {
            val name = "${stem}_$suffix"
            val expected = preservedLocalTableGuardSql(name, table, operation)
            val observed = db.queryList(
                "SELECT type,lower(tbl_name),sql FROM sqlite_schema WHERE lower(name)=?",
                bind = { it.bindText(1, name) },
            ) { row -> Triple(row.getText(0), row.getText(1), row.getText(2)) }
            require(observed.singleOrNull() == Triple("trigger", table, expected)) {
                "preservation guard $name is missing or modified"
            }
        }
    }
}

private fun preservedLocalTableGuardSql(name: String, table: String, operation: String): String =
    "CREATE TRIGGER ${quoteIdent(name)} BEFORE $operation ON ${quoteIdent(table)} " +
        "BEGIN SELECT RAISE(ABORT,'SQLITENOW_PRESERVATION_REQUIRED'); END"

internal suspend fun dropPreservedLocalTableGuards(db: SafeSQLiteConnection, tables: Set<String>) {
    for (table in tables.sorted()) {
        val stem = preservationGuardPrefix + sha256Hex(table.encodeToByteArray()).take(16)
        for (suffix in listOf("i", "u", "d")) {
            db.execSQL("DROP TRIGGER IF EXISTS ${quoteIdent("${stem}_$suffix")}")
        }
    }
}

internal suspend fun requireExactPreservedTableDeclaration(
    db: SafeSQLiteConnection,
    declared: Set<String>,
    oldManaged: Set<String>,
    targetManaged: Set<String>,
) {
    require(declared.intersect(oldManaged + targetManaged).isEmpty()) {
        "preserved local tables must be disjoint from both managed generations"
    }
    val applicationTables = db.queryList(
        """
        SELECT lower(name) FROM sqlite_schema
        WHERE type='table' AND lower(name) NOT LIKE 'sqlite_%'
        ORDER BY lower(name)
        """.trimIndent(),
    ) { row -> row.getText(0) }.toSet() -
        oversqliteRetainedTables - oversqliteOpeningTable - oldManaged - targetManaged
    require(applicationTables == declared) {
        "every application-owned table outside both managed generations must be declared; " +
            "expected=${applicationTables.sorted()} declared=${declared.sorted()}"
    }
}

internal suspend fun preservedStateDigest(
    db: SafeSQLiteConnection,
    tables: Set<String>,
): String {
    db.execSQL(
        "CREATE TEMP TABLE IF NOT EXISTS $preservationMultisetTable(" +
            "digest TEXT PRIMARY KEY, multiplicity INTEGER NOT NULL)",
    )
    db.execSQL("DELETE FROM $preservationMultisetTable")
    var digest = sha256Hex(preservationChainDigestDomain.encodeToByteArray())
    var totalRows = 0L
    try {
        for (table in tables.sorted()) {
            require(table.matches(Regex("[a-z0-9_]{1,63}"))) { "invalid preserved table identifier" }
            val objects = loadPreservedObjects(db, table, preservedLocalTableGuardNames(tables))
            objects.singleOrNull {
                it.type == "table" && it.name.lowercase() == table
            }?.sql ?: error("declared preserved table $table is missing")
            digest = preservationFold(digest, "table", table.encodeToByteArray())
            for (item in objects) {
                digest = preservationFold(digest, "object-type", item.type.encodeToByteArray())
                digest = preservationFold(digest, "object-name", item.name.encodeToByteArray())
                digest = preservationFold(digest, "object-sql", item.sql.encodeToByteArray())
            }

            val columns = loadPreservedColumns(db, table)
            require(columns.isNotEmpty()) { "preserved table $table has no visible shape" }
            for (column in columns) {
                for (value in column.frameValues()) {
                    digest = preservationFold(digest, "column", value.encodeToByteArray())
                }
            }

            val accessibleRowId = accessibleRowId(db, table, columns)
            digest = preservationFold(
                digest,
                "accessible-rowid",
                accessibleRowId.orEmpty().encodeToByteArray(),
            )
            digest = preservationFold(digest, "sqlite-sequence", sequenceFrame(db, table))

            val tempTable = "_sqlitenow_preservation_${sha256Hex(table.encodeToByteArray()).take(16)}"
            try {
                val stagedSourceRowId = stagePreservedRows(db, table, tempTable, columns, accessibleRowId)
                db.execSQL("DELETE FROM $preservationMultisetTable")
                var tableRows = 0L
                db.withPreparedStatement("SELECT rowid FROM $tempTable") { rows ->
                    while (rows.step()) {
                        val rowDigest = digestStagedRow(
                            db = db,
                            tempTable = tempTable,
                            tempRowId = rows.getLong(0),
                            columns = columns,
                            sourceRowIdColumn = stagedSourceRowId,
                        )
                        db.withPreparedStatement(
                            "INSERT INTO $preservationMultisetTable(digest,multiplicity) VALUES(?,1) " +
                                "ON CONFLICT(digest) DO UPDATE SET multiplicity=multiplicity+1",
                        ) { insert ->
                            insert.bindText(1, rowDigest)
                            insert.step()
                        }
                        tableRows++
                        totalRows++
                    }
                }
                digest = preservationFold(digest, "table-row-count", signedLongBytes(tableRows))
                db.withPreparedStatement(
                    "SELECT digest,multiplicity FROM $preservationMultisetTable ORDER BY digest COLLATE BINARY",
                ) { rows ->
                    while (rows.step()) {
                        digest = preservationFold(digest, "row-digest", rows.getText(0).encodeToByteArray())
                        digest = preservationFold(digest, "multiplicity", signedLongBytes(rows.getLong(1)))
                    }
                }
            } finally {
                db.execSQL("DROP TABLE IF EXISTS $tempTable")
            }
        }
    } finally {
        db.execSQL("DROP TABLE IF EXISTS $preservationMultisetTable")
    }
    return "$totalRows:$digest"
}

internal suspend fun localOnlyStateDigest(
    db: SafeSQLiteConnection,
    managedTables: Set<String>,
): String {
    val retainedWithoutMutableIdentity = oversqliteRetainedTables - "_sync_contract_identity"
    val tableDigest = preservedStateDigest(db, managedTables + retainedWithoutMutableIdentity)
    val identity = db.queryList(
        """
        SELECT database_id,service_id,generation_id,fingerprint
        FROM _sync_contract_identity WHERE singleton_key=1
        """.trimIndent(),
    ) { row ->
        listOf(row.getText(0), row.getText(1), row.getLong(2).toString(), row.getText(3))
    }
    require(identity.size == 1) { "local-only preservation requires one retained contract identity" }
    return preservationFold(
        tableDigest,
        "stable-contract-identity",
        identity.single().joinToString("\u001f").encodeToByteArray(),
    )
}

internal suspend fun localOnlyOpeningDigest(
    db: SafeSQLiteConnection,
    managedTables: Set<String>,
    preservedLocalTables: Set<String>,
): String = listOf(
    localOnlyStateDigest(db, managedTables),
    preservedStateDigest(db, preservedLocalTables),
).joinToString("\n")

internal suspend fun requireLocalOnlyOpeningDigest(
    db: SafeSQLiteConnection,
    expected: String,
    managedTables: Set<String>,
    preservedLocalTables: Set<String>,
    includePreservedLocalTables: Boolean,
) {
    val parts = expected.split('\n', limit = 2)
    require(parts.size == 2) { "local-only preservation witness is malformed" }
    require(localOnlyStateDigest(db, managedTables) == parts[0]) {
        "managed or retained sync state changed during local-only migration"
    }
    if (includePreservedLocalTables) {
        require(preservedStateDigest(db, preservedLocalTables) == parts[1]) {
            "declared local-only state changed outside the local-only migration"
        }
    }
}

private data class PreservedObject(val type: String, val name: String, val sql: String)

private suspend fun loadPreservedObjects(
    db: SafeSQLiteConnection,
    table: String,
    excludedGuardNames: Set<String>,
): List<PreservedObject> =
    db.queryList(
        """
        SELECT type,name,COALESCE(sql,'') FROM sqlite_schema
        WHERE (type='table' AND lower(name)=?) OR
              (type IN ('index','trigger') AND lower(tbl_name)=?)
        ORDER BY type COLLATE BINARY,name COLLATE BINARY
        """.trimIndent(),
        bind = {
            it.bindText(1, table)
            it.bindText(2, table)
        },
    ) { PreservedObject(it.getText(0), it.getText(1), it.getText(2)) }
        .filterNot { it.type == "trigger" && it.name.lowercase() in excludedGuardNames }

private data class PreservationColumn(
    val cid: Long,
    val name: String,
    val type: String,
    val notNull: Long,
    val defaultValue: String?,
    val primaryKey: Long,
    val hidden: Long,
) {
    fun frameValues(): List<String> = listOf(
        cid.toString(), name, type, notNull.toString(), defaultValue ?: "<null>",
        primaryKey.toString(), hidden.toString(),
    )
}

private suspend fun loadPreservedColumns(
    db: SafeSQLiteConnection,
    table: String,
): List<PreservationColumn> = db.queryList(
    "SELECT cid,name,type,\"notnull\",dflt_value,pk,hidden FROM pragma_table_xinfo(?) ORDER BY cid",
    bind = { it.bindText(1, table) },
) { row ->
    PreservationColumn(
        cid = row.getLong(0),
        name = row.getText(1),
        type = row.getText(2),
        notNull = row.getLong(3),
        defaultValue = if (row.isNull(4)) null else row.getText(4),
        primaryKey = row.getLong(5),
        hidden = row.getLong(6),
    )
}

private suspend fun accessibleRowId(
    db: SafeSQLiteConnection,
    table: String,
    columns: List<PreservationColumn>,
): String? {
    val withoutRowId = db.queryList(
        "SELECT wr FROM pragma_table_list WHERE schema='main' AND lower(name)=? AND type='table'",
        bind = { it.bindText(1, table) },
    ) { it.getLong(0) }.singleOrNull() ?: error("declared preserved table $table is missing")
    if (withoutRowId == 1L) return null
    val names = columns.map { it.name.lowercase() }.toSet()
    return listOf("rowid", "_rowid_", "oid").firstOrNull { it !in names }
}

private suspend fun stagePreservedRows(
    db: SafeSQLiteConnection,
    table: String,
    tempTable: String,
    columns: List<PreservationColumn>,
    accessibleRowId: String?,
): String? {
    db.execSQL("DROP TABLE IF EXISTS $tempTable")
    val columnNames = columns.map { it.name.lowercase() }.toSet()
    var sourceRowIdColumn = "_sqlitenow_source_rowid"
    while (sourceRowIdColumn.lowercase() in columnNames) {
        sourceRowIdColumn = "_$sourceRowIdColumn"
    }
    val sourceRowIdProjection = accessibleRowId?.let {
        "${quoteIdent(it)} AS ${quoteIdent(sourceRowIdColumn)},"
    }.orEmpty()
    val projection = columns.joinToString(",") { quoteIdent(it.name) }
    db.execSQL(
        "CREATE TEMP TABLE $tempTable AS SELECT $sourceRowIdProjection$projection " +
            "FROM ${quoteIdent(table)}",
    )
    return sourceRowIdColumn.takeIf { accessibleRowId != null }
}

private suspend fun digestStagedRow(
    db: SafeSQLiteConnection,
    tempTable: String,
    tempRowId: Long,
    columns: List<PreservationColumn>,
    sourceRowIdColumn: String?,
): String {
    var digest = sha256Hex(preservationRowDigestDomain.encodeToByteArray())
    if (sourceRowIdColumn != null) {
        val sourceRowId = db.queryList(
            "SELECT ${quoteIdent(sourceRowIdColumn)} FROM $tempTable WHERE rowid=?",
            bind = { it.bindLong(1, tempRowId) },
        ) { it.getLong(0) }.single()
        digest = preservationFold(digest, "rowid", signedLongBytes(sourceRowId))
    }
    for (column in columns) {
        digest = foldStagedCell(db, tempTable, tempRowId, column.name, digest)
    }
    return digest
}

private data class PreservationCellHeader(
    val type: String,
    val integer: Long?,
    val real: Double?,
    val byteLength: Long,
)

private suspend fun foldStagedCell(
    db: SafeSQLiteConnection,
    tempTable: String,
    rowId: Long,
    columnName: String,
    initial: String,
): String {
    val column = quoteIdent(columnName)
    val header = db.queryList(
        "SELECT typeof($column)," +
            "CASE WHEN typeof($column)='integer' THEN $column END," +
            "CASE WHEN typeof($column)='real' THEN $column END," +
            "length(CAST($column AS BLOB)) FROM $tempTable WHERE rowid=?",
        bind = { it.bindLong(1, rowId) },
    ) { row ->
        PreservationCellHeader(
            type = row.getText(0),
            integer = if (row.isNull(1)) null else row.getLong(1),
            real = if (row.isNull(2)) null else row.getDouble(2),
            byteLength = if (row.isNull(3)) 0L else row.getLong(3),
        )
    }.single()
    return when (header.type) {
        "null" -> preservationFold(initial, "null", byteArrayOf())
        "integer" -> preservationFold(initial, "integer", signedLongBytes(checkNotNull(header.integer)))
        "real" -> preservationFold(initial, "real", signedLongBytes(checkNotNull(header.real).toRawBits()))
        "text", "blob" -> foldChunkedBytes(
            db,
            tempTable,
            rowId,
            column,
            header.type,
            header.byteLength,
            initial,
        )
        else -> error("unsupported SQLite value type ${header.type}")
    }
}

private suspend fun foldChunkedBytes(
    db: SafeSQLiteConnection,
    tempTable: String,
    rowId: Long,
    column: String,
    type: String,
    byteLength: Long,
    initial: String,
): String {
    var digest = preservationFold(initial, type, signedLongBytes(byteLength))
    var offset = 1L
    while (offset <= byteLength) {
        val chunk = db.queryList(
            "SELECT substr(CAST($column AS BLOB),?,?) FROM $tempTable WHERE rowid=?",
            bind = {
                it.bindLong(1, offset)
                it.bindLong(2, preservationChunkBytes.toLong())
                it.bindLong(3, rowId)
            },
        ) { it.getBlob(0) }.single()
        require(chunk.isNotEmpty()) { "SQLite returned an empty preservation chunk before the declared boundary" }
        digest = preservationFold(digest, "chunk", chunk)
        offset += chunk.size
    }
    return digest
}

private suspend fun sequenceFrame(db: SafeSQLiteConnection, table: String): ByteArray {
    val exists = db.scalarLong(
        "SELECT count(*) FROM sqlite_schema WHERE type='table' AND lower(name)='sqlite_sequence'",
    ) == 1L
    if (!exists) return "absent".encodeToByteArray()
    val rows = db.queryList(
        "SELECT seq FROM sqlite_sequence WHERE lower(name)=?",
        bind = { it.bindText(1, table) },
    ) { it.getLong(0) }
    return if (rows.isEmpty()) "none".encodeToByteArray() else byteArrayOf(1) + signedLongBytes(rows.single())
}

private fun signedLongBytes(value: Long): ByteArray = ByteArray(8) { index ->
    (value ushr ((7 - index) * 8)).toByte()
}

private fun preservationFold(previous: String, label: String, value: ByteArray): String {
    val prefix = "$previous|${label.encodeToByteArray().size}:$label|${value.size}:".encodeToByteArray()
    return sha256Hex(prefix + value)
}
