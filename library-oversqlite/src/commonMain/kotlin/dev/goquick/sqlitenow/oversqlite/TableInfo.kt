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
import dev.goquick.sqlitenow.core.sqlite.getColumnNames
import dev.goquick.sqlitenow.core.sqlite.use

internal enum class ColumnKind {
    TEXT,
    INTEGER,
	REAL,
	BLOB,
	NUMERIC,
}

internal fun ColumnKind.isBlobKind(): Boolean = this == ColumnKind.BLOB

private fun classifyColumnKind(declaredType: String): ColumnKind {
	return when (sqliteAffinity(declaredType)) {
		"TEXT" -> ColumnKind.TEXT
		"INTEGER" -> ColumnKind.INTEGER
		"REAL" -> ColumnKind.REAL
		"BLOB" -> ColumnKind.BLOB
		"NUMERIC" -> ColumnKind.NUMERIC
		else -> error("unsupported SQLite affinity")
    }
}

internal data class ColumnInfo(
    val name: String,
    val declaredType: String,
    val isPrimaryKey: Boolean,
    val notNull: Boolean,
    val defaultValue: String?,
    val kind: ColumnKind = classifyColumnKind(declaredType),
    val hidden: Int = 0,
    val primaryKeyOrdinal: Int = if (isPrimaryKey) 1 else 0,
    val wireType: String? = null,
)

internal data class ForeignKeyInfo(
    val seq: Int,
    val refTable: String,
    val fromCol: String,
    val toCol: String,
    val onUpdate: String = "NO ACTION",
    val onDelete: String = "NO ACTION",
    val match: String = "NONE",
)

internal data class TableInfo(
    val table: String,
    val columns: List<ColumnInfo>,
    val foreignKeys: List<ForeignKeyInfo> = emptyList(),
    val foreignKeyColumnsLower: Set<String> = emptySet(),
    val tableKind: String = "table",
    val withoutRowId: Boolean = false,
    val strict: Boolean = false,
) {
    val columnNamesLower: List<String> = columns.map { it.name.lowercase() }
    val columnsByNameLower: Map<String, ColumnInfo> = columns.associateBy { it.name.lowercase() }
    val typesByNameLower: Map<String, String> = columns.associate { it.name.lowercase() to it.declaredType }
    val primaryKey: ColumnInfo? = columns.firstOrNull { it.isPrimaryKey }
    val primaryKeyIsBlob: Boolean = primaryKey?.kind?.isBlobKind() == true

    fun isBlobReferenceColumn(columnName: String): Boolean = foreignKeyColumnsLower.contains(columnName.lowercase())

    fun column(columnName: String): ColumnInfo {
        return columnsByNameLower[columnName.lowercase()]
            ?: error("table $table is missing column $columnName")
    }
}

internal class TableInfoCache {
    private val cache = mutableMapOf<String, TableInfo>()

    suspend fun get(db: SafeSQLiteConnection, table: String): TableInfo {
        val key = table.lowercase()
        cache[key]?.let { return it }

        val (cols, foreignKeys, foreignKeyColumns) = db.withExclusiveAccess {
            val cols = mutableListOf<ColumnInfo>()
            db.prepare("PRAGMA table_xinfo(${quoteIdent(key)})").use { st ->
                // Resolve column indexes by name to avoid magic numbers
                val idxByName: Map<String, Int> = run {
                    val names = st.getColumnNames().map { it.lowercase() }
                    names.mapIndexed { idx, n -> n to idx }.toMap()
                }
                val iName = idxByName["name"] ?: 1
                val iType = idxByName["type"] ?: 2
                val iNotNull = idxByName["notnull"] ?: 3
                val iDflt = idxByName["dflt_value"] ?: 4
                val iPk = idxByName["pk"] ?: 5
                val iHidden = idxByName["hidden"] ?: 6

                while (st.step()) {
                    val name = st.getText(iName)
                    val type = st.getText(iType)
                    val notNull = st.getLong(iNotNull) == 1L
                    val defaultVal = if (st.isNull(iDflt)) null else st.getText(iDflt)
                    val isPk = st.getLong(iPk) == 1L
                    cols += ColumnInfo(
                        name = name,
                        declaredType = type,
                        isPrimaryKey = isPk,
                        notNull = notNull,
                        defaultValue = defaultVal,
                        hidden = st.getLong(iHidden).toInt(),
                        primaryKeyOrdinal = st.getLong(iPk).toInt(),
                    )
                }
            }
            val foreignKeys = mutableListOf<ForeignKeyInfo>()
            val foreignKeyColumns = mutableSetOf<String>()
            db.prepare("PRAGMA foreign_key_list(${quoteIdent(key)})").use { st ->
                val idxByName: Map<String, Int> = run {
                    val names = st.getColumnNames().map { it.lowercase() }
                    names.mapIndexed { idx, n -> n to idx }.toMap()
                }
                val iSeq = idxByName["seq"] ?: 1
                val iTable = idxByName["table"] ?: 2
                val iFrom = idxByName["from"] ?: 3
                val iTo = idxByName["to"] ?: 4
                while (st.step()) {
                    val fromCol = st.getText(iFrom)
                    foreignKeys += ForeignKeyInfo(
                        seq = st.getLong(iSeq).toInt(),
                        refTable = st.getText(iTable),
                        fromCol = fromCol,
                        toCol = if (st.isNull(iTo)) "" else st.getText(iTo),
                        onUpdate = st.getText(idxByName.getValue("on_update")),
                        onDelete = st.getText(idxByName.getValue("on_delete")),
                        match = st.getText(idxByName.getValue("match")),
                    )
                    foreignKeyColumns += fromCol.lowercase()
                }
            }
            Triple(cols, foreignKeys, foreignKeyColumns)
        }

        val ti = db.withExclusiveAccess {
            db.prepare("SELECT name, type, wr, strict FROM pragma_table_list WHERE schema='main' AND name=? COLLATE NOCASE").use { st ->
                st.bindText(1, table)
                require(st.step()) { "managed table $table does not exist" }
                TableInfo(
                    table = st.getText(0),
                    tableKind = st.getText(1),
                    withoutRowId = st.getLong(2) == 1L,
                    strict = st.getLong(3) == 1L,
                    columns = cols,
                    foreignKeys = foreignKeys,
                    foreignKeyColumnsLower = foreignKeyColumns,
                )
            }
        }
        cache[key] = ti
        return ti
    }

    fun install(tables: Map<String, TableInfo>) {
        cache.clear()
        cache.putAll(tables)
    }

    fun clear() {
        cache.clear()
    }
}
