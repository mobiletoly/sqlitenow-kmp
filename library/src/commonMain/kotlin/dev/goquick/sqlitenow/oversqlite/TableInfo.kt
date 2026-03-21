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

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.getColumnNames
import dev.goquick.sqlitenow.core.sqlite.use

data class ColumnInfo(
    val name: String,
    val declaredType: String,
    val isPrimaryKey: Boolean,
    val notNull: Boolean,
    val defaultValue: String?,
)

data class TableInfo(
    val table: String,
    val columns: List<ColumnInfo>,
    val foreignKeyColumnsLower: Set<String> = emptySet(),
) {
    val columnNamesLower: List<String> = columns.map { it.name.lowercase() }
    val typesByNameLower: Map<String, String> = columns.associate { it.name.lowercase() to it.declaredType }
    val primaryKey: ColumnInfo? = columns.firstOrNull { it.isPrimaryKey }
    val primaryKeyIsBlob: Boolean = primaryKey?.declaredType?.lowercase()?.contains("blob") == true

    fun isBlobReferenceColumn(columnName: String): Boolean = foreignKeyColumnsLower.contains(columnName.lowercase())
}

class TableInfoCache {
    private val cache = mutableMapOf<String, TableInfo>()

    suspend fun get(db: SafeSQLiteConnection, table: String): TableInfo {
        val key = table.lowercase()
        cache[key]?.let { return it }

        val cols = mutableListOf<ColumnInfo>()
        db.prepare("PRAGMA table_info($key)").use { st ->
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
                )
            }
        }
        val foreignKeyColumns = mutableSetOf<String>()
        db.prepare("PRAGMA foreign_key_list($key)").use { st ->
            while (st.step()) {
                foreignKeyColumns += st.getText(3).lowercase()
            }
        }

        val ti = TableInfo(key, cols, foreignKeyColumns)
        cache[key] = ti
        return ti
    }

    fun clear() {
        cache.clear()
    }
}

object TableInfoProvider {
    // Legacy helper retained for tests and ad hoc callers; the runtime uses a client-scoped TableInfoCache.
    private val cacheByDb = mutableMapOf<SafeSQLiteConnection, TableInfoCache>()

    suspend fun get(db: SafeSQLiteConnection, table: String): TableInfo {
        return cacheByDb.getOrPut(db) { TableInfoCache() }.get(db, table)
    }

    fun clear(db: SafeSQLiteConnection) {
        cacheByDb.remove(db)
    }
}
