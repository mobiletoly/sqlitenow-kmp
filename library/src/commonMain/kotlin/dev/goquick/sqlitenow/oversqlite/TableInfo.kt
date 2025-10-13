/*
 * Copyright 2025 Anatoliy Pochkin
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
) {
    val columnNamesLower: List<String> = columns.map { it.name.lowercase() }
    val typesByNameLower: Map<String, String> = columns.associate { it.name.lowercase() to it.declaredType }
    val primaryKey: ColumnInfo? = columns.firstOrNull { it.isPrimaryKey }
    val primaryKeyIsBlob: Boolean = primaryKey?.declaredType?.lowercase()?.contains("blob") == true
}

object TableInfoProvider {
    // Cache is scoped per SafeSQLiteConnection to avoid cross-database contamination between tests/apps
    private val cacheByDb = mutableMapOf<SafeSQLiteConnection, MutableMap<String, TableInfo>>()

    suspend fun get(db: SafeSQLiteConnection, table: String): TableInfo {
        val key = table.lowercase()
        val dbCache = cacheByDb.getOrPut(db) { mutableMapOf() }
        dbCache[key]?.let { return it }

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
        val ti = TableInfo(key, cols)
        dbCache[key] = ti
        return ti
    }

    // Optional: allow callers to clear per-db cache (e.g., after migrations)
    fun clear(db: SafeSQLiteConnection) {
        cacheByDb.remove(db)
    }
}
