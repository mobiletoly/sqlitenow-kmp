package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection

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
