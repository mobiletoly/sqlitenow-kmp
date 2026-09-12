package dev.goquick.sqlitenow.oversqlite

/** Schema descriptions belong to one validated owner; no runtime values or handles live here. */
internal class ReplaySchemaSql(tables: List<ValidatedSyncTable>, infos: Map<String, TableInfo>, keys: Map<String, String>) {
    private val schema = infos.mapValues { (_, info) ->
        info.copy(columns = info.columns.toList(), foreignKeys = info.foreignKeys.toList(),
            foreignKeyColumnsLower = info.foreignKeyColumnsLower.toSet())
    }
    private val primaryKeys = keys.toMap()
    private val descriptions = tables.associate { table ->
        val name = table.tableName
        name to lazy {
            ReplayTableSql(name, schema.getValue(name), primaryKeys[name], schema)
        }
    }

    fun table(name: String): ReplayTableSql = descriptions.getValue(name).value
}

internal data class ReplayKeySql(val size: String, val value: String)

internal class ReplayTableSql(
    private val tableName: String,
    private val info: TableInfo,
    private val primaryKey: String?,
    private val schema: Map<String, TableInfo>,
) {
    val quotedTable = quoteIdent(tableName)
    val quotedColumns = info.columns.map { quoteIdent(it.name) }
    val keyExpression: String by lazy {
        replayKeyExpression(checkNotNull(info.primaryKey).name, info.primaryKeyIsBlob)
    }
    val initialKey: ReplayKeySql by lazy { keySql(afterCursor = false) }
    val afterCursorKey: ReplayKeySql by lazy { keySql(afterCursor = true) }
    private val foreignKeys = info.foreignKeys.map { fk -> lazy {
        val parent = schema.getValue(fk.refTable.lowercase())
        val expression = replayKeyExpression(fk.fromCol, parent.primaryKeyIsBlob)
        val tail = " FROM $quotedTable WHERE ${quoteIdent(checkNotNull(info.primaryKey).name)}=? AND ${quoteIdent(fk.fromCol)} IS NOT NULL"
        ReplayKeySql("SELECT length(CAST($expression AS BLOB))$tail", "SELECT $expression$tail")
    } }
    fun foreignKey(index: Int): ReplayKeySql = foreignKeys[index].value

    val payloadByteBound: String by lazy { livePayloadByteBound(info, "live_row") }
    val captureSize: String by lazy {
        "SELECT $payloadByteBound FROM $quotedTable live_row WHERE ${quoteIdent(checkNotNull(info.primaryKey).name)}=?"
    }
    val decisionCopySize: String by lazy {
        "SELECT COALESCE((SELECT $payloadByteBound FROM $quotedTable live_row WHERE ${quoteIdent(checkNotNull(info.primaryKey).name)}=?),0)+COALESCE((SELECT length(CAST(payload AS BLOB)) FROM _sync_dirty_rows WHERE schema_name=? AND table_name=? AND key_json=?),0)"
    }
    val liveProjection: String by lazy { buildJsonObjectExprHexAware(info, "live_row") }
    val liveSelect: String by lazy {
        """
            SELECT $liveProjection
            FROM $quotedTable live_row
            WHERE live_row.${quoteIdent(checkNotNull(primaryKey))} = ?
        """.trimIndent()
    }
    val authoritativeSelect: String by lazy {
        val values = info.columns.joinToString(",") { column ->
            val parameter = if (column.kind in setOf(ColumnKind.INTEGER, ColumnKind.NUMERIC, ColumnKind.REAL))
                "CAST(? AS ${sqliteAffinity(column.declaredType)})" else "?"
            "$parameter AS ${quoteIdent(column.name)}"
        }
        "SELECT ${buildJsonObjectExprHexAware(info, "observed")} FROM (SELECT $values) AS observed"
    }
    val upsert: String by lazy { buildReplayUpsertSql(tableName, info, checkNotNull(primaryKey)) }

    private fun keySql(afterCursor: Boolean): ReplayKeySql {
        val pk = quoteIdent(checkNotNull(info.primaryKey).name)
        val tail = " FROM $quotedTable" + (if (afterCursor) " WHERE $pk>?" else "") + " ORDER BY $pk LIMIT 1"
        return ReplayKeySql("SELECT length(CAST($keyExpression AS BLOB))$tail", "SELECT $keyExpression$tail")
    }
}

private fun replayKeyExpression(column: String, blob: Boolean): String =
    if (blob) "lower(hex(${quoteIdent(column)}))" else "CAST(${quoteIdent(column)} AS TEXT)"

private fun buildReplayUpsertSql(tableName: String, info: TableInfo, primaryKey: String): String {
    val columns = info.columns.map { quoteIdent(it.name) }
    val updates = info.columns.filterNot { it.name.equals(primaryKey, ignoreCase = true) }
        .map { "${quoteIdent(it.name)} = excluded.${quoteIdent(it.name)}" }
    return buildString {
        append("INSERT INTO ${quoteIdent(tableName)} (${columns.joinToString(", ")}) VALUES (")
        append(columns.indices.joinToString(", ") { "?" })
        append(")")
        if (updates.isEmpty()) append(" ON CONFLICT(${quoteIdent(primaryKey)}) DO NOTHING")
        else append(" ON CONFLICT(${quoteIdent(primaryKey)}) DO UPDATE SET ${updates.joinToString(", ")}")
    }
}

internal object ReplayFixedUnits {
    val apply: String by lazy { applyUnit("apply", 0, "") }
    val settlementDecisions: String by lazy { applyUnit("settlement-decisions", 0, "") }
    val reservation: String by lazy { applyUnit("reservation", 0, "") }
    val reservationReconciliation: String by lazy { applyUnit("reservation-reconciliation", 0, "") }
    val retainedReconciliation: String by lazy { applyUnit("retained-reconciliation", 0, "") }
}
