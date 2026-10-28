package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteStatement
import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowDiagnosticContext
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.serialization.json.*

/** Capability and exact incoming membership proofs run under the final writer. */
internal class OversqliteSnapshotPreservation(
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
    private val local: OversqliteLocalStore,
    private val survivors: OversqliteApplySurvivors,
    private val json: Json,
) {
    private val workspace = OversqliteWorkspace(db, config)

    private var metadata: SnapshotPreservationMetadata? = null
    private var counters: SnapshotPreservationCounters? = null

    suspend fun validate(state: RuntimeState, unit: String, entries: Long, unresolved: Long, analysed: Boolean, cache: StatementCache) {
        val slice = currentCoroutineContext()[OversqliteWorkSlice]
        check(slice?.preservationMetadata == null) { "preservation metadata already belongs to a writer" }
        counters = if (SqliteNowDiagnosticContext.current(currentCoroutineContext())?.span != null) SnapshotPreservationCounters() else null
        val memo = slice?.let { SnapshotPreservationMetadata(db, cache, workspace, unit, counters) }
        metadata = memo
        slice?.preservationMetadata = memo
        try {
            survivors.reserveSnapshotWork(unit, retryWithoutMetadata { work(state, unit, entries, cache) })
            if (!analysed && unresolved != 0L) exec(snapshotCycleClosure + "UPDATE _sync_apply_rows SET pending=7 WHERE ordinal IN(SELECT id FROM closure)", cache = cache)
            survivors.withPlannedRows(state, cache) { reader ->
            for (ordinal in 1..entries) {
                currentCoroutineContext().ensureActive()
                retryWithoutMetadata {
                    counters?.let { it.rowChecks++ }
                    reader.withRow(ordinal) { lease ->
                        val row = lease.row
                        val live = local.serializeExistingRow(row.table, row.pk, cache)
                        val same = lease.matches(state.validated.tableInfoByName.getValue(row.table), live)
                        if (!same) mutation(state, unit, lease, false, cache)
                        if (one("SELECT pending=7 FROM _sync_apply_rows WHERE ordinal=?", ordinal, cache = cache) { it.getLong(0) != 0L } == true)
                            mutation(state, unit, lease, true, cache)
                    }
                }
            }
            }
        } finally {
            memo?.clear()
            slice?.preservationMetadata = null
            metadata = null
            counters?.let { stats -> syncDiagnosticEvent("snapshot.preservation_metadata", counts = { stats.counts() }, countBasis = "actual_checks_and_metadata_reuse") }
            counters = null
        }
    }

    // A row proof only reads state. Unwind borrowed metadata before eviction and retry;
    // cancellation, suppressed cleanup failures and genuine capacity failures propagate.
    private suspend fun <T> retryWithoutMetadata(block: suspend () -> T): T {
        try { return block() }
        catch (capacity: SyncWorkspaceLimitException) {
            val memo = metadata
            if (capacity.suppressedExceptions.isNotEmpty() || memo == null || memo.charge == WorkspaceCharge()) throw capacity
            memo.disable()
            counters?.let { it.evictions++; it.fallbacks++ }
            return block()
        }
    }

    private suspend fun work(state: RuntimeState, unit: String, entries: Long, cache: StatementCache): Long {
        val catalog = checkNotNull(one("SELECT count(*),COALESCE(sum(length(CAST(name AS BLOB))),0) FROM sqlite_schema WHERE type='table'", cache = cache) { it.getLong(0) to it.getLong(1) })
        workspace.check(unit, WorkspaceCharge(16 + catalog.first * 8, 4096 + workspace.scale(unit, catalog.second, 16)), statementCache = cache)
        val triggers = db.scalarLong("SELECT count(*) FROM (SELECT name FROM sqlite_schema WHERE type='trigger' UNION ALL SELECT name FROM sqlite_temp_schema WHERE type='trigger')")
        var total = snapshotWorkAdd(snapshotWorkScale(db.scalarLong("SELECT count(*) FROM _sync_row_state"), 128),
            snapshotWorkScale(entries, snapshotWorkAdd(4096, snapshotWorkAdd(snapshotWorkScale(catalog.first,128), snapshotWorkScale(triggers,64)))))
        val memo = metadata
        if (memo != null && !memo.disabled && memo.tables == null && memo.retain(WorkspaceCharge(16 + catalog.first * 8, 4096 + workspace.scale(unit, catalog.second, 16)))) {
            memo.tables = syncDiagnosticAggregate("schema.catalog_enumeration") {
                counters?.let { it.catalogLoads++ }
                val names = mutableListOf<String>()
                db.withPreparedStatement("SELECT name FROM sqlite_schema WHERE type='table' AND name>'' AND name NOT LIKE 'sqlite_%' ORDER BY name", cache) { st ->
                    while (st.step()) {
                        currentCoroutineContext().ensureActive()
                        val table = st.getText(0)
                        if (table !in state.validated.tableInfoByName && !table.startsWith("_sync_")) names += table
                    }
                }
                names
            }
        }
        forEachUnmanaged(state, cache) { table ->
            val names = state.validated.tables.map { it.tableName }
            val marks = names.joinToString(",") { "?" }
            val fk = checkNotNull(one("SELECT count(*),count(DISTINCT id),COALESCE(sum(length(CAST(\"table\" AS BLOB))+length(CAST(\"from\" AS BLOB))+length(CAST(COALESCE(\"to\",'') AS BLOB))+length(on_delete)+length(on_update)),0) FROM pragma_foreign_key_list(?) WHERE lower(\"table\") IN ($marks)",
                *(listOf(table) + names).toTypedArray(), cache = cache) { Triple(it.getLong(0), it.getLong(1), it.getLong(2)) })
            if (memo != null && !memo.disabled && memo.retain(WorkspaceCharge(8 + fk.first * 12, 1024 + workspace.scale(unit, fk.third + utf8Size(table), 24)))) {
                counters?.let { it.foreignKeyLoads++ }
                val columns = mutableListOf<SnapshotPreservationForeignKeyColumn>()
                db.withPreparedStatement("SELECT id,lower(\"table\"),\"from\",\"to\",on_delete,on_update FROM pragma_foreign_key_list(?) WHERE lower(\"table\") IN ($marks) ORDER BY id,seq", cache) { st ->
                    st.bindText(1, table); names.forEachIndexed { index, name -> st.bindText(index + 2, name) }
                    while (st.step()) {
                        currentCoroutineContext().ensureActive()
                        columns += SnapshotPreservationForeignKeyColumn(st.getLong(0), st.getText(1), st.getText(2), if (st.isNull(3)) null else st.getText(3), st.getText(4), st.getText(5))
                    }
                }
                memo.foreignKeys[table] = columns
            }
            if (fk.second != 0L) {
                val rows = db.scalarLong("SELECT count(*) FROM ${quoteIdent(table)}")
                val scan = snapshotWorkAdd(256, snapshotWorkScale(rows, snapshotWorkAdd(32, snapshotWorkScale(fk.first,16))))
                total = snapshotWorkAdd(total, snapshotWorkAdd(rows, snapshotWorkScale(snapshotWorkScale(snapshotWorkScale(entries,fk.second),2), scan)))
            }
        }
        return total
    }

    private suspend fun forEachUnmanaged(state: RuntimeState, cache: StatementCache, block: suspend (String) -> Unit) {
        val tables = metadata?.tables
        if (tables != null) {
            counters?.let { it.hits++ }
            for (table in tables) { currentCoroutineContext().ensureActive(); block(table) }
        } else {
            counters?.let { it.catalogLoads++ }
            var last = ""
            while (true) {
                val table = nextUnmanaged(state, last, cache) ?: break
                last = table; block(table)
            }
        }
    }

    private suspend fun mutation(state: RuntimeState, unit: String, lease: SnapshotRowLease, vacating: Boolean, cache: StatementCache) {
        val row = lease.row
        val memo = metadata
        if (memo?.triggerTables?.contains(row.table) == true) counters?.let { it.hits++ }
        else {
            triggers(state, unit, row, cache)
            if (memo != null && memo.retain(WorkspaceCharge(2, 128 + utf8Size(row.table) * 6))) memo.triggerTables += row.table
        }
        forEachUnmanaged(state, cache) { child ->
            val columns = metadata?.foreignKeys?.get(child)
            if (columns != null) {
                counters?.let { it.hits++ }
                var previous = -1L
                for (column in columns) if (column.parent == row.table && column.id != previous) {
                    previous = column.id
                    incoming(state, unit, lease, child, column.id, vacating, cache, columns)
                }
            } else {
                var previous = -1L
                while (true) {
                    val fk = one("SELECT id FROM pragma_foreign_key_list(?) WHERE lower(\"table\")=? AND id>? ORDER BY id LIMIT 1",
                        child, row.table, previous, cache = cache) { it.getLong(0) } ?: break
                    previous = fk
                    incoming(state, unit, lease, child, fk, vacating, cache, null)
                }
            }
        }
    }

    private suspend fun triggers(state: RuntimeState, unit: String, row: ApplySurvivor, cache: StatementCache) = syncDiagnosticAggregate("snapshot.trigger_definitions") { triggersImpl(state, unit, row, cache) }

    private suspend fun triggersImpl(state: RuntimeState, unit: String, row: ApplySurvivor, cache: StatementCache) {
        counters?.let { it.triggerChecks++ }
        val table = state.validated.tables.single { it.tableName == row.table }
        val expected = expectedCaptureTriggers(state.validated.schemaForTable(row.table), table, state.validated.tableInfoByName.getValue(row.table)).toMap()
        val sql = "SELECT name,sql,temporary FROM (SELECT name,sql,tbl_name,0 AS temporary FROM sqlite_schema WHERE type='trigger' UNION ALL SELECT name,sql,tbl_name,1 FROM sqlite_temp_schema WHERE type='trigger') WHERE lower(tbl_name)=?"
        val charge = checkNotNull(one("SELECT count(*),COALESCE(sum(length(CAST(sql AS BLOB))+length(CAST(name AS BLOB))),0) FROM ($sql)", row.table, cache = cache) { it.getLong(0) to it.getLong(1) })
        workspace.check(unit, WorkspaceCharge(16 + charge.first * 4, 4096 + workspace.scale(unit, charge.second,16)), statementCache = cache)
        db.withPreparedStatement(sql, cache) { st ->
            st.bindText(1, row.table)
            while (st.step()) {
                val name = st.getText(0)
                val actual = definition(st.getText(1))
                if (st.getLong(2) != 0L || expected[name]?.let(::definition) != actual) blocked(row, name)
            }
        }
    }

    private suspend fun incoming(state: RuntimeState, unit: String, lease: SnapshotRowLease, child: String, id: Long, vacating: Boolean, cache: StatementCache, metadataColumns: List<SnapshotPreservationForeignKeyColumn>?) {
        val row = lease.row
        counters?.let { it.incomingChecks++ }
        val count = if (metadataColumns != null) {
            var count = 0L; var bytes = 0L
            for (column in metadataColumns) if (column.id == id) {
                count++; bytes += sqliteTextLength(column.from) + (column.to?.let(::sqliteTextLength) ?: 0) + sqliteTextLength(column.delete) + sqliteTextLength(column.update)
            }
            count to bytes
        } else checkNotNull(one("SELECT count(*),COALESCE(sum(length(\"from\")+length(COALESCE(\"to\",''))+length(on_delete)+length(on_update)),0) FROM pragma_foreign_key_list(?) WHERE id=?",
            child, id, cache = cache) { it.getLong(0) to it.getLong(1) })
        workspace.check(unit, WorkspaceCharge(16 + count.first * 8, 4096 + workspace.scale(unit,count.second,16)), statementCache = cache)
        val pk = checkNotNull(state.validated.tableInfoByName.getValue(row.table).primaryKey)
        val refs = mutableListOf<String>(); val joins = mutableListOf<String>()
        var delete = ""; var update = ""
        if (metadataColumns != null) {
            for (column in metadataColumns) if (column.id == id) {
                val to = column.to ?: pk.name
                joins += "p.${quoteIdent(to)}=c.${quoteIdent(column.from)}"; refs += to
                delete = column.delete; update = column.update
            }
        } else {
            db.withPreparedStatement("SELECT \"from\",\"to\",on_delete,on_update FROM pragma_foreign_key_list(?) WHERE id=? ORDER BY seq", cache) { st ->
                st.bindText(1, child); st.bindLong(2,id)
                while (st.step()) {
                    val from = st.getText(0); val to = if (st.isNull(1)) pk.name else st.getText(1)
                    joins += "p.${quoteIdent(to)}=c.${quoteIdent(from)}"; refs += to
                    delete = st.getText(2); update = st.getText(3)
                }
            }
        }
        val physical = if (pk.declaredType.contains("BLOB",ignoreCase = true)) decodeLocalUuidBytes(row.pk) else row.pk
        val present = one("SELECT EXISTS(SELECT 1 FROM ${quoteIdent(child)} c JOIN ${quoteIdent(row.table)} p ON ${joins.joinToString(" AND ")} WHERE p.${quoteIdent(pk.name)}=?)",
            physical, cache = cache) { it.getLong(0) != 0L } == true
        if (!present) return
        if (!lease.hasPayload || (vacating && delete != "NO ACTION") || (!vacating && update !in setOf("NO ACTION","RESTRICT"))) blocked(row)
        val payload = checkNotNull(lease.payloadObject())
        val source = if (row.wire) PayloadSource.AUTHORITATIVE_WIRE else PayloadSource.LOCAL_STATE
        val terms = refs.joinToString(" AND ") { "${quoteIdent(it)} IS ?" }
        val same = db.withPreparedStatement("SELECT $terms FROM ${quoteIdent(row.table)} WHERE ${quoteIdent(pk.name)}=?", cache) { st ->
            refs.forEachIndexed { index, column -> OversqliteValueCodec.bindPayloadValue(st,index+1,state.validated.tableInfoByName.getValue(row.table).column(column), payload.getValue(column.lowercase()),source) }
            if (physical is ByteArray) st.bindBlob(refs.size+1,physical) else st.bindText(refs.size+1, row.pk)
            st.step() && st.getLong(0) != 0L
        }
        if (!same) blocked(row)
    }

    private suspend fun nextUnmanaged(state: RuntimeState, last: String, cache: StatementCache): String? = syncDiagnosticAggregate("schema.catalog_enumeration") { nextUnmanagedImpl(state, last, cache) }

    private suspend fun nextUnmanagedImpl(state: RuntimeState, last: String, cache: StatementCache): String? {
        var cursor = last
        while (true) {
            val table = one("SELECT name FROM sqlite_schema WHERE type='table' AND name>? AND name NOT LIKE 'sqlite_%' ORDER BY name LIMIT 1", cursor, cache = cache) { it.getText(0) } ?: return null
            cursor = table
            if (table !in state.validated.tableInfoByName && !table.startsWith("_sync_")) return table
        }
    }

    private fun definition(sql: String): String = sql.trim().removeSuffix(";")
        .replaceFirst("CREATE TRIGGER IF NOT EXISTS ", "CREATE TRIGGER ")

    private fun blocked(row: ApplySurvivor, trigger: String = ""): Nothing = throw FinalStateApplyBlockedException(
        "unsafe_trigger_or_dependency", "snapshot", row.table, trigger = trigger, keys = listOf(row.pk))

    private suspend fun exec(sql: String, cache: StatementCache) { db.withPreparedStatement(sql,cache) { it.step() } }
    private suspend fun <T> one(sql: String, vararg args: Any?, cache: StatementCache, read: (SQLiteStatement) -> T): T? = db.withPreparedStatement(sql,cache) { st ->
        args.forEachIndexed { index, value -> when(value) {
            is String -> st.bindText(index+1,value); is Long -> st.bindLong(index+1,value); is ByteArray -> st.bindBlob(index+1,value)
            else -> error("invalid snapshot proof bind")
        } }
        if (st.step()) read(st) else null
    }
}

internal data class SnapshotPreservationForeignKeyColumn(val id: Long, val parent: String, val from: String, val to: String?, val delete: String, val update: String)

internal class SnapshotPreservationCounters {
    var catalogLoads = 0L; var foreignKeyLoads = 0L; var triggerChecks = 0L
    var rowChecks = 0L; var incomingChecks = 0L; var hits = 0L; var fallbacks = 0L; var evictions = 0L
    var peakEntries = 0L; var peakBytes = 0L
    fun counts() = mapOf("catalog_enumerations" to catalogLoads, "foreign_key_metadata_loads" to foreignKeyLoads,
        "trigger_tables_checked" to triggerChecks, "row_checks" to rowChecks, "incoming_row_checks" to incomingChecks,
        "cache_hits" to hits, "cache_fallbacks" to fallbacks, "cache_evictions" to evictions,
        "retained_peak_entries" to peakEntries, "retained_peak_bytes" to peakBytes)
}

/** One final writer only; borrowed metadata never survives a row-proof unwind. */
internal class SnapshotPreservationMetadata(
    val db: SafeSQLiteConnection, val writer: StatementCache, private val workspace: OversqliteWorkspace,
    private val unit: String, private val counters: SnapshotPreservationCounters?,
) {
    var charge = WorkspaceCharge(); private set
    var disabled = false; private set
    var tables: List<String>? = null
    val foreignKeys = mutableMapOf<String, List<SnapshotPreservationForeignKeyColumn>>()
    val triggerTables = mutableSetOf<String>()
    suspend fun retain(extra: WorkspaceCharge): Boolean {
        if (disabled) return false
        writer.requireOpenForReservationBinding()
        try { workspace.check(unit, extra, statementCache = writer) }
        catch (capacity: SyncWorkspaceLimitException) {
            if (capacity.suppressedExceptions.isNotEmpty()) throw capacity
            counters?.let { it.fallbacks++ }
            return false
        }
        charge = workspace.add(unit, charge, extra)
        counters?.let { it.peakEntries = maxOf(it.peakEntries, charge.entries); it.peakBytes = maxOf(it.peakBytes, charge.bytes) }
        return true
    }
    fun clear() { tables = null; foreignKeys.clear(); triggerTables.clear(); charge = WorkspaceCharge() }
    fun disable() { clear(); disabled = true }
}

// SQLite length(TEXT) counts Unicode code points, unlike Kotlin's UTF-16 length.
private fun sqliteTextLength(value: String): Long {
    var count = 0L; var i = 0
    while (i < value.length) {
        val c = value[i++]
        if (c == '\u0000') break
        if (c.isHighSurrogate() && i < value.length && value[i].isLowSurrogate()) i++
        count++
    }
    return count
}

private const val snapshotCycleClosure = """WITH RECURSIVE arcs(a,b) AS MATERIALIZED (
 SELECT a.ordinal,b.ordinal FROM _sync_apply_edges e
 JOIN _sync_apply_rows a ON a.table_name=e.parent_table AND a.local_pk=e.parent_pk
 JOIN _sync_apply_rows b ON b.table_name=e.child_table AND b.local_pk=e.child_pk
 WHERE e.edge_kind='u' AND a.pending NOT IN(3,6) AND b.pending NOT IN(3,6)),
 reach(a,b) AS (SELECT a,b FROM arcs UNION SELECT r.a,e.b FROM reach r JOIN arcs e ON e.a=r.b),
 closure(id) AS (SELECT a FROM reach WHERE a=b UNION SELECT child.ordinal FROM closure c
 JOIN _sync_apply_rows parent ON parent.ordinal=c.id
 JOIN _sync_apply_edges e ON e.edge_kind='f' AND e.parent_table=parent.table_name AND e.parent_pk=parent.local_pk
 JOIN _sync_apply_rows child ON child.table_name=e.child_table AND child.local_pk=e.child_pk) """
