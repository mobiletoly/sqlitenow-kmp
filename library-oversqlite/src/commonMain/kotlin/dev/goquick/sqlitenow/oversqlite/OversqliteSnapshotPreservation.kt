package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteStatement
import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
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

    suspend fun validate(state: RuntimeState, unit: String, entries: Long, unresolved: Long, analysed: Boolean, cache: StatementCache) {
        survivors.reserveSnapshotWork(unit, work(state, unit, entries, cache))
        if (!analysed && unresolved != 0L) exec(snapshotCycleClosure + "UPDATE _sync_apply_rows SET pending=7 WHERE ordinal IN(SELECT id FROM closure)", cache = cache)
        for (ordinal in 1..entries) {
            currentCoroutineContext().ensureActive()
            val (row, _) = survivors.loadPlannedRow(state, ordinal, cache, cache)
            val live = local.serializeExistingRow(row.table, row.pk, cache)
            val same = OversqliteValueCodec.equivalentPayloadTexts(state.validated.tableInfoByName.getValue(row.table),
                row.payload, live, if (row.wire) PayloadSource.AUTHORITATIVE_WIRE else PayloadSource.LOCAL_STATE, PayloadSource.LOCAL_STATE, json)
            if (!same) mutation(state, unit, row, false, cache)
            if (one("SELECT pending=7 FROM _sync_apply_rows WHERE ordinal=?", ordinal, cache = cache) { it.getLong(0) != 0L } == true)
                mutation(state, unit, row, true, cache)
        }
    }

    private suspend fun work(state: RuntimeState, unit: String, entries: Long, cache: StatementCache): Long {
        val catalog = checkNotNull(one("SELECT count(*),COALESCE(sum(length(CAST(name AS BLOB))),0) FROM sqlite_schema WHERE type='table'", cache = cache) { it.getLong(0) to it.getLong(1) })
        workspace.check(unit, WorkspaceCharge(16 + catalog.first * 8, 4096 + workspace.scale(unit, catalog.second, 16)), statementCache = cache)
        val triggers = db.scalarLong("SELECT count(*) FROM (SELECT name FROM sqlite_schema WHERE type='trigger' UNION ALL SELECT name FROM sqlite_temp_schema WHERE type='trigger')")
        var total = snapshotWorkAdd(snapshotWorkScale(db.scalarLong("SELECT count(*) FROM _sync_row_state"), 128),
            snapshotWorkScale(entries, snapshotWorkAdd(4096, snapshotWorkAdd(snapshotWorkScale(catalog.first,128), snapshotWorkScale(triggers,64)))))
        var last = ""
        while (true) {
            val table = nextUnmanaged(state, last, cache) ?: return total
            last = table
            val names = state.validated.tables.map { it.tableName }
            val marks = names.joinToString(",") { "?" }
            val fk = checkNotNull(one("SELECT count(*),count(DISTINCT id) FROM pragma_foreign_key_list(?) WHERE lower(\"table\") IN ($marks)",
                *(listOf(table) + names).toTypedArray(), cache = cache) { it.getLong(0) to it.getLong(1) })
            if (fk.second == 0L) continue
            val rows = db.scalarLong("SELECT count(*) FROM ${quoteIdent(table)}")
            val scan = snapshotWorkAdd(256, snapshotWorkScale(rows, snapshotWorkAdd(32, snapshotWorkScale(fk.first,16))))
            total = snapshotWorkAdd(total, snapshotWorkAdd(rows, snapshotWorkScale(snapshotWorkScale(snapshotWorkScale(entries,fk.second),2), scan)))
        }
    }

    private suspend fun mutation(state: RuntimeState, unit: String, row: ApplySurvivor, vacating: Boolean, cache: StatementCache) {
        triggers(state, unit, row, cache)
        var last = ""
        while (true) {
            val child = nextUnmanaged(state, last, cache) ?: return
            last = child
            var previous = -1L
            while (true) {
                val fk = one("SELECT id FROM pragma_foreign_key_list(?) WHERE lower(\"table\")=? AND id>? ORDER BY id LIMIT 1",
                    child, row.table, previous, cache = cache) { it.getLong(0) } ?: break
                previous = fk
                incoming(state, unit, row, child, fk, vacating, cache)
            }
        }
    }

    private suspend fun triggers(state: RuntimeState, unit: String, row: ApplySurvivor, cache: StatementCache) {
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

    private suspend fun incoming(state: RuntimeState, unit: String, row: ApplySurvivor, child: String, id: Long, vacating: Boolean, cache: StatementCache) {
        val count = checkNotNull(one("SELECT count(*),COALESCE(sum(length(\"from\")+length(COALESCE(\"to\",''))+length(on_delete)+length(on_update)),0) FROM pragma_foreign_key_list(?) WHERE id=?",
            child, id, cache = cache) { it.getLong(0) to it.getLong(1) })
        workspace.check(unit, WorkspaceCharge(16 + count.first * 8, 4096 + workspace.scale(unit,count.second,16)), statementCache = cache)
        val pk = checkNotNull(state.validated.tableInfoByName.getValue(row.table).primaryKey)
        val refs = mutableListOf<String>(); val joins = mutableListOf<String>()
        var delete = ""; var update = ""
        db.withPreparedStatement("SELECT \"from\",\"to\",on_delete,on_update FROM pragma_foreign_key_list(?) WHERE id=? ORDER BY seq", cache) { st ->
            st.bindText(1, child); st.bindLong(2,id)
            while (st.step()) {
                val from = st.getText(0); val to = if (st.isNull(1)) pk.name else st.getText(1)
                joins += "p.${quoteIdent(to)}=c.${quoteIdent(from)}"; refs += to
                delete = st.getText(2); update = st.getText(3)
            }
        }
        val physical = if (pk.declaredType.contains("BLOB",ignoreCase = true)) decodeLocalUuidBytes(row.pk) else row.pk
        val present = one("SELECT EXISTS(SELECT 1 FROM ${quoteIdent(child)} c JOIN ${quoteIdent(row.table)} p ON ${joins.joinToString(" AND ")} WHERE p.${quoteIdent(pk.name)}=?)",
            physical, cache = cache) { it.getLong(0) != 0L } == true
        if (!present) return
        if (row.payload == null || (vacating && delete != "NO ACTION") || (!vacating && update !in setOf("NO ACTION","RESTRICT"))) blocked(row)
        val payload = json.parseToJsonElement(checkNotNull(row.payload)).jsonObject
        val source = if (row.wire) PayloadSource.AUTHORITATIVE_WIRE else PayloadSource.LOCAL_STATE
        val terms = refs.joinToString(" AND ") { "${quoteIdent(it)} IS ?" }
        val same = db.withPreparedStatement("SELECT $terms FROM ${quoteIdent(row.table)} WHERE ${quoteIdent(pk.name)}=?", cache) { st ->
            refs.forEachIndexed { index, column -> OversqliteValueCodec.bindPayloadValue(st,index+1,state.validated.tableInfoByName.getValue(row.table).column(column), payload.getValue(column.lowercase()),source) }
            if (physical is ByteArray) st.bindBlob(refs.size+1,physical) else st.bindText(refs.size+1, row.pk)
            st.step() && st.getLong(0) != 0L
        }
        if (!same) blocked(row)
    }

    private suspend fun nextUnmanaged(state: RuntimeState, last: String, cache: StatementCache): String? {
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
