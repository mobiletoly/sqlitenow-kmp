package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.serialization.json.*

/** Incoming local-only writes do not advance managed dirty revisions. */
internal class OversqliteRecoveryGuard(
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
    private val operations: OversqliteClientOperationController,
) {
    private val workspace=OversqliteWorkspace(db,config)
    private suspend fun work(unit: String,n: Long) = operations.reserveSyncWork(unit,n,readyAtomic=true)
    suspend fun dependencies(tableFilter: String): String {
        val (count,bytes)=db.withPreparedStatement("SELECT count(*),COALESCE(sum(length(CAST(name AS BLOB))),0) FROM sqlite_schema") {st->check(st.step());st.getLong(0) to st.getLong(1)}
        val catalog=WorkspaceCharge(16+4*count,4096+16*bytes+32*count)
        workspace.check("recovery-dependencies",catalog)
        var remaining=count;var levels=0L
        while(remaining>0){levels++;remaining=remaining ushr 1}
        work("recovery-dependency-catalog",snapshotWorkAdd(snapshotWorkScale(count,128L*(1+config.syncTables.size)+16*levels),snapshotWorkScale(bytes,16*(levels+1))))
        val tables=mutableListOf<String>()
        db.withPreparedStatement("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'") {st->while(st.step())tables+=st.getText(0)}
        tables.sort()
        val hash=Sha256Accumulator()
        for(table in tables) {
            if(table.startsWith("_sync_") || config.syncTables.any {it.tableName.equals(table,true)})continue
            if(!relevant(table,tableFilter,catalog))continue
            val (rows,digest)=rows(table,catalog)
            hash.append(buildJsonArray {add(table);add(rows);add(digest)}.toString().encodeToByteArray())
        }
        return hash.finishHex()
    }
    private suspend fun relevant(child:String,table:String,catalog:WorkspaceCharge):Boolean {
        val (count,bytes)=db.withPreparedStatement("""SELECT count(*),COALESCE(sum(length(CAST("table" AS BLOB))),0) FROM pragma_foreign_key_list(?)""") {st->st.bindText(1,child);check(st.step());st.getLong(0) to st.getLong(1)}
        workspace.check("recovery-dependency-foreign-keys",WorkspaceCharge(catalog.entries+8,catalog.bytes+4096+16*bytes))
        work("recovery-dependency-catalog",snapshotWorkScale(count,128L*(1+config.syncTables.size)))
        return db.withPreparedStatement("""SELECT "table" FROM pragma_foreign_key_list(?)""") {st->
            st.bindText(1,child);var relevant=false
            while(st.step()) {val target=st.getText(0);if(table.isEmpty() || target.equals(table,true))relevant=relevant || config.syncTables.any {it.tableName.equals(target,true)}}
            relevant
        }
    }
    private suspend fun rows(table: String,catalog:WorkspaceCharge): Pair<Long,String> {
        val (count,metadata)=db.withPreparedStatement("SELECT count(*),COALESCE(sum(length(CAST(name AS BLOB))),0) FROM pragma_table_info(?)") { st->st.bindText(1,table);check(st.step());st.getLong(0) to st.getLong(1) }
        workspace.check("recovery-dependency-columns",WorkspaceCharge(catalog.entries+16+count*8,catalog.bytes+4096+4096*count+256*metadata))
        var remaining=count;var levels=0L
        while(remaining>0){levels++;remaining=remaining ushr 1}
        work("recovery-dependency-column-descriptions",snapshotWorkAdd(snapshotWorkScale(count,128+16*levels),snapshotWorkScale(metadata,64)))
        val columns=mutableListOf<String>()
        db.withPreparedStatement("SELECT name FROM pragma_table_info(?) ORDER BY cid") { st->st.bindText(1,table);while(st.step())columns+=st.getText(0) }
        val n=db.scalarLong("SELECT count(*) FROM ${quoteIdent(table)}")
        work("recovery-dependency-visits",snapshotWorkScale(n,128+16*count))
        val sizes=columns.joinToString("+") { "COALESCE(length(CAST(${quoteIdent(it)} AS BLOB)),0)" }
        val (total,maxRow)=db.withPreparedStatement("SELECT COALESCE(sum($sizes),0),COALESCE(max($sizes),0) FROM ${quoteIdent(table)}") { st->check(st.step());st.getLong(0) to st.getLong(1) }
        workspace.check("recovery-dependency-host",WorkspaceCharge(catalog.entries+32+8*count,catalog.bytes+8192+4096*count+256*metadata+32*maxRow))
        work("recovery-dependency-bytes",snapshotWorkScale(total,2))
        val selected=columns.joinToString(",") { name->val c=quoteIdent(name);"typeof($c),CASE WHEN typeof($c)='blob' THEN hex($c) WHEN typeof($c)='real' THEN printf('%!.26g',$c) ELSE CAST($c AS TEXT) END" }
        val sum=IntArray(32)
        db.withPreparedStatement("SELECT $selected FROM ${quoteIdent(table)}") { st->
            while(st.step()) {
                val row=buildJsonArray {repeat(columns.size*2) { add(if(st.isNull(it))JsonNull else JsonPrimitive(st.getText(it))) } }
                val digest=sha256Hex(row.toString().encodeToByteArray());var carry=0
                for(index in 31 downTo 0) {val value=sum[index]+digest.substring(index*2,index*2+2).toInt(16)+carry;sum[index]=value and 255;carry=value ushr 8}
            }
        }
        return n to sum.joinToString("") {it.toString(16).padStart(2,'0')}
    }
}
