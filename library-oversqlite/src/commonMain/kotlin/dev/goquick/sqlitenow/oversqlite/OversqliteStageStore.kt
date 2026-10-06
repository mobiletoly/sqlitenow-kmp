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

import androidx.sqlite.SQLiteStatement
import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.decodeFromString
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonObject

internal class OversqliteStageStore(
    internal val db: SafeSQLiteConnection,
    private val localStore: OversqliteLocalStore,
    private val syncStateStore: OversqliteSyncStateStore,
    private val json: Json,
    private val config: OversqliteConfig,
    private val operations: OversqliteClientOperationController,
) {
    internal var snapshotStatementCloseForTest: ((SQLiteStatement) -> Unit)? = null
    internal var reusableStatementCleanupForTest: ReusableStatementCleanup? = null

    private suspend fun <T> planOne(sql: String, vararg args: Any?, read: (SQLiteStatement) -> T): T? =
        db.withPreparedStatement(sql) { st ->
            bindPlan(st, args)
            if (st.step()) read(st) else null
        }

    private suspend fun planExec(sql: String, vararg args: Any?) {
        db.withPreparedStatement(sql) { st -> bindPlan(st, args); st.step() }
    }

    private fun bindPlan(st: SQLiteStatement, args: Array<out Any?>) {
        args.forEachIndexed { index, value -> when (value) {
            null -> st.bindNull(index + 1)
            is String -> st.bindText(index + 1, value)
            is Long -> st.bindLong(index + 1, value)
            is Int -> st.bindLong(index + 1, value.toLong())
            else -> error("invalid push-plan SQL argument")
        } }
    }

    private suspend fun loadPushPlan(forCleanup: Boolean = false): PushPlan? {
        val scalar = planOne("SELECT unit,guard,phase,entries,bytes,length(CAST(state AS BLOB)) FROM _sync_push_plan WHERE singleton_key=1") {
            PushPlan(it.getText(0), it.getText(1), phase=it.getText(2), entries=it.getLong(3), bytes=it.getLong(4)) to it.getLong(5)
        } ?: return null
        val p=scalar.first
        if(forCleanup || p.phase=="clear") {
            p.cleanupDiagnosticId=planOne("SELECT COALESCE(json_extract(state,'$.cleanupDiagnosticId'),'') FROM _sync_push_plan WHERE singleton_key=1") { it.getText(0) } ?: ""
            return p
        }
        pushSpace(p,1,checkedPushMultiply(scalar.second,12,p))
        return checkNotNull(planOne("SELECT state FROM _sync_push_plan WHERE singleton_key=1") { st ->
            json.decodeFromString<PushPlan>(st.getText(0)).also {
                check(it.unit==p.unit && it.guard==p.guard) { "invalid push descriptor binding" }
                it.phase=p.phase;it.entries=p.entries;it.bytes=p.bytes
            }
        })
    }

    private suspend fun savePushPlan(p: PushPlan) {
        planExec("""INSERT INTO _sync_push_plan VALUES(1,?,?,?,?,?,?)
            ON CONFLICT(singleton_key) DO UPDATE SET unit=excluded.unit,guard=excluded.guard,
            phase=excluded.phase,state=excluded.state,entries=excluded.entries,bytes=excluded.bytes""",
            p.unit,p.guard,p.phase,json.encodeToString(p),p.entries,p.bytes)
    }

    private suspend fun pushNode(p: PushPlan, ordinal: Long): PushNode {
        val size = planOne("SELECT charge FROM _sync_push_nodes WHERE unit=? AND ordinal=?",p.unit,ordinal) { it.getLong(0) }
            ?: error("missing guarded push node")
        pushSpace(p,2,checkedPushMultiply(size,2,p))
        return checkNotNull(planOne("""SELECT ordinal,schema_name,table_name,key_json,status,adjacency_cursor,
            blocked,diagnostic_id,intent_revision,dirty_ordinal,adjacency_phase,dfs_index,lowlink,parent,
            stack_ordinal,edge_cursor,component,selected FROM _sync_push_nodes WHERE unit=? AND ordinal=?""",p.unit,ordinal) {
            PushNode(it.getLong(0),it.getText(1),it.getText(2),it.getText(3),it.getText(4),it.getText(5),
                it.getText(6),it.getText(7),it.getLong(8),it.getLong(9),it.getLong(10),it.getLong(11),
                it.getLong(12),it.getLong(13),it.getLong(14),it.getLong(15),it.getLong(16),it.getLong(17))
        })
    }

    private fun checkedPushAdd(a: Long,b: Long,p: PushPlan): Long {
        if (a<0 || b<0 || b>Long.MAX_VALUE-a) throw SyncWorkspaceLimitException(p.unit,"bytes",config.maxSyncWorkspaceBytes,a,b,false)
        return a+b
    }
    private fun checkedPushMultiply(a: Long,b: Long,p: PushPlan): Long {
        if (a<0 || b<0 || (b!=0L && a>Long.MAX_VALUE/b)) throw SyncWorkspaceLimitException(p.unit,"bytes",config.maxSyncWorkspaceBytes,a,Long.MAX_VALUE,false)
        return a*b
    }
    private suspend fun pushSpace(p: PushPlan, extraEntries: Long=0,extraBytes: Long=0) {
        val workspace = OversqliteWorkspace(db, config)
        var descriptor = workspace.add(p.unit, WorkspaceCharge(p.entries,p.bytes), WorkspaceCharge(1,2048))
        descriptor = workspace.add(p.unit, descriptor, WorkspaceCharge(bytes = workspace.scale(p.unit,
            utf8Size(p.unit)+utf8Size(p.guard)+utf8Size(p.seedSchema)+utf8Size(p.seedTable)+utf8Size(p.seedKey)+utf8Size(p.initializationId),6)))
        workspace.check(p.unit,WorkspaceCharge(extraEntries,extraBytes),push=descriptor)
    }

    private fun pushStepWork(state: RuntimeState,p: PushPlan): Long {
        var work=32L
        for(table in state.validated.tables) {
            val info=state.validated.tableInfoByName.getValue(table.tableName)
            work=checkedPushAdd(work,checkedPushAdd(4,checkedPushAdd(checkedPushMultiply(info.columns.size.toLong(),16,p),checkedPushMultiply(info.foreignKeys.size.toLong(),32,p),p),p),p)
        }
        return work
    }

    private suspend fun pushGuard(state: RuntimeState,p: PushPlan): Triple<String,Long,String> {
        var bytes=checkNotNull(planOne("""SELECT 8192+6*(length(CAST(i.database_id AS BLOB))+length(CAST(i.service_id AS BLOB))+
            length(CAST(i.physical_shape AS BLOB))+length(CAST(i.fingerprint AS BLOB))+length(CAST(a.current_source_id AS BLOB))+
            length(CAST(a.attached_user_id AS BLOB))+length(CAST(a.pending_initialization_id AS BLOB))+length(CAST(o.source_id AS BLOB))+
            length(CAST(o.canonical_request_hash AS BLOB))) FROM _sync_contract_identity i CROSS JOIN _sync_attachment_state a CROSS JOIN _sync_outbox_bundle o""") { it.getLong(0) })
        var entries=32L
        for(table in state.validated.tables) {
            val info=state.validated.tableInfoByName.getValue(table.tableName)
            entries=checkedPushAdd(entries,checkedPushAdd(4,checkedPushMultiply(info.foreignKeys.size.toLong(),8,p),p),p)
            bytes=checkedPushAdd(bytes,checkedPushAdd(512,checkedPushMultiply(checkedPushAdd(utf8Size(table.tableName),utf8Size(table.syncKeyColumnName),p),12,p),p),p)
            for(key in state.validated.keyByTable.getValue(table.tableName)) {
                entries=checkedPushAdd(entries,2,p)
                bytes=checkedPushAdd(bytes,checkedPushAdd(64,checkedPushMultiply(utf8Size(key),12,p),p),p)
            }
            for(fk in info.foreignKeys) {
                val names=checkedPushAdd(checkedPushAdd(utf8Size(fk.refTable),utf8Size(fk.fromCol),p),checkedPushAdd(utf8Size(fk.toCol),checkedPushAdd(utf8Size(fk.onUpdate),utf8Size(fk.onDelete),p),p),p)
                bytes=checkedPushAdd(bytes,checkedPushAdd(512,checkedPushMultiply(names,12,p),p),p)
            }
        }
        pushSpace(p,entries,bytes)
        return checkNotNull(planOne("""SELECT i.database_id,i.service_id,i.generation_id,i.local_version,i.physical_shape,
            i.fingerprint,a.attachment_epoch,a.current_source_id,a.binding_state,a.attached_user_id,
            a.pending_initialization_id,a.last_bundle_seq_seen,f.dirty_revision,f.baseline_revision,
            s.next_source_bundle_id,o.state,o.source_id,o.source_bundle_id,o.canonical_request_hash
            FROM _sync_contract_identity i CROSS JOIN _sync_attachment_state a CROSS JOIN _sync_conflict_state f
            JOIN _sync_source_state s ON s.source_id=a.current_source_id CROSS JOIN _sync_outbox_bundle o""") { st ->
            val numeric=setOf(2,3,6,11,12,13,14,17)
            val context=buildJsonArray {
                for (i in 0..18) add(if(i in numeric) JsonPrimitive(st.getLong(i)) else JsonPrimitive(st.getText(i)))
                add(JsonPrimitive(state.admittedContract.limits.toString()))
                add(buildJsonArray {
                    for (table in state.validated.tables) {
                        add(buildJsonArray {
                            add(JsonPrimitive(table.tableName));add(JsonPrimitive(table.syncKeyColumnName))
                            add(buildJsonArray { for(key in state.validated.keyByTable.getValue(table.tableName)) add(JsonPrimitive(key)) })
                            add(buildJsonArray {
                                for(fk in state.validated.tableInfoByName.getValue(table.tableName).foreignKeys) add(buildJsonArray {
                                    add(JsonPrimitive(fk.seq));add(JsonPrimitive(fk.refTable));add(JsonPrimitive(fk.fromCol));add(JsonPrimitive(fk.toCol))
                                    add(JsonPrimitive(fk.onUpdate));add(JsonPrimitive(fk.onDelete));add(JsonPrimitive(fk.match))
                                })
                            })
                        })
                    }
                })
            }
            Triple(sha256Hex(context.toString().encodeToByteArray()),st.getLong(14),st.getText(10))
        })
    }

    @OptIn(kotlin.uuid.ExperimentalUuidApi::class)
    private fun newPushUnit(): String = sha256Hex((kotlin.uuid.Uuid.random().toString()+kotlin.uuid.Uuid.random().toString()).encodeToByteArray())

    private suspend fun clearPushStep(p: PushPlan): Boolean {
        for(table in listOf("_sync_push_edges","_sync_push_nodes")) {
            val row=planOne("SELECT ordinal,charge FROM $table WHERE unit=? ORDER BY ordinal LIMIT 1",p.unit) { it.getLong(0) to it.getLong(1) } ?: continue
            planExec("DELETE FROM $table WHERE unit=? AND ordinal=?",p.unit,row.first)
            p.entries--;p.bytes-=row.second;savePushPlan(p);return false
        }
        planExec("DELETE FROM _sync_push_plan WHERE singleton_key=1");return true
    }

    private suspend fun loadPushCapture(state: RuntimeState,p: PushPlan,schema: String,table: String,key: String): DirtyRowCapture {
        val size=checkNotNull(planOne("""SELECT 2048+64*(length(CAST(COALESCE(payload,'') AS BLOB))+
            length(CAST(COALESCE(base_payload,'') AS BLOB))+length(CAST(base_provenance AS BLOB))+
            length(CAST(key_json AS BLOB))) FROM _sync_dirty_rows WHERE schema_name=? AND table_name=? AND key_json=?""",schema,table,key) { it.getLong(0) })
        pushSpace(p,4,size)
        val row=checkNotNull(planOne("""SELECT d.schema_name,d.table_name,d.key_json,d.op,d.base_row_version,d.payload,
            d.dirty_ordinal,CASE WHEN rs.server_kind IN ('live','deleted') THEN 1 ELSE 0 END,
            COALESCE(rs.server_kind='deleted',0),d.base_kind,d.base_payload,d.base_provenance,COALESCE(rs.server_kind,'unknown')
            FROM _sync_dirty_rows d LEFT JOIN _sync_row_state rs ON rs.schema_name=d.schema_name AND
            rs.table_name=d.table_name AND rs.key_json=d.key_json WHERE d.schema_name=? AND d.table_name=? AND d.key_json=?""",
            schema,table,key) { it.toPushSnapshotRow() })
        val (pk,wire)=localStore.decodeDirtyKeyForPush(state,table,key)
        val op=if(row.op=="DELETE") "DELETE" else if(row.serverKind=="live") "UPDATE" else "INSERT"
        return DirtyRowCapture(schema,table,key,pk,wire,op,row.baseRowVersion,row.payload,row.dirtyOrdinal,row.base)
    }

    private fun SQLiteStatement.toPushSnapshotRow(): DirtySnapshotRow = DirtySnapshotRow(
        getText(0),getText(1),getText(2),getText(3),getLong(4),if(isNull(5)) null else getText(5),
        getLong(6),getLong(7)==1L,getLong(8)==1L,
        HistoricalBase(getText(9),if(isNull(10)) null else getText(10),getText(11)),getText(12),
    )

    private suspend fun seedPushStep(state: RuntimeState,p: PushPlan) {
        val args=arrayOf<Any?>(p.seedOrdinal,p.seedSchema,p.seedTable,p.seedKey)
        val tail=""" FROM _sync_dirty_rows d LEFT JOIN _sync_row_state rs ON rs.schema_name=d.schema_name AND
            rs.table_name=d.table_name AND rs.key_json=d.key_json WHERE (d.dirty_ordinal,d.schema_name,d.table_name,d.key_json)>(?,?,?,?)
            ORDER BY d.dirty_ordinal,d.schema_name,d.table_name,d.key_json LIMIT 1"""
        val size=planOne("SELECT 1024+12*(length(CAST(d.schema_name AS BLOB))+length(CAST(d.table_name AS BLOB))+length(CAST(d.key_json AS BLOB)))"+tail,*args) { it.getLong(0) }
        if(size==null) {p.subphase="adjacency";p.visit=1;return}
        pushSpace(p,2,checkedPushMultiply(size,2,p))
        val row=checkNotNull(planOne("SELECT d.schema_name,d.table_name,d.key_json,d.op,d.base_kind,COALESCE(rs.server_kind,'unknown'),d.dirty_ordinal"+tail,*args) {
            PushSeed(it.getText(0),it.getText(1),it.getText(2),it.getText(3),it.getText(4),it.getText(5),it.getLong(6))
        })
        if(row.op=="DELETE" && row.kind in setOf("unpublished","absent","deleted")) {
            val capture=loadPushCapture(state,p,row.schema,row.table,row.key)
            localStore.validateHistoricalBase(row.table,capture.baseRowVersion,capture.base)
            syncStateStore.deleteDirtyRow(row.schema,row.table,row.key)
            p.phase="clear";return
        }
        var blocked=if(row.kind=="unknown") "relationship_unknown" else ""
        if(row.base=="unavailable" || row.base=="") blocked="base"
        p.nextNode=checkedPushAdd(p.nextNode,1,p)
        val revision=checkNotNull(planOne("SELECT dirty_revision FROM _sync_conflict_state WHERE singleton_key=1") { it.getLong(0) })
        planExec("""INSERT INTO _sync_push_nodes(unit,schema_name,table_name,key_json,ordinal,intent_revision,
            dirty_ordinal,status,blocked,charge) VALUES(?,?,?,?,?,?,?,'discover',?,?)""",
            p.unit,row.schema,row.table,row.key,p.nextNode,revision,row.ordinal,blocked,size)
        p.entries=checkedPushAdd(p.entries,1,p);p.bytes=checkedPushAdd(p.bytes,size,p)
        p.seedOrdinal=row.ordinal;p.seedSchema=row.schema;p.seedTable=row.table;p.seedKey=row.key
    }

    private suspend fun blockPushNode(p: PushPlan,n: PushNode,reason: String) {
        planExec("UPDATE _sync_push_nodes SET blocked=CASE WHEN blocked='' THEN ? ELSE blocked END WHERE unit=? AND ordinal=?",reason,p.unit,n.ordinal)
    }

    private suspend fun putPushEdge(p: PushPlan,dependent: Long,prerequisite: Long) {
        if(planOne("SELECT 1 FROM _sync_push_edges WHERE unit=? AND dependent=? AND prerequisite=?",p.unit,dependent,prerequisite) { it.getLong(0) }!=null) return
        pushSpace(p,1,256);p.nextEdge=checkedPushAdd(p.nextEdge,1,p)
        planExec("INSERT INTO _sync_push_edges VALUES(?,?,?,?,256)",p.unit,dependent,prerequisite,p.nextEdge)
        p.entries=checkedPushAdd(p.entries,1,p);p.bytes=checkedPushAdd(p.bytes,256,p)
    }

    private suspend fun updatePushCursor(p: PushPlan,n: PushNode,cursor: String) {
        val delta=6*(utf8Size(cursor)-utf8Size(n.cursor))
        if(delta>0) pushSpace(p,0,delta)
        planExec("UPDATE _sync_push_nodes SET adjacency_cursor=?,charge=charge+? WHERE unit=? AND ordinal=?",cursor,delta,p.unit,n.ordinal)
        p.bytes+=delta
    }
    private suspend fun finishPushDiscovery(p: PushPlan,n: PushNode) {
        planExec("UPDATE _sync_push_nodes SET status='scc' WHERE unit=? AND ordinal=?",p.unit,n.ordinal);p.visit++
    }

    private suspend fun discoverPushStep(state: RuntimeState,p: PushPlan) {
        if(p.visit>p.nextNode) {p.phase="scc";p.subphase="walk";p.visit=0;return}
        val n=pushNode(p,p.visit)
        val op=checkNotNull(planOne("SELECT op FROM _sync_dirty_rows WHERE schema_name=? AND table_name=? AND key_json=?",n.schema,n.table,n.key) { it.getText(0) })
        if(n.adjacencyPhase==0L) {
            val capture=loadPushCapture(state,p,n.schema,n.table,n.key)
            try {localStore.validateHistoricalBase(n.table,capture.baseRowVersion,capture.base)}
            catch(missing: ResolutionBaseUnavailableException) {blockPushNode(p,n,"base")}
            if(n.blocked!="relationship_unknown") {
                try {
                    localStore.validateBaselineProjection(n.schema,n.table,n.key,"server",null)
                    localStore.validateBaselineProjection(n.schema,n.table,n.key,"desired",if(op=="DELETE") null else capture.localPayload)
                } catch(invalid: IllegalStateException) {blockPushNode(p,n,"historical_repair_required")}
            }
            if(op=="DELETE") for((table,info) in state.validated.tableInfoByName) {
                if(info.foreignKeys.any { it.refTable==n.table } &&
                    planOne("SELECT 1 FROM _sync_row_state WHERE relationships_known=0 AND table_name=? LIMIT 1",table) { it.getLong(0) }!=null)
                    blockPushNode(p,n,"relationship_unknown")
            }
            planExec("UPDATE _sync_push_nodes SET adjacency_phase=1 WHERE unit=? AND ordinal=?",p.unit,n.ordinal);return
        }
        if(op=="DELETE") discoverPushChild(p,n) else discoverPushParent(p,n)
    }

    private suspend fun discoverPushParent(p: PushPlan,n: PushNode) {
        val args=arrayOf<Any?>(n.schema,n.table,n.key,n.cursor)
        val tail=" FROM _sync_relationships WHERE view='desired' AND child_schema=? AND child_table=? AND child_key=? AND fk_column>? ORDER BY fk_column LIMIT 1"
        val size=planOne("SELECT 1024+12*(length(CAST(parent_schema AS BLOB))+length(CAST(parent_table AS BLOB))+length(CAST(parent_key AS BLOB))+length(CAST(fk_column AS BLOB)))"+tail,*args) { it.getLong(0) }
        if(size==null) {finishPushDiscovery(p,n);return};pushSpace(p,3,size)
        val ref=checkNotNull(planOne("SELECT parent_schema,parent_table,parent_key,fk_column"+tail,*args) { PushRelationship(it.getText(0),it.getText(1),it.getText(2),it.getText(3)) })
        val parent=checkNotNull(planOne("""SELECT COALESCE(rs.server_kind,'unknown'),COALESCE(d.op,''),COALESCE(pn.ordinal,0)
            FROM (SELECT 1) LEFT JOIN _sync_row_state rs ON rs.schema_name=? AND rs.table_name=? AND rs.key_json=?
            LEFT JOIN _sync_dirty_rows d ON d.schema_name=? AND d.table_name=? AND d.key_json=?
            LEFT JOIN _sync_push_nodes pn ON pn.unit=? AND pn.schema_name=? AND pn.table_name=? AND pn.key_json=?""",
            ref.schema,ref.table,ref.key,ref.schema,ref.table,ref.key,p.unit,ref.schema,ref.table,ref.key) { Triple(it.getText(0),it.getText(1),it.getLong(2)) })
        when {
            parent.first=="unknown" -> blockPushNode(p,n,"relationship_unknown")
            parent.second=="DELETE" -> blockPushNode(p,n,"dependency_unavailable")
            parent.first=="live" -> Unit
            parent.third!=0L && parent.second!="" -> putPushEdge(p,n.ordinal,parent.third)
            else -> blockPushNode(p,n,"dependency_unavailable")
        }
        updatePushCursor(p,n,ref.column)
    }

    private suspend fun discoverPushChild(p: PushPlan,n: PushNode) {
        val view=if(n.adjacencyPhase==2L) "desired" else "server"
        val cursor=if(n.cursor=="") listOf("","","","") else json.decodeFromString<List<String>>(n.cursor)
        check(cursor.size==4) { "invalid push adjacency cursor" }
        val args=arrayOf<Any?>(view,n.schema,n.table,n.key,*cursor.toTypedArray())
        val tail=""" FROM _sync_relationships WHERE view=? AND parent_schema=? AND parent_table=? AND parent_key=?
            AND (child_schema,child_table,child_key,fk_column)>(?,?,?,?) ORDER BY child_schema,child_table,child_key,fk_column LIMIT 1"""
        val size=planOne("SELECT 1024+24*(length(CAST(child_schema AS BLOB))+length(CAST(child_table AS BLOB))+length(CAST(child_key AS BLOB))+length(CAST(fk_column AS BLOB)))"+tail,*args) { it.getLong(0) }
        if(size==null) {
            if(n.adjacencyPhase==2L) finishPushDiscovery(p,n) else {
                updatePushCursor(p,n,"");planExec("UPDATE _sync_push_nodes SET adjacency_phase=2 WHERE unit=? AND ordinal=?",p.unit,n.ordinal)
            };return
        }
        pushSpace(p,4,size)
        val ref=checkNotNull(planOne("SELECT child_schema,child_table,child_key,fk_column"+tail,*args) { PushRelationship(it.getText(0),it.getText(1),it.getText(2),it.getText(3)) })
        if(view=="desired") blockPushNode(p,n,"dependency_unavailable") else {
            val child=planOne("""SELECT pn.ordinal FROM _sync_push_nodes pn JOIN _sync_dirty_rows d
                ON d.schema_name=pn.schema_name AND d.table_name=pn.table_name AND d.key_json=pn.key_json
                WHERE pn.unit=? AND pn.schema_name=? AND pn.table_name=? AND pn.key_json=?""",p.unit,ref.schema,ref.table,ref.key) { it.getLong(0) }
            val still=planOne("""SELECT 1 FROM _sync_relationships WHERE view='desired' AND child_schema=? AND child_table=?
                AND child_key=? AND fk_column=? AND parent_schema=? AND parent_table=? AND parent_key=?""",
                ref.schema,ref.table,ref.key,ref.column,n.schema,n.table,n.key) { it.getLong(0) }
            if(child==null || still!=null) blockPushNode(p,n,"dependency_unavailable") else putPushEdge(p,n.ordinal,child)
        }
        updatePushCursor(p,n,json.encodeToString(listOf(ref.schema,ref.table,ref.key,ref.column)))
    }

    private suspend fun enterPushDfs(p: PushPlan,ordinal: Long) {
        p.index=checkedPushAdd(p.index,1,p);p.stack=checkedPushAdd(p.stack,1,p)
        planExec("UPDATE _sync_push_nodes SET status='walking',dfs_index=?,lowlink=?,parent=?,on_stack=1,stack_ordinal=? WHERE unit=? AND ordinal=?",
            p.index,p.index,p.frame,p.stack,p.unit,ordinal)
        p.frame=ordinal
    }
    private suspend fun finishPushFrame(p: PushPlan,n: PushNode) {
        if(n.parent!=0L) planExec("UPDATE _sync_push_nodes SET lowlink=MIN(lowlink,?) WHERE unit=? AND ordinal=?",n.lowlink,p.unit,n.parent)
        p.frame=n.parent
    }
    private suspend fun sccPushStep(p: PushPlan) {
        if(p.subphase=="pop") {popPushScc(p);return}
        if(p.frame==0L) {
            val ordinal=planOne("SELECT ordinal FROM _sync_push_nodes WHERE unit=? AND status='scc' ORDER BY ordinal LIMIT 1",p.unit) { it.getLong(0) }
            if(ordinal==null) {p.phase="pack";p.subphase="counts";p.cursor=0} else enterPushDfs(p,ordinal)
            return
        }
        val n=pushNode(p,p.frame)
        val edge=planOne("SELECT ordinal,prerequisite FROM _sync_push_edges WHERE unit=? AND dependent=? AND ordinal>? ORDER BY ordinal LIMIT 1",p.unit,n.ordinal,n.edgeCursor) { it.getLong(0) to it.getLong(1) }
        if(edge==null) {
            if(n.index==n.lowlink) {p.subphase="pop";p.pop=n.ordinal} else {
                planExec("UPDATE _sync_push_nodes SET status='visited' WHERE unit=? AND ordinal=?",p.unit,n.ordinal);finishPushFrame(p,n)
            };return
        }
        planExec("UPDATE _sync_push_nodes SET edge_cursor=? WHERE unit=? AND ordinal=?",edge.first,p.unit,n.ordinal)
        val peer=pushNode(p,edge.second)
        if(peer.index==0L) {enterPushDfs(p,peer.ordinal);return}
        val onStack=checkNotNull(planOne("SELECT on_stack FROM _sync_push_nodes WHERE unit=? AND ordinal=?",p.unit,peer.ordinal) { it.getLong(0) })
        if(onStack==1L) planExec("UPDATE _sync_push_nodes SET lowlink=MIN(lowlink,?) WHERE unit=? AND ordinal=?",peer.index,p.unit,n.ordinal)
    }
    private suspend fun popPushScc(p: PushPlan) {
        val ordinal=checkNotNull(planOne("SELECT ordinal FROM _sync_push_nodes WHERE unit=? AND on_stack=1 ORDER BY stack_ordinal DESC LIMIT 1",p.unit) { it.getLong(0) })
        val n=pushNode(p,ordinal)
        planExec("UPDATE _sync_push_nodes SET on_stack=0,component=?,status='member' WHERE unit=? AND ordinal=?",p.pop,p.unit,ordinal)
        val replacement=checkNotNull(planOne("""SELECT ready_ordinal=0 OR (?,?,?,?)<(ready_ordinal,ready_schema,ready_table,ready_key),
            6*(?-length(CAST(ready_schema AS BLOB))-length(CAST(ready_table AS BLOB))-length(CAST(ready_key AS BLOB)))
            FROM _sync_push_nodes WHERE unit=? AND ordinal=?""",n.dirtyOrdinal,n.schema,n.table,n.key,
            utf8Size(n.schema)+utf8Size(n.table)+utf8Size(n.key),p.unit,p.pop) { (it.getLong(0)!=0L) to it.getLong(1) })
        if(replacement.first) {
            if(replacement.second>0) pushSpace(p,0,replacement.second)
            planExec("UPDATE _sync_push_nodes SET ready_ordinal=?,ready_schema=?,ready_table=?,ready_key=?,charge=charge+? WHERE unit=? AND ordinal=?",
                n.dirtyOrdinal,n.schema,n.table,n.key,replacement.second,p.unit,p.pop)
            p.bytes+=replacement.second
        }
        planExec("UPDATE _sync_push_nodes SET component_rows=component_rows+1,blocked=CASE WHEN blocked='' THEN ? ELSE blocked END WHERE unit=? AND ordinal=?",n.blocked,p.unit,p.pop)
        if(ordinal==p.pop) {p.subphase="walk";finishPushFrame(p,pushNode(p,p.pop))}
    }
    private fun comparePushRef(aSchema: String,aTable: String,aKey: String,bSchema: String,bTable: String,bKey: String): Int {
        // Canonical identity ordering is UTF-8 bytewise, matching SQLite BINARY collation.
        for((a,b) in listOf(aSchema to bSchema,aTable to bTable,aKey to bKey)) {
            val ab=a.encodeToByteArray();val bb=b.encodeToByteArray()
            for(i in 0 until minOf(ab.size,bb.size)) {
                val delta=(ab[i].toInt() and 255)-(bb[i].toInt() and 255);if(delta!=0) return delta
            }
            if(ab.size!=bb.size) return ab.size.compareTo(bb.size)
        }
        return 0
    }
    private suspend fun countPushDagStep(p: PushPlan) {
        val edge=planOne("""SELECT e.ordinal,d.component,r.component FROM _sync_push_edges e
            JOIN _sync_push_nodes d ON d.unit=e.unit AND d.ordinal=e.dependent
            JOIN _sync_push_nodes r ON r.unit=e.unit AND r.ordinal=e.prerequisite
            WHERE e.unit=? AND e.ordinal>? ORDER BY e.ordinal LIMIT 1""",p.unit,p.cursor) { Triple(it.getLong(0),it.getLong(1),it.getLong(2)) }
        if(edge==null) {p.subphase="queue_init";p.cursor=0;return}
        if(edge.second!=edge.third) planExec("UPDATE _sync_push_nodes SET prerequisites=prerequisites+1 WHERE unit=? AND ordinal=?",p.unit,edge.second)
        p.cursor=edge.first
    }
    private suspend fun initPushQueueStep(p: PushPlan) {
        if(p.cursor==p.nextNode) {p.subphase="choose";return};p.cursor++
        planExec("UPDATE _sync_push_nodes SET status=CASE WHEN prerequisites=0 THEN 'ready' ELSE 'waiting' END WHERE unit=? AND ordinal=? AND component=ordinal",p.unit,p.cursor)
    }
    private fun emptyPushEnvelopeBytes(p: PushPlan): Long = utf8Size(canonicalizeJsonElement(buildJsonObject {
        put("source_bundle_id",JsonPrimitive(p.sourceBundleId));put("canonical_request_hash",JsonPrimitive("0".repeat(64)))
        if(p.initializationId!="") put("initialization_id",JsonPrimitive(p.initializationId))
        put("rows",JsonArray(emptyList()))
    }))
    private fun addPushTuple(p: PushPlan,bytes: Long,count: Long,lastTable: Long,table: Long,tupleBytes: Long): Long {
        val extra=if(lastTable==table) tupleBytes+1 else {
            val group=JsonArray(listOf(JsonPrimitive(table),JsonPrimitive(count),JsonArray(emptyList())))
            tupleBytes+utf8Size(group.toString())+if(count==0L) 0 else 1
        }
        return checkedPushAdd(bytes,extra,p)
    }
    private suspend fun choosePushComponent(p: PushPlan) {
        val ordinal=planOne("SELECT ordinal FROM _sync_push_nodes WHERE unit=? AND status='ready' ORDER BY ready_ordinal,ready_schema,ready_table,ready_key LIMIT 1",p.unit) { it.getLong(0) }
        if(ordinal==null) {p.phase="ready";return}
        val n=pushNode(p,ordinal);p.current=ordinal;p.cursor=0;p.count=0;p.lastTable=-1
        if(n.blocked!="") {blockPushComponent(p,n.blocked);return}
        p.bodyBytes=emptyPushEnvelopeBytes(p)
        if(p.candidateRows==0L) {p.candidateBytes=p.bodyBytes;p.candidateLastTable=-1}
        p.measureCandidateBytes=p.candidateBytes;p.measureCandidateLastTable=p.candidateLastTable;p.subphase="measure"
    }
    private suspend fun blockPushComponent(p: PushPlan,reason: String) {
        if(p.blockedComponent==0L) {p.blockedComponent=p.current;p.blockedReason=reason} else {
            val earlier=checkNotNull(planOne("""SELECT (a.ready_ordinal,a.ready_schema,a.ready_table,a.ready_key)<(b.ready_ordinal,b.ready_schema,b.ready_table,b.ready_key)
                FROM _sync_push_nodes a JOIN _sync_push_nodes b ON b.unit=a.unit WHERE a.unit=? AND a.ordinal=? AND b.ordinal=?""",
                p.unit,p.current,p.blockedComponent) {it.getLong(0)!=0L})
            if(earlier) {p.blockedComponent=p.current;p.blockedReason=reason}
        }
        planExec("UPDATE _sync_push_nodes SET status='blocked',blocked=?,component_bytes=? WHERE unit=? AND ordinal=?",reason,p.bodyBytes,p.unit,p.current)
        p.cursor=0;p.count=0;p.subphase="diagnostic"
    }
    private suspend fun nextPushComponentMember(p: PushPlan): PushNode? {
        val ordinal=planOne("""SELECT n.ordinal FROM _sync_push_nodes n WHERE n.unit=? AND n.component=? AND
            (n.schema_name,n.table_name,n.key_json)>(
            COALESCE((SELECT schema_name FROM _sync_push_nodes WHERE unit=? AND ordinal=?),''),
            COALESCE((SELECT table_name FROM _sync_push_nodes WHERE unit=? AND ordinal=?),''),
            COALESCE((SELECT key_json FROM _sync_push_nodes WHERE unit=? AND ordinal=?),''))
            ORDER BY n.schema_name,n.table_name,n.key_json LIMIT 1""",p.unit,p.current,p.unit,p.cursor,p.unit,p.cursor,p.unit,p.cursor) { it.getLong(0) }
        return ordinal?.let { pushNode(p,it) }
    }
    private suspend fun measurePushComponentStep(state: RuntimeState,p: PushPlan) {
        val n=nextPushComponentMember(p)
        if(n==null) {finishPushMeasurement(state,p);return}
        val row=loadPushCapture(state,p,n.schema,n.table,n.key)
        try {localStore.validateHistoricalBase(row.tableName,row.baseRowVersion,row.base)}
        catch(missing: ResolutionBaseUnavailableException) {blockPushComponent(p,"base");return}
        for(view in listOf("server","desired")) localStore.validateBaselineProjection(row.schemaName,row.tableName,row.keyJson,view,row.localPayload)
        val payload=if(row.op=="DELETE") null else json.parseToJsonElement(canonicalizeJsonElement(localStore.processPayloadForUpload(row.tableName,row.localPayload ?: "")))
        val logical=PushRequestRow(row.schemaName,row.tableName,row.wireKey,row.op,row.baseRowVersion,payload)
        val expandedBytes = OversqliteCompactCodec.expandedRowDecodedBytes(logical)
        if(expandedBytes>state.admittedContract.limits.maxRowDecodedBytes) {
            p.bodyBytes=expandedBytes;blockPushComponent(p,"row");return
        }
        val encoded=OversqliteCompactCodec.encodePushRows(checkNotNull(state.validated.generation),listOf(logical))
        val group=encoded[0] as JsonArray;val table=(group[0] as JsonPrimitive).content.toLong()
        val tuple=(group[2] as JsonArray)[0];val tupleBytes=utf8Size(canonicalizeJsonElement(tuple))
        p.bodyBytes=addPushTuple(p,p.bodyBytes,p.count,p.lastTable,table,tupleBytes)
        p.measureCandidateBytes=addPushTuple(p,p.measureCandidateBytes,p.candidateRows+p.count,p.measureCandidateLastTable,table,tupleBytes)
        p.lastTable=table;p.measureCandidateLastTable=table;p.count++;p.cursor=n.ordinal
        planExec("UPDATE _sync_push_nodes SET diagnostic_ordinal=? WHERE unit=? AND ordinal=?",p.count,p.unit,n.ordinal)
    }
    private suspend fun finishPushMeasurement(state: RuntimeState,p: PushPlan) {
        val limits=state.admittedContract.limits
        val reason=when {
            p.count>1 && p.count>limits.maxPushRows -> "rows"
            p.count>1 && p.bodyBytes>limits.maxPushDecodedBytes -> "decoded_bytes"
            p.bodyBytes>limits.maxPushDecodedBytes -> "envelope"
            else -> ""
        }
        if(reason!="") {blockPushComponent(p,reason);return}
        val softRows=minOf(config.transferTargets.maxPushRows,limits.maxPushRows).toLong()
        val softBytes=minOf(config.transferTargets.maxPushDecodedBytes,limits.maxPushDecodedBytes)
        if(p.candidateRows>0 && (p.candidateRows+p.count>softRows || p.measureCandidateBytes>softBytes ||
                p.candidateRows+p.count>limits.maxPushRows || p.measureCandidateBytes>limits.maxPushDecodedBytes)) {
            p.phase="ready";return
        }
        planExec("UPDATE _sync_push_nodes SET component_bytes=?,status='selected' WHERE unit=? AND ordinal=?",p.bodyBytes,p.unit,p.current)
        p.candidateRows+=p.count;p.candidateBytes=p.measureCandidateBytes;p.candidateLastTable=p.measureCandidateLastTable
        p.cursor=0;p.subphase="mark"
    }
    private suspend fun markPushComponentStep(p: PushPlan,diagnostic: Boolean) {
        val n=nextPushComponentMember(p)
        if(n==null) {p.cursor=0;p.visit=0;p.subphase="propagate";return}
        if(diagnostic) {
            val root=pushNode(p,p.current)
            val id=if(root.blocked in setOf("rows","decoded_bytes")) sha256Hex("${p.unit}:${p.current}".encodeToByteArray()) else ""
            p.count++
            planExec("UPDATE _sync_push_nodes SET diagnostic_id=?,diagnostic_ordinal=? WHERE unit=? AND ordinal=?",id,p.count,p.unit,n.ordinal)
        } else {
            p.selectedNext++;planExec("UPDATE _sync_push_nodes SET selected=? WHERE unit=? AND ordinal=?",p.selectedNext,p.unit,n.ordinal)
        }
        p.cursor=n.ordinal
    }
    private suspend fun propagatePushComponentStep(p: PushPlan) {
        if(p.visit==0L) {
            val ordinal=planOne("SELECT ordinal FROM _sync_push_nodes WHERE unit=? AND component=? AND ordinal>? ORDER BY ordinal LIMIT 1",p.unit,p.current,p.cursor) { it.getLong(0) }
            if(ordinal==null) {p.subphase="choose";return};p.cursor=ordinal;p.visit=ordinal;p.count=0
        }
        val edge=planOne("""SELECT e.ordinal,n.component FROM _sync_push_edges e JOIN _sync_push_nodes n
            ON n.unit=e.unit AND n.ordinal=e.dependent WHERE e.unit=? AND e.prerequisite=? AND e.ordinal>? ORDER BY e.ordinal LIMIT 1""",p.unit,p.visit,p.count) { it.getLong(0) to it.getLong(1) }
        if(edge==null) {p.visit=0;return}
        if(edge.second!=p.current) {
            val root=pushNode(p,p.current);val reason=if(root.blocked=="") "" else "dependency_unavailable"
            planExec("""UPDATE _sync_push_nodes SET prerequisites=prerequisites-1,status=CASE WHEN prerequisites=1 THEN 'ready' ELSE status END,
                blocked=CASE WHEN blocked='' THEN ? ELSE blocked END WHERE unit=? AND ordinal=?""",reason,p.unit,edge.second)
        }
        p.count=edge.first
    }
    private suspend fun packPushStep(state: RuntimeState,p: PushPlan) {
        when(p.subphase) {
            "counts" -> countPushDagStep(p)
            "queue_init" -> initPushQueueStep(p)
            "choose" -> choosePushComponent(p)
            "measure" -> measurePushComponentStep(state,p)
            "mark" -> markPushComponentStep(p,false)
            "diagnostic" -> markPushComponentStep(p,true)
            "propagate" -> propagatePushComponentStep(p)
            else -> error("invalid push packing phase")
        }
    }

    private suspend fun advancePushPlan(state: RuntimeState): Pair<PushPlan?,Boolean> {
        if (planOne("SELECT state FROM _sync_outbox_bundle WHERE singleton_key=1") { it.getText(0) } != outboxStateNone)
            return null to true
        var p=loadPushPlan()
        val newPlan=p==null
        if(p==null) {
            p=PushPlan(newPushUnit(),"",cursorSecret=newPushUnit())
        }
        operations.reserveSyncWork(p.unit,pushStepWork(state,p))
        if(newPlan && planOne("SELECT 1 FROM _sync_dirty_rows LIMIT 1") { it.getLong(0) }==null)
            return null to true
        if(p.phase=="clear") {clearPushStep(p);return p to false}
        pushSpace(p)
        val (guard,next,init)=pushGuard(state,p)
        if(p.guard=="") {p.guard=guard;p.sourceBundleId=next;p.initializationId=init}
        else if(guard!=p.guard) {p.phase="clear";clearPushStep(p);return p to false}
        when(p.phase) {
            "discover" -> if(p.subphase=="seed") seedPushStep(state,p) else discoverPushStep(state,p)
            "scc" -> sccPushStep(p)
            "pack" -> packPushStep(state,p)
            "ready" -> return p to true
            else -> error("invalid retained push plan phase")
        }
        pushSpace(p);savePushPlan(p);return p to (p.phase=="ready")
    }

    suspend fun preparePushPlan(state: RuntimeState): PushPlan? {
        while(true) {
            val (p,ready)=db.transaction(TransactionMode.IMMEDIATE) {
                val step=advancePushPlan(state)
                currentCoroutineContext().ensureActive();step
            }
            if(ready) {
                if(p!=null && p.candidateRows==0L && p.blockedComponent!=0L) {
                    db.transaction(TransactionMode.IMMEDIATE) {
                        if(pushGuard(state,p).first!=p.guard) throw SyncWorkLimitException(p.unit,config.maxSyncWorkUnitsPerInvocation,operations.usedSyncWork())
                        throw pushBlockedError(state,p)
                    }
                }
                return p
            }
        }
    }

    suspend fun preparePush(state: RuntimeState,p: PushPlan, reservationToken: String? = null): PreparedPush {
        check(p.phase=="ready") { "push plan is not ready" }
        if(pushGuard(state,p).first!=p.guard) throw SyncWorkLimitException(p.unit,config.maxSyncWorkUnitsPerInvocation,operations.usedSyncWork())
        val perRowWork = pushStepWork(state, p)
        val readyWork = if (p.candidateRows > (Long.MAX_VALUE - 32) / perRowWork) Long.MAX_VALUE
            else 32 + p.candidateRows * perRowWork
        operations.reserveSyncWork(p.unit, readyWork, readyAtomic = true)
        var copyBytes=0L
        for(ordinal in 1..p.selectedNext) {
            val size=checkNotNull(planOne("""SELECT 2048+64*(length(CAST(COALESCE(d.payload,'') AS BLOB))+
                length(CAST(COALESCE(d.base_payload,'') AS BLOB))+length(CAST(d.base_provenance AS BLOB))+
                length(CAST(d.key_json AS BLOB))) FROM _sync_push_nodes n JOIN _sync_dirty_rows d
                ON d.schema_name=n.schema_name AND d.table_name=n.table_name AND d.key_json=n.key_json
                WHERE n.unit=? AND n.selected=?""",p.unit,ordinal) { it.getLong(0) })
            copyBytes=checkedPushAdd(copyBytes,size,p)
        }
        pushSpace(p,checkedPushMultiply(p.candidateRows,8,p),checkedPushAdd(copyBytes,checkedPushMultiply(p.candidateBytes,8,p),p))
        if(reservationToken!=null) OversqliteWorkspace(db,config).resizeInWriter(state,reservationToken,
            WorkspaceCharge(checkedPushMultiply(p.candidateRows,32,p),checkedPushAdd(copyBytes,checkedPushMultiply(p.candidateBytes,32,p),p)))
        check(p.candidateRows<=Int.MAX_VALUE.toLong()) { "selected row count exceeds host collection capacity" }
        val rows=ArrayList<DirtyRowCapture>(p.candidateRows.toInt())
        for(ordinal in 1..p.selectedNext) {
            val node=checkNotNull(planOne("SELECT ordinal FROM _sync_push_nodes WHERE unit=? AND selected=?",p.unit,ordinal) { it.getLong(0) })
            val n=pushNode(p,node);val row=loadPushCapture(state,p,n.schema,n.table,n.key)
            localStore.validateHistoricalBase(row.tableName,row.baseRowVersion,row.base)
            for(view in listOf("server","desired")) localStore.validateBaselineProjection(row.schemaName,row.tableName,row.keyJson,view,row.localPayload)
            rows+=row
        }
        return PreparedPush(p.sourceBundleId,rows,p.unit,p.guard,p.candidateBytes)
    }

    suspend fun invalidateFrozenPushPlan() {
        planExec("UPDATE _sync_push_plan SET phase='clear' WHERE singleton_key=1")
    }

    suspend fun clearPushPlanForLifecycle(onlyInvalid: Boolean=false,diagnosticId: String?=null) {
        var clearingUnit: String?=null
        while(true) {
            val done=db.transaction(TransactionMode.IMMEDIATE) {
                val p=loadPushPlan(forCleanup=true) ?: return@transaction true
                if(diagnosticId!=null && p.cleanupDiagnosticId!=diagnosticId && planOne("SELECT 1 FROM _sync_push_nodes WHERE unit=? AND diagnostic_id=? LIMIT 1",p.unit,diagnosticId) { it.getLong(0) }==null) return@transaction true
                if(onlyInvalid && p.phase!="clear") return@transaction true
                if(clearingUnit==null) clearingUnit=p.unit
                if(clearingUnit!=p.unit) return@transaction true
                operations.reserveSyncWork(p.unit, 8)
                p.phase="clear"
                if(diagnosticId!=null) p.cleanupDiagnosticId=diagnosticId
                val complete=clearPushStep(p)
                currentCoroutineContext().ensureActive()
                complete
            }
            if(done) return
        }
    }

    private suspend fun pushBlockedError(state: RuntimeState,p: PushPlan): Throwable {
        val data=checkNotNull(planOne("SELECT component_rows,component_bytes,blocked,diagnostic_id FROM _sync_push_nodes WHERE unit=? AND ordinal=?",p.unit,p.blockedComponent) {
            PushBlocked(it.getLong(0),it.getLong(1),it.getText(2),it.getText(3))
        })
        val (sample,more)=pushSample(state,p,p.blockedComponent,data.rows)
        return when(data.reason) {
            "rows","decoded_bytes" -> {
                val limit=if(data.reason=="rows") state.admittedContract.limits.maxPushRows.toLong() else state.admittedContract.limits.maxPushDecodedBytes
                val actual=if(data.reason=="rows") data.rows else data.bytes
                PushDependencyGroupExceedsAdmittedLimitException(data.reason,limit,actual,data.rows,data.bytes,data.id,sample,more)
            }
            "base" -> ResolutionBaseUnavailableException(p.unit,sample,more)
            "row" -> PushRowExceedsAdmittedLimitException(data.bytes,state.admittedContract.limits.maxRowDecodedBytes)
            "envelope" -> InvalidAdmittedContractException("complete one-row envelope exceeds max_push_decoded_bytes")
            else -> FKReconciliationRequiredException(data.reason,p.unit,sample,more)
        }
    }

    private suspend fun pushSample(state: RuntimeState,p: PushPlan,component: Long,count: Long): Pair<List<PushDependencyRecordRef>,Boolean> {
        val refs=mutableListOf<PushDependencyRecordRef>();var cursor=0L;var bytes=0L
        while(refs.size<16) {
            val ordinal=planOne("SELECT ordinal FROM _sync_push_nodes WHERE unit=? AND component=? AND diagnostic_ordinal>? ORDER BY diagnostic_ordinal LIMIT 1",p.unit,component,cursor) { it.getLong(0) } ?: break
            val n=pushNode(p,ordinal)
            pushSpace(p,refs.size.toLong()+2,bytes+1024+12*(utf8Size(n.key)+utf8Size(n.schema)+utf8Size(n.table)))
            val (_,key)=localStore.decodeDirtyKeyForPush(state,n.table,n.key)
            val ref=PushDependencyRecordRef(n.schema,n.table,key)
            val size=utf8Size(json.encodeToString(ref));if(bytes+size>4096) break
            refs+=ref;bytes+=size
            cursor=checkNotNull(planOne("SELECT diagnostic_ordinal FROM _sync_push_nodes WHERE unit=? AND ordinal=?",p.unit,ordinal) { it.getLong(0) })
        }
        return refs to (refs.size.toLong()<count)
    }

    private fun boundedPushDiagnosticId(id: String): String = if(id.length==64 && id.all { it in "0123456789abcdef" }) id else sha256Hex(id.encodeToByteArray())
    private fun pushDiagnosticCursor(p: PushPlan,id: String,ordinal: Long): String {
        val text=ordinal.toString()
        val secret=p.cursorSecret.encodeToByteArray()
        val innerPad=ByteArray(64) { index -> ((if(index<secret.size) secret[index].toInt() else 0) xor 0x36).toByte() }
        val outerPad=ByteArray(64) { index -> ((if(index<secret.size) secret[index].toInt() else 0) xor 0x5c).toByte() }
        val inner=sha256Hex(innerPad+"$id:$text".encodeToByteArray())
        val digest=ByteArray(32) { inner.substring(it*2,it*2+2).toInt(16).toByte() }
        return text+"."+sha256Hex(outerPad+digest)
    }
    private fun parsePushCursor(p: PushPlan,id: String,cursor: String): Long {
        if(cursor=="") return 0
        val stale=StalePushDependencyDiagnosticException(boundedPushDiagnosticId(id))
        if(cursor.length>85) throw stale
        val parts=cursor.split('.',limit=2);if(parts.size!=2) throw stale
        val ordinal=parts[0].toLongOrNull() ?: throw stale
        if(ordinal<=0 || pushDiagnosticCursor(p,id,ordinal)!=cursor) throw stale
        return ordinal
    }

    suspend fun readPushDependencyDiagnostic(state: RuntimeState,id: String,cursor: String?,pageSize: Int): PushDependencyDiagnosticPage {
        require(pageSize in 1..128) { "diagnostic page size must be between 1 and 128" }
        val stale=StalePushDependencyDiagnosticException(boundedPushDiagnosticId(id))
        if(boundedPushDiagnosticId(id)!=id) throw stale
        return db.transaction(TransactionMode.IMMEDIATE) {
            val p=loadPushPlan() ?: throw stale
            if(p.phase=="clear" || pushGuard(state,p).first!=p.guard) throw stale
            if(planOne("SELECT 1 FROM _sync_push_nodes WHERE unit=? AND diagnostic_id=? LIMIT 1",p.unit,id) { it.getLong(0) }==null) throw stale
            var after=parsePushCursor(p,id,cursor ?: "");var bytes=0L;var next: String?=null
            val refs=mutableListOf<PushDependencyRecordRef>()
            while(refs.size<pageSize) {
                operations.reserveSyncWork(p.unit, 8)
                val row=planOne("""SELECT ordinal,512+12*(length(CAST(schema_name AS BLOB))+length(CAST(table_name AS BLOB))+
                    length(CAST(key_json AS BLOB))) FROM _sync_push_nodes WHERE unit=? AND diagnostic_id=?
                    AND diagnostic_ordinal>? ORDER BY diagnostic_ordinal LIMIT 1""",p.unit,id,after) { it.getLong(0) to it.getLong(1) } ?: break
                pushSpace(p,refs.size.toLong()+2,bytes+row.second)
                val n=pushNode(p,row.first);val (_,key)=localStore.decodeDirtyKeyForPush(state,n.table,n.key)
                val ref=PushDependencyRecordRef(n.schema,n.table,key);val size=utf8Size(json.encodeToString(ref))
                if(bytes+size>64*1024 && refs.isNotEmpty()) {next=pushDiagnosticCursor(p,id,after);break}
                if(size>state.admittedContract.limits.maxRowDecodedBytes)
                    throw SyncWorkspaceLimitException(p.unit,"bytes",state.admittedContract.limits.maxRowDecodedBytes,0,size,false)
                refs+=ref;bytes+=size
                after=checkNotNull(planOne("SELECT diagnostic_ordinal FROM _sync_push_nodes WHERE unit=? AND ordinal=?",p.unit,row.first) { it.getLong(0) })
                if (operations.usedSyncWork() >= config.maxSyncWorkUnitsPerInvocation) {
                    if (planOne("SELECT 1 FROM _sync_push_nodes WHERE unit=? AND diagnostic_id=? AND diagnostic_ordinal>? LIMIT 1",p.unit,id,after) { true } == true)
                        next = pushDiagnosticCursor(p, id, after)
                    break
                }
            }
            if(refs.size==pageSize && planOne("SELECT 1 FROM _sync_push_nodes WHERE unit=? AND diagnostic_id=? AND diagnostic_ordinal>? LIMIT 1",p.unit,id,after) { it.getLong(0) }!=null)
                next=pushDiagnosticCursor(p,id,after)
            PushDependencyDiagnosticPage(id,refs,next)
        }
    }

    suspend fun releasePushDependencyDiagnostic(id: String) {
        if(boundedPushDiagnosticId(id)!=id) throw StalePushDependencyDiagnosticException(boundedPushDiagnosticId(id))
        clearPushPlanForLifecycle(diagnosticId=id)
    }

    fun recordConfirmedBundle(source: String, id: Long) = operations.recordConfirmedBundle(source,id)

    internal suspend fun ensureSnapshotWorkAvailable(unit: String) = operations.ensureSyncWorkAvailable(unit)

    internal suspend fun <T> withSnapshotTransportRetry(label: String, block: suspend () -> T): T =
        operations.withTransientRetry(label, block)

    suspend fun chargeBufferWork(unit: String, work: Long) = operations.reserveSyncWork(unit,work)

    suspend fun stageSnapshotChunk(
        state: RuntimeState,
        session: SnapshotSession,
        chunk: SnapshotChunkResponse,
        afterRowOrdinal: Long,
        finalizer: suspend () -> Unit = {},
    ) {
        val unit = applyUnit("snapshot-stage", session.snapshotBundleSeq, session.snapshotId)
        // A chunk and its progress receipt are one bounded durable step.
        operations.reserveSyncWork(unit, 8L * chunk.rows.size)
        db.transaction(TransactionMode.IMMEDIATE) {
            db.withStatementCache { workspaceStatementCache ->
                db.withPreparedStatement(
                    sql = """
                        INSERT INTO _sync_snapshot_stage (
                          snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """.trimIndent(),
                    closeStatement = { statement ->
                        snapshotStatementCloseForTest?.invoke(statement) ?: statement.close()
                    },
                ) { st ->
                    var rowOrdinal = afterRowOrdinal
                    var statementUsed = false
                    for (row in chunk.rows) {
                        currentCoroutineContext().ensureActive()
                        validateSnapshotRow(row)
                        require(row.table in state.validated.tableInfoByName) {
                            "snapshot row ${safeSyncTableDiagnosticIdentifier(row.schema, row.table)} " +
                                "is not configured for sync"
                        }
                        require(row.schema == state.validated.schemaForTable(row.table)) {
                            "snapshot row ${safeSyncTableDiagnosticIdentifier(row.schema, row.table)} " +
                                "does not match configured schema " +
                                safeSnapshotDiagnosticIdentifier(state.validated.schemaForTable(row.table))
                        }
                        val payload = row.payload as? JsonObject
                            ?: error(
                                "snapshot row payload must be a JSON object for " +
                                    safeSyncTableDiagnosticIdentifier(row.schema, row.table),
                            )
                        val normalizedPayload = localStore.validateAndNormalizeSnapshotPayload(
                            state = state,
                            tableName = row.table,
                            key = row.key,
                            payload = payload,
                        )
                        val (keyJson, _) = localStore.bundleRowKeyToLocalKey(state, row.table, row.key)
                        val workspace = OversqliteWorkspace(db, config)
                        val charge = workspace.stageCharge(unit, encodedJsonSize(normalizedPayload), session.snapshotId, row.schema, row.table, keyJson)
                        workspace.check(unit, workspace.add(unit, charge, WorkspaceCharge(5, workspace.scale(unit, charge.bytes, 5))),
                            statementCache = workspaceStatementCache)
                        rowOrdinal++
                        if (statementUsed) {
                            (reusableStatementCleanupForTest ?: DefaultReusableStatementCleanup)(st)
                        } else {
                            statementUsed = true
                        }

                        st.bindText(1, session.snapshotId)
                        st.bindLong(2, rowOrdinal)
                        st.bindText(3, row.schema)
                        st.bindText(4, row.table)
                        st.bindText(5, keyJson)
                        st.bindLong(6, row.rowVersion)
                        st.bindText(7, normalizedPayload.toString())
                        st.step()
                    }
                }
            }
            finalizer()
        }
    }

    suspend fun loadStagedSnapshotPage(
        state: RuntimeState,
        snapshotId: String,
        afterRowOrdinal: Long,
        maxRows: Int,
        maxBytes: Long,
    ): StagedSnapshotPage {
        require(maxRows > 0) { "snapshot apply page row limit must be positive" }
        require(maxBytes > 0L) { "snapshot apply page byte limit must be positive" }

        var selectedBytes = 0L
        var selectedCount = 0
        var lastOrdinal: Long? = null
        db.withExclusiveAccess {
            db.prepare(
                """
                SELECT row_ordinal,
                       length(CAST(schema_name AS BLOB)) +
                       length(CAST(table_name AS BLOB)) +
                       length(CAST(key_json AS BLOB)) +
                       length(CAST(payload AS BLOB)) AS retained_text_bytes
                FROM _sync_snapshot_stage
                WHERE snapshot_id = ? AND row_ordinal > ?
                ORDER BY row_ordinal
                LIMIT ?
                """.trimIndent(),
            ).use { st ->
                st.bindText(1, snapshotId)
                st.bindLong(2, afterRowOrdinal)
                st.bindLong(3, maxRows.toLong())
                while (st.step()) {
                    val rowOrdinal = st.getLong(0)
                    val retainedTextBytes = st.getLong(1)
                    require(retainedTextBytes >= 0L) {
                        "snapshot staged row metadata byte count must be non-negative"
                    }
                    val nextBytes = checkedAddSnapshotLong(selectedBytes, retainedTextBytes) {
                        "snapshot apply page byte total overflow"
                    }
                    if (nextBytes > maxBytes) {
                        if (selectedCount == 0) {
                            throw SnapshotApplyRowTooLargeException(
                                rowOrdinal = rowOrdinal,
                                retainedTextBytes = retainedTextBytes,
                                limit = maxBytes,
                            )
                        }
                        break
                    }
                    selectedBytes = nextBytes
                    selectedCount++
                    lastOrdinal = rowOrdinal
                }
            }
        }
        if (selectedCount == 0) {
            return StagedSnapshotPage(emptyList(), 0L)
        }
        val selectedLastOrdinal = checkNotNull(lastOrdinal) {
            "snapshot apply page selection is missing its last row ordinal"
        }

        val rows = db.withExclusiveAccess {
            db.prepare(
                """
                SELECT row_ordinal, schema_name, table_name, key_json, row_version, payload
                FROM _sync_snapshot_stage
                WHERE snapshot_id = ? AND row_ordinal > ? AND row_ordinal <= ?
                ORDER BY row_ordinal
                """.trimIndent(),
            ).use { st ->
                st.bindText(1, snapshotId)
                st.bindLong(2, afterRowOrdinal)
                st.bindLong(3, selectedLastOrdinal)
                buildList {
                    while (st.step()) {
                        add(
                            StagedSnapshotRow(
                                rowOrdinal = st.getLong(0),
                                schemaName = st.getText(1),
                                tableName = st.getText(2),
                                keyJson = st.getText(3),
                                rowVersion = st.getLong(4),
                                payload = st.getText(5),
                            ),
                        )
                    }
                }
            }
        }
        require(rows.size == selectedCount) {
            "snapshot staged page row count changed between metadata and row load"
        }
        require(rows.last().rowOrdinal == selectedLastOrdinal) {
            "snapshot staged page last ordinal changed between metadata and row load"
        }
        rows.forEach { row ->
            require(row.tableName in state.validated.tableInfoByName) {
                "staged snapshot row " +
                    safeSyncTableDiagnosticIdentifier(row.schemaName, row.tableName) +
                    " is not configured for sync"
            }
            require(row.schemaName == state.validated.schemaForTable(row.tableName)) {
                "staged snapshot row " +
                    safeSyncTableDiagnosticIdentifier(row.schemaName, row.tableName) +
                    " does not match configured schema " +
                    safeSnapshotDiagnosticIdentifier(state.validated.schemaForTable(row.tableName))
            }
        }
        return StagedSnapshotPage(rows = rows, retainedTextBytes = selectedBytes)
    }

    suspend fun deleteSnapshotStage(
        snapshotId: String,
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = "DELETE FROM _sync_snapshot_stage WHERE snapshot_id = ?",
            statementCache = statementCache,
        ) { st ->
            st.bindText(1, snapshotId)
            st.step()
        }
    }

    suspend fun clearAllSnapshotStages(
        statementCache: StatementCache? = null,
    ) {
        db.withPreparedStatement(
            sql = "DELETE FROM _sync_snapshot_stage",
            statementCache = statementCache,
        ) { st ->
            st.step()
        }
    }

}

internal const val pushPlanDdl = """CREATE TABLE IF NOT EXISTS _sync_push_plan (
 singleton_key INTEGER PRIMARY KEY CHECK(singleton_key=1), unit TEXT NOT NULL,
 guard TEXT NOT NULL, phase TEXT NOT NULL CHECK(phase IN ('clear','discover','scc','pack','ready')),
 state TEXT NOT NULL, entries INTEGER NOT NULL CHECK(entries>=0), bytes INTEGER NOT NULL CHECK(bytes>=0))"""

internal const val pushNodesDdl = """CREATE TABLE IF NOT EXISTS _sync_push_nodes (
 unit TEXT NOT NULL, schema_name TEXT NOT NULL, table_name TEXT NOT NULL, key_json TEXT NOT NULL,
 ordinal INTEGER NOT NULL, intent_revision INTEGER NOT NULL, dirty_ordinal INTEGER NOT NULL,
 status TEXT NOT NULL, adjacency_cursor TEXT NOT NULL DEFAULT '', adjacency_phase INTEGER NOT NULL DEFAULT 0,
 dfs_index INTEGER NOT NULL DEFAULT 0, lowlink INTEGER NOT NULL DEFAULT 0, parent INTEGER NOT NULL DEFAULT 0,
 on_stack INTEGER NOT NULL DEFAULT 0, stack_ordinal INTEGER NOT NULL DEFAULT 0, edge_cursor INTEGER NOT NULL DEFAULT 0,
 component INTEGER NOT NULL DEFAULT 0, component_rows INTEGER NOT NULL DEFAULT 0, component_bytes INTEGER NOT NULL DEFAULT 0,
 ready_ordinal INTEGER NOT NULL DEFAULT 0, ready_schema TEXT NOT NULL DEFAULT '', ready_table TEXT NOT NULL DEFAULT '', ready_key TEXT NOT NULL DEFAULT '',
 prerequisites INTEGER NOT NULL DEFAULT 0, blocked TEXT NOT NULL DEFAULT '', selected INTEGER NOT NULL DEFAULT 0,
 diagnostic_id TEXT NOT NULL DEFAULT '', diagnostic_ordinal INTEGER NOT NULL DEFAULT 0,
 charge INTEGER NOT NULL CHECK(charge>=0),
 PRIMARY KEY(unit,schema_name,table_name,key_json), UNIQUE(unit,ordinal))"""

internal const val pushEdgesDdl = """CREATE TABLE IF NOT EXISTS _sync_push_edges (
 unit TEXT NOT NULL, dependent INTEGER NOT NULL, prerequisite INTEGER NOT NULL, ordinal INTEGER NOT NULL,
 charge INTEGER NOT NULL CHECK(charge>=0), PRIMARY KEY(unit,dependent,prerequisite), UNIQUE(unit,ordinal))"""

internal val pushIndexDdl = mapOf(
    "_sync_push_nodes_frontier_idx" to """CREATE INDEX IF NOT EXISTS _sync_push_nodes_frontier_idx ON _sync_push_nodes(unit,status,ordinal)""",
    "_sync_push_nodes_stack_idx" to """CREATE INDEX IF NOT EXISTS _sync_push_nodes_stack_idx ON _sync_push_nodes(unit,on_stack,stack_ordinal)""",
    "_sync_push_nodes_component_idx" to """CREATE INDEX IF NOT EXISTS _sync_push_nodes_component_idx ON _sync_push_nodes(unit,component,schema_name,table_name,key_json)""",
    "_sync_push_nodes_ready_idx" to """CREATE INDEX IF NOT EXISTS _sync_push_nodes_ready_idx ON _sync_push_nodes(unit,status,ready_ordinal,ready_schema,ready_table,ready_key)""",
    "_sync_push_nodes_selected_idx" to """CREATE INDEX IF NOT EXISTS _sync_push_nodes_selected_idx ON _sync_push_nodes(unit,selected)""",
    "_sync_push_nodes_component_diagnostic_idx" to """CREATE INDEX IF NOT EXISTS _sync_push_nodes_component_diagnostic_idx ON _sync_push_nodes(unit,component,diagnostic_ordinal)""",
    "_sync_push_nodes_component_ordinal_idx" to """CREATE INDEX IF NOT EXISTS _sync_push_nodes_component_ordinal_idx ON _sync_push_nodes(unit,component,ordinal)""",
    "_sync_push_nodes_diagnostic_idx" to """CREATE INDEX IF NOT EXISTS _sync_push_nodes_diagnostic_idx ON _sync_push_nodes(unit,diagnostic_id,diagnostic_ordinal)""",
    "_sync_push_edges_outgoing_idx" to """CREATE INDEX IF NOT EXISTS _sync_push_edges_outgoing_idx ON _sync_push_edges(unit,dependent,ordinal)""",
    "_sync_push_edges_incoming_idx" to """CREATE INDEX IF NOT EXISTS _sync_push_edges_incoming_idx ON _sync_push_edges(unit,prerequisite,ordinal)"""
)

@Serializable
internal data class PushPlan(
    var unit: String, var guard: String, var phase: String = "discover",
    var entries: Long = 0, var bytes: Long = 0,
    var seedOrdinal: Long = 0, var seedSchema: String = "", var seedTable: String = "", var seedKey: String = "",
    var nextNode: Long = 0, var visit: Long = 0, var nextEdge: Long = 0,
    var index: Long = 0, var stack: Long = 0, var frame: Long = 0, var pop: Long = 0,
    var subphase: String = "seed", var cursor: Long = 0, var current: Long = 0, var count: Long = 0,
    var bodyBytes: Long = 0, var lastTable: Long = -1, var candidateRows: Long = 0, var candidateBytes: Long = 0,
    var selectedNext: Long = 0, var blockedComponent: Long = 0, var blockedReason: String = "",
    var candidateLastTable: Long = -1, var measureCandidateBytes: Long = 0, var measureCandidateLastTable: Long = -1,
    var sourceBundleId: Long = 0, var initializationId: String = "", var cursorSecret: String = "",
    var cleanupDiagnosticId: String = "",
)
internal data class PushNode(
    val ordinal: Long, val schema: String, val table: String, val key: String, val status: String,
    val cursor: String, val blocked: String, val diagnostic: String, val revision: Long,
    val dirtyOrdinal: Long, val adjacencyPhase: Long, val index: Long, val lowlink: Long,
    val parent: Long, val stack: Long, val edgeCursor: Long, val component: Long, val selected: Long,
)

private data class PushSeed(val schema: String,val table: String,val key: String,val op: String,val base: String,val kind: String,val ordinal: Long)
private data class PushRelationship(val schema: String,val table: String,val key: String,val column: String)

private data class PushReady(val ordinal: Long,val schema: String,val table: String,val key: String)

private data class PushBlocked(val rows: Long,val bytes: Long,val reason: String,val id: String)
