package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step

internal val authoritativeRowStateDdl = """CREATE TABLE IF NOT EXISTS _sync_row_state (
 schema_name TEXT NOT NULL, table_name TEXT NOT NULL, key_json TEXT NOT NULL,
 row_version INTEGER NOT NULL DEFAULT 0,
 server_kind TEXT NOT NULL CHECK(server_kind IN ('live','deleted','absent','unpublished','unknown')),
 server_payload TEXT,
 server_provenance TEXT NOT NULL CHECK(json_valid(server_provenance) AND json_type(server_provenance)='object'),
 relationships_known INTEGER NOT NULL CHECK(relationships_known IN (0,1) AND relationships_known=(server_kind!='unknown')),
 server_revision INTEGER NOT NULL DEFAULT 0 CHECK(typeof(server_revision)='integer' AND server_revision>=0),
 updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
 PRIMARY KEY(schema_name,table_name,key_json),
 CHECK(typeof(row_version)='integer' AND CASE server_kind
 WHEN 'live' THEN row_version>0 AND server_payload IS NOT NULL AND json_valid(server_payload) AND json_type(server_payload)='object'
 WHEN 'deleted' THEN row_version>0 AND server_payload IS NULL
 ELSE row_version=0 AND server_payload IS NULL END)
)""".trimIndent()
internal val relationshipProjectionDdl = """CREATE TABLE IF NOT EXISTS _sync_relationships (
 view TEXT NOT NULL CHECK(view IN ('server','desired')),
 child_schema TEXT NOT NULL, child_table TEXT NOT NULL, child_key TEXT NOT NULL,
 fk_column TEXT NOT NULL, parent_schema TEXT NOT NULL, parent_table TEXT NOT NULL, parent_key TEXT NOT NULL,
 owner_revision INTEGER NOT NULL CHECK(typeof(owner_revision)='integer' AND owner_revision>=0),
 PRIMARY KEY(view,child_schema,child_table,child_key,fk_column)
)""".trimIndent()

internal fun baselineProvenanceSql(origin: String, version: String, sequence: String): String = """json_object(
 'database_id',(SELECT database_id FROM _sync_contract_identity WHERE singleton_key=1),
 'service_id',(SELECT service_id FROM _sync_contract_identity WHERE singleton_key=1),
 'account_scope',json_object(
 'binding',CASE WHEN (SELECT kind FROM _sync_operation_state WHERE singleton_key=1)='remote_replace' THEN 'attached' ELSE (SELECT binding_state FROM _sync_attachment_state WHERE singleton_key=1) END,
 'account_id',CASE WHEN (SELECT kind FROM _sync_operation_state WHERE singleton_key=1)='remote_replace' THEN (SELECT target_user_id FROM _sync_operation_state WHERE singleton_key=1) ELSE (SELECT attached_user_id FROM _sync_attachment_state WHERE singleton_key=1) END),
 'generation_id',(SELECT generation_id FROM _sync_contract_identity WHERE singleton_key=1),
 'generation_fingerprint',(SELECT fingerprint FROM _sync_contract_identity WHERE singleton_key=1),
 'admission',CASE WHEN (SELECT fingerprint FROM _sync_contract_identity WHERE singleton_key=1)='' THEN 'unadmitted' ELSE 'admitted' END,
 'local_version',(SELECT local_version FROM _sync_contract_identity WHERE singleton_key=1),
 'physical_shape',(SELECT physical_shape FROM _sync_contract_identity WHERE singleton_key=1),
 'origin_source_id',(SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key=1),
 'origin',$origin,'bundle_sequence',$sequence,'authoritative_version',$version)""".trimIndent()

internal fun projectionDeleteSql(view: String, row: String): String =
    "DELETE FROM _sync_relationships WHERE view='$view' AND child_schema=$row.schema_name AND child_table=$row.table_name AND child_key=$row.key_json;"

internal fun relationshipProjectionInsertSql(view: String, payload: String, revision: String, predicate: String): String = """
 INSERT INTO _sync_relationships(view,child_schema,child_table,child_key,fk_column,parent_schema,parent_table,parent_key,owner_revision)
 SELECT '$view',NEW.schema_name,NEW.table_name,NEW.key_json,lower(fk."from"),COALESCE(parent.schema_name,NEW.schema_name),lower(fk."table"),
 json_object(lower(COALESCE(fk."to",(SELECT name FROM pragma_table_info(fk."table") WHERE pk=1))),json_extract($payload,'$."'||lower(fk."from")||'".value')),$revision
 FROM pragma_foreign_key_list(NEW.table_name) AS fk
 LEFT JOIN _sync_managed_tables AS parent ON parent.table_name=lower(fk."table")
 WHERE $predicate AND json_extract($payload,'$."'||lower(fk."from")||'".storage')!='null';
""".trimIndent()

internal val baselineIndexDdl = mapOf(
 "_sync_relationships_parent_idx" to "CREATE INDEX IF NOT EXISTS _sync_relationships_parent_idx ON _sync_relationships(view,parent_schema,parent_table,parent_key,child_schema,child_table,child_key,fk_column)",
 "_sync_row_state_unknown_idx" to "CREATE INDEX IF NOT EXISTS _sync_row_state_unknown_idx ON _sync_row_state(relationships_known,table_name,key_json)",
 "_sync_dirty_rows_order_idx" to "CREATE INDEX IF NOT EXISTS _sync_dirty_rows_order_idx ON _sync_dirty_rows(dirty_ordinal,schema_name,table_name,key_json)",
)

internal fun expectedBaselineRevisionTriggerSql(operation: String): String {
 val oldDelete = if (operation == "insert") "" else projectionDeleteSql("server","OLD")
 val clause = if (operation == "update") "UPDATE OF schema_name,table_name,key_json,row_version,server_kind,server_payload,server_provenance,relationships_known" else operation
 val insert = if (operation == "delete") "" else
     "UPDATE _sync_row_state SET server_revision=(SELECT baseline_revision FROM _sync_conflict_state WHERE singleton_key=1) WHERE schema_name=NEW.schema_name AND table_name=NEW.table_name AND key_json=NEW.key_json;" +
     relationshipProjectionInsertSql("server","NEW.server_payload","(SELECT baseline_revision FROM _sync_conflict_state WHERE singleton_key=1)","NEW.server_kind='live'")
 return "CREATE TRIGGER IF NOT EXISTS _sync_baseline_revision_$operation AFTER $clause ON _sync_row_state BEGIN UPDATE _sync_conflict_state SET baseline_revision=baseline_revision+1 WHERE singleton_key=1; $oldDelete $insert END"
}

internal fun expectedConflictRevisionTriggerSql(operation: String): String {
 val oldDelete=if (operation=="insert") "" else projectionDeleteSql("desired","OLD")
 val insert=if (operation=="delete") "" else relationshipProjectionInsertSql("desired","NEW.payload","(SELECT dirty_revision FROM _sync_conflict_state WHERE singleton_key=1)","NEW.op!='DELETE'")
 return "CREATE TRIGGER IF NOT EXISTS _sync_conflict_revision_$operation AFTER $operation ON _sync_dirty_rows BEGIN UPDATE _sync_conflict_state SET dirty_revision=dirty_revision+1 WHERE singleton_key=1; $oldDelete $insert END"
}

internal suspend fun bindLocalBaselineContext(db: dev.goquick.sqlitenow.core.SafeSQLiteConnection) {
 for ((table,column) in listOf("_sync_row_state" to "server_provenance","_sync_dirty_rows" to "base_provenance")) {
  db.execSQL("UPDATE $table SET $column=json_set($column,'$.account_scope',json_object('binding','attached','account_id',(SELECT attached_user_id FROM _sync_attachment_state WHERE singleton_key=1))) WHERE json_extract($column,'$.account_scope.binding')='anonymous' AND (SELECT binding_state FROM _sync_attachment_state WHERE singleton_key=1)='attached'")
  db.execSQL("UPDATE $table SET $column=json_set($column,'$.generation_fingerprint',(SELECT fingerprint FROM _sync_contract_identity WHERE singleton_key=1),'$.admission','admitted') WHERE json_extract($column,'$.admission')='unadmitted' AND (SELECT fingerprint FROM _sync_contract_identity WHERE singleton_key=1)!=''")
 }
}

// The desired projection has no independent authority; this checks its retained owner.
private fun storedDesiredProjectionCheckSql(): String {
    return """
WITH owner AS (
 SELECT schema_name,table_name,key_json,CASE WHEN op='DELETE' THEN 'deleted' ELSE 'live' END AS server_kind,
 payload AS server_payload,1 AS relationships_known,(SELECT dirty_revision FROM _sync_conflict_state WHERE singleton_key=1) AS server_revision FROM _sync_dirty_rows WHERE schema_name=? AND table_name=? AND key_json=?
), expected AS (
 SELECT lower(fk."from") AS column_name,COALESCE(parent.schema_name,owner.schema_name) AS parent_schema,
 lower(fk."table") AS parent_table,
 json_object(lower(COALESCE(fk."to",(SELECT name FROM pragma_table_info(fk."table") WHERE pk=1))),json_extract(owner.server_payload,'$."'||lower(fk."from")||'".value')) AS parent_key
 FROM owner,pragma_foreign_key_list(owner.table_name) AS fk
 LEFT JOIN _sync_managed_tables AS parent ON parent.table_name=lower(fk."table")
 WHERE owner.server_kind='live' AND json_extract(owner.server_payload,'$."'||lower(fk."from")||'".storage')!='null'
), actual AS (
 SELECT edge.* FROM _sync_relationships AS edge JOIN owner
 ON edge.view='desired' AND edge.child_schema=owner.schema_name AND edge.child_table=owner.table_name AND edge.child_key=owner.key_json
 LIMIT (SELECT count(*)+1 FROM owner,pragma_foreign_key_list(owner.table_name))
)
SELECT NOT EXISTS(SELECT 1 FROM owner WHERE relationships_known!=1 OR server_kind='unknown')
 AND (SELECT count(*) FROM expected)=(SELECT count(*) FROM actual)
 AND NOT EXISTS(SELECT 1 FROM actual LEFT JOIN expected ON expected.column_name=actual.fk_column
 WHERE expected.column_name IS NULL OR expected.parent_schema!=actual.parent_schema OR expected.parent_table!=actual.parent_table OR expected.parent_key!=actual.parent_key
 OR actual.owner_revision>(SELECT server_revision FROM owner) OR actual.owner_revision<1)
""".trimIndent()
}

private val storedDesiredProjectionCheckSql = storedDesiredProjectionCheckSql()

internal suspend fun validateStoredDesiredProjection(db: dev.goquick.sqlitenow.core.SafeSQLiteConnection, schema: String, table: String, key: String) {
    db.withPreparedStatement(storedDesiredProjectionCheckSql) { st ->
        st.bindText(1, schema)
        st.bindText(2, table)
        st.bindText(3, key)
        check(st.step() && st.getLong(0) == 1L) { "desired relationship projection is inconsistent" }
    }
}
