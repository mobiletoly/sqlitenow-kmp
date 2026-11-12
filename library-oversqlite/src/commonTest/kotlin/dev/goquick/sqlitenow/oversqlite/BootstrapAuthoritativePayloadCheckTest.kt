package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The typed bootstrap's authoritative check requires each stored payload to be exactly the canonical object the baseline
 * statement builds from the stage row. Every change to the payload's members, storage tags, values or representation, and
 * every authoritative field outside the payload, must be rejected.
 */
internal class BootstrapAuthoritativePayloadCheckTest : SnapshotBootstrapTestSupport() {
    private val columns = listOf(
        ColumnInfo("data", "BLOB", false, false, null, wireType = "bytes"),
        ColumnInfo("id", "TEXT", true, true, null, wireType = "text"),
        ColumnInfo("label", "TEXT", false, false, null, wireType = "text"),
        ColumnInfo("n", "INTEGER", false, true, null, wireType = "int64"),
        ColumnInfo("r", "REAL", false, false, null, wireType = "float64"),
    )
    private val table = GenerationTable("main", "items", listOf("id"), columns.map {
        GenerationColumn(it.name, checkNotNull(it.wireType), !it.notNull && !it.isPrimaryKey)
    }, emptyList())
    private val generation = SyncGeneration("1", "", listOf(table)).let { it.copy(fingerprint = it.computeFingerprint()) }
    private val schema = BootstrapTableSchema(0, table, TableInfo("items", columns), columns,
        listOf(BootstrapUniqueKey(listOf(1 to "BINARY"))), emptyList(), "a".repeat(64))
    private val rows = listOf(
        listOf(TypedValue.Blob(byteArrayOf(0, -1)), TypedValue.Text("one"), TypedValue.Text("Name, \"quoted\" <tag>"),
            TypedValue.Integer(Long.MIN_VALUE.toString()), TypedValue.Real("5e-324")),
        listOf(TypedValue.Blob(byteArrayOf()), TypedValue.Text("two"), TypedValue.Null, TypedValue.Integer(Long.MAX_VALUE.toString()),
            TypedValue.Real("-0.0")),
        listOf(TypedValue.Null, TypedValue.Text("three"), TypedValue.Text("Zoë 名前"), TypedValue.Integer("9007199254740993"), TypedValue.Null),
    )

    @Test fun storedPayloadMustEqualTheCanonicalObjectBuiltFromTheStage() = runOwnedWebDatabaseTest {
        val db = newDb()
        try {
            val binding = installedFixture(db)
            assertEquals(0L, verify(db, binding), "the payload the baseline wrote is accepted")

            val payloadMutations = mapOf(
                "changed value" to "json_set(server_payload,'$.n.value','1')",
                "changed storage tag" to "json_set(server_payload,'$.n.storage','text')",
                "JSON number instead of string" to "json_set(server_payload,'$.n.value',9223372036854775807)",
                "null value made a string" to "json_set(server_payload,'$.label.value','')",
                "extra member" to "json_set(server_payload,'$.extra',json('{\"storage\":\"null\",\"value\":null}'))",
                "extra field inside a member" to "json_set(server_payload,'$.n.extra',1)",
                "missing member" to "json_remove(server_payload,'$.label')",
                "duplicate member" to "replace(server_payload,'{\"data\":','{\"data\":{\"storage\":\"blob\",\"value\":\"\"},\"data\":')",
                "reordered members" to "json_object('r',json_extract(server_payload,'$.r'),'n',json_extract(server_payload,'$.n'),'label',json_extract(server_payload,'$.label'),'id',json_extract(server_payload,'$.id'),'data',json_extract(server_payload,'$.data'))",
                "whitespace" to "replace(server_payload,'\":','\": ')",
            )
            for ((name, expression) in payloadMutations) {
                assertRejected(db, binding, name, "UPDATE _sync_row_state SET server_payload=$expression WHERE key_json='{\"id\":\"two\"}'")
            }
            val stateMutations = mapOf(
                "kind" to "server_kind='tombstone'",
                "row version" to "row_version=row_version+1",
                "relationships flag" to "relationships_known=0",
                "server revision" to "server_revision=server_revision+1",
                "provenance" to "server_provenance=json_set(server_provenance,'$.bundle_sequence',99)",
                "context" to "server_provenance=json_set(server_provenance,'$.context_id',6)",
            )
            for ((name, assignment) in stateMutations) {
                assertRejected(db, binding, name, "UPDATE _sync_row_state SET $assignment WHERE key_json='{\"id\":\"two\"}'")
            }
            assertRejected(db, binding, "missing row", "DELETE FROM _sync_row_state WHERE key_json='{\"id\":\"two\"}'")
        } finally { db.close() }
    }

    private suspend fun installedFixture(db: SafeSQLiteConnection): SnapshotTableBinding {
        db.execSQL("CREATE TABLE items(data BLOB,id TEXT PRIMARY KEY NOT NULL,label TEXT,n INTEGER NOT NULL,r REAL)")
        val binding = SnapshotDescriptor.create(generation, listOf(schema), 999) {}.tables.single().also {
            db.execSQL(it.createSql); it.indexSql.forEach { sql -> db.execSQL(sql) }
        }
        rows.forEachIndexed { index, row ->
            db.withPreparedStatement(binding.insertSql) { statement ->
                statement.bindText(1, "snapshot"); statement.bindLong(2, index + 1L)
                statement.bindText(3, "{\"id\":\"${(row[1] as TypedValue.Text).value}\"}"); statement.bindLong(4, 1); statement.bindLong(5, 1)
                row.forEachIndexed { column, value -> OversqliteValueCodec.bindTypedValue(statement, column + 6, columns[column], value) }
                statement.step()
            }
        }
        db.execSQL("""CREATE TABLE _sync_row_state(schema_name TEXT,table_name TEXT,key_json TEXT,row_version INTEGER,
            server_kind TEXT,server_payload TEXT,server_provenance TEXT,relationships_known INTEGER,server_revision INTEGER,
            server_context_id INTEGER GENERATED ALWAYS AS(json_extract(server_provenance,'$.context_id')) VIRTUAL,
            PRIMARY KEY(schema_name,table_name,key_json))""")
        db.withPreparedStatement(binding.baselineSql) {
            it.bindLong(1, 1); it.bindLong(2, 5); it.bindLong(3, 0)
            it.bindText(4, "snapshot"); it.bindLong(5, 0); it.bindLong(6, 3); it.step()
        }
        assertEquals(3L, db.scalarLong("SELECT count(*) FROM _sync_row_state"))
        return binding
    }

    private suspend fun verify(db: SafeSQLiteConnection, binding: SnapshotTableBinding): Long =
        db.withPreparedStatement(binding.authoritativeVerificationSql) {
            it.bindText(1, "snapshot"); it.bindLong(2, 0); it.bindLong(3, 5); it.bindLong(4, 1); it.bindLong(5, 5)
            assertTrue(it.step()); it.getLong(0)
        }

    /** Applies [mutation] inside a savepoint, requires the check to reject it, then rolls it back. */
    private suspend fun assertRejected(db: SafeSQLiteConnection, binding: SnapshotTableBinding, name: String, mutation: String) {
        db.execSQL("SAVEPOINT mutation")
        try {
            db.execSQL(mutation)
            assertEquals(1L, db.scalarLong("SELECT changes()"), "$name must change exactly one row")
            assertEquals(1L, verify(db, binding), "$name must be rejected")
        } finally { db.execSQL("ROLLBACK TO mutation"); db.execSQL("RELEASE mutation") }
        assertEquals(0L, verify(db, binding), "the fixture is restored after $name")
    }
}
