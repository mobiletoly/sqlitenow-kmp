package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import java.security.MessageDigest
import kotlinx.serialization.json.*
import kotlin.test.*

internal val applyCleanupSql = listOf("_sync_apply_edges", "_sync_apply_rows").map {
    "DELETE FROM $it WHERE ordinal IN (SELECT ordinal FROM $it ORDER BY ordinal LIMIT 128)"
} + listOf("SELECT changes()", "DELETE FROM _sync_apply_plan")

/** Physical observation includes uncached statements and every cached step. No observer SQL is issued. */
internal class ApplyStatementObservation private constructor(
    private val db: SafeSQLiteConnection,
    private val delegate: SQLiteConnection,
    private val identityPrefix: String?,
) : SQLiteConnection by delegate {
    internal data class Handle(val sql: String, val writer: Int, var steps: Long = 0,
        var resets: Long = 0, var clears: Long = 0, var closes: Long = 0)
    val handles = mutableListOf<Handle>()
    var writers = 0; private set
    val steps = sortedMapOf<String, Long>()
    private val bindings = MessageDigest.getInstance("SHA-256")
    private var ownerChecks = 0
    private fun deterministic(sql: String) = sql.contains("FROM \"nodes\"") ||
        sql.contains("FROM \"categories\"") || sql.startsWith("INSERT OR IGNORE INTO _sync_apply_edges") ||
        sql.startsWith("SELECT ordinal,table_name,local_pk,payload")
    override fun prepare(sql: String): SQLiteStatement {
        val statement = delegate.prepare(sql)
        val handle = Handle(sql, writers).also(handles::add)
        val bound = sortedMapOf<Int, String>()
        return object : SQLiteStatement by statement {
            override fun bindText(index: Int, value: String) { bound[index] = "text:$value"; statement.bindText(index, value) }
            override fun bindLong(index: Int, value: Long) { bound[index] = "long:$value"; statement.bindLong(index, value) }
            override fun bindDouble(index: Int, value: Double) { bound[index] = "double:$value"; statement.bindDouble(index, value) }
            override fun bindNull(index: Int) { bound[index] = "null"; statement.bindNull(index) }
            override fun bindBlob(index: Int, value: ByteArray) { bound[index] = "blob:${value.contentHashCode()}"; statement.bindBlob(index, value) }
            override fun step(): Boolean {
                if (sql == "PRAGMA defer_foreign_keys = ON") writers++
                if (sql.startsWith("UPDATE _sync_operation_state SET workspace_context=?")) {
                    val ledger = bound.getValue(1).removePrefix("text:")
                    val owners = if (ledger.isEmpty()) JsonArray(emptyList()) else Json.parseToJsonElement(ledger).jsonArray
                    for (owner in owners) {
                        val entry = owner.jsonObject
                        assertEquals(setOf("token", "unit", "context", "entries", "bytes"), entry.keys)
                        for (key in listOf("token", "unit", "context"))
                            assertTrue(entry.getValue(key).jsonPrimitive.content.matches(Regex("[0-9a-f]{64}")))
                        assertTrue(entry.getValue("entries").jsonPrimitive.long >= 0)
                        assertTrue(entry.getValue("bytes").jsonPrimitive.long >= 0)
                    }
                    assertEquals(owners.sumOf { it.jsonObject.getValue("entries").jsonPrimitive.long }, bound.getValue(2).removePrefix("long:").toLong())
                    assertEquals(owners.sumOf { it.jsonObject.getValue("bytes").jsonPrimitive.long }, bound.getValue(3).removePrefix("long:").toLong())
                    assertEquals(owners.size, owners.map { it.jsonObject.getValue("token") }.distinct().size)
                    ownerChecks++
                }
                handle.steps++; steps[sql] = (steps[sql] ?: 0) + 1
                if (deterministic(sql)) {
                    val values = bound.toString().let { if (identityPrefix == null) it else it.replace(identityPrefix, "fixture-") }
                    bindings.update((sql + values + "\n").toByteArray())
                }
                return statement.step()
            }
            override fun reset() { handle.resets++; statement.reset() }
            override fun clearBindings() { handle.clears++; bound.clear(); statement.clearBindings() }
            override fun close() { handle.closes++; statement.close() }
        }
    }
    fun assertClosed() {
        handles.forEach { assertEquals(1L, it.closes, it.sql); assertEquals(it.resets, it.clears, it.sql) }
    }
    fun assertReused(sqls: Collection<String>, cachedOnly: Boolean = false) {
        for (sql in sqls) {
            val selected = handles.filter { it.sql == sql && (!cachedOnly || it.resets > 0) }
            assertTrue(selected.isNotEmpty(), sql)
            selected.groupBy { it.writer }.forEach { (writer, owned) ->
                assertEquals(1, owned.size, "one handle per writer=$writer SQL=$sql")
            }
            selected.forEach { assertTrue(it.resets > 0, "borrowed handle released: $sql") }
        }
    }
    fun report(operation: String, rows: Int, sample: Int, localMs: Double, publicMs: Double,
        diagnostics: WorkSliceDiagnostics, accounting: A3AccountingStats): String = buildJsonObject {
        put("operation", operation); put("rows", rows); put("sample", sample)
        put("prepares", handles.size); put("steps", steps.values.sum()); put("writers", writers)
        put("edges", steps.filterKeys { it.startsWith("INSERT OR IGNORE INTO _sync_apply_edges") }.values.sum())
        put("localMs", localMs); put("publicMs", publicMs); put("work", diagnostics.totalWork)
        put("ownerChecks", ownerChecks)
        put("diagnostics", diagnostics.toString()); put("accounting", accounting.toString())
        put("bindingDigest", bindings.digest().joinToString("") { "%02x".format(it) })
        put("sql", buildJsonArray { for ((sql, count) in steps) add(buildJsonObject {
            val hs = handles.filter { it.sql == sql }
            put("text", sql); put("steps", count); put("prepares", hs.size)
            put("resets", hs.sumOf { it.resets }); put("clears", hs.sumOf { it.clears }); put("closes", hs.sumOf { it.closes })
            put("maxHandlesPerWriter", hs.groupBy { it.writer }.values.maxOf { it.size })
        }) })
    }.toString()
    fun publish(operation: String, rows: Int, sample: Int, localMs: Double, publicMs: Double,
        diagnostics: WorkSliceDiagnostics, accounting: A3AccountingStats) {
        val report = Json.parseToJsonElement(report(operation, rows, sample, localMs, publicMs, diagnostics, accounting)).jsonObject
        println("APPLY_STATEMENT_SQL " + JsonObject(report - "sql"))
        for (family in report.getValue("sql").jsonArray)
            println("APPLY_STATEMENT_FAMILY " + buildJsonObject {
                put("operation", operation); put("rows", rows); put("sample", sample); put("family", family)
            })
    }
    suspend fun restore() = db.withExclusiveAccess {
        val field = SafeSQLiteConnection::class.java.getDeclaredField("ref").also { it.isAccessible = true }
        assertSame(this@ApplyStatementObservation, field.get(db)); field.set(db, delegate)
    }
    companion object {
        suspend fun install(db: SafeSQLiteConnection, identityPrefix: String? = null): ApplyStatementObservation = db.withExclusiveAccess {
            val field = SafeSQLiteConnection::class.java.getDeclaredField("ref").also { it.isAccessible = true }
            ApplyStatementObservation(db, field.get(db) as SQLiteConnection, identityPrefix).also { field.set(db, it) }
        }
    }
}

internal abstract class ApplyStatementTestSupport : RealServerSupport() {
    protected val applyNodeTables = listOf(SyncTable("nodes", "id"))
    protected class Fixture(val server: MockSyncServer, val db: SafeSQLiteConnection, val http: HttpClient,
        var client: DefaultOversqliteClient)
    protected suspend fun withApplyFixture(rows: Int, work: Long = 1_000_000, path: String? = null,
        block: suspend (Fixture) -> Unit) {
        val server = MockSyncServer()
        val table = GenerationTable("main", "nodes", listOf("id"), listOf(
            GenerationColumn("id", "text", false), GenerationColumn("name", "text", false),
            GenerationColumn("parent_id", "text", true)), listOf(
            GenerationForeignKey(listOf("parent_id"), "main", "nodes", listOf("id"), "cascade", "no_action")))
        server.generationOverride = SyncGeneration("1", "", listOf(table)).let { it.copy(fingerprint = it.computeFingerprint()) }
        val db = if (path == null) newDb() else newFileBackedDb(path)
        val http = server.newHttpClient()
        val client = newApplyClient(db, http, work)
        val fixture = Fixture(server, db, http, client)
        try {
            db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES nodes(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO nodes VALUES('Q','unrelated parent',NULL),('W','root',NULL)")
            client.pushPending().getOrThrow()
            db.transaction {
                db.execSQL("UPDATE _sync_apply_state SET apply_mode=1")
                repeat(rows) { db.execSQL("INSERT INTO nodes VALUES('Z${it.toString().padStart(5, '0')}','unrelated','Q')") }
                db.execSQL("UPDATE _sync_apply_state SET apply_mode=0")
            }
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            block(fixture)
        } finally { fixture.client.closeFromCoordinatedOwner(); http.close(); db.close() }
    }
    protected fun newApplyClient(db: SafeSQLiteConnection, http: HttpClient, work: Long = 1_000_000) =
        newClient(db, http, syncTables = applyNodeTables, workUnits = work, resolver = DeferConflictResolver)

    protected fun rootEffect(name: String, key: String = "W", version: Long = 2) = BundleRow("main", "nodes", mapOf("id" to key), "UPDATE", version,
        buildJsonObject { put("id", key); put("name", name); put("parent_id", JsonNull) })

    protected suspend fun conflict(f: Fixture, name: String, key: String = "W"): PendingConflict {
        f.db.execSQL("UPDATE nodes SET name='local rejected' WHERE id='$key'")
        f.server.conflictOverride = { row, _, _ -> PushConflictDetails(row.schema,row.table,row.key,row.op,row.baseRowVersion,
            row.baseRowVersion + 1,false,rootEffect(name, key).payload) }
        assertIs<ConflictNeedsResolutionException>(f.client.pushPending().exceptionOrNull())
        return checkNotNull(f.client.pendingConflict().getOrThrow())
    }
    protected suspend fun assertApplyClean(db: SafeSQLiteConnection, table: String = "nodes") {
        for (t in listOf("_sync_dirty_rows", "_sync_outbox_rows", "_sync_apply_rows", "_sync_apply_edges", "_sync_apply_plan"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM $t"), t)
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
        assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
        assertEquals(0L, scalarLong(db, "SELECT apply_mode FROM _sync_apply_state"))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM $table WHERE name IS NULL"))
    }
    protected suspend fun measure(f: Fixture, operation: String, rows: Int, sample: Int): ApplyStatementObservation {
        val name = "remote-$sample"
        val handle = if (operation == "conflict") conflict(f, name) else null
        if (operation == "pull") f.server.addRemoteBundleForTest(listOf(rootEffect(name, version = sample + 2L)))
        var observation: ApplyStatementObservation? = null
        var localStarted = 0L
        if (operation == "pull") f.server.beforePullResponse = { observation = ApplyStatementObservation.install(f.db); localStarted = System.nanoTime() }
        else { observation = ApplyStatementObservation.install(f.db); localStarted = System.nanoTime() }
        val started = System.nanoTime()
        val observed: ApplyStatementObservation
        try {
            if (handle == null) f.client.pullToStable().getOrThrow()
            else f.client.resolveConflict(handle, MergeResult.AcceptServer).getOrThrow()
            val ended = System.nanoTime()
            observed = checkNotNull(observation)
            observed.publish(operation, rows, sample,
                (ended-localStarted)/1_000_000.0,(ended-started)/1_000_000.0,f.client.workSliceDiagnostics(),f.client.workspaceDiagnostics())
        } finally { observation?.restore(); f.server.beforePullResponse = null }
        observed.assertClosed()
        assertEquals(name, scalarText(f.db, "SELECT name FROM nodes WHERE id='W'"))
        assertEquals(rows.toLong(), scalarLong(f.db, "SELECT count(*) FROM nodes WHERE parent_id='Q' AND name='unrelated'"))
        assertEquals(rows + 2L, scalarLong(f.db, "SELECT count(*) FROM nodes"))
        assertApplyClean(f.db)
        return observed
    }
}
