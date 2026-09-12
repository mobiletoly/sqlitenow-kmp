package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteStatement
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.*
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

/** Counts real transaction-owned handles separately from durable workspace charges. */
internal class ReplayStatementReuseObservationTest : SurvivorTestSupport() {
    @Test fun preparationsAreBoundedBySchema() = runOwnedWebDatabaseTest(timeout = 180.seconds) {
        var populatedShapes: Set<String>? = null
        var populatedBytes: Int? = null
        for (size in listOf(0, 1000, 10000)) {
            val path = createTempSqliteNowTestDbPath("statement-reuse-$size")
            try {
                withSurvivorFixture(workUnits = 20_000_000, dbPath = path) { server, db, _, client ->
                    try {
                        db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
                        server.normalizePushEffects = { effects() }
                        server.beforePushResponse = { throw CancellationException("lost committed response") }
                        assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
                        val request = server.pushBodies.last()
                        db.transaction {
                            repeat(size) { db.execSQL("INSERT INTO nodes VALUES('Z${it.toString().padStart(5, '0')}','${"x".repeat(16)}',NULL)") }
                        }
                        val dirtySql = "SELECT coalesce(json_group_array(payload),'[]') FROM (SELECT payload FROM _sync_dirty_rows WHERE json_extract(key_json,'$.id') LIKE 'Z%' ORDER BY key_json)"
                        val dirty = scalarText(db, dirtySql)
                        val handles = ReuseHandles()
                        val raw = ReplaySqlObservation.install(db)
                        client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(statementCacheOperations = handles.operations))
                        try { client.pushPending().getOrThrow() }
                        finally { client.setSnapshotApplyFaultInjectorForTest(null); raw.restore() }
                        handles.assertClosed()
                        assertEquals(405L + 39L * size, raw.counts.values.sumOf { it.steps }, "unchanged logical SQL work")
                        assertEquals(28L + 4L * size, raw.counts.entries.single { it.key.startsWith("INSERT INTO _sync_apply_plan(") }.value.steps)
                        val stats = client.workspaceDiagnostics()
                        if (size <= 1000) ReplaySqlStructuralTest().assertReplayOracle(size, buildJsonObject {
                            put("bindingTrace", raw.bindingTraceHash())
                            put("selectedExecutions", raw.selectedExecutions)
                            put("accounting", stats.toString())
                            put("sql", buildJsonObject { raw.counts.forEach { (sql, count) ->
                                put(sql, buildJsonObject {
                                    put("prepares", count.prepares); put("steps", count.steps)
                                    put("resets", count.resets); put("clears", count.clears); put("closes", count.closes)
                                })
                            } })
                        })
                        assertEquals(4L * size, stats.primaryReads)
                        assertEquals(8L * size, stats.indexedVisits)
                        assertEquals(4L * size, stats.duplicateChecks)
                        assertEquals(3L, stats.restorationPasses)
                        assertEquals(3L, stats.savedCopies)
                        assertEquals(468L, stats.savedBytes)
                        assertEquals(3864L + 1104L * size, stats.work)
                        requireSurvivors(db)
                        assertEquals(dirty, scalarText(db, dirtySql))
                        assertEquals(size.toLong(), scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
                        assertEquals(request, server.pushBodies.last())
                        assertEquals(3, server.pushRequestCount)
                        assertEquals(3L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
                        assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
                        assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+apply_mode FROM _sync_operation_state CROSS JOIN _sync_apply_state"))
                        val owner = replayValidatedOwner(client)
                        val family = handles.sql.filter { replayStatementShape(it) }.toSet()
                        val tables = owner.tables.size
                        val foreignKeys = owner.tableInfoByName.values.sumOf { it.foreignKeys.size }
                        assertTrue(family.size <= 22 + 6 * tables + 2 * foreignKeys, "schema-bound replay handles")
                        val bytes = handles.sql.sumOf { it.encodeToByteArray().size }
                        if (size > 0) {
                            populatedShapes?.let { assertEquals(it, handles.sql.toSet(), "backlog does not grow cache shape set") }
                            populatedBytes?.let { assertEquals(it, bytes, "backlog does not grow retained SQL bytes") }
                            populatedShapes = handles.sql.toSet(); populatedBytes = bytes
                        }
                        println("REPLAY_REUSE_OBSERVATION " + buildJsonObject {
                            put("size", size); put("physicalPrepares", raw.counts.values.sumOf { it.prepares })
                            put("logicalSteps", raw.counts.values.sumOf { it.steps })
                            put("transactionHandles", handles.sql.size); put("replayHandles", family.size)
                            put("retainedSqlUtf8Bytes", bytes); put("accounting", stats.toString())
                        })
                        val durable = baselineQualificationVector(db, listOf("nodes"))
                        client.closeFromCoordinatedOwner(); db.close()
                        val reopened = newFileBackedDb(path)
                        try { assertEquals(durable, baselineQualificationVector(reopened, listOf("nodes"))) }
                        finally { reopened.close() }
                    } finally { client.closeFromCoordinatedOwner() }
                }
            } finally { deleteTempSqliteNowTestDbArtifacts(path) }
        }
    }

    @Test fun multipleTablesAndForeignKeysKeepDistinctShapes() = runOwnedWebDatabaseTest(timeout = 180.seconds) {
        val server = MockSyncServer()
        val tables = listOf(SyncTable("parents", "id"), SyncTable("children", "id"))
        server.generationOverride = SyncGeneration("1", "", listOf(
            GenerationTable("main", "parents", listOf("id"), listOf(GenerationColumn("id", "text", false), GenerationColumn("name", "text", false)), emptyList()),
            GenerationTable("main", "children", listOf("id"), listOf(GenerationColumn("id", "text", false), GenerationColumn("left_id", "text", true), GenerationColumn("right_id", "text", true)), listOf(
                GenerationForeignKey(listOf("left_id"), "main", "parents", listOf("id"), "no_action", "no_action"),
                GenerationForeignKey(listOf("right_id"), "main", "parents", listOf("id"), "no_action", "no_action"))),
        )).let { it.copy(fingerprint = it.computeFingerprint()) }
        val db = newDb(); val http = server.newHttpClient()
        val client = newClient(db, http, syncTables = tables, workUnits = 1_000_000)
        try {
            db.execSQL("CREATE TABLE parents(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)")
            db.execSQL("CREATE TABLE children(id TEXT PRIMARY KEY NOT NULL,left_id TEXT REFERENCES parents(id),right_id TEXT REFERENCES parents(id))")
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO parents VALUES('P','parent'),('Q','other')")
            db.execSQL("INSERT INTO children VALUES('C','P','Q')")
            client.pushPending().getOrThrow()
            db.execSQL("UPDATE parents SET name='updated' WHERE id='P'")
            server.beforePushResponse = { throw CancellationException("lost committed response") }
            assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
            db.execSQL("UPDATE children SET left_id='Q',right_id='P' WHERE id='C'")
            val handles = ReuseHandles()
            client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(statementCacheOperations = handles.operations))
            try { client.pushPending().getOrThrow() }
            finally { client.setSnapshotApplyFaultInjectorForTest(null) }
            handles.assertClosed()
            for (column in listOf("left_id", "right_id")) {
                assertTrue(handles.sql.any { it.startsWith("SELECT CAST(\"$column\" AS TEXT)") }, column)
                assertTrue(handles.sql.any { it.startsWith("SELECT length(CAST(CAST(\"$column\"") }, column)
            }
            for (table in listOf("parents", "children"))
                assertTrue(handles.sql.any { it.startsWith("SELECT 2+") && it.contains("FROM \"$table\"") }, table)
            assertEquals("Q", scalarText(db, "SELECT left_id FROM children"))
            assertEquals("P", scalarText(db, "SELECT right_id FROM children"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertTrue(handles.sql.count(::replayStatementShape) <= 22 + 6 * 2 + 2 * 2)
        } finally { client.closeFromCoordinatedOwner(); http.close(); db.close() }
    }

    private class ReuseHandles {
        val sql = mutableListOf<String>()
        private val closed = mutableListOf<String>()
        val operations = StatementCacheOperations(prepare = { db, text ->
            assertFalse(text in sql, "one physical preparation per transaction shape: $text")
            sql += text
            val delegate = db.prepare(text)
            object : SQLiteStatement by delegate {
                private var active = false
                private var reset = false
                override fun step(): Boolean {
                    if (replayStatementShape(text)) assertFalse(active, "one-step replay helper cannot reenter a live lease")
                    active = true; reset = false
                    return delegate.step()
                }
                override fun reset() { delegate.reset(); active = false; reset = true }
                override fun clearBindings() { assertTrue(reset, "reset precedes clear"); delegate.clearBindings() }
                override fun close() { assertFalse(active, "release before owner close"); delegate.close(); closed += text }
            }
        })
        fun assertClosed() { assertEquals(sql.asReversed(), closed, "reverse owner disposal before commit") }
    }
}

private fun replayStatementShape(sql: String): Boolean =
    sql.startsWith("INSERT INTO _sync_apply_plan(") || sql.startsWith("INSERT INTO _sync_apply_rows(") ||
        sql.startsWith("INSERT OR IGNORE INTO _sync_apply_edges") || sql.startsWith("UPDATE _sync_apply_rows SET pending=2") ||
        (sql.startsWith("SELECT ") && (sql.contains(" FROM _sync_apply_rows ") || sql.contains(" FROM _sync_apply_edges ") ||
            sql.startsWith("SELECT unit,guard,phase,root_cursor,") || sql.startsWith("SELECT 64+6*(length(CAST(schema_name") ||
            sql.startsWith("SELECT schema_name,table_name,key_json,payload FROM _sync_dirty_rows") ||
            sql.startsWith("SELECT 2+") || sql.startsWith("SELECT CAST(\"") || sql.startsWith("SELECT length(CAST(CAST(") ||
            (sql.startsWith("SELECT json_object(") && sql.contains(" live_row")) ||
            sql.contains("NULL,0,0,0,0,0,p.singleton_key")))
