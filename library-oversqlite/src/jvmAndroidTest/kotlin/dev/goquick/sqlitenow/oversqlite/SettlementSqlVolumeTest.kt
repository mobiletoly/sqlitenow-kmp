package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

/** Observe the complete post-response settlement, not only statement-cache misses. */
internal class SettlementSqlVolumeTest : CrossTargetSyncTestSupport() {
    @Test fun oneRowSettlementReusesRelationshipChangeStatement() = runOwnedWebDatabaseTest {
        withFixture(32) { server, db, client ->
            db.execSQL("CREATE TABLE apply_probe(value INTEGER NOT NULL)")
            db.execSQL("INSERT INTO apply_probe VALUES(0)")
            db.execSQL("CREATE TRIGGER observe_apply AFTER UPDATE ON nodes BEGIN UPDATE apply_probe SET value=value+1; END")
            val counts = settle(server, db, client, 1)
            assertEquals(64L, counts.indexChangesSteps, "both FK columns visit each unrelated row")
            assertEquals(1L, counts.indexChangesPrepares, "one changes handle per settlement writer")
            assertEquals(64L, counts.edgeSteps, "duplicate edges still execute INSERT OR IGNORE")
            assertEquals(2L, scalarLong(db, "SELECT value FROM apply_probe"), "application trigger retains local write plus acknowledgement replay")
            assertSettled(db, 32, 1)
        }
    }

    @Test fun wholeSettlementCostAcrossDatabaseSizes() = runOwnedWebDatabaseTest(timeout = 180.seconds) {
        var shapeCounts: Pair<Long, Long>? = null
        for (size in listOf(100, 1000, 10000)) {
            withFixture(size) { server, db, client ->
                // Warm the same path once; retain three independent one-row pushes.
                settle(server, db, client, 0)
                repeat(3) { sample ->
                    val counts = settle(server, db, client, sample + 1)
                    assertEquals(0L, counts.indexChangesSteps)
                    assertEquals(0L, counts.edgeSteps)
                    assertEquals(0L, counts.scanSteps, "unchanged acknowledgement never enumerates unrelated relationships")
                    val current = counts.prepares to counts.steps
                    shapeCounts?.let { assertEquals(it, current, "whole-settlement SQL count is independent of unrelated rows") }
                    shapeCounts = current
                    assertSettled(db, size, sample + 1)
                    println("SETTLEMENT_SQL rows=$size sample=$sample $counts")
                }
            }
        }
    }

    @Test fun normalizedValuesUseSurvivorPreparation() = runOwnedWebDatabaseTest {
        withFixture(32) { server, db, client ->
            server.normalizePushEffects = { effects -> effects.map { row ->
                row.copy(payload = JsonObject(row.payload!!.jsonObject + ("name" to JsonPrimitive("normalized"))))
            } }
            val counts = settle(server, db, client, 1)
            assertEquals(64L, counts.edgeSteps)
            assertSettled(db, 32, 1, expectedName = "normalized")
        }
    }

    @Test fun temporaryApplicationTriggersRetainReplay() = runOwnedWebDatabaseTest {
        withFixture(32) { server, db, client ->
            db.execSQL("CREATE TABLE apply_probe(value INTEGER NOT NULL)")
            db.execSQL("INSERT INTO apply_probe VALUES(0)")
            db.execSQL("CREATE TEMP TRIGGER observe_temp_apply AFTER UPDATE ON main.nodes BEGIN UPDATE apply_probe SET value=value+1; END")
            val counts = settle(server, db, client, 1)
            assertEquals(64L, counts.edgeSteps)
            assertEquals(2L, scalarLong(db, "SELECT value FROM apply_probe"))
            assertSettled(db, 32, 1)
        }
    }

    @Test fun editsDuringUploadKeepPendingIntent() = runOwnedWebDatabaseTest {
        for (editedKey in listOf("W", "Q")) withFixture(32) { server, db, client ->
            val counts = settle(server, db, client, 1) {
                db.execSQL("UPDATE nodes SET name='new local intent' WHERE id='$editedKey'")
            }
            assertEquals(if (editedKey == "W") 64L else 0L, counts.edgeSteps, "uploaded edits retain full preservation; other pending keys do not require indexing")
            assertEquals("new local intent", scalarText(db, "SELECT name FROM nodes WHERE id='$editedKey'"))
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
        }
    }

    @Test fun cancelledAcknowledgementReplaysFrozenOutboxAfterRecreation() = runOwnedWebDatabaseTest(timeout = 180.seconds) {
        for (work in listOf(1L, 8L, 17L, 1_000_000L)) for (addTrigger in listOf(false, true)) withFixture(32, workUnits = work) { server, db, client ->
            db.execSQL("UPDATE nodes SET name='water-1' WHERE id='W'")
            var pending: List<String>? = null
            server.beforePushResponse = {
                db.execSQL("INSERT INTO nodes VALUES('I','new pending child','W','Q')")
                pending = pendingState(db)
                server.beforePushResponse = null
            }
            client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(beforeCommit = {
                if (scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state") == 2L)
                    throw CancellationException("cancel acknowledged metadata before commit")
            }))
            try { assertFailsWith<CancellationException> { client.pushPending().getOrThrow() } }
            finally { client.setSnapshotApplyFaultInjectorForTest(null) }
            assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            val frozen = server.pushBodies.last()
            client.closeFromCoordinatedOwner()
            if (addTrigger) {
                db.execSQL("CREATE TABLE apply_probe(value INTEGER NOT NULL)")
                db.execSQL("INSERT INTO apply_probe VALUES(0)")
                db.execSQL("CREATE TRIGGER recovery_apply AFTER UPDATE ON nodes BEGIN UPDATE apply_probe SET value=value+1; END")
            }
            val http = server.newHttpClient()
            val restarted = newClient(db, http, syncTables = listOf(SyncTable("nodes", "id")), workUnits = work)
            try {
                restarted.openAndConnect("user-1").getOrThrow()
                val observer = SqlObservation.install(db)
                try { restarted.pushPending().getOrThrow() }
                finally { observer.restore() }
                assertEquals(if (addTrigger) 2L * (32 + 1) else 0L, observer.counts.edgeSteps, "both FK columns of the pending child join the retained fallback index")
                if (addTrigger) assertEquals(1L, scalarLong(db, "SELECT value FROM apply_probe"), "changed eligibility cannot reuse a root-only plan")
                assertEquals(frozen, server.pushBodies.last(), "retry retains the exact frozen request")
                assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                assertEquals(3L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
                assertEquals(pending, pendingState(db), "pending payload/base/provenance/relationships survive rollback and retry")
                assertSettled(db, 32, 1, pendingRows = 1)
            } finally { restarted.closeFromCoordinatedOwner(); http.close() }
        }
    }

    @Test fun acknowledgementStillValidatesParentEvidence() = runOwnedWebDatabaseTest {
        withFixture(32) { server, db, client ->
            db.execSQL("UPDATE nodes SET parent_id='Q' WHERE id='W'")
            server.beforePushResponse = {
                db.execSQL("UPDATE _sync_row_state SET server_kind='unknown',server_payload=NULL,row_version=0,relationships_known=0,server_provenance=json_set(server_provenance,'$.authoritative_version',0,'$.origin','explicit_conflict','$.bundle_sequence',NULL) WHERE table_name='nodes' AND key_json='{\"id\":\"Q\"}'")
            }
            assertIs<FKReconciliationRequiredException>(client.pushPending().exceptionOrNull())
            assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals("Q", scalarText(db, "SELECT parent_id FROM nodes WHERE id='W'"))
        }
    }

    @Test fun pendingIntentSettlementCostAcrossDatabaseSizes() = runOwnedWebDatabaseTest(timeout = 180.seconds) {
        val observations = mutableListOf<Pair<Int, Counts>>()
        for ((size, pendingCount) in listOf(100 to 1, 1000 to 1, 10000 to 1, 10000 to 32)) {
            withFixture(size, workUnits = 1_000_000) { server, db, client ->
                repeat(pendingCount) { db.execSQL("INSERT INTO nodes VALUES('I$it','published','W','Q')") }
                client.pushPending().getOrThrow()
                repeat(4) { sample ->
                    var pending: List<String>? = null
                    val counts = settle(server, db, client, sample) {
                        repeat(pendingCount) { db.execSQL("UPDATE nodes SET name='pending-$sample' WHERE id='I$it'") }
                        pending = pendingState(db)
                    }
                    assertEquals(pending, pendingState(db), "pending owners and desired relationships stay byte-identical")
                    assertSettled(db, size, sample, pendingRows = pendingCount)
                    if (sample > 0) {
                        observations += pendingCount to counts
                        println("PENDING_SETTLEMENT_SQL rows=$size pending=$pendingCount sample=$sample $counts")
                    }
                    // Drain only outside the measured boundary; the next frozen push contains one row.
                    client.pushPending().getOrThrow()
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
                }
            }
        }
        // Assert after collecting all sizes so an untouched-source baseline retains every measurement.
        observations.forEach { (_, counts) ->
            assertEquals(0L, counts.edgeSteps, "unchanged acknowledgements with pending intent never build the global index")
            assertEquals(0L, counts.scanSteps)
        }
        assertEquals(1, observations.filter { it.first == 1 }.map { it.second.prepares to it.second.steps }.distinct().size,
            "fixed pending population must not scale SQL calls with clean database size")
    }

    @Test fun unrelatedPendingOperationsRetainTheirHistoricalOwners() = runOwnedWebDatabaseTest {
        for (operation in listOf("INSERT", "UPDATE", "DELETE")) for (related in listOf(false, true)) {
            withFixture(32) { server, db, client ->
                val parent = if (related) "W" else "Q"
                if (operation != "INSERT") {
                    db.execSQL("INSERT INTO nodes VALUES('I','published','$parent',NULL)")
                    client.pushPending().getOrThrow()
                }
                var pending: List<String>? = null
                val counts = settle(server, db, client, 1) {
                    db.execSQL(when (operation) {
                        "INSERT" -> "INSERT INTO nodes VALUES('I','new pending','$parent',NULL)"
                        "UPDATE" -> "UPDATE nodes SET name='new pending' WHERE id='I'"
                        else -> "DELETE FROM nodes WHERE id='I'"
                    })
                    pending = pendingState(db)
                }
                assertEquals(pending, pendingState(db), "$operation related=$related preserves historical B and desired projection")
                assertEquals(if (operation == "DELETE") 0L else 1L, scalarLong(db, "SELECT count(*) FROM nodes WHERE id='I'"))
                assertSettled(db, 32, 1, pendingRows = 1)
                assertEquals(0L, counts.edgeSteps)
            }
        }
    }

    @Test fun pendingPhysicalExceptionIsPreservedWithoutBusinessWrites() = runOwnedWebDatabaseTest {
        withFixture(32) { server, db, client ->
            var pending: List<String>? = null
            val counts = settle(server, db, client, 1) {
                db.execSQL("INSERT INTO nodes VALUES('I','pending desired','W','Q')")
                pending = pendingState(db)
                db.execSQL("UPDATE _sync_apply_state SET apply_mode=1")
                try { db.execSQL("UPDATE nodes SET name='physical exception' WHERE id='I'") }
                finally { db.execSQL("UPDATE _sync_apply_state SET apply_mode=0") }
            }
            assertEquals(pending, pendingState(db))
            assertEquals("physical exception", scalarText(db, "SELECT name FROM nodes WHERE id='I'"))
            assertSettled(db, 32, 1, pendingRows = 1)
            assertEquals(0L, counts.edgeSteps)
        }
    }

    @Test fun fallbackCleanupReusesHandlesAcrossPageBoundaries() = runOwnedWebDatabaseTest(timeout = 180.seconds) {
        val observations = mutableListOf<Counts>()
        for (size in listOf(0, 1, 128, 129, 257)) withFixture(size) { server, db, client ->
            db.execSQL("CREATE TRIGGER force_replay AFTER UPDATE ON nodes BEGIN SELECT 1; END")
            val counts = settle(server, db, client, 1, key = "Q")
            assertEquals(2L * size, counts.edgeSteps)
            assertEquals(size / 128L + if (size % 128 == 0) 1 else 2, counts.cleanupEdgeSteps)
            val rows = size + 1L
            assertEquals(rows / 128L + if (rows % 128 == 0L) 1 else 2, counts.cleanupRowSteps)
            assertEquals(counts.cleanupEdgeSteps + counts.cleanupRowSteps, counts.cleanupChangesSteps)
            assertEquals(1L, counts.cleanupPlanSteps)
            assertEquals("water-1", scalarText(db, "SELECT name FROM nodes WHERE id='Q'"))
            assertEquals(size.toLong(), scalarLong(db, "SELECT count(*) FROM nodes WHERE parent_id='Q' AND other_id='Q'"))
            for (table in listOf("_sync_dirty_rows", "_sync_apply_rows", "_sync_apply_edges", "_sync_apply_plan", "_sync_outbox_rows"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM $table"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
            println("CLEANUP_SETTLEMENT_SQL rows=$size $counts")
            observations += counts
        }
        observations.forEach {
            assertEquals(1L, it.cleanupEdgePrepares)
            assertEquals(1L, it.cleanupRowPrepares)
            assertEquals(1L, it.cleanupPlanPrepares)
            assertEquals(1L, it.changesPrepares, "index and cleanup share one writer-owned changes handle")
        }
    }

    private suspend fun pendingState(db: SafeSQLiteConnection): List<String> =
        listOf("_sync_dirty_rows" to "", "_sync_relationships" to " WHERE view='desired'").map { (table, filter) ->
            val columns = db.queryList("PRAGMA table_info($table)") { it.getText(1) }
            val projection = columns.joinToString(",") { "'$it',json_object('storage',typeof($it),'value',CAST($it AS TEXT))" }
            scalarText(db, "SELECT json_group_array(json_object($projection)) FROM (SELECT * FROM $table$filter ORDER BY ${columns.joinToString(",")})")
        }

    private suspend fun withFixture(size: Int, workUnits: Long = 20_000_000, block: suspend (MockSyncServer, SafeSQLiteConnection, DefaultOversqliteClient) -> Unit) {
        val server = MockSyncServer()
        val fks = listOf("parent_id", "other_id").map {
            GenerationForeignKey(listOf(it), "main", "nodes", listOf("id"), "cascade", "no_action")
        }
        server.generationOverride = SyncGeneration("1", "", listOf(GenerationTable("main", "nodes", listOf("id"),
            listOf(GenerationColumn("id", "text", false), GenerationColumn("name", "text", false),
                GenerationColumn("parent_id", "text", true), GenerationColumn("other_id", "text", true)), fks)))
            .let { it.copy(fingerprint = it.computeFingerprint()) }
        val db = newDb()
        val http = server.newHttpClient()
        val client = newClient(db, http, syncTables = listOf(SyncTable("nodes", "id")), workUnits = workUnits)
        try {
            db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL,parent_id TEXT REFERENCES nodes(id) ON DELETE CASCADE,other_id TEXT REFERENCES nodes(id) ON DELETE CASCADE)")
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO nodes VALUES('Q','unrelated parent',NULL,NULL),('W','water',NULL,NULL)")
            client.pushPending().getOrThrow()
            // Synthetic clean relationship population, outside the measured boundary.
            // The two FKs deliberately collapse to one retained edge per child.
            db.transaction {
                db.execSQL("UPDATE _sync_apply_state SET apply_mode=1")
                repeat(size) {
                    db.execSQL("INSERT INTO nodes VALUES('Z${it.toString().padStart(5, '0')}','unrelated','Q','Q')")
                }
                db.execSQL("UPDATE _sync_apply_state SET apply_mode=0")
            }
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            block(server, db, client)
        } finally { client.closeFromCoordinatedOwner(); http.close(); db.close() }
    }

    private suspend fun settle(server: MockSyncServer, db: SafeSQLiteConnection, client: DefaultOversqliteClient, value: Int, key: String = "W", beforeResponse: suspend () -> Unit = {}): Counts {
        db.execSQL("UPDATE nodes SET name='water-$value' WHERE id='$key'")
        assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
        var observation: SqlObservation? = null
        var started = 0L
        server.beforePushResponse = {
            beforeResponse()
            observation = SqlObservation.install(db)
            started = System.nanoTime()
        }
        val callStarted = System.nanoTime()
        try {
            client.pushPending().getOrThrow()
            val elapsed = System.nanoTime() - started
            return checkNotNull(observation).counts.copy(elapsedMs = elapsed / 1_000_000.0, callMs = (System.nanoTime() - callStarted) / 1_000_000.0)
        } finally { observation?.restore(); server.beforePushResponse = null }
    }

    private suspend fun assertSettled(db: SafeSQLiteConnection, size: Int, value: Int, expectedName: String = "water-$value", pendingRows: Int = 0) {
        assertEquals(expectedName, scalarText(db, "SELECT name FROM nodes WHERE id='W'"))
        assertEquals(size.toLong(), scalarLong(db, "SELECT count(*) FROM nodes WHERE parent_id='Q' AND other_id='Q'"))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
        assertEquals(pendingRows.toLong(), scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
        for (table in listOf("_sync_outbox_rows", "_sync_apply_rows", "_sync_apply_edges", "_sync_apply_plan"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM $table"), table)
        assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+apply_mode FROM _sync_operation_state CROSS JOIN _sync_apply_state"))
    }

    private data class Counts(var prepares: Long = 0, var steps: Long = 0, var changesPrepares: Long = 0,
        var changesSteps: Long = 0, var indexChangesPrepares: Long = 0, var indexChangesSteps: Long = 0,
        var edgeSteps: Long = 0, var scanSteps: Long = 0, var elapsedMs: Double = 0.0, var callMs: Double = 0.0,
        var cleanupEdgePrepares: Long = 0, var cleanupRowPrepares: Long = 0, var cleanupPlanPrepares: Long = 0,
        var cleanupEdgeSteps: Long = 0, var cleanupRowSteps: Long = 0, var cleanupPlanSteps: Long = 0, var cleanupChangesSteps: Long = 0)

    /** Test-only raw SQLite wrapper; counts cached executions as well as prepares. */
    private class SqlObservation(private val db: SafeSQLiteConnection, private val delegate: SQLiteConnection) : SQLiteConnection by delegate {
        val counts = Counts()
        private var lastStepWasEdge = false
        override fun prepare(sql: String): SQLiteStatement {
            counts.prepares++
            if (sql.startsWith("DELETE FROM _sync_apply_edges WHERE ordinal IN")) counts.cleanupEdgePrepares++
            if (sql.startsWith("DELETE FROM _sync_apply_rows WHERE ordinal IN")) counts.cleanupRowPrepares++
            if (sql == "DELETE FROM _sync_apply_plan") counts.cleanupPlanPrepares++
            if (sql == "SELECT changes()") counts.changesPrepares++
            if (sql == "SELECT changes()" && lastStepWasEdge) counts.indexChangesPrepares++
            val statement = delegate.prepare(sql)
            return object : SQLiteStatement by statement {
                override fun step(): Boolean {
                    counts.steps++
                    if (sql.startsWith("DELETE FROM _sync_apply_edges WHERE ordinal IN")) counts.cleanupEdgeSteps++
                    if (sql.startsWith("DELETE FROM _sync_apply_rows WHERE ordinal IN")) counts.cleanupRowSteps++
                    if (sql == "DELETE FROM _sync_apply_plan") counts.cleanupPlanSteps++
                    if (sql == "SELECT changes()" && !lastStepWasEdge) counts.cleanupChangesSteps++
                    if (sql == "SELECT changes()") counts.changesSteps++
                    if (sql == "SELECT changes()" && lastStepWasEdge) counts.indexChangesSteps++
                    if (sql.startsWith("INSERT OR IGNORE INTO _sync_apply_edges")) counts.edgeSteps++
                    if (sql.contains("FROM \"nodes\"") && (sql.startsWith("SELECT CAST(") || sql.startsWith("SELECT length(CAST(CAST("))) counts.scanSteps++
                    lastStepWasEdge = sql.startsWith("INSERT OR IGNORE INTO _sync_apply_edges")
                    return statement.step()
                }
            }
        }
        suspend fun restore() = db.withExclusiveAccess {
            val field = SafeSQLiteConnection::class.java.getDeclaredField("ref").also { it.isAccessible = true }
            assertSame(this@SqlObservation, field.get(db))
            field.set(db, delegate)
        }
        companion object {
            suspend fun install(db: SafeSQLiteConnection): SqlObservation = db.withExclusiveAccess {
                val field = SafeSQLiteConnection::class.java.getDeclaredField("ref").also { it.isAccessible = true }
                SqlObservation(db, field.get(db) as SQLiteConnection).also { field.set(db, it) }
            }
        }
    }
}
