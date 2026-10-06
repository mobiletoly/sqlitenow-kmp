package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.TimeSource

/** Synthetic capacity and atomic installation witnesses. */
internal class SnapshotWorkCapacityTest : CrossTargetSyncTestSupport() {
    @Test
    fun observedSizeInstallsAsOneAtomicSnapshot() = runOwnedWebDatabaseTest {
        withSnapshot(3_399) { db, client, _ ->
            val started = TimeSource.Monotonic.markNow()
            client.openAndConnect("user-1").getOrThrow()
            assertInstalled(db, 3_399)
            println("snapshot_capacity rows=3399 tables=14 max_fks=3 install_ms=${started.elapsedNow().inWholeMilliseconds}")
        }
    }

    @Test
    fun firstRowBeyondFormerWorkLimitInstalls() = runOwnedWebDatabaseTest {
        withSnapshot(753) { db, client, _ ->
            client.openAndConnect("user-1").getOrThrow()
            assertInstalled(db, 753)
        }
    }

    @Test
    fun formerBoundaryInstalls() = runOwnedWebDatabaseTest {
        withSnapshot(752) { db, client, _ ->
            client.openAndConnect("user-1").getOrThrow()
            assertInstalled(db, 752)
        }
    }

    @Test
    fun applicationTriggerRowVisitsAreNotBoundedByCurrentReservation() = runOwnedWebDatabaseTest {
        withSnapshot(14) { db, client, _ ->
            db.execSQL("CREATE TABLE local_trigger_targets(id INTEGER PRIMARY KEY, visits INTEGER NOT NULL)")
            db.execSQL("WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<20000) " +
                "INSERT INTO local_trigger_targets SELECT i,0 FROM n")
            db.execSQL("CREATE TRIGGER visit_local_rows AFTER INSERT ON ${names.last()} BEGIN " +
                "UPDATE local_trigger_targets SET visits=visits+1; END")
            client.openAndConnect("user-1").getOrThrow()
            val visits = scalarLong(db, "SELECT sum(visits) FROM local_trigger_targets")
            val charged = client.workspaceDiagnostics().work
            assertEquals(20_000L, visits)
            assertTrue(visits > charged, "trigger row updates alone exceed all charged invocation work: $charged")
            assertEquals(14L, scalarLong(db, "SELECT count(*) FROM _sync_row_state"))
            println("snapshot_trigger_witness managed_rows=14 trigger_updated_rows=$visits charged_work=$charged")
        }
    }

    private suspend fun assertInstalled(db: SafeSQLiteConnection, rows: Int) {
        assertEquals(rows.toLong(), tables.sumOf { scalarLong(db, "SELECT count(*) FROM ${it.tableName}") })
        assertEquals(rows.toLong(), scalarLong(db, "SELECT count(*) FROM _sync_row_state"))
        assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        for (table in listOf("_sync_snapshot_stage", "_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges", "_sync_dirty_rows", "_sync_outbox_rows")) {
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM $table"), table)
        }
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
        assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes FROM _sync_operation_state"))
    }

    private val names = (0 until 14).map { "records_${it.toString().padStart(2, '0')}" }
    private val tables = names.map { SyncTable(it, syncKeyColumnName = "id") }

    private fun snapshotRows(rows: Int, version: Long, namePrefix: String): List<BundleRow> = (0 until rows).map { index ->
        val table = if (index < 13) names[index] else names.last()
        val id = if (index < 13) "parent" else "row-${index.toString().padStart(5, '0')}"
        BundleRow("main", table, mapOf("id" to id), "INSERT", version, buildJsonObject {
            put("id", id); put("name", "$namePrefix-$index")
            if (index >= 13) for (parent in 0..2) put("parent_$parent", "parent")
        })
    }

    private suspend fun withSnapshot(rows: Int,
        block: suspend (SafeSQLiteConnection, DefaultOversqliteClient, MockSyncServer) -> Unit) {
        val server = MockSyncServer()
        server.generationOverride = capacityGeneration()
        server.addRemoteBundleForTest(snapshotRows(rows, 1, "synthetic"))
        val db = newDb()
        val http = server.newHttpClient()
        var client: DefaultOversqliteClient? = null
        try {
            for ((index, name) in names.withIndex()) {
                val foreignKeys = if (index != 13) "" else (0..2).joinToString("") {
                    ", parent_$it TEXT NOT NULL REFERENCES ${names[it]}(id) DEFERRABLE INITIALLY DEFERRED"
                }
                db.execSQL("CREATE TABLE $name(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL$foreignKeys)")
            }
            // No workspace/work overrides: exercise the ordinary public defaults.
            val owned = DefaultOversqliteClient.forTesting(db = db, http = http,
                config = OversqliteConfig(generationId = 1, databaseId = "capacity", serviceId = "fixture", syncTables = tables))
            client = owned
            block(db, owned, server)
        } finally {
            client?.close()
            http.close()
            db.close()
        }
    }

    private fun capacityGeneration(): SyncGeneration {
        val generationTables = names.mapIndexed { index, name ->
            val parents = if (index == 13) (0..2).toList() else emptyList()
            GenerationTable("main", name, listOf("id"),
                listOf(GenerationColumn("id", "text", false), GenerationColumn("name", "text", false)) +
                    parents.map { GenerationColumn("parent_$it", "text", false) },
                parents.map { GenerationForeignKey(listOf("parent_$it"), "main", names[it], listOf("id"), "no_action", "no_action") })
        }
        return SyncGeneration("1", "", generationTables).let { it.copy(fingerprint = it.computeFingerprint()) }
    }
}
