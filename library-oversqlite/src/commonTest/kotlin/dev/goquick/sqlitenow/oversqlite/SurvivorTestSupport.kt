package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.serialization.json.Json
import kotlin.test.assertEquals

internal abstract class SurvivorTestSupport : CrossTargetSyncTestSupport() {
    protected val nodeTables = listOf(SyncTable("nodes", syncKeyColumnName = "id"))
    protected fun effects() = listOf(
        BundleRow("main", "nodes", mapOf("id" to "P"), "DELETE", 2, null),
        BundleRow("main", "nodes", mapOf("id" to "C"), "UPDATE", 2,
            Json.parseToJsonElement("""{"id":"C","name":"child","parent_id":"Q"}""")),
    )
    protected suspend fun withSurvivorFixture(
        action: String = "CASCADE",
        workUnits: Long = 1000000,
        leaseGroup: Any = Any(),
        dbPath: String? = null,
        block: suspend (MockSyncServer, SafeSQLiteConnection, io.ktor.client.HttpClient, DefaultOversqliteClient) -> Unit,
    ) {
        val server = MockSyncServer()
        val table = GenerationTable("main", "nodes", listOf("id"), listOf(
            GenerationColumn("id", "text", false), GenerationColumn("name", "text", false),
            GenerationColumn("parent_id", "text", true),
        ), listOf(GenerationForeignKey(listOf("parent_id"), "main", "nodes", listOf("id"), action.lowercase().replace(' ', '_'), "no_action")))
        server.generationOverride = SyncGeneration("1", "", listOf(table)).let { it.copy(fingerprint = it.computeFingerprint()) }
        val db = if (dbPath == null) newDb() else newFileBackedDb(dbPath)
        val http = server.newHttpClient()
        try {
            db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE $action DEFERRABLE INITIALLY DEFERRED)")
            val client = newClient(db, http, syncTables = nodeTables,workUnits=workUnits,leaseGroup=leaseGroup)
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO nodes VALUES('P','parent',NULL),('Q','other',NULL),('C','child','P'),('D','grandchild','C')")
            client.pushPending().getOrThrow()
            block(server, db, http, client)
        } finally { http.close(); db.close() }
    }

    protected suspend fun interruptApplyAfterCommittedSlice(client: DefaultOversqliteClient, operation: suspend () -> Unit) {
        var commits = 0
        client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(beforeCommit = {
            if (++commits == 2) throw kotlinx.coroutines.CancellationException("interrupt after a durable preparation slice")
        }))
        try {
            kotlin.test.assertFailsWith<kotlinx.coroutines.CancellationException> { operation() }
        } finally {
            client.setSnapshotApplyFaultInjectorForTest(null)
        }
        kotlin.test.assertEquals(2, commits)
    }

    protected suspend fun requireSurvivors(db: SafeSQLiteConnection) {
        assertEquals("Q", scalarText(db, "SELECT parent_id FROM nodes WHERE id='C'"))
        assertEquals("child", scalarText(db, "SELECT name FROM nodes WHERE id='C'"))
        assertEquals("C", scalarText(db, "SELECT parent_id FROM nodes WHERE id='D'"))
        assertEquals("grandchild", scalarText(db, "SELECT name FROM nodes WHERE id='D'"))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM nodes WHERE id='P'"))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
    }
}
