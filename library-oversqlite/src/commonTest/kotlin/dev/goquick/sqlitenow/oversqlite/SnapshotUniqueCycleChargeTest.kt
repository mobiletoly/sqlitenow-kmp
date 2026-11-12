package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.seconds

/**
 * A snapshot installed over existing rows uses the general installer. Its unique-cycle working set
 * (the cycle closure over unique transfer edges) exists only when unique values move between rows;
 * without such a move the restore must not be charged for it, or the general route refuses accounts
 * that its stage and plan fit comfortably.
 */
internal class SnapshotUniqueCycleChargeTest : SurvivorTestSupport() {
    private val rows = 100

    // Holds the staged rows and the plan's references (2 entries per row plus 3), together with
    // preservation's fixed schema-catalog metadata (16 + 8 per table: 256 entries here), about 459 in
    // all; but not the additional 4 entries per row (400) the cycle working set is charged when unique
    // values move, about 603.
    private val entryLimit = 530L

    @Test
    fun rebuildWithoutUniqueMovesIsNotChargedForTheCycleWorkingSet() = runOwnedWebDatabaseTest(timeout = 170.seconds) {
        val outcome = rebuildUnder(entryLimit, swapUniqueNames = false)
        assertNull(outcome.failure, "rebuild must fit the workspace: ${outcome.failure}")
        assertEquals(rows.toLong(), outcome.installedRows)
        assertEquals("renamed", outcome.nameOf("n-005"))
    }

    @Test
    fun rebuildThatSwapsUniqueValuesIsStillChargedForTheCycleWorkingSet() = runOwnedWebDatabaseTest(timeout = 170.seconds) {
        val refused = assertNotNull(rebuildUnder(entryLimit, swapUniqueNames = true).failure)
        assertEquals("entries", refused.dimension)
    }

    @Test
    fun rebuildThatSwapsUniqueValuesCompletesUnderAmpleLimits() = runOwnedWebDatabaseTest(timeout = 170.seconds) {
        val completed = rebuildUnder(100_000, swapUniqueNames = true)
        assertNull(completed.failure, "rebuild must complete under default test limits: ${completed.failure}")
        assertEquals(rows.toLong(), completed.installedRows)
        assertEquals("name-001", completed.nameOf("n-000"))
        assertEquals("name-000", completed.nameOf("n-001"))
    }

    private class Outcome(val failure: SyncWorkspaceLimitException?, val installedRows: Long, val names: Map<String, String>) {
        fun nameOf(id: String): String? = names[id]
    }

    private suspend fun rebuildUnder(workspaceEntries: Long, swapUniqueNames: Boolean): Outcome {
        val server = MockSyncServer()
        val table = GenerationTable("main", "nodes", listOf("id"), listOf(
            GenerationColumn("id", "text", false), GenerationColumn("name", "text", false),
            GenerationColumn("parent_id", "text", true),
        ), listOf(GenerationForeignKey(listOf("parent_id"), "main", "nodes", listOf("id"), "cascade", "no_action")))
        server.generationOverride = SyncGeneration("1", "", listOf(table)).let { it.copy(fingerprint = it.computeFingerprint()) }
        val db = newDb()
        val http = server.newHttpClient()
        try {
            db.execSQL("CREATE TABLE nodes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES nodes(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
            db.execSQL("CREATE UNIQUE INDEX nodes_unique_name ON nodes(name)")
            val writer = newClient(db, http, syncTables = nodeTables)
            writer.openAndConnect("user-1").getOrThrow()
            db.execSQL("WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<${rows - 1}) " +
                "INSERT INTO nodes SELECT printf('n-%03d',i),printf('name-%03d',i),NULL FROM n")
            writer.pushPending().getOrThrow()
            writer.closeFromCoordinatedOwner()

            // The server's state moves on; the device then rebuilds over its existing rows.
            fun update(id: String, name: String) = BundleRow("main", "nodes", mapOf("id" to id), "UPDATE", 2,
                Json.parseToJsonElement("""{"id":"$id","name":"$name","parent_id":null}"""))
            server.addRemoteBundleForTest(
                if (swapUniqueNames) listOf(update("n-000", "name-001"), update("n-001", "name-000"))
                else listOf(update("n-005", "renamed")),
            )
            val client = newClient(db, http, syncTables = nodeTables, workspaceEntries = workspaceEntries)
            try {
                client.openAndConnect("user-1").getOrThrow()
                val failure = client.rebuild().exceptionOrNull()
                val limit = generateSequence(failure) { it.cause }.filterIsInstance<SyncWorkspaceLimitException>().firstOrNull()
                if (failure != null && limit == null) throw failure
                val names = buildMap {
                    db.withPreparedStatement("SELECT id,name FROM nodes") { st ->
                        while (st.step()) put(st.getText(0), st.getText(1))
                    }
                }
                return Outcome(limit, scalarLong(db, "SELECT count(*) FROM nodes"), names)
            } finally {
                client.closeFromCoordinatedOwner()
            }
        } finally {
            http.close(); db.close()
        }
    }
}
