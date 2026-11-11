package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Pull planning discovers dependents by walking foreign-key relationships out of the pulled rows.
 * It only ever looks a relationship up by a row already in the plan, so relationships whose parent
 * table cannot be reached from the pulled rows' tables are never read and need not be indexed.
 * These tests record every relationship the planner indexes and hold the final state to the same
 * survivor guarantees as before.
 */
internal class PullDependencyIndexScopeTest : CrossTargetSyncTestSupport() {
    // parents <- children <- toys form one family; crews <- sailors another; notes stands alone.
    private val tableNames = listOf("children", "crews", "notes", "parents", "sailors", "toys")

    private fun text(name: String, nullable: Boolean = false) = GenerationColumn(name, "text", nullable)
    private fun reference(column: String, table: String) =
        GenerationForeignKey(listOf(column), "main", table, listOf("id"), "cascade", "no_action")

    private val generation = SyncGeneration("1", "", listOf(
        GenerationTable("main", "children", listOf("id"), listOf(text("id"), text("name"), text("parent_id", true)), listOf(reference("parent_id", "parents"))),
        GenerationTable("main", "crews", listOf("id"), listOf(text("id"), text("name")), emptyList()),
        GenerationTable("main", "notes", listOf("id"), listOf(text("id"), text("name")), emptyList()),
        GenerationTable("main", "parents", listOf("id"), listOf(text("id"), text("name")), emptyList()),
        GenerationTable("main", "sailors", listOf("id"), listOf(text("id"), text("name"), text("crew_id", true)), listOf(reference("crew_id", "crews"))),
        GenerationTable("main", "toys", listOf("id"), listOf(text("id"), text("name"), text("child_id", true)), listOf(reference("child_id", "children"))),
    )).let { it.copy(fingerprint = it.computeFingerprint()) }

    private suspend fun withFamilies(
        notes: Int = 5,
        pullWorkUnits: Long? = null,
        block: suspend (MockSyncServer, SafeSQLiteConnection, DefaultOversqliteClient) -> Unit,
    ) {
        val server = MockSyncServer()
        server.generationOverride = generation
        val db = newDb()
        val http = server.newHttpClient()
        val syncTables = tableNames.map { SyncTable(it, syncKeyColumnName = "id") }
        var client: DefaultOversqliteClient? = null
        try {
            db.execSQL("CREATE TABLE parents(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
            db.execSQL("CREATE TABLE children(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, parent_id TEXT REFERENCES parents(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
            db.execSQL("CREATE TABLE toys(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, child_id TEXT REFERENCES children(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
            db.execSQL("CREATE TABLE crews(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
            db.execSQL("CREATE TABLE sailors(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, crew_id TEXT REFERENCES crews(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED)")
            db.execSQL("CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
            client = newClient(db, http, syncTables = syncTables)
            client.openAndConnect("user-1").getOrThrow()
            db.transaction {
                db.execSQL("INSERT INTO parents VALUES('P','parent'),('Q','other parent')")
                db.execSQL("INSERT INTO children VALUES('C','child','P')")
                repeat(30) { db.execSQL("INSERT INTO children VALUES('K$it','sibling','Q')") }
                db.execSQL("INSERT INTO toys VALUES('T','toy','C')")
                db.execSQL("INSERT INTO crews VALUES('W','crew')")
                repeat(20) { db.execSQL("INSERT INTO sailors VALUES('S$it','sailor','W')") }
                repeat(notes) { db.execSQL("INSERT INTO notes VALUES('N$it','note')") }
            }
            do client.pushPending().getOrThrow() while (scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows") > 0L)
            if (pullWorkUnits != null) {
                client.closeFromCoordinatedOwner()
                client = newClient(db, http, syncTables = syncTables, workUnits = pullWorkUnits)
                client.openAndConnect("user-1").getOrThrow()
            }
            db.execSQL("CREATE TEMP TABLE indexed_relationships(parent_table TEXT NOT NULL, child_table TEXT NOT NULL)")
            db.execSQL(
                "CREATE TEMP TRIGGER record_indexed_relationship AFTER INSERT ON main._sync_apply_edges " +
                    "BEGIN INSERT INTO indexed_relationships VALUES(NEW.parent_table, NEW.child_table); END",
            )
            block(server, db, client)
        } finally {
            try { client?.closeFromCoordinatedOwner() } finally { http.close(); db.close() }
        }
    }

    private suspend fun nextBundleSeq(db: SafeSQLiteConnection) =
        scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state") + 1

    private fun update(table: String, id: String, version: Long, payload: String) =
        BundleRow("main", table, mapOf("id" to id), "UPDATE", version, Json.parseToJsonElement(payload))

    private suspend fun indexedRelationships(db: SafeSQLiteConnection) = scalarText(
        db,
        "SELECT coalesce(group_concat(pair, ' '), '') FROM (SELECT DISTINCT parent_table || '<-' || child_table AS pair FROM indexed_relationships ORDER BY 1)",
    )

    private suspend fun assertUntouchedFamiliesAndCleanState(db: SafeSQLiteConnection, bundleSeq: Long) {
        assertEquals(20L, scalarLong(db, "SELECT count(*) FROM sailors WHERE crew_id='W' AND name='sailor'"))
        assertEquals(30L, scalarLong(db, "SELECT count(*) FROM children WHERE parent_id='Q' AND name='sibling'"))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_apply_plan"))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_apply_edges"))
        assertEquals(bundleSeq, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
    }

    @Test
    fun pullOfStandaloneTableIndexesNoRelationships() = runOwnedWebDatabaseTest {
        withFamilies { server, db, client ->
            val seq = nextBundleSeq(db)
            server.addRemoteBundleForTest(listOf(update("notes", "N0", seq, """{"id":"N0","name":"remote note"}""")))
            client.pullToStable().getOrThrow()
            assertEquals("remote note", scalarText(db, "SELECT name FROM notes WHERE id='N0'"))
            assertEquals("", indexedRelationships(db), "nothing references notes")
            assertEquals("C", scalarText(db, "SELECT child_id FROM toys WHERE id='T'"))
            assertUntouchedFamiliesAndCleanState(db, seq)
        }
    }

    @Test
    fun pullOfUnreferencedChildTableIndexesNoRelationships() = runOwnedWebDatabaseTest {
        withFamilies { server, db, client ->
            val seq = nextBundleSeq(db)
            server.addRemoteBundleForTest(listOf(update("sailors", "S0", seq, """{"id":"S0","name":"remote sailor","crew_id":"W"}""")))
            client.pullToStable().getOrThrow()
            assertEquals("remote sailor", scalarText(db, "SELECT name FROM sailors WHERE id='S0'"))
            assertEquals("", indexedRelationships(db), "sailors refers to crews, but nothing refers to sailors")
            assertEquals(19L, scalarLong(db, "SELECT count(*) FROM sailors WHERE crew_id='W' AND name='sailor'"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
            assertEquals(seq, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        }
    }

    @Test
    fun pullOfMiddleTableIndexesOnlyRelationshipsBelowIt() = runOwnedWebDatabaseTest {
        withFamilies { server, db, client ->
            val seq = nextBundleSeq(db)
            server.addRemoteBundleForTest(listOf(update("children", "C", seq, """{"id":"C","name":"remote child","parent_id":"P"}""")))
            client.pullToStable().getOrThrow()
            assertEquals("remote child", scalarText(db, "SELECT name FROM children WHERE id='C'"))
            // children's own reference up to parents is not walked; toys below it is.
            assertEquals("children<-toys", indexedRelationships(db))
            assertEquals("C", scalarText(db, "SELECT child_id FROM toys WHERE id='T'"))
            assertUntouchedFamiliesAndCleanState(db, seq)
        }
    }

    @Test
    fun pullOfParentRestoresSurvivorsAcrossTablesAndSkipsOtherFamilies() = runOwnedWebDatabaseTest {
        withFamilies { server, db, client ->
            val seq = nextBundleSeq(db)
            // Deleting P cascades through C to T locally; the bundle keeps C under Q, so T must survive.
            server.addRemoteBundleForTest(listOf(
                BundleRow("main", "parents", mapOf("id" to "P"), "DELETE", seq, null),
                update("children", "C", seq, """{"id":"C","name":"child","parent_id":"Q"}"""),
            ))
            client.pullToStable().getOrThrow()
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM parents WHERE id='P'"))
            assertEquals("Q", scalarText(db, "SELECT parent_id FROM children WHERE id='C'"))
            assertEquals("C", scalarText(db, "SELECT child_id FROM toys WHERE id='T'"))
            assertEquals("toy", scalarText(db, "SELECT name FROM toys WHERE id='T'"))
            assertEquals("children<-toys parents<-children", indexedRelationships(db))
            assertUntouchedFamiliesAndCleanState(db, seq)
        }
    }

    @Test
    fun pullResumedAcrossWorkSlicesStillCompletesOnTheFullIndex() = runOwnedWebDatabaseTest {
        val notes = 150
        withFamilies(notes = notes, pullWorkUnits = 1800) { server, db, client ->
            val seq = nextBundleSeq(db)
            server.addRemoteBundleForTest(List(notes) { update("notes", "N$it", seq, """{"id":"N$it","name":"remote note"}""") })
            client.pullToStable().getOrThrow()
            assertEquals(notes.toLong(), scalarLong(db, "SELECT count(*) FROM notes WHERE name='remote note'"))
            // The pulled tables are known only to the writer that read them. A later slice cannot
            // tell which relationships are reachable, so it indexes all of them, as before.
            assertTrue("crews<-sailors" in indexedRelationships(db), "resumed planning must fall back to the full index: ${indexedRelationships(db)}")
            assertEquals("C", scalarText(db, "SELECT child_id FROM toys WHERE id='T'"))
            assertUntouchedFamiliesAndCleanState(db, seq)
        }
    }
}
