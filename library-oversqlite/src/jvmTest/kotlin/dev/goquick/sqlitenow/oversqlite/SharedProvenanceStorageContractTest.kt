package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.*
import kotlin.io.path.readText
import kotlin.test.*

/** Executes the proposed local layout before its runtime consumers are changed. */
class SharedProvenanceStorageContractTest {
    private val fixture get() = Json.parseToJsonElement(oversqliteContractFixture("local-planning/provenance-storage.json").readText()).jsonObject
    private val owners = listOf("authoritative", "dirty", "frozen")

    @Test fun completeContextEqualityFamily() = runBlocking {
        withLayout { db, f ->
            assertEquals(listOf("complete-context-equality", "reference-integrity", "bounded-cleanup", "logical-storage"),
                f.getValue("families").jsonArray.map { it.jsonPrimitive.content })
            assertEquals(f.getValue("contextDdl").jsonPrimitive.content, provenanceContextDdl)
            assertEquals(f.getValue("stateDdl").jsonPrimitive.content, provenanceStateDdl)
            assertEquals(f.getValue("immutableTriggerDdl").jsonPrimitive.content, provenanceImmutableTriggerDdl)
            val original = values(f)
            val first = intern(db, f, original)
            assertEquals(first, intern(db, f, original))
            for (column in listOf(0, 1, 3, 5, 8, 9)) {
                val changed = original.toMutableList()
                changed[column] += "-different"
                if (column == 5) changed[column] = "b".repeat(64)
                assertNotEquals(first, intern(db, f, changed), "complete equality includes column $column")
            }
            assertFails { db.execSQL("UPDATE _sync_provenance_contexts SET service_id='mutated' WHERE context_id=$first") }
            println("PROVENANCE_FAMILY complete-context-equality PASS")
        }
    }

    @Test fun referenceIntegrityFamily() = runBlocking {
        withLayout { db, f ->
            val id = intern(db, f, values(f))
            for (owner in owners) {
                db.execSQL("INSERT INTO owner_$owner VALUES(1,'${observation(id)}')")
                assertEquals(id, db.scalarLong("SELECT context_id FROM owner_$owner"))
                assertFails { db.execSQL("DELETE FROM _sync_provenance_contexts WHERE context_id=$id") }
                assertFails { db.execSQL("INSERT INTO owner_$owner VALUES(2,'${observation(99999)}')") }
                for (bad in listOf("{}", "{\"context_id\":$id}", "{\"context_id\":$id,\"origin\":\"snapshot\",\"bundle_sequence\":null,\"extra\":0}",
                    "{\"context_id\":$id,\"context_id\":$id,\"origin\":\"snapshot\",\"bundle_sequence\":null}")) {
                    assertFails { db.execSQL("INSERT INTO owner_$owner VALUES(3,'$bad')") }
                }
            }
            assertEquals(0L, db.scalarLong("SELECT count(*) FROM pragma_foreign_key_check"))
            println("PROVENANCE_FAMILY reference-integrity PASS")
        }
    }

    @Test fun boundedCleanupFamily() = runBlocking {
        withLayout { db, f ->
            val ids = (1..70).map { n -> intern(db, f, values(f).toMutableList().also { it[9] = "source-$n" }) }
            db.execSQL("UPDATE _sync_provenance_state SET active_context_id=${ids.first()}")
            for (id in ids) db.execSQL("INSERT INTO owner_authoritative VALUES($id,'${observation(id)}')")
            var cursor = 0L
            repeat(3) {
                val result = cleanupSlice(db, cursor, f.getValue("cleanup").jsonObject.getValue("candidatesPerSlice").jsonPrimitive.int)
                assertTrue(result.examined <= 32)
                assertTrue(result.probes <= result.examined * 4)
                assertEquals(0, result.deleted)
                assertTrue(result.cursor > cursor)
                cursor = result.cursor
            }
            assertEquals(70L, db.scalarLong("SELECT count(*) FROM _sync_provenance_contexts"))
            // Point probes have an index even when every candidate is referenced.
            for (owner in owners) {
                val plan = db.queryList("EXPLAIN QUERY PLAN SELECT 1 FROM owner_$owner WHERE context_id=1 LIMIT 1") { it.getText(3) }.joinToString()
                assertTrue("INDEX" in plan, plan)
            }
            db.execSQL("DELETE FROM owner_authoritative")
            cursor = 0
            repeat(3) { cursor = cleanupSlice(db, cursor, 32).cursor }
            assertEquals(1L, db.scalarLong("SELECT count(*) FROM _sync_provenance_contexts"))
            assertEquals(ids.first(), db.scalarLong("SELECT context_id FROM _sync_provenance_contexts"))
            println("PROVENANCE_FAMILY bounded-cleanup PASS")
        }
    }

    @Test fun logicalStorageFamily() = runBlocking {
        withLayout { db, f ->
            val storage = f.getValue("storage").jsonObject
            val rows = storage.getValue("rows").jsonPrimitive.int
            val fields = values(f).toMutableList().also { it[8] = "s".repeat(storage.getValue("physicalShapeBytes").jsonPrimitive.int) }
            val id = intern(db, f, fields)
            db.execSQL("UPDATE _sync_provenance_state SET active_context_id=$id")
            db.withStatementCache { cache ->
                repeat(rows) { n -> db.withPreparedStatement("INSERT INTO owner_authoritative(identity,observation) VALUES(?,?)", cache) {
                    it.bindLong(1, n.toLong()); it.bindText(2, observation(id)); it.step()
                } }
            }
            val retained = db.scalarLong("SELECT sum(length(CAST(observation AS BLOB))+8) FROM owner_authoritative") +
                fields.sumOf { it.encodeToByteArray().size }.toLong() + 8 * 4 + "complete-context-v1".length
            val legacy = (JsonArray(fields.map(::JsonPrimitive)).toString().encodeToByteArray().size + 100L) * rows
            assertEquals(1L, db.scalarLong("SELECT count(*) FROM _sync_provenance_contexts"))
            assertTrue(retained * 100 <= legacy * (100 - storage.getValue("minimumReductionPercent").jsonPrimitive.int))
            println("PROVENANCE_FAMILY logical-storage rows=$rows before=$legacy after=$retained PASS")
        }
    }

    private suspend fun withLayout(block: suspend (SafeSQLiteConnection, JsonObject) -> Unit) {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", false)
        try {
            val f = fixture
            db.execSQL("PRAGMA foreign_keys=ON")
            db.execSQL(f.getValue("contextDdl").jsonPrimitive.content)
            db.execSQL(f.getValue("stateDdl").jsonPrimitive.content)
            db.execSQL(f.getValue("immutableTriggerDdl").jsonPrimitive.content)
            db.execSQL("INSERT INTO _sync_provenance_state(singleton_key,format_identity) VALUES(1,'complete-context-v1')")
            for (owner in owners) {
                db.execSQL(f.getValue("ownerDdl").jsonPrimitive.content.replace("NAME", owner))
                db.execSQL("CREATE INDEX owner_${owner}_context_idx ON owner_$owner(context_id)")
            }
            block(db, f)
        } finally { db.close() }
    }

    private fun values(f: JsonObject) = f.getValue("contextValues").jsonArray.map { it.jsonPrimitive.content }
    private fun observation(id: Long) = "{\"context_id\":$id,\"origin\":\"snapshot\",\"bundle_sequence\":9}"
    private suspend fun intern(db: SafeSQLiteConnection, f: JsonObject, values: List<String>): Long {
        val columns = f.getValue("contextColumns").jsonArray.map { it.jsonPrimitive.content }
        suspend fun bind(sql: String, read: Boolean): Long = db.withPreparedStatement(sql) { st ->
            values.forEachIndexed { index, value -> if (index == 4 || index == 7) st.bindLong(index + 1, value.toLong()) else st.bindText(index + 1, value) }
            val found = st.step()
            if (read) { check(found); st.getLong(0) } else 0
        }
        bind("INSERT OR IGNORE INTO _sync_provenance_contexts(${columns.joinToString()}) VALUES(${columns.joinToString { "?" }})", false)
        return bind("SELECT context_id FROM _sync_provenance_contexts WHERE ${columns.joinToString(" AND ") { "$it=?" }}", true)
    }

    private data class Slice(val cursor: Long, val examined: Int, val probes: Int, val deleted: Int)
    private suspend fun cleanupSlice(db: SafeSQLiteConnection, cursor: Long, limit: Int): Slice = db.transaction(TransactionMode.IMMEDIATE) {
        val candidates = db.queryList("SELECT context_id FROM _sync_provenance_contexts WHERE context_id>$cursor ORDER BY context_id LIMIT $limit") { it.getLong(0) }
        var probes = 0; var deleted = 0
        for (id in candidates) {
            val roots = listOf("_sync_provenance_state" to "active_context_id") + owners.map { "owner_$it" to "context_id" }
            var rooted = false
            for ((table, column) in roots) {
                probes++
                if (db.scalarLong("SELECT EXISTS(SELECT 1 FROM $table WHERE $column=$id LIMIT 1)") == 1L) { rooted = true; break }
            }
            if (!rooted) { db.execSQL("DELETE FROM _sync_provenance_contexts WHERE context_id=$id"); deleted++ }
        }
        val next = candidates.lastOrNull() ?: 0
        db.execSQL("UPDATE _sync_provenance_state SET cleanup_cursor=$next")
        Slice(next, candidates.size, probes, deleted)
    }
}
