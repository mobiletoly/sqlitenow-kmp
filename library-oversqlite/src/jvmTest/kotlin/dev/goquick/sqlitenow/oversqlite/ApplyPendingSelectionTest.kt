package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.TransactionMode
import kotlin.test.*

internal class ApplyPendingSelectionTest : SurvivorTestSupport() {
    @Test fun productionSelectorsUsePartialIndexesAndPreserveOrderAcrossLatePrefixesAndRollback() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, _, client ->
            try {
                val observation = ReplaySqlObservation.install(db)
                val queries = try { client.rebuild().getOrThrow(); observation.counts.keys.toList() } finally { observation.restore() }
                for ((done, name) in listOf(3 to "_sync_apply_rows_uninstalled_ordinal_idx", 5 to "_sync_apply_rows_unscheduled_ordinal_idx")) {
                    val sql = queries.single { it.startsWith("SELECT r.ordinal") && it.contains("INDEXED BY $name") }
                    val plan = db.queryList("EXPLAIN QUERY PLAN $sql") { it.getText(3) }
                    assertTrue(plan.any { name in it }, plan.toString())
                    assertFalse(plan.any { "USE TEMP B-TREE FOR ORDER BY" in it }, plan.toString())
                    for (rows in listOf(16, 64, 2034)) {
                        db.execSQL("DELETE FROM _sync_apply_edges"); db.execSQL("DELETE FROM _sync_apply_rows")
                        db.transaction(TransactionMode.IMMEDIATE) {
                            for (n in 1..rows) db.execSQL("INSERT INTO _sync_apply_rows(ordinal,table_name,local_pk,wire,pending,bytes) VALUES($n,'nodes','K$n',0,${if (n < rows - 1) done else 2},0)")
                        }
                        suspend fun next(): Long? = db.withPreparedStatement(sql) { if (it.step()) it.getLong(0) else null }
                        assertEquals((rows - 1).toLong(), next())
                        db.execSQL("INSERT INTO _sync_apply_edges VALUES(1,'nodes','K${rows-1}','nodes','K$rows',0,'u')")
                        assertEquals(rows.toLong(), next(), "blocked parent retains its place until its child completes")
                        assertFailsWith<IllegalStateException> {
                            db.transaction(TransactionMode.IMMEDIATE) { db.execSQL("UPDATE _sync_apply_rows SET pending=$done WHERE ordinal=$rows"); assertEquals((rows-1).toLong(),next()); error("rollback fixture") }
                        }
                        assertEquals(rows.toLong(), next())
                        db.execSQL("UPDATE _sync_apply_rows SET pending=$done WHERE ordinal=$rows")
                        assertEquals((rows-1).toLong(), next())
                        db.execSQL("UPDATE _sync_apply_rows SET pending=$done")
                        assertNull(next())
                    }
                }
                db.execSQL("DELETE FROM _sync_apply_edges"); db.execSQL("DELETE FROM _sync_apply_rows")
            } finally { client.closeFromCoordinatedOwner() }
        }
    }

    @Test fun freshReopenAndMissingOrMalformedIndexesFailWithoutRepair() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, http, seed ->
            seed.closeFromCoordinatedOwner()
            for ((name, ddl) in applyIndexDdl.filterKeys { it.endsWith("_ordinal_idx") && it != "_sync_apply_rows_role_ordinal_idx" }) {
                db.execSQL("DROP INDEX $name")
                val before = db.queryList("SELECT type,name,COALESCE(sql,'') FROM sqlite_schema ORDER BY type,name") { it.getText(0)+it.getText(1)+it.getText(2) }
                assertFails { inspectRetainedRuntime(db) }
                assertEquals(before, db.queryList("SELECT type,name,COALESCE(sql,'') FROM sqlite_schema ORDER BY type,name") { it.getText(0)+it.getText(1)+it.getText(2) })
                db.execSQL("CREATE INDEX $name ON _sync_apply_rows(ordinal) WHERE pending<>7")
                assertFails { inspectRetainedRuntime(db) }
                db.execSQL("DROP INDEX $name"); db.execSQL(ddl)
            }
            assertEquals("CURRENT", inspectRetainedRuntime(db))
            val reopened = newClient(db, http, syncTables = nodeTables)
            try { reopened.open().getOrThrow() } finally { reopened.closeFromCoordinatedOwner() }
        }
    }
}
