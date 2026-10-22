package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import androidx.sqlite.async.step
import kotlinx.serialization.json.Json

internal class FinalStateUniqueApplyTest : SurvivorTestSupport() {
    @Test
    fun transfersAndCycles() = runOwnedWebDatabaseTest {
        for (cycle in listOf(false, true)) withSurvivorFixture { server, db, _, client ->
            db.execSQL("CREATE UNIQUE INDEX unique_name ON nodes(name)")
            server.addRemoteBundleForTest(
                listOf(
                    BundleRow(
                        "main",
                        "nodes",
                        mapOf("id" to "P"),
                        "UPDATE",
                        2,
                        Json.parseToJsonElement("""{"id":"P","name":"other","parent_id":null}"""),
                    ),
                    BundleRow(
                        "main",
                        "nodes",
                        mapOf("id" to "Q"),
                        "UPDATE",
                        2,
                        Json.parseToJsonElement(
                            """{"id":"Q","name":"${if(cycle)"parent" else "free"}","parent_id":null}"""
                        ),
                    ),
                )
            )
            client.pullToStable().getOrThrow()
            assertEquals("other", scalarText(db, "SELECT name FROM nodes WHERE id='P'"))
            assertEquals(
                if (cycle) "parent" else "free",
                scalarText(db, "SELECT name FROM nodes WHERE id='Q'"),
            )
            assertEquals("P", scalarText(db, "SELECT parent_id FROM nodes WHERE id='C'"))
            assertEquals("C", scalarText(db, "SELECT parent_id FROM nodes WHERE id='D'"))
            assertEquals(
                2L,
                scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"),
            )
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_apply_rows"))
        }
    }

    @Test
    fun unsafeTriggerPreservesAndRetries() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, _, client ->
            db.execSQL("CREATE UNIQUE INDEX unique_name ON nodes(name)")
            db.execSQL(
                "CREATE TRIGGER application_delete AFTER DELETE ON nodes BEGIN DELETE FROM nodes WHERE id='C'; END"
            )
            server.addRemoteBundleForTest(
                listOf(
                    BundleRow(
                        "main",
                        "nodes",
                        mapOf("id" to "P"),
                        "UPDATE",
                        2,
                        Json.parseToJsonElement("""{"id":"P","name":"other","parent_id":null}"""),
                    ),
                    BundleRow(
                        "main",
                        "nodes",
                        mapOf("id" to "Q"),
                        "UPDATE",
                        2,
                        Json.parseToJsonElement("""{"id":"Q","name":"parent","parent_id":null}"""),
                    ),
                )
            )
            val failure = client.pullToStable().exceptionOrNull()
            assertEquals(
                "application_delete",
                assertIs<FinalStateApplyBlockedException>(failure).trigger,
            )
            assertEquals("parent", scalarText(db, "SELECT name FROM nodes WHERE id='P'"))
            assertEquals("P", scalarText(db, "SELECT parent_id FROM nodes WHERE id='C'"))
            assertEquals(
                1L,
                scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"),
            )
            db.execSQL("DROP TRIGGER application_delete")
            client.pullToStable().getOrThrow()
            assertEquals(
                2L,
                scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"),
            )
        }
    }

    @Test
    fun snapshotClearsUnstagedHolder() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, _, client ->
            db.execSQL("CREATE UNIQUE INDEX unique_name ON nodes(name)")
            server.addRemoteBundleForTest(
                listOf(
                    BundleRow("main", "nodes", mapOf("id" to "P"), "DELETE", 2, null),
                    BundleRow("main", "nodes", mapOf("id" to "C"), "DELETE", 2, null),
                    BundleRow("main", "nodes", mapOf("id" to "D"), "DELETE", 2, null),
                    BundleRow(
                        "main",
                        "nodes",
                        mapOf("id" to "new"),
                        "INSERT",
                        2,
                        Json.parseToJsonElement("""{"id":"new","name":"parent","parent_id":null}"""),
                    ),
                )
            )
            client.rebuild().getOrThrow()
            assertEquals("parent", scalarText(db, "SELECT name FROM nodes WHERE id='new'"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM nodes WHERE id='P'"))
            assertEquals(
                2L,
                scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"),
            )
        }
    }

    @Test
    fun cancellationAfterCycleMutationRollsBackAndRetries() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, _, client ->
            db.execSQL("CREATE UNIQUE INDEX unique_name ON nodes(name)")
            server.addRemoteBundleForTest(
                listOf(
                    BundleRow(
                        "main",
                        "nodes",
                        mapOf("id" to "P"),
                        "UPDATE",
                        2,
                        Json.parseToJsonElement("""{"id":"P","name":"other","parent_id":null}"""),
                    ),
                    BundleRow(
                        "main",
                        "nodes",
                        mapOf("id" to "Q"),
                        "UPDATE",
                        2,
                        Json.parseToJsonElement("""{"id":"Q","name":"parent","parent_id":null}"""),
                    ),
                )
            )
            client.setSnapshotApplyFaultInjectorForTest(
                SnapshotApplyFaultInjector(
                    beforeCommit = {
                        throw kotlinx.coroutines.CancellationException(
                            "after scheduled business mutation"
                        )
                    }
                )
            )
            kotlin.test.assertFailsWith<kotlinx.coroutines.CancellationException> {
                client.pullToStable().getOrThrow()
            }
            assertEquals("parent", scalarText(db, "SELECT name FROM nodes WHERE id='P'"))
            assertEquals("other", scalarText(db, "SELECT name FROM nodes WHERE id='Q'"))
            assertEquals(4L, scalarLong(db, "SELECT count(*) FROM nodes"))
            assertEquals(
                1L,
                scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"),
            )
            client.setSnapshotApplyFaultInjectorForTest(null)
            client.pullToStable().getOrThrow()
            assertEquals(
                2L,
                scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"),
            )
        }
    }

    @Test fun schemaVersionQueriesAreReusable() = runOwnedWebDatabaseTest {
        val db=newDb()
        try {
            db.execSQL("CREATE TABLE test_values(id TEXT PRIMARY KEY)")
            db.transaction(dev.goquick.sqlitenow.core.TransactionMode.IMMEDIATE) {
                val cache=StatementCache(db)
                try {
                    repeat(2) {
                        for (sql in listOf("PRAGMA main.schema_version","PRAGMA temp.schema_version")) {
                            println("schema_guard_before=$sql")
                            val version=db.withPreparedStatement(sql,statementCache=cache) { st ->
                                kotlin.test.assertTrue(st.step());st.getLong(0)
                            }
                            println("schema_guard_after=$sql version=$version")
                        }
                    }
                } finally { cache.close() }
            }
        } finally { db.close() }
    }
}
