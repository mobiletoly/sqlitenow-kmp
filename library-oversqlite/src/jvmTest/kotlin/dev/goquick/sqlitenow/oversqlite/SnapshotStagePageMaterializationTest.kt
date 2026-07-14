package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SnapshotStagePageMaterializationTest : BundleClientContractTestSupport() {
    @Test
    fun boundedPagesApplyAllRowsAndPreserveCanonicalKeys() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        db.execSQL(
            """
            CREATE TABLE snapshot_apply_order (
              row_ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id TEXT NOT NULL
            )
            """.trimIndent(),
        )
        db.execSQL(
            """
            CREATE TRIGGER record_snapshot_apply_order
            AFTER INSERT ON users
            BEGIN
              INSERT INTO snapshot_apply_order(user_id) VALUES (NEW.id);
            END
            """.trimIndent(),
        )
        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            snapshotApplyBatchRows = 2,
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                snapshotRoutes(
                    snapshotId = "minimal-stage-page",
                    snapshotBundleSeq = 17,
                    rows = listOf(
                        SnapshotChunkRow("users", """{"id":"user-1"}""", 21, """{"id":"user-1","name":"Ada"}"""),
                        SnapshotChunkRow("users", """{"id":"user-2"}""", 22, """{"id":"user-2","name":"Grace"}"""),
                        SnapshotChunkRow("users", """{"id":"user-3"}""", 23, """{"id":"user-3","name":"Lin"}"""),
                    ),
                    byteCount = 96,
                )
            },
        ) { client ->
            client.rebuild().getOrThrow()

            assertEquals(3L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals("Ada", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals("Grace", scalarText(db, "SELECT name FROM users WHERE id = 'user-2'"))
            assertEquals("Lin", scalarText(db, "SELECT name FROM users WHERE id = 'user-3'"))
            val appliedUserIds = mutableListOf<String>()
            db.prepare(
                "SELECT user_id FROM snapshot_apply_order ORDER BY row_ordinal",
            ).use { statement ->
                while (statement.step()) appliedUserIds += statement.getText(0)
            }
            assertEquals(
                listOf("user-1", "user-2", "user-3"),
                appliedUserIds,
            )
            assertEquals(
                22L,
                scalarLong(
                    db,
                    "SELECT row_version FROM _sync_row_state " +
                        "WHERE table_name = 'users' AND key_json = '{\"id\":\"user-2\"}'",
                ),
            )
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            val diagnostics = client.snapshotRestoreDiagnosticsForTest()
            assertEquals(2L, diagnostics.applyPages)
            assertEquals(2, diagnostics.maxLiveApplyPageRows)
            assertEquals(3L, diagnostics.finalAppliedRows)
        }
    }

    @Test
    fun invalidSnapshotKeyStillFailsDuringStagingAndRollsBack() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        withConnectedClient(
            db = db,
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            configureServer = {
                snapshotRoutes(
                    snapshotId = "invalid-stage-key",
                    snapshotBundleSeq = 18,
                    rows = listOf(
                        SnapshotChunkRow(
                            table = "users",
                            keyJson = """{"not_id":"user-1"}""",
                            rowVersion = 24,
                            payloadJson = """{"id":"user-1","name":"Ada"}""",
                        ),
                    ),
                )
            },
        ) { client ->
            assertTrue(client.rebuild().isFailure)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))
        }
    }
}
