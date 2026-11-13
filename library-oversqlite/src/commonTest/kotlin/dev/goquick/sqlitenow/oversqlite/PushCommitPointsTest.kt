package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.common.PlatformType
import dev.goquick.sqlitenow.common.SqliteNowLogger
import dev.goquick.sqlitenow.common.platform
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.SqliteNowDiagnostics
import dev.goquick.sqlitenow.core.SqlTracePolicy
import io.ktor.client.HttpClient
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.random.Random
import kotlin.test.*

/**
 * A one-row push reaches storage in exactly the commits the protocol needs: freeze before upload,
 * outcome before settlement, settlement before acknowledgement, and the acknowledgement. The buffer
 * reservation, push planning, upload buffers and plan cleanup ride along in those commits.
 */
internal class PushCommitPointsTest : CrossTargetSyncTestSupport() {
    /**
     * Transaction boundaries come from the connection's diagnostic events and writes from its SQL
     * trace; both are delivered synchronously on the connection, so one list keeps their order.
     */
    private class Trace : SqliteNowLogger {
        val lines = mutableListOf<String>()
        var mark = 0
        fun start() { mark = lines.size }

        private fun capture(message: () -> String) { message().let { if (it.startsWith("SQLITENOW_DIAG ")) lines += it } }
        override fun e(throwable: Throwable?, message: () -> String) = capture(message)
        override fun w(throwable: Throwable?, message: () -> String) = capture(message)
        override fun i(throwable: Throwable?, message: () -> String) = capture(message)
        override fun d(throwable: Throwable?, message: () -> String) = capture(message)

        /** Writes per committed transaction that changed rows, since [start]; an autocommit write counts as its own commit. */
        fun writingCommits(): List<Int> {
            val commits = mutableListOf<Int>()
            var inTransaction = false
            var writes = 0
            for (line in lines.drop(mark)) {
                if (line.startsWith("SQLITENOW_DIAG ")) {
                    val event = Json.parseToJsonElement(line.substringAfter("SQLITENOW_DIAG ")).jsonObject
                    val name = event["name"]?.jsonPrimitive?.content
                    when {
                        name == "db.transaction_admission" && event["outcome"]?.jsonPrimitive?.content == "new" -> { inTransaction = true; writes = 0 }
                        name == "db.transaction_committed" -> { if (inTransaction && writes > 0) commits += writes; inTransaction = false }
                        name == "db.transaction_rolled_back" -> inTransaction = false
                    }
                    continue
                }
                val shape = line.substringAfter(" statement=", "").substringAfter(' ', "")
                if (writeShape.containsMatchIn(shape)) { if (inTransaction) writes++ else commits += 1 }
            }
            return commits
        }

        private val writeShape = Regex("^(WITH .*)?(INSERT|UPDATE|DELETE|REPLACE)\\b")
    }

    private suspend fun fixture(workspaceBytes: Long = 256L * 1024 * 1024,
        body: suspend (MockSyncServer, SafeSQLiteConnection, DefaultOversqliteClient, Trace) -> Unit) {
        val trace = Trace()
        val previous = sqliteNowLogger
        sqliteNowLogger = trace
        val diagnostics = SqliteNowDiagnostics(true)
        // In the browser every name is one worker database, so each fixture needs its own.
        val web = platform() == PlatformType.JS
        val name = if (web) "push-commit-points-${Random.nextLong()}.db" else ":memory:"
        val db = BundledSqliteConnectionProvider.openConnection(name, false, SqliteConnectionConfig(diagnostics = diagnostics,
            sqlTrace = SqlTracePolicy({ trace.lines += it }, maxEvents = 100_000, durationMillis = 120_000)))
        val server = MockSyncServer()
        val http: HttpClient = server.newHttpClient()
        createUsersAndPostsTables(db)
        val client = DefaultOversqliteClient.forOpening(db, OversqliteConfig(1, listOf(SyncTable("users", "id"), SyncTable("posts", "id")),
            "fixture", "main", diagnostics = diagnostics, maxSyncWorkspaceBytes = workspaceBytes), http, ServerWinsResolver, Any())
        try {
            client.openAndConnect("user-1").getOrThrow()
            insertUser(db, "item", "first")
            body(server, db, client, trace)
        } finally {
            try {
                client.closeFromCoordinatedOwner(); http.close(); db.close()
                if (web) cleanupDirectWorkerTestDatabase(name)
            } finally { sqliteNowLogger = previous }
        }
    }

    private suspend fun assertSettled(db: SafeSQLiteConnection) {
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_upload_journal"))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_push_plan"))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_push_nodes"))
        assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
    }

    @Test fun oneRowPushWritesOnlyTheFourProtocolCommits() = runOwnedWebDatabaseTest {
        fixture { server, db, client, trace ->
            trace.start()
            client.pushPending().getOrThrow()
            assertEquals(4, trace.writingCommits().size, "freeze, outcome, settlement, acknowledgement: ${trace.writingCommits()}")
            assertEquals(1, server.pushRequestCount)
            assertEquals(1, server.ackRequests.size)
            assertSettled(db)

            updateUserName(db, "item", "second")
            trace.start()
            client.pushPending().getOrThrow()
            assertEquals(4, trace.writingCommits().size, "a later push needs no cleanup of the earlier plan")
            assertSettled(db)
        }
    }

    @Test fun refusedFreezeKeepsPlanningAndLeavesIntentUnfrozen() = runOwnedWebDatabaseTest {
        fixture { server, db, client, _ ->
            db.execSQL("CREATE TRIGGER refuse_freeze BEFORE INSERT ON _sync_upload_journal BEGIN SELECT RAISE(ABORT,'refused freeze'); END")
            assertTrue(client.pushPending().isFailure)
            assertEquals(0, server.pushRequestCount, "nothing is sent without a committed freeze")
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"), "local intent stays dirty")
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_upload_journal"))
            assertEquals("ready", scalarText(db, "SELECT phase FROM _sync_push_plan"), "planning progress commits before a freeze refusal")
            assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))

            db.execSQL("DROP TRIGGER refuse_freeze")
            client.pushPending().getOrThrow()
            assertEquals(1, server.pushRequestCount)
            assertSettled(db)
        }
    }

    @Test fun failureLateInTheMergedFreezeRestoresMovedIntentAndFreezesOnce() = runOwnedWebDatabaseTest {
        fixture { server, db, client, trace ->
            // Fires only inside the merged writer: its plan cleanup runs after the dirty row moved to the
            // outbox and the journal was written, while that journal is still frozen.
            db.execSQL("""CREATE TRIGGER refuse_merged_cleanup BEFORE DELETE ON _sync_push_nodes
                WHEN EXISTS(SELECT 1 FROM _sync_upload_journal WHERE phase='frozen')
                BEGIN SELECT RAISE(ABORT,'refused merged cleanup'); END""")
            trace.start()
            client.pushPending().getOrThrow()
            assertEquals(1, server.pushRequestCount, "the restored dirty row is frozen and sent exactly once")
            assertTrue(trace.writingCommits().size > 4, "the separate freeze ran: ${trace.writingCommits()}")
            db.execSQL("DROP TRIGGER refuse_merged_cleanup")
            assertSettled(db)
        }
    }

    @Test fun responseAllowanceIsGivenBackBeforeSettlement() = runOwnedWebDatabaseTest {
        fixture { _, db, client, _ ->
            db.execSQL("CREATE TABLE reservation_log(seq INTEGER PRIMARY KEY, bytes INTEGER NOT NULL)")
            db.execSQL("""CREATE TRIGGER reservation_log_update AFTER UPDATE OF workspace_context ON _sync_operation_state
                BEGIN INSERT INTO reservation_log(bytes) VALUES(NEW.reserved_bytes); END""")
            client.pushPending().getOrThrow()
            db.execSQL("DROP TRIGGER reservation_log_update")
            val allowance = 3L * 65536 * 64
            assertTrue(scalarLong(db, "SELECT max(bytes) FROM reservation_log") >= allowance, "the allowance was charged at freeze")
            val beforeRelease = scalarLong(db, "SELECT bytes FROM reservation_log WHERE bytes>0 ORDER BY seq DESC LIMIT 1")
            assertTrue(beforeRelease < allowance, "settlement sees only the buffers admitted, not the allowance: $beforeRelease")
            assertEquals(0L, scalarLong(db, "SELECT bytes FROM reservation_log ORDER BY seq DESC LIMIT 1"))
            assertSettled(db)
        }
    }

    @Test fun refusedAcknowledgementCommitKeepsTheObligationAndReleasesBuffers() = runOwnedWebDatabaseTest {
        fixture { server, db, client, _ ->
            db.execSQL("CREATE TRIGGER refuse_ack BEFORE DELETE ON _sync_upload_journal BEGIN SELECT RAISE(ABORT,'refused acknowledgement'); END")
            assertTrue(client.pushPending().isFailure)
            assertEquals(1, server.ackRequests.size)
            assertEquals("ack_pending", scalarText(db, "SELECT phase FROM _sync_upload_journal"), "the settled upload still owes its acknowledgement")
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))

            db.execSQL("DROP TRIGGER refuse_ack")
            client.pushPending().getOrThrow()
            assertEquals(1, server.pushRequestCount, "acknowledging again sends no new upload")
            assertSettled(db)
        }
    }

    @Test fun responseAllowanceThatDoesNotFitIsSkippedNotRefused() = runOwnedWebDatabaseTest {
        // Room for the push and its response as read, but not for the up-front response allowance.
        fixture(workspaceBytes = 10L * 1024 * 1024) { server, db, client, trace ->
            trace.start()
            client.pushPending().getOrThrow()
            assertEquals(1, server.pushRequestCount)
            assertTrue(trace.writingCommits().size > 4, "response buffers are charged as they are read: ${trace.writingCommits()}")
            assertSettled(db)
        }
    }
}
