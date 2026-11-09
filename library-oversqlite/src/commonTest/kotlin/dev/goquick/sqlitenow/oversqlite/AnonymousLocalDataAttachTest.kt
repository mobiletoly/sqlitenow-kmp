package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * Anonymous local rows are seeded only into an uninitialized scope. Against a scope that already
 * has server data, attach must refuse instead of discarding them, and only the explicit destructive
 * discard may clear the way.
 */
internal class AnonymousLocalDataAttachTest : CrossTargetSyncTestSupport() {

    @Test
    fun attachRefusesToReplaceUnsyncedAnonymousRowsUntilExplicitlyDiscarded() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val serverDb = newDb()
        val anonymousDb = newDb()
        val serverHttp = server.newHttpClient()
        val anonymousHttp = server.newHttpClient()
        try {
            createUsersAndPostsTables(serverDb)
            createUsersAndPostsTables(anonymousDb)

            // Another device already owns the account scope.
            val seedingClient = newClient(serverDb, serverHttp)
            val seeded = assertIs<AttachResult.Connected>(seedingClient.openAndConnect("user-1").getOrThrow())
            assertEquals(AttachOutcome.STARTED_EMPTY, seeded.outcome)
            insertUser(serverDb, "u1", "Server")
            assertEquals(PushOutcome.COMMITTED, seedingClient.pushPending().getOrThrow().outcome)

            // This device worked anonymously: one row adopted at open, one captured by triggers.
            insertUser(anonymousDb, "anon-1", "Adopted before open")
            val client = newClient(anonymousDb, anonymousHttp)
            client.open().getOrThrow()
            assertEquals(2L, scalarLong(anonymousDb, "PRAGMA synchronous"), "open must make commits durable")
            insertUser(anonymousDb, "anon-2", "Captured after open")
            assertEquals(2L, scalarLong(anonymousDb, "SELECT count(*) FROM _sync_dirty_rows"))

            val blocked = assertIs<AttachResult.BlockedLocalPendingData>(client.attach("user-1").getOrThrow())
            assertEquals(2L, blocked.pendingRowCount)
            assertEquals(2L, scalarLong(anonymousDb, "SELECT count(*) FROM users"))
            assertEquals(2L, scalarLong(anonymousDb, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals("anonymous", scalarText(anonymousDb, "SELECT binding_state FROM _sync_attachment_state"))
            assertEquals(operationKindNone, scalarText(anonymousDb, "SELECT kind FROM _sync_operation_state"))

            // A repeated attach is still refused; the local rows are untouched.
            assertIs<AttachResult.BlockedLocalPendingData>(client.attach("user-1").getOrThrow())
            assertEquals("Adopted before open", scalarText(anonymousDb, "SELECT name FROM users WHERE id = 'anon-1'"))

            client.discardAnonymousLocalData().getOrThrow()
            assertEquals(0L, scalarLong(anonymousDb, "SELECT count(*) FROM users"))
            assertEquals(0L, scalarLong(anonymousDb, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(anonymousDb, "SELECT count(*) FROM _sync_row_state"))

            val connected = assertIs<AttachResult.Connected>(client.attach("user-1").getOrThrow())
            assertEquals(AttachOutcome.USED_REMOTE_STATE, connected.outcome)
            assertEquals("Server", scalarText(anonymousDb, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals(1L, scalarLong(anonymousDb, "SELECT count(*) FROM users"))

            // The destructive exit is anonymous-only.
            assertTrue(client.discardAnonymousLocalData().isFailure)
            assertEquals(1L, scalarLong(anonymousDb, "SELECT count(*) FROM users"))
        } finally {
            serverHttp.close()
            anonymousHttp.close()
            serverDb.close()
            anonymousDb.close()
        }
    }

    @Test
    fun attachStillSeedsAnonymousRowsIntoUninitializedScope() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            insertUser(db, "anon-1", "Seed")
            val client = newClient(db, http)
            val connected = assertIs<AttachResult.Connected>(client.openAndConnect("user-1").getOrThrow())
            assertEquals(AttachOutcome.SEEDED_FROM_LOCAL, connected.outcome)
            assertEquals(PushOutcome.COMMITTED, client.pushPending().getOrThrow().outcome)
            assertEquals("Seed", scalarText(db, "SELECT name FROM users WHERE id = 'anon-1'"))
        } finally {
            http.close()
            db.close()
        }
    }
}
