package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * A local edit of a row that another device deleted must not be dropped when the resolver keeps
 * local intent: the retry re-inserts the row over the server tombstone.
 */
internal class UpdateVersusRemoteDeleteConflictTest : CrossTargetSyncTestSupport() {

    @Test
    fun keptLocalUpdateReinsertsRowDeletedRemotely() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val deleterDb = newDb()
        val editorDb = newDb()
        val deleterHttp = server.newHttpClient()
        val editorHttp = server.newHttpClient()
        try {
            createUsersAndPostsTables(deleterDb)
            createUsersAndPostsTables(editorDb)
            val deleter = newClient(deleterDb, deleterHttp)
            val editor = newClient(editorDb, editorHttp, resolver = ClientWinsResolver)
            deleter.openAndConnect("user-1").getOrThrow()
            editor.openAndConnect("user-1").getOrThrow()

            insertUser(deleterDb, "u1", "Original")
            deleter.sync().getOrThrow()
            editor.sync().getOrThrow()
            assertEquals("Original", scalarText(editorDb, "SELECT name FROM users WHERE id = 'u1'"))

            deleterDb.execSQL("DELETE FROM users WHERE id = 'u1'")
            deleter.sync().getOrThrow()

            updateUserName(editorDb, "u1", "Edited offline")
            val push = editor.pushPending().getOrThrow()
            assertEquals(PushOutcome.COMMITTED, push.outcome)
            assertEquals(0L, scalarLong(editorDb, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals("Edited offline", scalarText(editorDb, "SELECT name FROM users WHERE id = 'u1'"))
            assertEquals("live", scalarText(editorDb, "SELECT server_kind FROM _sync_row_state WHERE table_name = 'users'"))

            deleter.pullToStable().getOrThrow()
            assertEquals("Edited offline", scalarText(deleterDb, "SELECT name FROM users WHERE id = 'u1'"))
        } finally {
            deleterHttp.close()
            editorHttp.close()
            deleterDb.close()
            editorDb.close()
        }
    }
}
