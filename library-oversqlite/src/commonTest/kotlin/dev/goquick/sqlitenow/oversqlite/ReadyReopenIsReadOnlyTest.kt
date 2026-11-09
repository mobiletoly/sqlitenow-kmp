package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test
import kotlin.test.assertEquals

internal class ReadyReopenIsReadOnlyTest : CrossTargetSyncTestSupport() {
    /**
     * Reopening a ready, attached database with nothing to do must not write: same-value updates
     * still rewrite pages and change the stored file, which byte-exact owners observe.
     */
    @Test fun readyReopenOfAnAttachedDatabaseChangesNoRows() = runOwnedWebDatabaseTest {
        val db = newDb()
        val server = MockSyncServer()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val first = newClient(db, http)
            first.openAndConnect("user-1").getOrThrow()
            insertUser(db, "P", "parent")
            first.pushPending().getOrThrow()
            first.closeFromCoordinatedOwner()

            val reopened = newClient(db, http)
            try {
                val before = db.scalarLong("SELECT total_changes()")
                reopened.open().getOrThrow()
                assertEquals(0L, db.scalarLong("SELECT total_changes()") - before, "ready reopen changes no rows")
            } finally {
                reopened.closeFromCoordinatedOwner()
            }
        } finally {
            http.close()
            db.close()
        }
    }
}
