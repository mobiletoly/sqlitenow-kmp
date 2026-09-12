package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

internal class DetachWriteReservationTest : CrossTargetSyncTestSupport() {
    @Test
    fun directDetach_blocksWhenWriteCommitsBeforeWriterReservation() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val http = server.newHttpClient()
        val client = newClient(db, http)
        val beforeReservation = CompletableDeferred<Unit>()
        val releaseReservation = CompletableDeferred<Unit>()
        try {
            createUsersAndPostsTables(db)
            client.openAndConnect("user-1").getOrThrow()
            client.setBeforeDetachWriteReservationForTest {
                beforeReservation.complete(Unit)
                releaseReservation.await()
            }
            val detach = async(Dispatchers.Default) { client.detach().getOrThrow() }
            try {
                withContext(Dispatchers.Default) {
                    withTimeout(2_000) { beforeReservation.await() }
                }
                insertUser(db, "late-direct", "Preserved")
                releaseReservation.complete(Unit)

                assertEquals(DetachOutcome.BLOCKED_UNSYNCED_DATA, detach.await())
                assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM users WHERE id = 'late-direct'"))
                assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
                assertEquals("attached", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"))
            } finally {
                releaseReservation.complete(Unit)
                detach.cancelAndJoin()
            }
        } finally {
            client.close()
            http.close()
            db.close()
        }
    }

    @Test
    fun syncThenDetach_uploadsWriteCommittedBeforeWriterReservation() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val http = server.newHttpClient()
        val client = newClient(db, http)
        val beforeReservation = CompletableDeferred<Unit>()
        val releaseReservation = CompletableDeferred<Unit>()
        try {
            createUsersAndPostsTables(db)
            client.openAndConnect("user-1").getOrThrow()
            client.setBeforeDetachWriteReservationForTest {
                if (!beforeReservation.isCompleted) {
                    beforeReservation.complete(Unit)
                    releaseReservation.await()
                }
            }
            val syncThenDetach = async(Dispatchers.Default) { client.syncThenDetach().getOrThrow() }
            try {
                withContext(Dispatchers.Default) {
                    withTimeout(2_000) { beforeReservation.await() }
                }
                insertUser(db, "late-sync-detach", "Uploaded")
                releaseReservation.complete(Unit)

                val result = syncThenDetach.await()
                assertEquals(DetachOutcome.DETACHED, result.detach)
                assertEquals(2, result.syncRounds)
                assertEquals(1, server.pushRequestCount)
                assertTrue(result.isSuccess())
            } finally {
                releaseReservation.complete(Unit)
                syncThenDetach.cancelAndJoin()
            }
        } finally {
            client.close()
            http.close()
            db.close()
        }
    }
}
