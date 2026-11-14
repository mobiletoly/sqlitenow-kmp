package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

internal class GeneralSnapshotCapacityPreflightTest : CrossTargetSyncTestSupport() {
    @Test
    fun retainedEntriesLowerBoundIsTwoPerRowPlusTheFixedOwners() {
        assertEquals(2L, generalSnapshotRetainedEntriesLowerBound(0))
        assertEquals(200_002L, generalSnapshotRetainedEntriesLowerBound(100_000))
        assertEquals(Long.MAX_VALUE, generalSnapshotRetainedEntriesLowerBound(Long.MAX_VALUE))
        assertEquals(Long.MAX_VALUE, generalSnapshotRetainedEntriesLowerBound(Long.MAX_VALUE / 2))
    }

    // Runs on a real dispatcher: under runTest's virtual time the client's bounded best-effort
    // session release can time out at once while the mock engine answers on another thread,
    // which leaves the server session open and fails the release assertions under load.
    private suspend fun withRebuildClient(
        entryQuota: Long,
        remoteRows: Int,
        block: suspend (dev.goquick.sqlitenow.core.SafeSQLiteConnection, DefaultOversqliteClient, MockSyncServer) -> Unit,
    ) = withContext(Dispatchers.Default) {
        val server = MockSyncServer()
        val db = newDb()
        val http = server.newHttpClient()
        createUsersAndPostsTables(db)
        val client = newClient(db, http, workspaceEntries = entryQuota)
        try {
            client.openAndConnect("user-1").getOrThrow()
            client.sync().getOrThrow()
            server.addRemoteBundleForTest(List(remoteRows) { index ->
                BundleRow(
                    "main", "users", mapOf("id" to "remote-$index"), "INSERT", 1,
                    buildJsonObject { put("id", JsonPrimitive("remote-$index")); put("name", JsonPrimitive("name")) },
                )
            })
            block(db, client, server)
        } finally {
            client.closeFromCoordinatedOwner(); http.close(); db.close()
        }
    }

    @Test
    fun aSnapshotThatCannotFitTheEntryQuotaIsRefusedBeforeAnyChunkIsRequested() = runOwnedWebDatabaseTest {
        // Two owners, two entries per row and eight to admit a row: 996 rows need 2,002 of 2,000.
        // The quota is large enough for the route admission that runs before the session is created.
        withRebuildClient(entryQuota = 2_000, remoteRows = 996) { db, client, server ->
            val createsBefore = server.snapshotSessionCreateRequestCount

            val failure = client.rebuild().exceptionOrNull()

            assertIs<SyncWorkspaceLimitException>(failure)
            assertEquals("entries", failure.dimension)
            assertEquals(2_000L, failure.limit, failure.message)
            assertEquals(1_994L, failure.used)
            assertEquals(generalSnapshotRowAdmissionEntries, failure.reserved)
            assertEquals(1, server.snapshotSessionCreateRequestCount - createsBefore)
            assertTrue(server.snapshotChunkRequests.isEmpty(), "nothing is downloaded")
            assertTrue(server.snapshotSessionIdsForTest().isEmpty(), "the server session is released")
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_snapshot_stage"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM users"))

            // Nothing was retained, so the next attempt asks the server again and is refused again.
            assertIs<SyncWorkspaceLimitException>(client.rebuild().exceptionOrNull())
            assertEquals(2, server.snapshotSessionCreateRequestCount - createsBefore)
            assertTrue(server.snapshotChunkRequests.isEmpty())
            assertTrue(server.snapshotSessionIdsForTest().isEmpty())
        }
    }

    @Test
    fun aSnapshotAtTheLowerBoundIsNotRefusedEarly() = runOwnedWebDatabaseTest {
        // 995 rows need exactly 2,000 by the lower bound, so the snapshot is downloaded.
        withRebuildClient(entryQuota = 2_000, remoteRows = 995) { _, client, server ->
            client.rebuild()

            assertTrue(server.snapshotChunkRequests.isNotEmpty())
        }
    }
}
