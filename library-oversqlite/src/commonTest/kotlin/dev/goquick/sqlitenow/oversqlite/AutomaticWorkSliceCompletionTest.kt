package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

internal class AutomaticWorkSliceCompletionTest : CrossTargetSyncTestSupport() {
    @Test fun oneAutomaticPullAbsorbsLocalSlicesWithoutFailureBackoff() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val leaderDb = newDb(); val followerDb = newDb()
        val leaderHttp = server.newHttpClient(); val followerHttp = server.newHttpClient()
        createUsersAndPostsTables(leaderDb); createUsersAndPostsTables(followerDb)
        val leader = newClient(leaderDb, leaderHttp)
        val follower = newClient(followerDb, followerHttp, workUnits = 1)
        try {
            leader.openAndConnect("user-1").getOrThrow(); follower.openAndConnect("user-1").getOrThrow()
            repeat(6) { insertUser(leaderDb, "slice-$it", "complete name $it") }
            leader.pushPending().getOrThrow()
            val requestsBefore = server.pullRequestCount
            val worker = launch(Dispatchers.Default) {
                follower.runAutomaticDownloads(OversqliteAutomaticDownloadConfig(automaticDownloadIntervalMillis = 10_000))
            }
            try {
                withContext(Dispatchers.Default) {
                    withTimeout(5_000) {
                        while (scalarLong(followerDb, "SELECT last_bundle_seq_seen FROM _sync_attachment_state") != 1L) delay(10)
                    }
                }
            } finally { worker.cancelAndJoin() }
            assertEquals(requestsBefore + 1, server.pullRequestCount, "local slices do not restart transport")
            assertTrue(follower.workSliceDiagnostics().workSlices > 0)
            assertEquals(6L, scalarLong(followerDb, "SELECT count(*) FROM users"))
            repeat(6) { assertEquals("complete name $it", scalarText(followerDb, "SELECT name FROM users WHERE id='slice-$it'")) }
            assertEquals(0L, scalarLong(followerDb, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
        } finally {
            leader.closeFromCoordinatedOwner(); follower.closeFromCoordinatedOwner()
            leaderHttp.close(); followerHttp.close(); leaderDb.close(); followerDb.close()
        }
    }
}
