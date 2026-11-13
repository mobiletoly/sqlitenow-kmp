package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

internal class PullPageSizingSyncTest : CrossTargetSyncTestSupport() {
    /** A remote bundle of inserts; a pulled row's version is the sequence of its bundle. */
    private fun MockSyncServer.addRemoteUsers(bundleSeq: Long, vararg users: Pair<String, String>) {
        val assigned = addRemoteBundleForTest(users.map { (id, name) ->
            BundleRow(
                "main", "users", mapOf("id" to id), "INSERT", bundleSeq,
                buildJsonObject { put("id", JsonPrimitive(id)); put("name", JsonPrimitive(name)) },
            )
        })
        check(assigned == bundleSeq) { "expected bundle $bundleSeq, server assigned $assigned" }
    }

    private suspend fun withConnectedClient(
        server: MockSyncServer,
        block: suspend (dev.goquick.sqlitenow.core.SafeSQLiteConnection, DefaultOversqliteClient) -> Unit,
    ) {
        val db = newDb()
        val http = server.newHttpClient()
        createUsersAndPostsTables(db)
        val client = newClient(db, http)
        try {
            client.openAndConnect("user-1").getOrThrow()
            client.sync().getOrThrow()
            server.pullMaxBundleRequests.clear()
            block(db, client)
        } finally {
            client.closeFromCoordinatedOwner(); http.close(); db.close()
        }
    }

    @Test
    fun aBacklogOfSmallBundlesIsPulledInGrowingPages() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        withConnectedClient(server) { db, client ->
            repeat(100) { server.addRemoteUsers(it + 1L, "small-$it" to "name $it") }

            client.sync().getOrThrow()

            assertEquals(listOf(16, 64, 256), server.pullMaxBundleRequests)
            assertEquals(100L, scalarLong(db, "SELECT count(*) FROM users"))
            assertEquals(100L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        }
    }

    @Test
    fun aBacklogOfAFewBundlesIsStillOneRequest() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        withConnectedClient(server) { db, client ->
            repeat(oversqlitePullPageInitialBundles) { server.addRemoteUsers(it + 1L, "few-$it" to "name $it") }

            client.sync().getOrThrow()

            assertEquals(listOf(oversqlitePullPageInitialBundles), server.pullMaxBundleRequests)
            assertEquals(oversqlitePullPageInitialBundles.toLong(), scalarLong(db, "SELECT count(*) FROM users"))
        }
    }

    @Test
    fun aPageAboveTheByteTargetIsAskedForAgainWithFewerBundles() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        withConnectedClient(server) { db, client ->
            // Sixteen of these bundles decode to about 4.8 MB, above the 4 MiB page target.
            val name = "n".repeat(300_000)
            repeat(20) { server.addRemoteUsers(it + 1L, "large-$it" to name) }

            client.sync().getOrThrow()

            assertEquals(listOf(16, 4, 6, 6, 6), server.pullMaxBundleRequests)
            assertEquals(20L, scalarLong(db, "SELECT count(*) FROM users WHERE length(name)=300000"))
            assertEquals(20L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        }
    }

    @Test
    fun oneBundleAboveTheByteTargetIsStillPulled() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        withConnectedClient(server) { db, client ->
            // Six rows in one bundle decode to about 4.8 MB. Each row stays under the browser
            // SQLite driver's 1 MiB row cap, so the case runs on every target.
            val name = "n".repeat(800_000)
            server.addRemoteUsers(1L, *Array(6) { "huge-$it" to name })
            server.addRemoteUsers(2L, "after" to "small")

            client.sync().getOrThrow()

            assertEquals(listOf(16, 4, 1, 1), server.pullMaxBundleRequests)
            assertEquals(6L, scalarLong(db, "SELECT count(*) FROM users WHERE length(name)=800000"))
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM users WHERE id='after'"))
            assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        }
    }

    @Test
    fun aResponseAboveTheAdvertisedLimitStillFailsWithoutAnotherRequest() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        server.wireLimits = testWireLimits().copy(maxRowDecodedBytes = 1_000L, maxPullDecodedBytes = 2_000L)
        withConnectedClient(server) { db, client ->
            val name = "n".repeat(700)
            server.addRemoteUsers(1L, "over-a" to name, "over-b" to name, "over-c" to name)

            val failure = client.sync().exceptionOrNull()

            assertIs<SnapshotResponseBodyTooLargeException>(failure)
            assertEquals(2_000L, failure.limit)
            assertEquals(listOf(16), server.pullMaxBundleRequests)
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM users"))
            assertEquals(0L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        }
    }

    @Test
    fun oneBundleAboveAnAdvertisedLimitLargerThanTheTargetFailsAfterShrinkingToIt() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val advertised = 5L * 1024L * 1024L
        server.wireLimits = testWireLimits().copy(maxPullDecodedBytes = advertised)
        withConnectedClient(server) { db, client ->
            val name = "n".repeat(800_000)
            server.addRemoteUsers(1L, *Array(7) { "over-$it" to name }) // about 5.6 MB in one bundle

            val failure = client.sync().exceptionOrNull()

            assertIs<SnapshotResponseBodyTooLargeException>(failure)
            assertEquals(advertised, failure.limit)
            assertEquals(listOf(16, 4, 1), server.pullMaxBundleRequests)
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM users"))
            assertEquals(0L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        }
    }

    @Test
    fun anOversizedErrorBodyIsAFailureAndIsNotAskedForAgain() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        withConnectedClient(server) { db, client ->
            server.addRemoteUsers(1L, "one" to "name")
            server.oversizedPullErrorBodyOnceForTest = true

            val failure = client.sync().exceptionOrNull()

            assertIs<SnapshotResponseBodyTooLargeException>(failure)
            assertEquals(listOf(16), server.pullMaxBundleRequests)
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM users"))
        }
    }

    @Test
    fun shrinkingAfterPagesWereAppliedNeitherSkipsNorRepeatsABundle() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        withConnectedClient(server) { db, client ->
            val name = "n".repeat(300_000)
            repeat(20) { server.addRemoteUsers(it + 1L, "small-$it" to "name $it") }
            repeat(20) { server.addRemoteUsers(it + 21L, "large-$it" to name) }

            client.sync().getOrThrow()

            // 16 small bundles; then 64 asked: 4 small and 20 large, about 6 MB, refused; then 16
            // (4 small, 12 large); then the remaining 8 large.
            assertEquals(listOf(16, 64, 16, 9), server.pullMaxBundleRequests)
            assertEquals(40L, scalarLong(db, "SELECT count(*) FROM users"))
            assertEquals(20L, scalarLong(db, "SELECT count(*) FROM users WHERE length(name)=300000"))
            assertEquals(40L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        }
    }

    @Test
    fun aTransientFailureInTheMiddleOfABacklogStillAppliesEveryBundleOnce() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        withConnectedClient(server) { db, client ->
            repeat(100) { server.addRemoteUsers(it + 1L, "small-$it" to "name $it") }
            // Fail the second pull request of the backlog once.
            server.beforePullResponse = { server.beforePullResponse = { server.remainingTransientPullFailures = 1 } }

            var report = client.sync()
            if (report.isFailure) report = client.sync()
            report.getOrThrow()

            // The 16 applied bundles stay applied; the pull then starts again from a small page.
            assertEquals(listOf(16, 64, 16, 64, 256), server.pullMaxBundleRequests)
            assertEquals(100L, scalarLong(db, "SELECT count(*) FROM users"))
            assertEquals(100L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        }
    }
}
