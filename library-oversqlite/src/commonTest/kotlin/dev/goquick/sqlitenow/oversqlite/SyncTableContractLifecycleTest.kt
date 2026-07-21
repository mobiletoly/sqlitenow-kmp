package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlinx.coroutines.test.runTest

internal class SyncTableContractLifecycleTest : CrossTargetSyncTestSupport() {
    @Test
    fun mismatchRejectsInitialAttachBeforeConnectOrLifecycleBinding() = runTest {
        val server = MockSyncServer().apply {
            registeredTableSpecs = listOf("users", "posts", "monitoring_focus").map { table ->
                RegisteredTableSpec(
                    schema = "business",
                    table = table,
                    syncKeyColumns = listOf("id"),
                )
            }
        }
        val db = newDb()
        val http = server.newHttpClient()
        createUsersAndPostsTables(db)
        val client = newClient(db, http, schema = "business")
        try {
            client.open().getOrThrow()

            val mismatch = assertIs<SyncTableContractMismatchException>(
                client.attach("user-1").exceptionOrNull(),
            )

            assertEquals(listOf("business.monitoring_focus"), mismatch.serverOnlyTables)
            assertEquals(0, server.connectRequestCount)
            assertEquals(
                attachmentBindingAnonymous,
                scalarText(db, "SELECT binding_state FROM _sync_attachment_state WHERE singleton_key = 1"),
            )
            assertEquals(
                "",
                scalarText(db, "SELECT attached_user_id FROM _sync_attachment_state WHERE singleton_key = 1"),
            )
        } finally {
            client.close()
            http.close()
            db.close()
        }
    }

    @Test
    fun mismatchRejectsEveryRemoteOperationBeforeRemoteOrDurableSyncWork() = runTest {
        val server = MockSyncServer()
        val db = newDb()
        val http = server.newHttpClient()
        createUsersAndPostsTables(db)
        val client = newClient(db, http)
        try {
            client.openAndConnect("user-1").getOrThrow()
            insertUser(db, "user-1", "Ada")
            server.registeredTableSpecs = testRegisteredTableSpecs(
                "users",
                "posts",
                "monitoring_focus",
            )

            val initialPushCreates = server.pushSessionCreateRequestCount
            val initialPulls = server.pullRequestCount
            val initialSnapshots = server.snapshotSessionCreateRequestCount
            val initialCapabilities = server.capabilitySourceIds.size
            val initialDirtyRows = scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows")
            val initialOutboxRows = scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows")

            listOf(
                client.pushPending(),
                client.pullToStable(),
                client.sync(),
                client.rebuild(),
            ).forEach { result ->
                val mismatch = assertIs<SyncTableContractMismatchException>(result.exceptionOrNull())
                assertEquals(listOf("main.monitoring_focus"), mismatch.serverOnlyTables)
            }

            assertEquals(initialCapabilities + 4, server.capabilitySourceIds.size)
            assertEquals(initialPushCreates, server.pushSessionCreateRequestCount)
            assertEquals(initialPulls, server.pullRequestCount)
            assertEquals(initialSnapshots, server.snapshotSessionCreateRequestCount)
            assertEquals(initialDirtyRows, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(initialOutboxRows, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(
                attachmentBindingAttached,
                scalarText(db, "SELECT binding_state FROM _sync_attachment_state WHERE singleton_key = 1"),
            )
            assertEquals(
                operationKindNone,
                scalarText(db, "SELECT kind FROM _sync_operation_state WHERE singleton_key = 1"),
            )
        } finally {
            client.close()
            http.close()
            db.close()
        }
    }
}
