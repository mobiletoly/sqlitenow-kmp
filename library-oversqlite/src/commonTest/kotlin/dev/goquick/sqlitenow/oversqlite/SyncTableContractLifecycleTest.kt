package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

internal class SyncTableContractLifecycleTest : CrossTargetSyncTestSupport() {
    @Test
    fun mismatchRejectsInitialAttachBeforeConnectOrLifecycleBinding() = runOwnedWebDatabaseTest {
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

            val mismatch = assertIs<IllegalArgumentException>(
                client.attach("user-1").exceptionOrNull(),
            )

            assertEquals("selected generation must match the complete managed local table set", mismatch.message)
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
    fun ordinaryOperationsUseImmutableAdmittedContractWithoutRediscovery() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val http = server.newHttpClient()
        createUsersAndPostsTables(db)
        val client = newClient(db, http)
        try {
            client.openAndConnect("user-1").getOrThrow()
            insertUser(db, "user-1", "Ada")
            server.protocolVersion = "v2"

            val initialDiscoveries = server.discoverySourceIds.size

            client.pushPending().getOrThrow()
            client.pullToStable().getOrThrow()

            assertEquals(initialDiscoveries, server.discoverySourceIds.size)
            assertEquals(1, server.pushRequestCount)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
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
