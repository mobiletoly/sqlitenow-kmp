package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking

class BundleMultiDeviceContractTest : BundleClientContractTestSupport() {
    @Test
    fun sameUserDifferentDevices_convergeAcrossPushAndPull() = runBlocking {
        val dbA = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        val dbB = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(dbA)
        createUsersTable(dbB)

        val server = newServer()
        val syncServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        syncServer.install(server)
        server.start()
        val httpA = newHttpClient(server)
        val httpB = newHttpClient(server)
        try {
            val clientA = newClient(dbA, httpA, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            val clientB = newClient(dbB, httpB, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))

            clientA.bootstrap("user-1", "device-a").getOrThrow()
            clientB.bootstrap("user-1", "device-b").getOrThrow()

            dbA.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada from A')")
            clientA.pushPending().getOrThrow()

            assertEquals("Ada from A", scalarText(dbA, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals(1L, clientA.lastBundleSeqSeen().getOrThrow())

            clientB.pullToStable().getOrThrow()

            assertEquals("Ada from A", scalarText(dbB, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals(1L, clientB.lastBundleSeqSeen().getOrThrow())

            dbB.execSQL("UPDATE users SET name = 'Ada from B' WHERE id = 'user-1'")
            clientB.pushPending().getOrThrow()

            assertEquals("Ada from B", scalarText(dbB, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals(2L, clientB.lastBundleSeqSeen().getOrThrow())

            clientA.pullToStable().getOrThrow()

            assertEquals("Ada from B", scalarText(dbA, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals("Ada from B", scalarText(dbB, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals(2L, clientA.lastBundleSeqSeen().getOrThrow())
            assertEquals(listOf("device-a", "device-b"), syncServer.createRequests.map { it.sourceId })
            assertEquals(listOf(1L, 1L), syncServer.createRequests.map { it.sourceBundleId })
        } finally {
            httpA.close()
            httpB.close()
            server.stop(0)
            dbA.close()
            dbB.close()
        }
    }
}
