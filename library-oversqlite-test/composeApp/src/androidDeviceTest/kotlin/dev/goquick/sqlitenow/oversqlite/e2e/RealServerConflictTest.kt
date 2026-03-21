package dev.goquick.sqlitenow.oversqlite.e2e

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.oversqlite.PendingPushReplayException
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class RealServerConflictTest {
    @Test
    fun conflictingEdits_keepDirtyRowsAndPreserveServerAuthoritativeState() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val deviceA = randomDeviceId("conflict-a")
        val deviceB = randomDeviceId("conflict-b")
        val observerDevice = randomDeviceId("conflict-observer")

        val tokenA = issueDummySigninToken(config.baseUrl, userId, deviceA)
        val tokenB = issueDummySigninToken(config.baseUrl, userId, deviceB)
        val tokenObserver = issueDummySigninToken(config.baseUrl, userId, observerDevice)
        val httpA = newAuthenticatedHttpClient(config.baseUrl, tokenA)
        val httpB = newAuthenticatedHttpClient(config.baseUrl, tokenB)
        val httpObserver = newAuthenticatedHttpClient(config.baseUrl, tokenObserver)
        val dbA = newInMemoryDb()
        val dbB = newInMemoryDb()
        val observerDb = newInMemoryDb()
        try {
            createBusinessSubsetTables(dbA)
            createBusinessSubsetTables(dbB)
            createBusinessSubsetTables(observerDb)

            val clientA = newRealServerClient(dbA, config, httpA)
            val clientB = newRealServerClient(dbB, config, httpB)
            val observerClient = newRealServerClient(observerDb, config, httpObserver)

            clientA.bootstrap(userId, deviceA).getOrThrow()
            clientB.bootstrap(userId, deviceB).getOrThrow()
            observerClient.bootstrap(userId, observerDevice).getOrThrow()

            val rowId = randomRowId()
            dbA.execSQL(
                """
                INSERT INTO users(id, name, email)
                VALUES('$rowId', 'Grace', 'grace@example.com')
                """.trimIndent()
            )
            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()
            observerClient.pullToStable().getOrThrow()

            dbA.execSQL("UPDATE users SET name = 'Grace Server' WHERE id = '$rowId'")
            dbB.execSQL("UPDATE users SET name = 'Grace Client' WHERE id = '$rowId'")

            clientA.pushPending().getOrThrow()

            val conflict = clientB.pushPending().exceptionOrNull()
            assertTrue(conflict != null)
            assertTrue(conflict?.message?.contains("409") == true)
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(1L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_push_outbound"))
            assertEquals("Grace Client", scalarText(dbB, "SELECT name FROM users WHERE id = '$rowId'"))

            val pullBlocked = clientB.pullToStable().exceptionOrNull()
            assertTrue(pullBlocked != null)
            assertTrue(pullBlocked is PendingPushReplayException)

            observerClient.pullToStable().getOrThrow()
            assertEquals("Grace Server", scalarText(observerDb, "SELECT name FROM users WHERE id = '$rowId'"))
            assertEquals(0L, scalarLong(observerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            httpA.close()
            httpB.close()
            httpObserver.close()
            dbA.close()
            dbB.close()
            observerDb.close()
        }
    }
}
