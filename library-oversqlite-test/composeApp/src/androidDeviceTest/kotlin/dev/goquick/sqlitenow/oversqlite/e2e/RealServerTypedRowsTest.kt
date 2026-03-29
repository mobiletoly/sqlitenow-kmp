package dev.goquick.sqlitenow.oversqlite.e2e

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.oversqlite.RebuildMode
import dev.goquick.sqlitenow.oversqlite.SyncTable
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class RealServerTypedRowsTest {
    @Test
    fun typedRows_pushPullHydrateAndImmediatePullStayConsistent() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId("typed-rows-user")
        val seedDevice = randomSourceId("typed-seed")
        val activeDevice = randomSourceId("typed-active")
        val hydrateDevice = randomSourceId("typed-hydrate")
        val syncTables = listOf(SyncTable("typed_rows", syncKeyColumnName = "id"))

        val seedToken = issueDummySigninToken(config.baseUrl, userId, seedDevice)
        val activeToken = issueDummySigninToken(config.baseUrl, userId, activeDevice)
        val hydrateToken = issueDummySigninToken(config.baseUrl, userId, hydrateDevice)
        val seedHttp = newAuthenticatedHttpClient(config.baseUrl, seedToken)
        val activeHttp = newAuthenticatedHttpClient(config.baseUrl, activeToken)
        val hydrateHttp = newAuthenticatedHttpClient(config.baseUrl, hydrateToken)
        val seedDb = newInMemoryDb()
        val activeDb = newInMemoryDb()
        val hydrateDb = newInMemoryDb()
        try {
            createBusinessRichSchemaTables(seedDb)
            createBusinessRichSchemaTables(activeDb)
            createBusinessRichSchemaTables(hydrateDb)

            val seedClient = newRealServerClient(seedDb, config, seedHttp, syncTables = syncTables)
            val activeClient = newRealServerClient(activeDb, config, activeHttp, syncTables = syncTables)
            val hydrateClient = newRealServerClient(hydrateDb, config, hydrateHttp, syncTables = syncTables)

            seedClient.openAndAttach(userId, seedDevice).getOrThrow()
            activeClient.openAndAttach(userId, activeDevice).getOrThrow()
            hydrateClient.openAndAttach(userId, hydrateDevice).getOrThrow()

            val seedRow = TypedRowFixture(
                id = randomRowId(),
                name = "Seed Typed Row",
                note = null,
                countValue = 42L,
                enabledFlag = 1L,
                ratingLiteral = "1.25",
                ratingExpectedText = "1.25",
                dataHex = "00112233445566778899aabbccddeeff",
                createdAt = "2026-03-24T18:42:11Z",
            )
            insertTypedRow(seedDb, seedRow)
            seedClient.pushPending().getOrThrow()

            val activeRow = TypedRowFixture(
                id = randomRowId(),
                name = "Active Typed Row",
                note = "second-device",
                countValue = null,
                enabledFlag = 0L,
                ratingLiteral = "6.57111473696007",
                ratingExpectedText = "6.57111473696007",
                dataHex = null,
                createdAt = null,
            )
            insertTypedRow(activeDb, activeRow)

            activeClient.pushPending().getOrThrow()
            activeClient.pullToStable().getOrThrow()
            hydrateClient.rebuild(RebuildMode.KEEP_SOURCE).getOrThrow()

            assertEquals(2L, scalarLong(activeDb, "SELECT COUNT(*) FROM typed_rows"))
            assertEquals(2L, scalarLong(hydrateDb, "SELECT COUNT(*) FROM typed_rows"))
            assertEquals(0L, scalarLong(activeDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(hydrateDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))

            assertTypedRowState(activeDb, seedRow)
            assertTypedRowState(activeDb, activeRow)
            assertTypedRowState(hydrateDb, seedRow)
            assertTypedRowState(hydrateDb, activeRow)
        } finally {
            seedHttp.close()
            activeHttp.close()
            hydrateHttp.close()
            seedDb.close()
            activeDb.close()
            hydrateDb.close()
        }
    }
}
