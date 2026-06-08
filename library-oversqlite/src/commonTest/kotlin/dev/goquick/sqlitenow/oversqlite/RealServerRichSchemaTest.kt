package dev.goquick.sqlitenow.oversqlite

import io.ktor.client.HttpClient
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

internal class RealServerRichSchemaTest : RealServerSupport() {
    @Test
    fun businessRichV0Schema_pushPullAndRebuild_workAgainstRealServer() = runTest {
        val config = requireRealServerConfig() ?: return@runTest
        resetRealServerState(config.baseUrl)

        val userId = randomRealServerId("realserver-rich-user")
        val seedDb = newDb()
        val pullDb = newDb()
        val hydrateDb = newDb()
        var seedHttp: HttpClient? = null
        var pullHttp: HttpClient? = null
        var hydrateHttp: HttpClient? = null
        try {
            createBusinessRichSchemaTables(seedDb)
            createBusinessRichSchemaTables(pullDb)
            createBusinessRichSchemaTables(hydrateDb)

            val seedSource = bootstrapManagedSourceId(seedDb, config.baseUrl, syncTables = businessRichSyncTables)
            val pullSource = bootstrapManagedSourceId(pullDb, config.baseUrl, syncTables = businessRichSyncTables)
            val hydrateSource = bootstrapManagedSourceId(hydrateDb, config.baseUrl, syncTables = businessRichSyncTables)
            seedHttp = newRealServerHttpClient(
                config.baseUrl,
                issueDummySigninToken(config.baseUrl, userId, seedSource),
            )
            pullHttp = newRealServerHttpClient(
                config.baseUrl,
                issueDummySigninToken(config.baseUrl, userId, pullSource),
            )
            hydrateHttp = newRealServerHttpClient(
                config.baseUrl,
                issueDummySigninToken(config.baseUrl, userId, hydrateSource),
            )

            val seedClient = newRealServerClient(seedDb, seedHttp, syncTables = businessRichSyncTables)
            val pullClient = newRealServerClient(pullDb, pullHttp, syncTables = businessRichSyncTables)
            val hydrateClient = newRealServerClient(hydrateDb, hydrateHttp, syncTables = businessRichSyncTables)

            seedClient.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.STARTED_EMPTY,
                actual = seedClient.attach(userId).getOrThrow(),
                expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
            )
            pullClient.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                actual = pullClient.attach(userId).getOrThrow(),
            )
            hydrateClient.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                actual = hydrateClient.attach(userId).getOrThrow(),
            )

            val fixture = insertBusinessRichGraph(seedDb, "business-rich-v0")
            assertEquals(PushOutcome.COMMITTED, seedClient.pushPending().getOrThrow().outcome)
            pullClient.pullToStable().getOrThrow()
            hydrateClient.rebuild().getOrThrow()

            assertBusinessRichGraph(pullDb, fixture, "business-rich-v0")
            assertBusinessRichGraph(hydrateDb, fixture, "business-rich-v0")
            assertFalse(pullClient.syncStatus().getOrThrow().pending.hasPendingSyncData)
            assertFalse(hydrateClient.syncStatus().getOrThrow().pending.hasPendingSyncData)
        } finally {
            seedHttp?.close()
            pullHttp?.close()
            hydrateHttp?.close()
            seedDb.close()
            pullDb.close()
            hydrateDb.close()
        }
    }
}
