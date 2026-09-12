package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue

class AsyncSqliteApiContractTest {
    @Test
    fun expectedBeforePublicClientConstructionConfigurationAndResultsRemainSourceCompatible() =
        runBlocking {
            val connection = BundledSqliteConnectionProvider.openConnection(
                dbName = ":memory:",
                debug = false,
            )
            val http = HttpClient(MockEngine { error("async API construction fixture performs no I/O") })
            val config = OversqliteConfig(
                generationId = 1, databaseId = "fixture", serviceId = "main",
                syncTables = listOf(
                    SyncTable(tableName = "note", syncKeyColumnName = "id"),
                ),
            )
            val client: OversqliteClient = DefaultOversqliteClient(
                db = connection,
                config = config,
                http = http,
                resolver = ServerWinsResolver,
            )
            try {
                assertIs<DefaultOversqliteClient>(client)
                assertSame(OversqliteProgress.Idle, client.progress.value)
                assertEquals(200, config.transferTargets.maxPushRows)
                assertEquals(1L * 1024L * 1024L, config.transferTargets.maxPushDecodedBytes)
                assertEquals(1000, config.transferTargets.maxPullBundles)
                assertEquals(1000, config.transferTargets.maxSnapshotChunkRows)
                assertEquals(4L * 1024L * 1024L, config.transferTargets.maxSnapshotChunkDecodedBytes)

                val pending = PendingSyncStatus(
                    hasPendingSyncData = false,
                    pendingRowCount = 0,
                    blocksDetach = false,
                )
                val status = SyncStatus(
                    authority = AuthorityStatus.AUTHORITATIVE_EMPTY,
                    pending = pending,
                    lastBundleSeqSeen = 0,
                )
                val restore = RestoreSummary(bundleSeq = 3, rowCount = 5)
                val push = PushReport(PushOutcome.COMMITTED, status)
                val remote = RemoteSyncReport(
                    outcome = RemoteSyncOutcome.APPLIED_SNAPSHOT,
                    status = status,
                    restore = restore,
                )
                val sync = SyncReport(
                    pushOutcome = push.outcome,
                    remoteOutcome = remote.outcome,
                    status = status,
                    restore = restore,
                )
                val detached = SyncThenDetachResult(
                    lastSync = sync,
                    detach = DetachOutcome.DETACHED,
                    syncRounds = 1,
                    remainingPendingRowCount = 0,
                )
                val source = SourceInfo(
                        generationId = 1, contractConfirmed = false,
                    currentSourceId = "opaque-source",
                    rebuildRequired = false,
                    sourceRecoveryRequired = false,
                )

                assertTrue(detached.isSuccess())
                assertEquals(5, detached.lastSync?.restore?.rowCount)
                assertEquals("opaque-source", source.currentSourceId)
            } finally {
                client.close()
                http.close()
                connection.close()
            }
        }
}
