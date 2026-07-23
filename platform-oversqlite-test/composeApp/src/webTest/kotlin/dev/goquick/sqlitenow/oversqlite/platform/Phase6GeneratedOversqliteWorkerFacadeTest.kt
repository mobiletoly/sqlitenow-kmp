package dev.goquick.sqlitenow.oversqlite.platform

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.worker.SqliteWorkerSQLiteConnection
import dev.goquick.sqlitenow.oversqlite.DefaultOversqliteClient
import dev.goquick.sqlitenow.oversqlite.OversqliteClient
import dev.goquick.sqlitenow.oversqlite.Phase6OwnedStorage
import dev.goquick.sqlitenow.oversqlite.ServerWinsResolver
import dev.goquick.sqlitenow.oversqlite.SyncTable
import dev.goquick.sqlitenow.oversqlite.oversqliteTestConnectionProvider
import dev.goquick.sqlitenow.oversqlite.platform.generated.RealServerGeneratedDatabase
import dev.goquick.sqlitenow.oversqlite.platform.generated.VersionBasedDatabaseMigrations
import dev.goquick.sqlitenow.oversqlite.webRuntimeKind
import io.ktor.client.HttpClient
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest

internal class Phase6GeneratedOversqliteWorkerFacadeTest : PlatformCrossTargetTestSupport() {
    @Test
    fun generatedConfigConstructsAndOpensAgainstDefaultWorkerConnection() = runTest {
        val ownedStorage = Phase6OwnedStorage()
        val dbName = ownedStorage.newDatabaseName()
        val connection = oversqliteTestConnectionProvider()
            .openConnection(dbName, debug = true)
        try {
            val worker = connection.ref as? SqliteWorkerSQLiteConnection
            assertTrue(worker != null, "The generated façade evidence must use the worker connection.")
            createUsersAndPostsTables(connection)
            val generatedFacade = RealServerGeneratedDatabase(
                dbName = ":memory:",
                migration = VersionBasedDatabaseMigrations(),
                debug = true,
            )
            val server = MockSyncServer()
            val client = generatedFacade.newPhase6OversqliteClientForTest(
                injectedConnection = connection,
                httpClient = server.newHttpClient(),
            )
            try {
                client.open().getOrThrow()
                assertEquals(
                    2L,
                    scalarLong(connection, "SELECT COUNT(*) FROM _sync_managed_tables"),
                )
                assertEquals(
                    "anonymous",
                    scalarText(connection, "SELECT binding_state FROM _sync_attachment_state"),
                )
            } finally {
                client.close()
            }
            val metrics = worker.metricsForTest()
            assertEquals(
                if (webRuntimeKind() == "js-node") "js-node-worker" else "browser-worker",
                metrics.runtimeKind,
            )
            assertEquals(
                if (metrics.runtimeKind == "js-node-worker") "memory" else "direct-opfs",
                metrics.storageMode,
            )
            assertEquals(0L, metrics.integerNumberViolations)
            assertEquals(0, metrics.liveStatements)
            assertEquals(0L, metrics.snapshotExports)
        } finally {
            connection.close()
            ownedStorage.cleanup()
        }
    }
}

/**
 * Phase 6 test-only connection injection. It deliberately reproduces the generated factory body
 * without adding a production constructor parameter or changing generated/public API shape.
 */
private fun RealServerGeneratedDatabase.newPhase6OversqliteClientForTest(
    injectedConnection: SafeSQLiteConnection,
    httpClient: HttpClient,
): OversqliteClient {
    val syncTables = listOf(
        SyncTable("users", syncKeyColumnName = "id"),
        SyncTable("posts", syncKeyColumnName = "id"),
    )
    val config = buildOversqliteConfig(
        schema = "main",
        uploadLimit = 8,
        downloadLimit = 8,
        syncTables = syncTables,
    )
    return DefaultOversqliteClient(
        db = injectedConnection,
        config = config,
        http = httpClient,
        resolver = ServerWinsResolver,
    )
}
