package dev.goquick.sqlitenow.oversqlite.platform

import dev.goquick.sqlitenow.core.worker.SqliteWorkerSQLiteConnection
import dev.goquick.sqlitenow.oversqlite.OwnedWorkerStorage
import dev.goquick.sqlitenow.oversqlite.ServerWinsResolver
import dev.goquick.sqlitenow.oversqlite.platform.generated.RealServerGeneratedDatabase
import dev.goquick.sqlitenow.oversqlite.platform.generated.VersionBasedDatabaseMigrations
import dev.goquick.sqlitenow.oversqlite.webRuntimeKind
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest

internal class GeneratedOversqliteWorkerFacadeTest : PlatformCrossTargetTestSupport() {
    @Test
    fun generatedConfigConstructsAndOpensAgainstDefaultWorkerConnection() = runTest {
        val ownedStorage = OwnedWorkerStorage()
        val dbName = ownedStorage.newDatabaseName()
        val generatedFacade = RealServerGeneratedDatabase(
            dbName = dbName,
            migration = VersionBasedDatabaseMigrations(),
            debug = true,
        )
        val server = MockSyncServer(
            registeredTableSpecs = testRegisteredTableSpecs(
                "categories", "file_reviews", "files", "posts",
                "team_members", "teams", "typed_rows", "users",
            ),
        )
        val http = server.newHttpClient()
        val opening = generatedFacade.openForSync(
            config = generatedFacade.buildOversqliteConfig(
                generationId = 1L,
                databaseId = "platform-sync",
                serviceId = "platform-mock",
            ),
            httpClient = http,
            resolver = ServerWinsResolver,
        )
        try {
            opening.advance()
            val worker = generatedFacade.connection().ref as? SqliteWorkerSQLiteConnection
            assertTrue(worker != null, "The generated façade evidence must use the worker connection.")
            val client = generatedFacade.newOversqliteClient(
                generationId = 1L,
                databaseId = "platform-sync",
                serviceId = "platform-mock",
                httpClient = http,
                resolver = ServerWinsResolver,
            )
            assertEquals(
                8L,
                scalarLong(generatedFacade.connection(), "SELECT COUNT(*) FROM _sync_managed_tables"),
            )
            assertEquals(
                "anonymous",
                scalarText(generatedFacade.connection(), "SELECT binding_state FROM _sync_attachment_state"),
            )
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
            opening.close()
            http.close()
            ownedStorage.cleanup()
        }
    }
}
