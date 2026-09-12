package dev.goquick.sqlitenow.oversqlite.platform

import android.os.Bundle
import androidx.test.platform.app.InstrumentationRegistry
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.oversqlite.*
import io.ktor.client.HttpClient
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.runBlocking
import org.junit.Assume.assumeTrue

internal suspend fun newInMemoryDb(debug: Boolean = false): PlatformTestDatabase =
    PlatformTestDatabase(
        dbName = ":memory:",
        connectionProvider = BundledSqliteConnectionProvider,
        schemaStatements = mutableListOf(),
        connectionConfig = null,
    )

internal suspend fun newCoordinatedClient(
    db: PlatformTestDatabase,
    http: HttpClient,
    syncTables: List<SyncTable>,
): OversqliteClient = db.openClient(
    config = OversqliteConfig(
        generationId = 1L,
        databaseId = "platform-sync",
        serviceId = "platform-mock",
        syncTables = syncTables,
    ),
    http = http,
    resolver = ServerWinsResolver,
)

internal fun runBlockingTest(block: suspend () -> Unit) {
    runBlocking { block() }
}

internal fun requirePlatformSuite() {
    assumeTrue(
        "opt-in oversqlite platform tests are disabled; pass OVERSQLITE_PLATFORM_TESTS=true",
        platformSuiteEnabled(),
    )
}

internal fun instrumentationArgs(): Bundle =
    InstrumentationRegistry.getArguments()

internal fun newNoopHttpClient(): HttpClient {
    return HttpClient {
        install(ContentNegotiation) {
            json(testJson)
        }
    }
}
