package dev.goquick.sqlitenow.oversqlite.platform

import android.os.Bundle
import androidx.test.platform.app.InstrumentationRegistry
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.oversqlite.platformSuiteEnabled
import io.ktor.client.HttpClient
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import org.junit.Assume.assumeTrue

internal val testJson = Json { ignoreUnknownKeys = true }

internal suspend fun newInMemoryDb(debug: Boolean = false): SafeSQLiteConnection =
    BundledSqliteConnectionProvider.openConnection(":memory:", debug)

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
