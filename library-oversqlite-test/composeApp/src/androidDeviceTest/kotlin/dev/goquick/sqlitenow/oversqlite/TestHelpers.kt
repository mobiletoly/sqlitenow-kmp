package dev.goquick.sqlitenow.oversqlite

import android.os.Bundle
import androidx.test.platform.app.InstrumentationRegistry
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.junit.Assume.assumeTrue
import java.util.UUID

internal val testJson = Json { ignoreUnknownKeys = true }

internal suspend fun newInMemoryDb(debug: Boolean = false): SafeSQLiteConnection =
    BundledSqliteConnectionProvider.openConnection(":memory:", debug)

internal fun runBlockingTest(block: suspend () -> Unit) {
    runBlocking { block() }
}

internal fun newNoopHttpClient(): HttpClient {
    return HttpClient {
        install(ContentNegotiation) {
            json(testJson)
        }
    }
}

internal data class RealServerConfig(
    val baseUrl: String,
    val schema: String,
)

internal fun requireRealServerConfig(): RealServerConfig {
    val args = instrumentationArgs()
    val enabled = args.getString("oversqliteRealServer")?.trim().orEmpty()
    assumeTrue(
        "opt-in real-server oversqlite tests are disabled; pass -Pandroid.testInstrumentationRunnerArguments.oversqliteRealServer=true",
        enabled.equals("true", ignoreCase = true) || enabled == "1"
    )
    return RealServerConfig(
        baseUrl = args.getString("oversqliteE2EBaseUrl")?.trim().takeUnless { it.isNullOrEmpty() }
            ?: "http://10.0.2.2:8080",
        schema = args.getString("oversqliteE2ESchema")?.trim().takeUnless { it.isNullOrEmpty() }
            ?: "business",
    )
}

internal fun instrumentationArgs(): Bundle =
    InstrumentationRegistry.getArguments()

internal fun newAuthenticatedHttpClient(baseUrl: String, token: String): HttpClient {
    return HttpClient(OkHttp) {
        install(ContentNegotiation) {
            json(testJson)
        }
        defaultRequest {
            url(baseUrl)
            header(HttpHeaders.Authorization, "Bearer $token")
        }
    }
}

internal suspend fun issueDummySigninToken(baseUrl: String, userId: String, deviceId: String): String {
    val http = HttpClient(OkHttp) {
        install(ContentNegotiation) {
            json(testJson)
        }
        defaultRequest {
            url(baseUrl)
        }
    }
    return try {
        http.post("/dummy-signin") {
            header(HttpHeaders.ContentType, ContentType.Application.Json.toString())
            setBody(DummySigninRequest(user = userId, password = "anything", device = deviceId))
        }.body<DummySigninResponse>().token
    } finally {
        http.close()
    }
}

internal suspend fun resetRealServerState(baseUrl: String) {
    val http = HttpClient(OkHttp) {
        install(ContentNegotiation) {
            json(testJson)
        }
        defaultRequest {
            url(baseUrl)
        }
    }
    try {
        http.post("/test/reset") {
            header(HttpHeaders.ContentType, ContentType.Application.Json.toString())
            setBody("{}")
        }
    } finally {
        http.close()
    }
}

internal suspend fun createBusinessSubsetTables(db: SafeSQLiteConnection) {
    db.execSQL(
        """
        CREATE TABLE users (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """.trimIndent()
    )
    db.execSQL(
        """
        CREATE TABLE posts (
            id TEXT PRIMARY KEY NOT NULL,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            author_id TEXT REFERENCES users(id) DEFERRABLE INITIALLY DEFERRED,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """.trimIndent()
    )
}

internal fun newSyncTablesForBusinessSubset(): List<SyncTable> =
    listOf(
        SyncTable("users", syncKeyColumnName = "id"),
        SyncTable("posts", syncKeyColumnName = "id"),
    )

internal fun randomUserId(prefix: String = "oversqlite-e2e-user"): String =
    "$prefix-${UUID.randomUUID()}"

internal fun randomDeviceId(prefix: String = "device"): String =
    "$prefix-${UUID.randomUUID()}"

internal fun randomRowId(): String =
    UUID.randomUUID().toString()

@Serializable
private data class DummySigninRequest(
    val user: String,
    val password: String,
    val device: String,
)

@Serializable
private data class DummySigninResponse(
    val token: String,
    @SerialName("expires_in") val expiresIn: Long,
    val user: String,
    val device: String,
)
