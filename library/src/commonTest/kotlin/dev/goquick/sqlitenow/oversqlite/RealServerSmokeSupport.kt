package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.common.initializePlatformTestContext
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlin.random.Random

internal expect fun realServerSmokeEnv(name: String): String?

internal data class RealServerSmokeConfig(
    val baseUrl: String,
)

internal open class RealServerSmokeSupport : CrossTargetSyncTestSupport() {
    private val smokeJson = Json { ignoreUnknownKeys = true }

    protected suspend fun requireRealServerSmokeConfig(): RealServerSmokeConfig? {
        initializePlatformTestContext()
        val baseUrl = realServerSmokeEnv("OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL")
            ?.trim()
            .takeUnless { it.isNullOrEmpty() }
            ?: "http://localhost:8080"
        val availability = serverHealthAvailable(baseUrl)
        if (!availability) {
            println("Skipping real-server smoke tests; server unavailable at $baseUrl")
            return null
        }
        return RealServerSmokeConfig(baseUrl = baseUrl)
    }

    protected fun newRealServerHttpClient(
        baseUrl: String,
        token: String? = null,
    ): HttpClient {
        return HttpClient {
            install(ContentNegotiation) {
                json(smokeJson)
            }
            defaultRequest {
                url(baseUrl)
                if (token != null) {
                    header(HttpHeaders.Authorization, "Bearer $token")
                }
            }
        }
    }

    protected suspend fun issueDummySigninToken(
        baseUrl: String,
        userId: String,
        deviceId: String,
    ): String {
        val http = newRealServerHttpClient(baseUrl)
        return try {
            val response = http.post("/dummy-signin") {
                header(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody(DummySigninRequest(user = userId, password = "anything", device = deviceId))
            }
            check(response.status == HttpStatusCode.OK) {
                "dummy-signin failed: HTTP ${response.status} - ${response.bodyAsText()}"
            }
            response.body<DummySigninResponse>().token
        } finally {
            http.close()
        }
    }

    protected suspend fun resetRealServerState(baseUrl: String) {
        val http = newRealServerHttpClient(baseUrl)
        try {
            val response = http.post("/test/reset") {
                header(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                setBody("{}")
            }
            check(response.status == HttpStatusCode.OK) {
                "server reset failed: HTTP ${response.status} - ${response.bodyAsText()}"
            }
        } finally {
            http.close()
        }
    }

    protected suspend fun createBusinessSubsetTables(db: SafeSQLiteConnection) {
        db.execSQL(
            """
            CREATE TABLE users (
                id TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """.trimIndent(),
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
            """.trimIndent(),
        )
    }

    protected suspend fun insertBusinessUserAndPost(
        db: SafeSQLiteConnection,
        userId: String,
        postId: String,
        suffix: String,
    ) {
        db.execSQL(
            """
            INSERT INTO users(id, name, email)
            VALUES('$userId', 'User $suffix', '$suffix@example.com')
            """.trimIndent(),
        )
        db.execSQL(
            """
            INSERT INTO posts(id, title, content, author_id)
            VALUES('$postId', 'Title $suffix', 'Payload $suffix', '$userId')
            """.trimIndent(),
        )
    }

    protected fun newRealServerClient(
        db: SafeSQLiteConnection,
        http: HttpClient,
        uploadLimit: Int = 8,
        downloadLimit: Int = 8,
    ): DefaultOversqliteClient {
        return DefaultOversqliteClient(
            db = db,
            config = OversqliteConfig(
                schema = "business",
                syncTables = listOf(
                    SyncTable("users", syncKeyColumnName = "id"),
                    SyncTable("posts", syncKeyColumnName = "id"),
                ),
                uploadLimit = uploadLimit,
                downloadLimit = downloadLimit,
            ),
            http = http,
            tablesUpdateListener = { },
        )
    }

    protected fun randomSmokeId(prefix: String): String {
        val partA = Random.nextInt().toString().removePrefix("-")
        val partB = Random.nextInt().toString().removePrefix("-")
        return "$prefix-$partA-$partB"
    }

    private suspend fun serverHealthAvailable(baseUrl: String): Boolean {
        val http = newRealServerHttpClient(baseUrl)
        return try {
            runCatching {
                val response = http.get("/health")
                response.status == HttpStatusCode.OK
            }.getOrDefault(false)
        } finally {
            http.close()
        }
    }
}

@Serializable
private data class DummySigninRequest(
    val user: String,
    val password: String,
    val device: String,
)

@Serializable
private data class DummySigninResponse(
    val token: String,
)
