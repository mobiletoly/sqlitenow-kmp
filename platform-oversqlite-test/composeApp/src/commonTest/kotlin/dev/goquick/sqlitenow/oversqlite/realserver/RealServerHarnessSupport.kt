package dev.goquick.sqlitenow.oversqlite.realserver

import dev.goquick.sqlitenow.oversqlite.*
import dev.goquick.sqlitenow.oversqlite.platform.PlatformCrossTargetTestSupport
import dev.goquick.sqlitenow.oversqlite.platformsupport.assertConnectedOutcome

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
import io.ktor.client.plugins.observer.ResponseObserver
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerialName
import kotlinx.serialization.json.Json
import kotlin.random.Random
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

internal expect fun realServerEnv(name: String): String?

internal data class RealServerHarnessConfig(
    val baseUrl: String,
    val heavyMode: Boolean,
)

internal open class RealServerHarnessSupport : PlatformCrossTargetTestSupport() {
    private val realServerJson = Json { ignoreUnknownKeys = true }
    private val expectedRealServerAppName = "nethttp-server-example"

    protected suspend fun requireRealServerConfig(): RealServerHarnessConfig? {
        if (!realServerFlagEnabled("OVERSQLITE_REALSERVER_TESTS")) {
            println(
                "Skipping realserver tests; set OVERSQLITE_REALSERVER_TESTS=true to enable them locally.",
            )
            return null
        }
        val baseUrl = realServerEnv("OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL")
            ?.trim()
            .takeUnless { it.isNullOrEmpty() }
            ?: "http://localhost:8080"
        val availability = serverHealthAvailable(baseUrl)
        if (!availability) {
            println("Skipping realserver tests; server unavailable at $baseUrl")
            return null
        }
        val status = fetchRealServerStatus(baseUrl)
        check(status.appName == expectedRealServerAppName) {
            "Realserver tests require app_name='$expectedRealServerAppName' at $baseUrl, " +
                "but server reported '${status.appName}'. Start examples/nethttp_server instead."
        }
        return RealServerHarnessConfig(
            baseUrl = baseUrl,
            heavyMode = realServerFlagEnabled("OVERSQLITE_REALSERVER_HEAVY"),
        )
    }

    protected fun newRealServerHttpClient(
        baseUrl: String,
        token: String? = null,
        afterResponse: suspend (String) -> Unit = {},
    ): HttpClient {
        return HttpClient {
            install(ContentNegotiation) {
                json(realServerJson)
            }
            install(ResponseObserver) {
                onResponse { response ->
                    afterResponse(response.call.request.url.encodedPath)
                }
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
                setBody(RealServerHarnessDummySigninRequest(user = userId, password = "anything", device = deviceId))
            }
            check(response.status == HttpStatusCode.OK) {
                "dummy-signin failed: HTTP ${response.status} - ${response.bodyAsText()}"
            }
            response.body<RealServerHarnessDummySigninResponse>().token
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

    protected suspend fun bootstrapManagedSourceId(
        db: SafeSQLiteConnection,
        baseUrl: String,
    ): String {
        val http = newRealServerHttpClient(baseUrl)
        val client = newRealServerClient(db, http)
        return try {
            client.open().getOrThrow()
            client.sourceInfo().getOrThrow().currentSourceId
        } finally {
            client.close()
            http.close()
        }
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
        snapshotChunkRows: Int = 1000,
        transientRetryPolicy: OversqliteTransientRetryPolicy = OversqliteTransientRetryPolicy(),
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
                snapshotChunkRows = snapshotChunkRows,
                transientRetryPolicy = transientRetryPolicy,
            ),
            http = http,
        )
    }

    protected fun randomRealServerId(prefix: String): String {
        val partA = Random.nextInt().toString().removePrefix("-")
        val partB = Random.nextInt().toString().removePrefix("-")
        return "$prefix-$partA-$partB"
    }

    @OptIn(ExperimentalUuidApi::class)
    protected fun randomRealServerUuid(): String = Uuid.random().toString()

    private suspend fun serverHealthAvailable(baseUrl: String): Boolean {
        val http = newRealServerHttpClient(baseUrl)
        return try {
            runCatching {
                val response = http.get("/syncx/health")
                response.status == HttpStatusCode.OK
            }.getOrDefault(false)
        } finally {
            http.close()
        }
    }

    private suspend fun fetchRealServerStatus(baseUrl: String): RealServerHarnessStatusResponse {
        val http = newRealServerHttpClient(baseUrl)
        return try {
            val response = http.get("/syncx/status")
            check(response.status == HttpStatusCode.OK) {
                "realserver status probe failed: HTTP ${response.status} - ${response.bodyAsText()}"
            }
            response.body<RealServerHarnessStatusResponse>()
        } finally {
            http.close()
        }
    }

    protected fun realServerHeavyModeEnabled(config: RealServerHarnessConfig): Boolean = config.heavyMode

    private fun realServerFlagEnabled(name: String): Boolean {
        return when (realServerEnv(name)?.trim()?.lowercase()) {
            "1", "true", "yes", "on" -> true
            else -> false
        }
    }
}

@Serializable
private data class RealServerHarnessDummySigninRequest(
    val user: String,
    val password: String,
    val device: String,
)

@Serializable
private data class RealServerHarnessDummySigninResponse(
    val token: String,
)

@Serializable
private data class RealServerHarnessStatusResponse(
    @SerialName("app_name")
    val appName: String,
)
