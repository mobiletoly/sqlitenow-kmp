package dev.goquick.sqlitenow.oversqlite.realserver

import dev.goquick.sqlitenow.oversqlite.*
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.oversqlite.platform.PlatformTestDatabase
import dev.goquick.sqlitenow.oversqlite.platform.PlatformCrossTargetTestSupport
import dev.goquick.sqlitenow.oversqlite.platformsupport.assertConnectedOutcome
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.plugins.observer.ResponseObserver
import io.ktor.client.request.HttpRequestPipeline
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
import kotlinx.serialization.SerialName
import kotlinx.serialization.json.Json
import kotlinx.coroutines.CancellationException
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
        check(baseUrl != null) {
            "Realserver tests require an explicit OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL for their owned fixture"
        }
        check(serverHealthAvailable(baseUrl)) {
            "Realserver tests are enabled, but /syncx/health is unavailable at $baseUrl"
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
        tokenProvider: () -> String? = { token },
        afterResponse: suspend (String) -> Unit = {},
        beforeRequest: suspend (String) -> Unit = {},
    ): HttpClient {
        return HttpClient {
            install(ContentNegotiation) {
                json(realServerJson)
            }
            install("RealServerRequestObserver") {
                requestPipeline.intercept(HttpRequestPipeline.Before) {
                    beforeRequest(context.url.build().encodedPath)
                    proceed()
                }
            }
            install(ResponseObserver) {
                onResponse { response ->
                    afterResponse(response.call.request.url.encodedPath)
                }
            }
            defaultRequest {
                url(baseUrl)
                tokenProvider()?.let { currentToken ->
                    header(HttpHeaders.Authorization, "Bearer $currentToken")
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
            val response = http.get("/syncx/status")
            check(response.status == HttpStatusCode.OK) {
                "fresh realserver status probe failed: HTTP ${response.status} - ${response.bodyAsText()}"
            }
        } finally {
            http.close()
        }
    }

    protected suspend fun createBusinessSubsetTables(db: PlatformTestDatabase) {
        createProductionRealServerTables(db)
    }

    protected suspend fun createBusinessSubsetTables(db: SafeSQLiteConnection) {
        createProductionRealServerTables(db)
    }

    protected suspend fun bootstrapManagedSourceId(
        db: PlatformTestDatabase,
        baseUrl: String,
    ): String {
        // Dummy sign-in does not bind the token to this value. Avoid opening the coordinated
        // fixture here: JS Node databases are intentionally transient, so close/reopen would
        // discard the local state that the real test is about to admit.
        return randomRealServerId("signin-${(db.databaseName + baseUrl).hashCode()}")
    }

    protected suspend fun bootstrapManagedSourceId(
        db: SafeSQLiteConnection,
        baseUrl: String,
    ): String =
        bootstrapSourceId(baseUrl) { http ->
            newRealServerClient(db, http)
        }

    protected suspend fun bootstrapSourceId(
        baseUrl: String,
        clientFactory: (HttpClient) -> OversqliteClient,
    ): String {
        val http = newRealServerHttpClient(baseUrl)
        val client = clientFactory(http)
        return try {
            client.open().getOrThrow()
            client.sourceInfo().getOrThrow().currentSourceId
        } finally {
            client.close()
            http.close()
        }
    }

    protected suspend fun insertBusinessUserAndPost(
        db: PlatformTestDatabase,
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
        db: PlatformTestDatabase,
        http: HttpClient,
        maxPushRows: Int = 8,
        maxPullBundles: Int = 8,
        maxSnapshotChunkRows: Int = 1000,
        transientRetryPolicy: OversqliteTransientRetryPolicy = OversqliteTransientRetryPolicy(),
    ): OversqliteClient {
        return CoordinatedRealServerClient(
            db = db,
            config = OversqliteConfig(
                generationId = 1L, databaseId = "platform-sync", serviceId = "nethttp-server-example",
                syncTables = productionRealServerSyncTables,
                transferTargets = OversqliteTransferTargets(
                    maxPushRows = maxPushRows,
                    maxPullBundles = maxPullBundles,
                    maxSnapshotChunkRows = maxSnapshotChunkRows,
                ),
                transientRetryPolicy = transientRetryPolicy,
            ),
            http = http,
        )
    }

    protected fun newRealServerClient(
        db: SafeSQLiteConnection,
        http: HttpClient,
        maxPushRows: Int = 8,
        maxPullBundles: Int = 8,
        maxSnapshotChunkRows: Int = 1000,
        transientRetryPolicy: OversqliteTransientRetryPolicy = OversqliteTransientRetryPolicy(),
    ): OversqliteClient {
        return DefaultOversqliteClient(
            db = db,
            config = OversqliteConfig(
                generationId = 1L, databaseId = "platform-sync", serviceId = "nethttp-server-example",
                syncTables = productionRealServerSyncTables,
                transferTargets = OversqliteTransferTargets(
                    maxPushRows = maxPushRows,
                    maxPullBundles = maxPullBundles,
                    maxSnapshotChunkRows = maxSnapshotChunkRows,
                ),
                transientRetryPolicy = transientRetryPolicy,
            ),
            http = http,
        )
    }

    protected suspend fun runRealServerAutomaticDownloads(
        client: OversqliteClient,
        automaticDownloadConfig: OversqliteAutomaticDownloadConfig,
    ): Nothing {
        return (client as CoordinatedRealServerClient).runAutomaticDownloads(automaticDownloadConfig)
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

private class CoordinatedRealServerClient(
    private val db: PlatformTestDatabase,
    private val config: OversqliteConfig,
    private val http: HttpClient,
) : OversqliteClient {
    private var delegate: OversqliteClient? = null

    private fun requireDelegate(): OversqliteClient = checkNotNull(delegate) {
        "coordinated real-server client requires open()"
    }

    override val progress get() = requireDelegate().progress

    override suspend fun pauseUploads() = requireDelegate().pauseUploads()
    override suspend fun resumeUploads() = requireDelegate().resumeUploads()
    override suspend fun pauseDownloads() = requireDelegate().pauseDownloads()
    override suspend fun resumeDownloads() = requireDelegate().resumeDownloads()

    override suspend fun open(): Result<Unit> {
        if (delegate != null) return Result.success(Unit)
        val next = try {
            db.openClient(
                config = config,
                http = http,
                resolver = DeferConflictResolver,
            )
        } catch (failure: CancellationException) {
            throw failure
        } catch (failure: Throwable) {
            return Result.failure(failure)
        }
        delegate = next
        return Result.success(Unit)
    }

    suspend fun runAutomaticDownloads(
        automaticDownloadConfig: OversqliteAutomaticDownloadConfig,
    ): Nothing = requireDelegate().runAutomaticDownloads(automaticDownloadConfig)

    override suspend fun attach(userId: String) = requireDelegate().attach(userId)
    override suspend fun sourceInfo() = requireDelegate().sourceInfo()
    override suspend fun readPushDependencyDiagnostic(diagnosticId: String, cursor: String?, pageSize: Int) =
        requireDelegate().readPushDependencyDiagnostic(diagnosticId, cursor, pageSize)
    override suspend fun releasePushDependencyDiagnostic(diagnosticId: String) =
        requireDelegate().releasePushDependencyDiagnostic(diagnosticId)
    override suspend fun syncStatus() = requireDelegate().syncStatus()
    override suspend fun detach() = requireDelegate().detach()
    override suspend fun pushPending() = requireDelegate().pushPending()
    override suspend fun pendingConflict() = requireDelegate().pendingConflict()
    override suspend fun resolveConflict(selected: PendingConflict, result: MergeResult) =
        requireDelegate().resolveConflict(selected, result)
    override suspend fun retryConflict(selected: PendingConflict) = requireDelegate().retryConflict(selected)
    override suspend fun pullToStable() = requireDelegate().pullToStable()
    override suspend fun sync() = requireDelegate().sync()
    override suspend fun syncThenDetach() = requireDelegate().syncThenDetach()
    override suspend fun rebuild() = requireDelegate().rebuild()

    override fun close() {
        // The fixture owns the coordinated opening and closes it with the database. Keeping the
        // connection alive here lets lifecycle tests inspect local state between client instances.
        delegate = null
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
