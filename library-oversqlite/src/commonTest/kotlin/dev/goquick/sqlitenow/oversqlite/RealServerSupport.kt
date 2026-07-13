package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.common.initializePlatformTestContext
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.plugins.observer.ResponseObserver
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.request.HttpRequestPipeline
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.kotlinx.json.json
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerialName
import kotlinx.serialization.json.Json
import kotlin.test.assertEquals
import kotlin.random.Random
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

internal expect fun realServerEnv(name: String): String?

internal data class RealServerConfig(
    val baseUrl: String,
    val heavyMode: Boolean,
)

internal open class RealServerSupport : CrossTargetSyncTestSupport() {
    private val realServerJson = Json { ignoreUnknownKeys = true }
    private val expectedRealServerAppName = "nethttp-server-example"
    protected val businessRichSyncTables = listOf(
        SyncTable("users", syncKeyColumnName = "id"),
        SyncTable("posts", syncKeyColumnName = "id"),
        SyncTable("categories", syncKeyColumnName = "id"),
        SyncTable("teams", syncKeyColumnName = "id"),
        SyncTable("team_members", syncKeyColumnName = "id"),
        SyncTable("files", syncKeyColumnName = "id"),
        SyncTable("file_reviews", syncKeyColumnName = "id"),
		SyncTable(
			"typed_rows",
			syncKeyColumnName = "id",
		),
    )

    protected suspend fun requireRealServerConfig(): RealServerConfig? {
        initializePlatformTestContext()
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
        return RealServerConfig(
            baseUrl = baseUrl,
            heavyMode = realServerFlagEnabled("OVERSQLITE_REALSERVER_HEAVY"),
        )
    }

    protected fun newRealServerHttpClient(
        baseUrl: String,
        token: String? = null,
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
            val response = http.get("/syncx/status")
            check(response.status == HttpStatusCode.OK) {
                "fresh realserver status probe failed: HTTP ${response.status} - ${response.bodyAsText()}"
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

    protected suspend fun createBusinessRichSchemaTables(db: SafeSQLiteConnection) {
        createBusinessSubsetTables(db)
        db.execSQL(
            """
            CREATE TABLE categories (
                id TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
            )
            """.trimIndent(),
        )
        db.execSQL(
            """
            CREATE TABLE teams (
                id TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                captain_member_id TEXT REFERENCES team_members(id) DEFERRABLE INITIALLY DEFERRED
            )
            """.trimIndent(),
        )
        db.execSQL(
            """
            CREATE TABLE team_members (
                id TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                team_id TEXT NOT NULL REFERENCES teams(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
            )
            """.trimIndent(),
        )
        db.execSQL(
            """
            CREATE TABLE files (
                id BLOB PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                data BLOB NOT NULL
            )
            """.trimIndent(),
        )
        db.execSQL(
            """
            CREATE TABLE file_reviews (
                id BLOB PRIMARY KEY NOT NULL,
                review TEXT NOT NULL,
                file_id BLOB NOT NULL REFERENCES files(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
            )
            """.trimIndent(),
        )
        db.execSQL(
            """
            CREATE TABLE typed_rows (
                id TEXT PRIMARY KEY NOT NULL,
                name TEXT NOT NULL,
                note TEXT NULL,
            count_value INTEGER NULL,
            small_count INTEGER NULL,
            medium_count INTEGER NULL,
            exact_amount TEXT NULL,
            enabled_flag INTEGER NOT NULL,
            rating REAL NULL,
            float4_value REAL NULL,
                data BLOB NULL,
                created_at TEXT NULL
            )
            """.trimIndent(),
        )
    }

    protected suspend fun bootstrapManagedSourceId(
        db: SafeSQLiteConnection,
        baseUrl: String,
        syncTables: List<SyncTable> = listOf(
            SyncTable("users", syncKeyColumnName = "id"),
            SyncTable("posts", syncKeyColumnName = "id"),
        ),
    ): String {
        val http = newRealServerHttpClient(baseUrl)
        val client = newRealServerClient(db, http, syncTables = syncTables)
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

    protected suspend fun insertBusinessRichGraph(db: SafeSQLiteConnection, suffix: String): BusinessRichFixture {
        val fixture = BusinessRichFixture(
            userId = randomRealServerUuid(),
            postId = randomRealServerUuid(),
            categoryRootId = randomRealServerUuid(),
            categoryChildId = randomRealServerUuid(),
            teamId = randomRealServerUuid(),
            captainId = randomRealServerUuid(),
            memberId = randomRealServerUuid(),
            fileId = randomRealServerUuid(),
            reviewId = randomRealServerUuid(),
            typedRowId = randomRealServerUuid(),
            fileDataHex = "00112233445566778899aabbccddeeff",
            typedDataHex = "cafebabe",
        )
        insertBusinessUserAndPost(db, fixture.userId, fixture.postId, suffix)
        db.execSQL(
            """
            INSERT INTO categories(id, name, parent_id)
            VALUES('${fixture.categoryRootId}', 'Category root $suffix', NULL)
            """.trimIndent(),
        )
        db.execSQL(
            """
            INSERT INTO categories(id, name, parent_id)
            VALUES('${fixture.categoryChildId}', 'Category child $suffix', '${fixture.categoryRootId}')
            """.trimIndent(),
        )
        db.execSQL("BEGIN")
        db.execSQL(
            """
            INSERT INTO teams(id, name, captain_member_id)
            VALUES('${fixture.teamId}', 'Team $suffix', '${fixture.captainId}')
            """.trimIndent(),
        )
        db.execSQL(
            """
            INSERT INTO team_members(id, name, team_id)
            VALUES('${fixture.captainId}', 'Captain $suffix', '${fixture.teamId}')
            """.trimIndent(),
        )
        db.execSQL(
            """
            INSERT INTO team_members(id, name, team_id)
            VALUES('${fixture.memberId}', 'Member $suffix', '${fixture.teamId}')
            """.trimIndent(),
        )
        db.execSQL("COMMIT")
        val fileIdHex = uuidTextToBlobHex(fixture.fileId)
        val reviewIdHex = uuidTextToBlobHex(fixture.reviewId)
        db.execSQL(
            """
            INSERT INTO files(id, name, data)
            VALUES(x'$fileIdHex', 'File $suffix', x'${fixture.fileDataHex}')
            """.trimIndent(),
        )
        db.execSQL(
            """
            INSERT INTO file_reviews(id, review, file_id)
            VALUES(x'$reviewIdHex', 'Review $suffix', x'$fileIdHex')
            """.trimIndent(),
        )
        db.execSQL(
            """
            INSERT INTO typed_rows(id, name, note, count_value, enabled_flag, rating, data, created_at)
            VALUES('${fixture.typedRowId}', 'Typed $suffix', NULL, 42, 1, 1.25, x'${fixture.typedDataHex}', '2026-03-24T18:42:11Z')
            """.trimIndent(),
        )
        return fixture
    }

    protected suspend fun assertBusinessRichGraph(db: SafeSQLiteConnection, fixture: BusinessRichFixture, suffix: String) {
        val fileIdHex = uuidTextToBlobHex(fixture.fileId)
        val reviewIdHex = uuidTextToBlobHex(fixture.reviewId)
        assertEquals("User $suffix", scalarText(db, "SELECT name FROM users WHERE id = '${fixture.userId}'"))
        assertEquals("Title $suffix", scalarText(db, "SELECT title FROM posts WHERE id = '${fixture.postId}'"))
        assertEquals(
            1L,
            scalarLong(
                db,
                """
                SELECT COUNT(*)
                FROM categories c
                JOIN categories p ON p.id = c.parent_id
                WHERE c.id = '${fixture.categoryChildId}'
                  AND p.id = '${fixture.categoryRootId}'
                """.trimIndent(),
            ),
        )
        assertEquals("Team $suffix", scalarText(db, "SELECT name FROM teams WHERE id = '${fixture.teamId}'"))
        assertEquals("Captain $suffix", scalarText(db, "SELECT name FROM team_members WHERE id = '${fixture.captainId}'"))
        assertEquals("File $suffix", scalarText(db, "SELECT name FROM files WHERE lower(hex(id)) = '$fileIdHex'"))
        assertEquals(fixture.fileDataHex, scalarText(db, "SELECT lower(hex(data)) FROM files WHERE lower(hex(id)) = '$fileIdHex'"))
        assertEquals("Review $suffix", scalarText(db, "SELECT review FROM file_reviews WHERE lower(hex(id)) = '$reviewIdHex'"))
        assertEquals(fileIdHex, scalarText(db, "SELECT lower(hex(file_id)) FROM file_reviews WHERE lower(hex(id)) = '$reviewIdHex'"))
        assertEquals("Typed $suffix", scalarText(db, "SELECT name FROM typed_rows WHERE id = '${fixture.typedRowId}'"))
        assertEquals(42L, scalarLong(db, "SELECT count_value FROM typed_rows WHERE id = '${fixture.typedRowId}'"))
        assertEquals("1.25", scalarText(db, "SELECT CAST(rating AS TEXT) FROM typed_rows WHERE id = '${fixture.typedRowId}'"))
        assertEquals(fixture.typedDataHex, scalarText(db, "SELECT lower(hex(data)) FROM typed_rows WHERE id = '${fixture.typedRowId}'"))
    }

    protected fun newRealServerClient(
        db: SafeSQLiteConnection,
        http: HttpClient,
        syncTables: List<SyncTable> = listOf(
            SyncTable("users", syncKeyColumnName = "id"),
            SyncTable("posts", syncKeyColumnName = "id"),
        ),
        uploadLimit: Int = 8,
        downloadLimit: Int = 8,
        snapshotChunkRows: Int = 1000,
        transientRetryPolicy: OversqliteTransientRetryPolicy = OversqliteTransientRetryPolicy(),
    ): DefaultOversqliteClient {
        return DefaultOversqliteClient(
            db = db,
            config = OversqliteConfig(
                schema = "business",
                syncTables = syncTables,
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

    protected fun uuidTextToBlobHex(uuidText: String): String =
        uuidText.replace("-", "").lowercase()

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

    private suspend fun fetchRealServerStatus(baseUrl: String): RealServerStatusResponse {
        val http = newRealServerHttpClient(baseUrl)
        return try {
            val response = http.get("/syncx/status")
            check(response.status == HttpStatusCode.OK) {
                "realserver status probe failed: HTTP ${response.status} - ${response.bodyAsText()}"
            }
            response.body<RealServerStatusResponse>()
        } finally {
            http.close()
        }
    }

    protected fun realServerHeavyModeEnabled(config: RealServerConfig): Boolean = config.heavyMode

    private fun realServerFlagEnabled(name: String): Boolean {
        return when (realServerEnv(name)?.trim()?.lowercase()) {
            "1", "true", "yes", "on" -> true
            else -> false
        }
    }
}

internal data class BusinessRichFixture(
    val userId: String,
    val postId: String,
    val categoryRootId: String,
    val categoryChildId: String,
    val teamId: String,
    val captainId: String,
    val memberId: String,
    val fileId: String,
    val reviewId: String,
    val typedRowId: String,
    val fileDataHex: String,
    val typedDataHex: String,
)

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

@Serializable
private data class RealServerStatusResponse(
    @SerialName("app_name")
    val appName: String,
)
