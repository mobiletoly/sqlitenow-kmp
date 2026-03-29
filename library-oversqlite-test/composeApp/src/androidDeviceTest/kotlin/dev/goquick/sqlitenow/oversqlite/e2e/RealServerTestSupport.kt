package dev.goquick.sqlitenow.oversqlite.e2e

import android.os.Bundle
import androidx.test.platform.app.InstrumentationRegistry
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.oversqlite.DefaultOversqliteClient
import dev.goquick.sqlitenow.oversqlite.OversqliteConfig
import dev.goquick.sqlitenow.oversqlite.OversqliteTransientRetryPolicy
import dev.goquick.sqlitenow.oversqlite.Resolver
import dev.goquick.sqlitenow.oversqlite.ServerWinsResolver
import dev.goquick.sqlitenow.oversqlite.SyncTable
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.okhttp.OkHttp
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
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.Protocol
import okhttp3.Response
import okhttp3.ResponseBody.Companion.toResponseBody
import org.junit.Assert.assertEquals
import org.junit.Assume.assumeTrue
import java.time.OffsetDateTime
import java.util.UUID

private val e2eJson = Json { ignoreUnknownKeys = true }
private const val expectedRealServerAppName = "nethttp-server-example"

internal val richSchemaSyncTables = listOf(
    SyncTable("users", syncKeyColumnName = "id"),
    SyncTable("posts", syncKeyColumnName = "id"),
    SyncTable("categories", syncKeyColumnName = "id"),
    SyncTable("teams", syncKeyColumnName = "id"),
    SyncTable("team_members", syncKeyColumnName = "id"),
    SyncTable("files", syncKeyColumnName = "id"),
    SyncTable("file_reviews", syncKeyColumnName = "id"),
    SyncTable("typed_rows", syncKeyColumnName = "id"),
)

internal data class HotGraphIds(
    val userId: String,
    val postId: String,
    val categoryRootId: String,
    val categoryChildId: String,
    val categoryLeafId: String,
    val teamId: String,
    val captainId: String,
    val memberId: String,
    val fileId: String,
    val reviewId: String,
)

internal data class BlobPairRow(
    val fileId: String,
    val reviewId: String,
    val label: String,
    val dataHex: String,
)

internal data class TypedRowFixture(
    val id: String,
    val name: String,
    val note: String?,
    val countValue: Long?,
    val enabledFlag: Long,
    val ratingLiteral: String?,
    val ratingExpectedText: String?,
    val dataHex: String?,
    val createdAt: String?,
)

internal data class RealServerConfig(
    val baseUrl: String,
    val schema: String,
)

internal data class InterceptedHttpResponse(
    val statusCode: Int,
    val body: String,
    val contentType: String = "application/json",
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

private fun instrumentationArgs(): Bundle =
    InstrumentationRegistry.getArguments()

internal suspend fun newInMemoryDb(debug: Boolean = false): SafeSQLiteConnection =
    BundledSqliteConnectionProvider.openConnection(":memory:", debug)

internal fun newAuthenticatedHttpClient(
    baseUrl: String,
    token: String,
    beforeSend: suspend (String) -> Unit = {},
    afterResponse: suspend (String) -> Unit = {},
    overrideResponse: (String) -> InterceptedHttpResponse? = { null },
): HttpClient {
    return HttpClient(OkHttp) {
        engine {
            config {
                addInterceptor { chain ->
                    val path = chain.request().url.encodedPath
                    runBlocking {
                        beforeSend(path)
                    }
                    val response =
                        overrideResponse(path)?.let { intercepted ->
                            Response.Builder()
                                .request(chain.request())
                                .protocol(Protocol.HTTP_1_1)
                                .code(intercepted.statusCode)
                                .message("Intercepted")
                                .body(intercepted.body.toResponseBody(intercepted.contentType.toMediaType()))
                                .build()
                        } ?: chain.proceed(chain.request())
                    runBlocking {
                        afterResponse(path)
                    }
                    response
                }
            }
        }
        install(ContentNegotiation) {
            json(e2eJson)
        }
        defaultRequest {
            url(baseUrl)
            header(HttpHeaders.Authorization, "Bearer $token")
        }
    }
}

internal suspend fun issueDummySigninToken(baseUrl: String, userId: String, sourceId: String): String {
    val http = HttpClient(OkHttp) {
        install(ContentNegotiation) {
            json(e2eJson)
        }
        defaultRequest {
            url(baseUrl)
        }
    }
    return try {
        http.post("/dummy-signin") {
            header(HttpHeaders.ContentType, ContentType.Application.Json.toString())
            setBody(DummySigninRequest(user = userId, password = "anything", device = sourceId))
        }.body<DummySigninResponse>().token
    } finally {
        http.close()
    }
}

internal suspend fun resetRealServerState(baseUrl: String) {
    val http = HttpClient(OkHttp) {
        install(ContentNegotiation) {
            json(e2eJson)
        }
        defaultRequest {
            url(baseUrl)
        }
    }
    try {
        val status = http.get("/status")
        check(status.status == HttpStatusCode.OK) {
            "real-server Android e2e status probe failed: HTTP ${status.status} - ${status.bodyAsText()}"
        }
        val appName = status.body<RealServerStatusResponse>().appName
        check(appName == expectedRealServerAppName) {
            "Real-server Android e2e tests require app_name='$expectedRealServerAppName' at $baseUrl, " +
                "but server reported '$appName'. Start examples/nethttp_server instead."
        }
        http.post("/test/reset") {
            header(HttpHeaders.ContentType, ContentType.Application.Json.toString())
            setBody("{}")
        }
    } finally {
        http.close()
    }
}

internal suspend fun setRetainedBundleFloor(
    baseUrl: String,
    userId: String,
    retainedBundleFloor: Long,
) {
    val http = HttpClient(OkHttp) {
        install(ContentNegotiation) {
            json(e2eJson)
        }
        defaultRequest {
            url(baseUrl)
        }
    }
    try {
        val response = http.post("/test/retention-floor") {
            header(HttpHeaders.ContentType, ContentType.Application.Json.toString())
            setBody(
                RetainedFloorRequest(
                    userId = userId,
                    retainedBundleFloor = retainedBundleFloor,
                )
            )
        }
        check(response.status == HttpStatusCode.OK) {
            "failed to set retained bundle floor: HTTP ${response.status} - ${response.bodyAsText()}"
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

internal suspend fun insertBusinessUserAndPost(
    db: SafeSQLiteConnection,
    userId: String,
    postId: String,
    suffix: String,
) {
    db.execSQL(
        """
        INSERT INTO users(id, name, email)
        VALUES('$userId', 'User $suffix', '$suffix@example.com')
        """.trimIndent()
    )
    db.execSQL(
        """
        INSERT INTO posts(id, title, content, author_id)
        VALUES('$postId', 'Title $suffix', 'Payload $suffix', '$userId')
        """.trimIndent()
    )
}

internal suspend fun createBusinessRichSchemaTables(db: SafeSQLiteConnection) {
    createBusinessSubsetTables(db)
    db.execSQL(
        """
        CREATE TABLE categories (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            parent_id TEXT REFERENCES categories(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
        )
        """.trimIndent()
    )
    db.execSQL(
        """
        CREATE TABLE teams (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            captain_member_id TEXT REFERENCES team_members(id) DEFERRABLE INITIALLY DEFERRED
        )
        """.trimIndent()
    )
    db.execSQL(
        """
        CREATE TABLE team_members (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            team_id TEXT NOT NULL REFERENCES teams(id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
        )
        """.trimIndent()
    )
    db.execSQL(
        """
        CREATE TABLE files (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            data BLOB NOT NULL
        )
        """.trimIndent()
    )
    db.execSQL(
        """
        CREATE TABLE file_reviews (
            id TEXT PRIMARY KEY NOT NULL,
            review TEXT NOT NULL,
            file_id TEXT NOT NULL REFERENCES files(id) DEFERRABLE INITIALLY DEFERRED
        )
        """.trimIndent()
    )
    db.execSQL(
        """
        CREATE TABLE typed_rows (
            id TEXT PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            note TEXT NULL,
            count_value INTEGER NULL,
            enabled_flag INTEGER NOT NULL,
            rating REAL NULL,
            data BLOB NULL,
            created_at TEXT NULL
        )
        """.trimIndent()
    )
}

internal suspend fun createBusinessBlobKeyTables(db: SafeSQLiteConnection) {
    db.execSQL(
        """
        CREATE TABLE files (
            id BLOB PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            data BLOB NOT NULL
        )
        """.trimIndent()
    )
    db.execSQL(
        """
        CREATE TABLE file_reviews (
            id BLOB PRIMARY KEY NOT NULL,
            review TEXT NOT NULL,
            file_id BLOB NOT NULL REFERENCES files(id) DEFERRABLE INITIALLY DEFERRED
        )
        """.trimIndent()
    )
}

internal suspend fun createBusinessIntegerKeyTables(db: SafeSQLiteConnection) {
    db.execSQL(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY NOT NULL,
            name TEXT NOT NULL
        )
        """.trimIndent()
    )
}

internal fun randomUserId(prefix: String = "oversqlite-e2e-user"): String =
    "$prefix-${UUID.randomUUID()}"

internal fun randomSourceId(prefix: String = "source"): String =
    "$prefix-${UUID.randomUUID()}"

internal fun randomRowId(): String =
    UUID.randomUUID().toString()

internal suspend fun insertUserAndPostBatch(
    db: SafeSQLiteConnection,
    batchPrefix: String,
    count: Int,
    titlePrefix: String,
    contentPrefix: String,
) {
    repeat(count) { index ->
        val suffix = "$batchPrefix-$index"
        val authorId = randomRowId()
        val postId = randomRowId()
        db.execSQL(
            """
            INSERT INTO users(id, name, email)
            VALUES('$authorId', 'User $suffix', '$suffix@example.com')
            """.trimIndent()
        )
        db.execSQL(
            """
            INSERT INTO posts(id, title, content, author_id)
            VALUES('$postId', '$titlePrefix $suffix', '$contentPrefix $suffix', '$authorId')
            """.trimIndent()
        )
    }
}

internal suspend fun insertRichSchemaBatch(
    db: SafeSQLiteConnection,
    batchPrefix: String,
) {
    insertUserAndPostBatch(
        db = db,
        batchPrefix = batchPrefix,
        count = 2,
        titlePrefix = "Rich title",
        contentPrefix = "Rich payload",
    )

    val rootId = randomRowId()
    val childId = randomRowId()
    val leafId = randomRowId()
    db.execSQL(
        """
        INSERT INTO categories(id, name, parent_id)
        VALUES('$rootId', 'Category root $batchPrefix', NULL)
        """.trimIndent()
    )
    db.execSQL(
        """
        INSERT INTO categories(id, name, parent_id)
        VALUES('$childId', 'Category child $batchPrefix', '$rootId')
        """.trimIndent()
    )
    db.execSQL(
        """
        INSERT INTO categories(id, name, parent_id)
        VALUES('$leafId', 'Category leaf $batchPrefix', '$childId')
        """.trimIndent()
    )

    val teamId = randomRowId()
    val captainId = randomRowId()
    val memberId = randomRowId()
    db.execSQL("BEGIN")
    db.execSQL(
        """
        INSERT INTO teams(id, name, captain_member_id)
        VALUES('$teamId', 'Team $batchPrefix', '$captainId')
        """.trimIndent()
    )
    db.execSQL(
        """
        INSERT INTO team_members(id, name, team_id)
        VALUES('$captainId', 'Captain $batchPrefix', '$teamId')
        """.trimIndent()
    )
    db.execSQL(
        """
        INSERT INTO team_members(id, name, team_id)
        VALUES('$memberId', 'Member $batchPrefix', '$teamId')
        """.trimIndent()
    )
    db.execSQL("COMMIT")

    val fileAId = randomRowId()
    val fileBId = randomRowId()
    val reviewAId = randomRowId()
    val reviewBId = randomRowId()
    val fileADataHex = randomRowId().replace("-", "")
    val fileBDataHex = randomRowId().replace("-", "")
    db.execSQL(
        """
        INSERT INTO files(id, name, data)
        VALUES('$fileAId', 'File A $batchPrefix', x'$fileADataHex')
        """.trimIndent()
    )
    db.execSQL(
        """
        INSERT INTO files(id, name, data)
        VALUES('$fileBId', 'File B $batchPrefix', x'$fileBDataHex')
        """.trimIndent()
    )
    db.execSQL(
        """
        INSERT INTO file_reviews(id, review, file_id)
        VALUES('$reviewAId', 'Review A $batchPrefix', '$fileAId')
        """.trimIndent()
    )
    db.execSQL(
        """
        INSERT INTO file_reviews(id, review, file_id)
        VALUES('$reviewBId', 'Review B $batchPrefix', '$fileBId')
        """.trimIndent()
    )
}

internal suspend fun insertCategoryGraph(
    db: SafeSQLiteConnection,
    label: String,
) {
    val rootId = randomRowId()
    val childId = randomRowId()
    val leafId = randomRowId()
    db.execSQL(
        """
        INSERT INTO categories(id, name, parent_id)
        VALUES('$rootId', 'Category root $label', NULL)
        """.trimIndent()
    )
    db.execSQL(
        """
        INSERT INTO categories(id, name, parent_id)
        VALUES('$childId', 'Category child $label', '$rootId')
        """.trimIndent()
    )
    db.execSQL(
        """
        INSERT INTO categories(id, name, parent_id)
        VALUES('$leafId', 'Category leaf $label', '$childId')
        """.trimIndent()
    )
}

internal suspend fun insertTeamGraph(
    db: SafeSQLiteConnection,
    label: String,
) {
    val teamId = randomRowId()
    val captainId = randomRowId()
    val memberId = randomRowId()
    db.execSQL("BEGIN")
    db.execSQL(
        """
        INSERT INTO teams(id, name, captain_member_id)
        VALUES('$teamId', 'Team $label', '$captainId')
        """.trimIndent()
    )
    db.execSQL(
        """
        INSERT INTO team_members(id, name, team_id)
        VALUES('$captainId', 'Captain $label', '$teamId')
        """.trimIndent()
    )
    db.execSQL(
        """
        INSERT INTO team_members(id, name, team_id)
        VALUES('$memberId', 'Member $label', '$teamId')
        """.trimIndent()
    )
    db.execSQL("COMMIT")
}

internal suspend fun insertBlobPair(
    db: SafeSQLiteConnection,
    label: String,
): BlobPairRow {
    val fileId = randomRowId()
    val reviewId = randomRowId()
    val dataHex = randomRowId().replace("-", "")
    val fileIdHex = uuidTextToBlobHex(fileId)
    val reviewIdHex = uuidTextToBlobHex(reviewId)
    db.execSQL(
        """
        INSERT INTO files(id, name, data)
        VALUES(x'$fileIdHex', 'File $label', x'$dataHex')
        """.trimIndent()
    )
    db.execSQL(
        """
        INSERT INTO file_reviews(id, review, file_id)
        VALUES(x'$reviewIdHex', 'Review $label', x'$fileIdHex')
        """.trimIndent()
    )
    return BlobPairRow(
        fileId = fileId,
        reviewId = reviewId,
        label = label,
        dataHex = dataHex,
    )
}

internal suspend fun insertTypedRow(
    db: SafeSQLiteConnection,
    row: TypedRowFixture,
) {
    val noteValue = row.note?.let { "'$it'" } ?: "NULL"
    val countValue = row.countValue?.toString() ?: "NULL"
    val ratingValue = row.ratingLiteral ?: "NULL"
    val dataValue = row.dataHex?.let { "x'$it'" } ?: "NULL"
    val createdAtValue = row.createdAt?.let { "'$it'" } ?: "NULL"
    db.execSQL(
        """
        INSERT INTO typed_rows(id, name, note, count_value, enabled_flag, rating, data, created_at)
        VALUES('${row.id}', '${row.name}', $noteValue, $countValue, ${row.enabledFlag}, $ratingValue, $dataValue, $createdAtValue)
        """.trimIndent()
    )
}

internal suspend fun assertTypedRowState(
    db: SafeSQLiteConnection,
    row: TypedRowFixture,
) {
    assertEquals(row.name, scalarText(db, "SELECT name FROM typed_rows WHERE id = '${row.id}'"))
    if (row.note == null) {
        assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM typed_rows WHERE id = '${row.id}' AND note IS NULL"))
    } else {
        assertEquals(row.note, scalarText(db, "SELECT note FROM typed_rows WHERE id = '${row.id}'"))
    }
    if (row.countValue == null) {
        assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM typed_rows WHERE id = '${row.id}' AND count_value IS NULL"))
    } else {
        assertEquals(row.countValue, scalarLong(db, "SELECT count_value FROM typed_rows WHERE id = '${row.id}'"))
    }
    assertEquals(row.enabledFlag, scalarLong(db, "SELECT enabled_flag FROM typed_rows WHERE id = '${row.id}'"))
    if (row.ratingExpectedText == null) {
        assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM typed_rows WHERE id = '${row.id}' AND rating IS NULL"))
    } else {
        assertEquals(row.ratingExpectedText, scalarText(db, "SELECT quote(rating) FROM typed_rows WHERE id = '${row.id}'"))
    }
    if (row.dataHex == null) {
        assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM typed_rows WHERE id = '${row.id}' AND data IS NULL"))
    } else {
        assertEquals((row.dataHex.length / 2).toLong(), scalarLong(db, "SELECT length(data) FROM typed_rows WHERE id = '${row.id}'"))
        assertEquals(row.dataHex.uppercase(), scalarText(db, "SELECT hex(data) FROM typed_rows WHERE id = '${row.id}'"))
    }
    if (row.createdAt == null) {
        assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM typed_rows WHERE id = '${row.id}' AND created_at IS NULL"))
    } else {
        assertEquals(
            OffsetDateTime.parse(row.createdAt).toInstant(),
            OffsetDateTime.parse(scalarText(db, "SELECT created_at FROM typed_rows WHERE id = '${row.id}'")).toInstant(),
        )
    }
}

internal suspend fun insertHotGraph(
    db: SafeSQLiteConnection,
): HotGraphIds {
    val ids = HotGraphIds(
        userId = randomRowId(),
        postId = randomRowId(),
        categoryRootId = randomRowId(),
        categoryChildId = randomRowId(),
        categoryLeafId = randomRowId(),
        teamId = randomRowId(),
        captainId = randomRowId(),
        memberId = randomRowId(),
        fileId = randomRowId(),
        reviewId = randomRowId(),
    )

    db.execSQL(
        """
        INSERT INTO users(id, name, email)
        VALUES('${ids.userId}', 'Hot User 0', 'hot-user-0@example.com')
        """.trimIndent()
    )
    db.execSQL(
        """
        INSERT INTO posts(id, title, content, author_id)
        VALUES('${ids.postId}', 'Hot Post 0', 'hot-content-0', '${ids.userId}')
        """.trimIndent()
    )
    db.execSQL(
        """
        INSERT INTO categories(id, name, parent_id)
        VALUES('${ids.categoryRootId}', 'Hot Category Root 0', NULL)
        """.trimIndent()
    )
    db.execSQL(
        """
        INSERT INTO categories(id, name, parent_id)
        VALUES('${ids.categoryChildId}', 'Hot Category Child 0', '${ids.categoryRootId}')
        """.trimIndent()
    )
    db.execSQL(
        """
        INSERT INTO categories(id, name, parent_id)
        VALUES('${ids.categoryLeafId}', 'Hot Category Leaf 0', '${ids.categoryChildId}')
        """.trimIndent()
    )
    db.execSQL("BEGIN")
    db.execSQL(
        """
        INSERT INTO teams(id, name, captain_member_id)
        VALUES('${ids.teamId}', 'Hot Team 0', '${ids.captainId}')
        """.trimIndent()
    )
    db.execSQL(
        """
        INSERT INTO team_members(id, name, team_id)
        VALUES('${ids.captainId}', 'Hot Captain 0', '${ids.teamId}')
        """.trimIndent()
    )
    db.execSQL(
        """
        INSERT INTO team_members(id, name, team_id)
        VALUES('${ids.memberId}', 'Hot Member 0', '${ids.teamId}')
        """.trimIndent()
    )
    db.execSQL("COMMIT")
    db.execSQL(
        """
        INSERT INTO files(id, name, data)
        VALUES('${ids.fileId}', 'Hot File 0', x'00112233445566778899aabbccddeeff')
        """.trimIndent()
    )
    db.execSQL(
        """
        INSERT INTO file_reviews(id, review, file_id)
        VALUES('${ids.reviewId}', 'Hot Review 0', '${ids.fileId}')
        """.trimIndent()
    )

    return ids
}

internal suspend fun mutateHotGraph(
    db: SafeSQLiteConnection,
    ids: HotGraphIds,
    round: Int,
) {
    val blobHex = round.toString(16).padStart(2, '0').repeat(16)
    db.execSQL(
        """
        UPDATE users
        SET name = 'Hot User $round', email = 'hot-user-$round@example.com'
        WHERE id = '${ids.userId}'
        """.trimIndent()
    )
    db.execSQL(
        """
        UPDATE posts
        SET title = 'Hot Post $round', content = 'hot-content-$round'
        WHERE id = '${ids.postId}'
        """.trimIndent()
    )
    db.execSQL(
        """
        UPDATE categories
        SET name = 'Hot Category Child $round'
        WHERE id = '${ids.categoryChildId}'
        """.trimIndent()
    )
    db.execSQL(
        """
        UPDATE categories
        SET name = 'Hot Category Leaf $round'
        WHERE id = '${ids.categoryLeafId}'
        """.trimIndent()
    )
    db.execSQL(
        """
        UPDATE teams
        SET name = 'Hot Team $round'
        WHERE id = '${ids.teamId}'
        """.trimIndent()
    )
    db.execSQL(
        """
        UPDATE team_members
        SET name = 'Hot Captain $round'
        WHERE id = '${ids.captainId}'
        """.trimIndent()
    )
    db.execSQL(
        """
        UPDATE team_members
        SET name = 'Hot Member $round'
        WHERE id = '${ids.memberId}'
        """.trimIndent()
    )
    db.execSQL(
        """
        UPDATE files
        SET name = 'Hot File $round', data = x'$blobHex'
        WHERE id = '${ids.fileId}'
        """.trimIndent()
    )
    db.execSQL(
        """
        UPDATE file_reviews
        SET review = 'Hot Review $round'
        WHERE id = '${ids.reviewId}'
        """.trimIndent()
    )
}

internal suspend fun assertHotGraphDrivenCounts(
    db: SafeSQLiteConnection,
    rounds: Int,
    extraRichBatches: Int = 0,
) {
    val totalBatches = rounds + extraRichBatches
    assertEquals((1 + 2 * totalBatches).toLong(), scalarLong(db, "SELECT COUNT(*) FROM users"))
    assertEquals((1 + 2 * totalBatches).toLong(), scalarLong(db, "SELECT COUNT(*) FROM posts"))
    assertEquals((3 + 3 * totalBatches).toLong(), scalarLong(db, "SELECT COUNT(*) FROM categories"))
    assertEquals((1 + totalBatches).toLong(), scalarLong(db, "SELECT COUNT(*) FROM teams"))
    assertEquals((2 + 2 * totalBatches).toLong(), scalarLong(db, "SELECT COUNT(*) FROM team_members"))
    assertEquals((1 + 2 * totalBatches).toLong(), scalarLong(db, "SELECT COUNT(*) FROM files"))
    assertEquals((1 + 2 * totalBatches).toLong(), scalarLong(db, "SELECT COUNT(*) FROM file_reviews"))
}

internal suspend fun assertHotGraphState(
    db: SafeSQLiteConnection,
    ids: HotGraphIds,
    finalRound: Int,
) {
    assertEquals("Hot User $finalRound", scalarText(db, "SELECT name FROM users WHERE id = '${ids.userId}'"))
    assertEquals("hot-user-$finalRound@example.com", scalarText(db, "SELECT email FROM users WHERE id = '${ids.userId}'"))
    assertEquals("Hot Post $finalRound", scalarText(db, "SELECT title FROM posts WHERE id = '${ids.postId}'"))
    assertEquals("hot-content-$finalRound", scalarText(db, "SELECT content FROM posts WHERE id = '${ids.postId}'"))
    assertEquals("Hot Category Child $finalRound", scalarText(db, "SELECT name FROM categories WHERE id = '${ids.categoryChildId}'"))
    assertEquals("Hot Category Leaf $finalRound", scalarText(db, "SELECT name FROM categories WHERE id = '${ids.categoryLeafId}'"))
    assertEquals("Hot Team $finalRound", scalarText(db, "SELECT name FROM teams WHERE id = '${ids.teamId}'"))
    assertEquals("Hot Captain $finalRound", scalarText(db, "SELECT name FROM team_members WHERE id = '${ids.captainId}'"))
    assertEquals("Hot Member $finalRound", scalarText(db, "SELECT name FROM team_members WHERE id = '${ids.memberId}'"))
    assertEquals("Hot File $finalRound", scalarText(db, "SELECT name FROM files WHERE id = '${ids.fileId}'"))
    assertEquals("Hot Review $finalRound", scalarText(db, "SELECT review FROM file_reviews WHERE id = '${ids.reviewId}'"))
    assertEquals(16L, scalarLong(db, "SELECT length(data) FROM files WHERE id = '${ids.fileId}'"))
}

internal suspend fun assertRoundPresence(
    db: SafeSQLiteConnection,
    label: String,
) {
    assertEquals(
        1L,
        scalarLong(
            db,
            """
            SELECT COUNT(*)
            FROM categories c
            JOIN categories p ON p.id = c.parent_id
            WHERE c.name = 'Category leaf $label'
              AND p.name = 'Category child $label'
            """.trimIndent(),
        ),
    )
    assertEquals(
        1L,
        scalarLong(
            db,
            """
            SELECT COUNT(*)
            FROM teams t
            JOIN team_members captain ON captain.id = t.captain_member_id
            WHERE t.name = 'Team $label'
              AND captain.name = 'Captain $label'
            """.trimIndent(),
        ),
    )
    assertEquals(
        1L,
        scalarLong(
            db,
            """
            SELECT COUNT(*)
            FROM files f
            JOIN file_reviews r ON r.file_id = f.id
            WHERE f.name = 'File B $label'
              AND r.review = 'Review B $label'
              AND length(f.data) = 16
            """.trimIndent(),
        ),
    )
}

internal suspend fun assertForeignKeyIntegrity(
    db: SafeSQLiteConnection,
) {
    assertEquals(
        0L,
        scalarLong(
            db,
            """
            SELECT COUNT(*)
            FROM posts p
            LEFT JOIN users u ON u.id = p.author_id
            WHERE u.id IS NULL
            """.trimIndent(),
        ),
    )
    assertEquals(
        0L,
        scalarLong(
            db,
            """
            SELECT COUNT(*)
            FROM categories c
            LEFT JOIN categories p ON p.id = c.parent_id
            WHERE c.parent_id IS NOT NULL AND p.id IS NULL
            """.trimIndent(),
        ),
    )
    assertEquals(
        0L,
        scalarLong(
            db,
            """
            SELECT COUNT(*)
            FROM teams t
            LEFT JOIN team_members m ON m.id = t.captain_member_id
            WHERE m.id IS NULL
            """.trimIndent(),
        ),
    )
    assertEquals(
        0L,
        scalarLong(
            db,
            """
            SELECT COUNT(*)
            FROM team_members m
            LEFT JOIN teams t ON t.id = m.team_id
            WHERE t.id IS NULL
            """.trimIndent(),
        ),
    )
    assertEquals(
        0L,
        scalarLong(
            db,
            """
            SELECT COUNT(*)
            FROM file_reviews r
            LEFT JOIN files f ON f.id = r.file_id
            WHERE f.id IS NULL
            """.trimIndent(),
        ),
    )
}

internal fun newRealServerClient(
    db: SafeSQLiteConnection,
    config: RealServerConfig,
    http: HttpClient,
    syncTables: List<SyncTable> = listOf(
        SyncTable("users", syncKeyColumnName = "id"),
        SyncTable("posts", syncKeyColumnName = "id"),
    ),
    uploadLimit: Int = 200,
    downloadLimit: Int = 1000,
    transientRetryPolicy: OversqliteTransientRetryPolicy = OversqliteTransientRetryPolicy(),
    resolver: Resolver = ServerWinsResolver,
): DefaultOversqliteClient {
    return newRealServerClient(
        db = db,
        http = http,
        oversqliteConfig = OversqliteConfig(
            schema = config.schema,
            uploadLimit = uploadLimit,
            downloadLimit = downloadLimit,
            syncTables = syncTables,
            transientRetryPolicy = transientRetryPolicy,
        ),
        resolver = resolver,
    )
}

internal fun newRealServerClient(
    db: SafeSQLiteConnection,
    http: HttpClient,
    oversqliteConfig: OversqliteConfig,
    resolver: Resolver = ServerWinsResolver,
): DefaultOversqliteClient {
    return DefaultOversqliteClient(
        db = db,
        config = oversqliteConfig,
        http = http,
        resolver = resolver,
    )
}

internal fun uuidTextToBlobHex(uuidText: String): String =
    uuidText.replace("-", "").lowercase()

internal suspend fun scalarLong(db: SafeSQLiteConnection, sql: String): Long {
    return db.prepare(sql).use { st ->
        check(st.step())
        st.getLong(0)
    }
}

internal suspend fun scalarText(db: SafeSQLiteConnection, sql: String): String {
    return db.prepare(sql).use { st ->
        check(st.step())
        st.getText(0)
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
    @SerialName("expires_in") val expiresIn: Long,
    val user: String,
    val device: String,
)

@Serializable
private data class RetainedFloorRequest(
    @SerialName("user_id") val userId: String,
    @SerialName("retained_bundle_floor") val retainedBundleFloor: Long,
)

@Serializable
private data class RealServerStatusResponse(
    @SerialName("app_name") val appName: String,
)
