package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import io.ktor.client.HttpClient
import io.ktor.client.plugins.auth.Auth
import io.ktor.client.plugins.auth.providers.BearerTokens
import io.ktor.client.plugins.auth.providers.bearer
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

const val skipAllOversqliteTest = false

internal suspend fun newInMemoryDb(debug: Boolean = false): SafeSQLiteConnection =
    BundledSqliteConnectionProvider.openConnection(":memory:", debug)

internal fun blockingNewInMemoryDb(debug: Boolean = false): SafeSQLiteConnection =
    runBlocking { newInMemoryDb(debug) }

// Deterministic resolver for tests: ensures convergence by using lexicographic ordering
// This prevents ping-pong conflicts between devices by making consistent decisions
internal object ClientWinsResolver : Resolver {
    override fun merge(
        table: String,
        pk: String,
        serverRow: JsonElement?,
        localPayload: JsonElement?
    ): MergeResult {
        try {
            val serverObj = serverRow as? JsonObject
                ?: Json.parseToJsonElement(serverRow.toString()) as JsonObject
            val serverPayload = serverObj["payload"]

            if (serverPayload == null) {
                // Server payload is null, keep local version
                return MergeResult.KeepLocal(localPayload!!)
            }

            // Use deterministic comparison: compare string representations lexicographically
            // This ensures both devices make the same decision when they see the same conflict
            val localStr = localPayload!!.toString()
            val serverStr = serverPayload.toString()

            return if (localStr <= serverStr) {
                MergeResult.KeepLocal(localPayload)
            } else {
                MergeResult.AcceptServer
            }

        } catch (e: Exception) {
            // If we can't parse server data, default to keeping local
            return MergeResult.KeepLocal(localPayload!!)
        }
    }
}

// Minimal HS256 JWT generator for tests
internal fun generateJwt(sub: String, did: String, secret: String): String {
    fun b64url(bytes: ByteArray): String =
        Base64.getUrlEncoder().withoutPadding().encodeToString(bytes)

    val headerJson = "{" + "\"alg\":\"HS256\",\"typ\":\"JWT\"" + "}"
    val exp = (System.currentTimeMillis() / 1000L) + 3600
    val payloadJson = "{" +
            "\"sub\":\"" + sub + "\"," +
            "\"did\":\"" + did + "\"," +
            "\"exp\":" + exp +
            "}"
    val header = b64url(headerJson.toByteArray(Charsets.UTF_8))
    val payload = b64url(payloadJson.toByteArray(Charsets.UTF_8))
    val signingInput = (header + "." + payload).toByteArray(Charsets.UTF_8)
    val mac = Mac.getInstance("HmacSHA256")
    val key = SecretKeySpec(secret.toByteArray(Charsets.UTF_8), "HmacSHA256")
    mac.init(key)
    val sig = b64url(mac.doFinal(signingInput))
    return "$header.$payload.$sig"
}

// Simple helper for running suspending code in instrumentation tests
internal fun runBlockingTest(block: suspend () -> Unit) {
    runBlocking { block() }
}

// Retry helpers
internal suspend fun retrySuspend(times: Int, block: suspend () -> Boolean): Boolean {
    repeat(times) {
        if (block()) return true
        delay(100)
    }
    return false
}

// Shared business schema creation for tests
internal suspend fun createBusinessTables(db: SafeSQLiteConnection) {
    // Enable foreign key constraints for testing
    db.execSQL("PRAGMA foreign_keys = ON")

    db.execSQL(
        """
        CREATE TABLE users (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          email TEXT NOT NULL
        ) WITHOUT ROWID
        """.trimIndent()
    )

    db.execSQL(
        """
        CREATE TABLE posts (
          id TEXT PRIMARY KEY,
          title TEXT NOT NULL,
          content TEXT,
          author_id TEXT NOT NULL REFERENCES users(id)
        ) WITHOUT ROWID
        """.trimIndent()
    )
}

// Helper to create authenticated HttpClient for tests
internal fun createAuthenticatedHttpClient(userSub: String, deviceId: String): HttpClient {
    return HttpClient {
        install(ContentNegotiation) {
            json(Json { ignoreUnknownKeys = true })
        }
        install(Auth) {
            bearer {
                loadTokens {
                    BearerTokens(
                        accessToken = generateJwt(userSub, deviceId, "your-secret-key-change-in-production"),
                        refreshToken = null
                    )
                }
                refreshTokens {
                    // For tests, just generate a new token
                    BearerTokens(
                        accessToken = generateJwt(userSub, deviceId, "your-secret-key-change-in-production"),
                        refreshToken = null
                    )
                }
            }
        }
        defaultRequest {
            url("http://10.0.2.2:8080")
        }
    }
}

// Shared client creation for tests
internal fun createSyncTestClient(
    db: SafeSQLiteConnection,
    userSub: String,
    deviceId: String,
    tables: List<String>,
    verboseLogs: Boolean = false
): DefaultOversqliteClient {
    val syncTables = tables.map { SyncTable(tableName = it) }
    val cfg = OversqliteConfig(schema = "business", syncTables = syncTables, verboseLogs = verboseLogs)
    val httpClient = createAuthenticatedHttpClient(userSub, deviceId)
    return DefaultOversqliteClient(
        db = db,
        config = cfg,
        http = httpClient,
        resolver = ClientWinsResolver,
        tablesUpdateListener = { }
    )
}

// Shared client creation for tests with custom primary keys
internal fun createSyncTestClientWithCustomPK(
    db: SafeSQLiteConnection,
    userSub: String,
    deviceId: String,
    syncTables: List<SyncTable>
): DefaultOversqliteClient {
    val cfg = OversqliteConfig(schema = "business", syncTables = syncTables)
    val httpClient = createAuthenticatedHttpClient(userSub, deviceId)
    return DefaultOversqliteClient(
        db = db,
        config = cfg,
        http = httpClient,
        resolver = ClientWinsResolver,
        tablesUpdateListener = { }
    )
}

// SQLite helpers
internal suspend fun scalarLong(db: SafeSQLiteConnection, sql: String): Long {
    val st = db.prepare(sql)
    return try {
        if (st.step()) st.getLong(0) else 0L
    } finally {
        st.close()
    }
}

internal suspend fun scalarText(db: SafeSQLiteConnection, sql: String): String {
    val st = db.prepare(sql)
    return try {
        if (st.step()) st.getText(0) else ""
    } finally {
        st.close()
    }
}

internal suspend fun count(db: SafeSQLiteConnection, table: String, whereClause: String): Int {
    val sql = "SELECT COUNT(*) FROM $table WHERE $whereClause"
    db.prepare(sql).use { st ->
        return if (st.step()) st.getLong(0).toInt() else 0
    }
}

// Upload/Download result validation helpers
internal fun assertUploadSuccess(result: Result<UploadSummary>, expectedApplied: Int? = null) {
    assert(result.isSuccess) { "Upload failed: ${result.exceptionOrNull()}" }
    val summary = result.getOrThrow()

    // Check for conflicts, errors, and invalid changes
    assert(summary.conflict == 0) {
        "Upload had ${summary.conflict} conflicts. Summary: $summary"
    }
    assert(summary.invalid == 0) {
        "Upload had ${summary.invalid} invalid changes. Reasons: ${summary.invalidReasons}. Summary: $summary"
    }
    assert(summary.materializeError == 0) {
        "Upload had ${summary.materializeError} materialize errors. Summary: $summary"
    }

    // Check that all changes were applied
    assert(summary.applied == summary.total) {
        "Not all changes were applied: ${summary.applied}/${summary.total}. Summary: $summary"
    }

    // Check expected applied count if specified
    expectedApplied?.let { expected ->
        assert(summary.applied == expected) {
            "Expected $expected applied changes, got ${summary.applied}. Summary: $summary"
        }
    }

    // Check for error messages
    summary.firstErrorMessage?.let { errorMsg ->
        throw AssertionError("Upload had error message: $errorMsg. Summary: $summary")
    }
}

internal fun assertDownloadSuccess(result: Result<*>) {
    assert(result.isSuccess) { "Download failed: ${result.exceptionOrNull()}" }
}

// More lenient version that allows conflicts but still checks for success
internal fun assertUploadSuccessWithConflicts(result: Result<UploadSummary>, allowConflicts: Boolean = true) {
    assert(result.isSuccess) { "Upload failed: ${result.exceptionOrNull()}" }
    val summary = result.getOrThrow()

    // Check for invalid changes and materialize errors (these are always bad)
    assert(summary.invalid == 0) {
        "Upload had ${summary.invalid} invalid changes. Reasons: ${summary.invalidReasons}. Summary: $summary"
    }
    assert(summary.materializeError == 0) {
        "Upload had ${summary.materializeError} materialize errors. Summary: $summary"
    }

    if (!allowConflicts) {
        assert(summary.conflict == 0) {
            "Upload had ${summary.conflict} conflicts. Summary: $summary"
        }
    }
}

// Helper to get upload summary for detailed checking
internal fun getUploadSummary(result: Result<UploadSummary>): UploadSummary {
    assert(result.isSuccess) { "Upload failed: ${result.exceptionOrNull()}" }
    return result.getOrThrow()
}
