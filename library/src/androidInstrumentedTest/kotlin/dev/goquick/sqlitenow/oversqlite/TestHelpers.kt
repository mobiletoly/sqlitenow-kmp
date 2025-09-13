package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.plugins.auth.providers.bearer
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.url
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking

const val skipAllTest = false

// Deterministic resolver for tests: ensures convergence by using lexicographic ordering
// This prevents ping-pong conflicts between devices by making consistent decisions
internal object ClientWinsResolver : Resolver {
    override fun merge(
        table: String,
        pk: String,
        serverRow: kotlinx.serialization.json.JsonElement?,
        localPayload: kotlinx.serialization.json.JsonElement?
    ): MergeResult {
        try {
            val serverObj = serverRow as? kotlinx.serialization.json.JsonObject
                ?: kotlinx.serialization.json.Json.parseToJsonElement(serverRow.toString()) as kotlinx.serialization.json.JsonObject
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
        java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(bytes)

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
    val mac = javax.crypto.Mac.getInstance("HmacSHA256")
    val key = javax.crypto.spec.SecretKeySpec(secret.toByteArray(Charsets.UTF_8), "HmacSHA256")
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
        )
        """.trimIndent()
    )

    db.execSQL(
        """
        CREATE TABLE posts (
          id TEXT PRIMARY KEY,
          title TEXT NOT NULL,
          content TEXT,
          author_id TEXT NOT NULL REFERENCES users(id)
        )
        """.trimIndent()
    )
}

// Helper to create authenticated HttpClient for tests
internal fun createAuthenticatedHttpClient(userSub: String, deviceId: String): io.ktor.client.HttpClient {
    return io.ktor.client.HttpClient {
        install(io.ktor.client.plugins.contentnegotiation.ContentNegotiation) {
            json(kotlinx.serialization.json.Json { ignoreUnknownKeys = true })
        }
        install(io.ktor.client.plugins.auth.Auth) {
            bearer {
                loadTokens {
                    io.ktor.client.plugins.auth.providers.BearerTokens(
                        accessToken = generateJwt(userSub, deviceId, "your-secret-key-change-in-production"),
                        refreshToken = null
                    )
                }
                refreshTokens {
                    // For tests, just generate a new token
                    io.ktor.client.plugins.auth.providers.BearerTokens(
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
    tables: List<String>
): DefaultOversqliteClient {
    val cfg = OversqliteConfig(schema = "business", tables = tables)
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