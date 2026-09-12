package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.DatabaseMigrations
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowDatabase
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.contentType
import io.ktor.serialization.kotlinx.json.json
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

class CoordinatedOpeningLiveAuthorityPilotJvmTest {
    @Test
    fun twoDeviceGenerationOneToTwoPilotUsesLiveAuthority() = runBlocking {
        val baseUrl = System.getenv("COORDINATED_OPENING_AUTHORITY_BASE_URL")?.trim().orEmpty()
        if (baseUrl.isEmpty()) {
            println("Skipping coordinated opening live-authority pilot; set COORDINATED_OPENING_AUTHORITY_BASE_URL")
            return@runBlocking
        }

        val userId = "coordinatedopening-kmp-${System.nanoTime()}"
        val token = issueToken(baseUrl, userId)
        val httpA = authorityHttp(baseUrl, token)
        val httpB = authorityHttp(baseUrl, token)
        val directory = Files.createTempDirectory("sqlitenow-coordinatedopening-live-pilot")
        val pathA = directory.resolve("device-a.sqlite3").toString()
        val pathB = directory.resolve("device-b.sqlite3").toString()
        val generationOneA = pilotDatabase(pathA, targetVersion = 1)
        val generationOneB = pilotDatabase(pathB, targetVersion = 1)
        var openingA: OversqliteOpening<SqliteNowDatabase>? = null
        var openingB: OversqliteOpening<SqliteNowDatabase>? = null
        var targetOpening: OversqliteOpening<SqliteNowDatabase>? = null

        try {
            openingA = generationOneA.openForSync(pilotConfig(1), httpA)
            assertNull(openingA.readyOrNull())
            assertFalse(generationOneA.isOpen())
            assertFailsWith<IllegalStateException> { generationOneA.connection() }
            assertEquals(OversqliteOpeningOutcome.READY, openingA.advance().outcome)
            println("COORDINATED_OPENING-PILOT: device A generation 1 ready")
            assertTrue(openingA.readyOrNull()!!.client.attach(userId).getOrThrow() is AttachResult.Connected)

            generationOneA.connection().execSQL(
                "CREATE TABLE drafts(id TEXT PRIMARY KEY NOT NULL, body BLOB NOT NULL)",
            )
            generationOneA.connection().withPreparedStatement(
                "INSERT INTO drafts(id,body) VALUES(?,?)",
            ) { statement ->
                statement.bindText(1, "draft-1")
                statement.bindBlob(2, byteArrayOf(0, 1, 2, -1))
                statement.step()
            }
            generationOneA.connection().execSQL(
                "INSERT INTO notes(id,title) VALUES" +
                    "('11111111-1111-4111-8111-111111111111','base-conflict')," +
                    "('22222222-2222-4222-8222-222222222222','base-delete')",
            )
            openingA.readyOrNull()!!.client.sync().getOrThrow()
            println("COORDINATED_OPENING-PILOT: device A seed synchronized")

            openingB = generationOneB.openForSync(pilotConfig(1), httpB)
            assertEquals(OversqliteOpeningOutcome.READY, openingB.advance().outcome)
            assertTrue(openingB.readyOrNull()!!.client.attach(userId).getOrThrow() is AttachResult.Connected)
            println("COORDINATED_OPENING-PILOT: device B generation 1 restored")
            assertEquals(2L, scalarLong(generationOneB.connection(), "SELECT count(*) FROM notes"))

            generationOneB.connection().execSQL(
                "UPDATE notes SET title='server-wins' WHERE id='11111111-1111-4111-8111-111111111111'",
            )
            openingB.readyOrNull()!!.client.sync().getOrThrow()
            println("COORDINATED_OPENING-PILOT: device B conflicting update synchronized")

            generationOneA.connection().execSQL(
                "UPDATE notes SET title='client-conflict' WHERE id='11111111-1111-4111-8111-111111111111'",
            )
            generationOneA.connection().execSQL(
                "DELETE FROM notes WHERE id='22222222-2222-4222-8222-222222222222'",
            )
            generationOneA.connection().execSQL(
                "INSERT INTO notes(id,title) VALUES('33333333-3333-4333-8333-333333333333','client-insert')",
            )
            openingA.close()
            openingA = null

            val migrationSqlCalls = AtomicInteger()
            val callbackCalls = AtomicInteger()
            val generationTwoA = pilotDatabase(
                path = pathA,
                targetVersion = 2,
                migrationSqlCalls = migrationSqlCalls,
                callbackCalls = callbackCalls,
            )
            targetOpening = generationTwoA.openForSync(
                config = pilotConfig(2),
                httpClient = httpA,
                upgradePlan = OversqliteUpgradePlan(
                    migrationId = "coordinatedopening-notes-v1-v2",
                    fromGenerationId = 1,
                    toGenerationId = 2,
                    fromLocalVersion = 1,
                    toLocalVersion = 2,
                    preservedLocalTables = setOf("drafts"),
                ),
            )
            assertNull(targetOpening.readyOrNull(), "target database/client must remain unpublished before U8")
            assertFalse(generationTwoA.isOpen(), "target DDL and ready connection must remain unpublished before advance")

            val paused = targetOpening.advance()
            println("COORDINATED_OPENING-PILOT: device A target advance returned ${paused.phase}/${paused.outcome}")
            assertEquals(OversqliteOpeningOutcome.NEEDS_RESOLUTION, paused.outcome)
            val conflict = assertNotNull(targetOpening.pendingConflict().getOrThrow())
            assertEquals("notes", conflict.conflict.table)

            generationOneB.connection().execSQL(
                "INSERT INTO notes(id,title) VALUES('44444444-4444-4444-8444-444444444444','old-device-live')",
            )
            openingB.readyOrNull()!!.client.sync().getOrThrow()
            println("COORDINATED_OPENING-PILOT: device B remained writable during device A conflict pause")
            assertEquals(1L, scalarLong(
                generationOneB.connection(),
                "SELECT count(*) FROM notes WHERE id='44444444-4444-4444-8444-444444444444'",
            ))

            targetOpening.resolveConflict(conflict, MergeResult.AcceptServer).getOrThrow()
            println("COORDINATED_OPENING-PILOT: conflict accepted server state")
            val ready = targetOpening.advance()
            println("COORDINATED_OPENING-PILOT: device A generation 2 advance returned ${ready.phase}/${ready.outcome}")
            assertEquals(OversqliteOpeningOutcome.READY, ready.outcome)
            assertEquals("ready", ready.phase)
            assertNotNull(targetOpening.readyOrNull())
            assertEquals(1, migrationSqlCalls.get(), "numbered migration 2 must execute once")
            assertEquals(1, callbackCalls.get(), "existing migration callback must execute once")
            assertEquals(2L, scalarLong(generationTwoA.connection(), "PRAGMA user_version"))
            assertEquals(1L, scalarLong(
                generationTwoA.connection(),
                "SELECT count(*) FROM pragma_table_xinfo('notes') WHERE name='details' AND \"notnull\"=0",
            ))
            assertEquals("server-wins", scalarText(
                generationTwoA.connection(),
                "SELECT title FROM notes WHERE id='11111111-1111-4111-8111-111111111111'",
            ))
            assertEquals(0L, scalarLong(
                generationTwoA.connection(),
                "SELECT count(*) FROM notes WHERE id='22222222-2222-4222-8222-222222222222'",
            ))
            assertEquals(1L, scalarLong(
                generationTwoA.connection(),
                "SELECT count(*) FROM notes WHERE id='33333333-3333-4333-8333-333333333333' AND title='client-insert'",
            ))
            assertEquals(1L, scalarLong(
                generationTwoA.connection(),
                "SELECT count(*) FROM notes WHERE id='44444444-4444-4444-8444-444444444444' AND details IS NULL",
            ))
            assertEquals(1L, scalarLong(generationTwoA.connection(), "SELECT count(*) FROM drafts"))
            assertEquals("000102FF", scalarText(generationTwoA.connection(), "SELECT hex(body) FROM drafts WHERE id='draft-1'"))
        } finally {
            runCatching { targetOpening?.close() }
            runCatching { openingA?.close() }
            runCatching { openingB?.close() }
            httpA.close()
            httpB.close()
            directory.toFile().deleteRecursively()
        }
    }

    private fun pilotDatabase(
        path: String,
        targetVersion: Int,
        migrationSqlCalls: AtomicInteger = AtomicInteger(),
        callbackCalls: AtomicInteger = AtomicInteger(),
    ): SqliteNowDatabase = SqliteNowDatabase(
        dbName = path,
        migration = object : DatabaseMigrations {
            override val targetVersion: Int = targetVersion

            override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
                if (currentVersion == -1) {
                    conn.execSQL(
                        if (targetVersion == 1) {
                            "CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL,title TEXT NOT NULL)"
                        } else {
                            "CREATE TABLE notes(id TEXT PRIMARY KEY NOT NULL,title TEXT NOT NULL,details TEXT NULL)"
                        },
                    )
                    return targetVersion
                }
                if (currentVersion == 1 && targetVersion == 2) {
                    conn.execSQL("ALTER TABLE notes ADD COLUMN details TEXT NULL")
                    migrationSqlCalls.incrementAndGet()
                    callbackCalls.incrementAndGet()
                    return 2
                }
                return currentVersion
            }
        },
        syncManaged = true,
    )

    private fun pilotConfig(generationId: Long): OversqliteConfig = OversqliteConfig(
        generationId = generationId,
        databaseId = "coordinatedopening-live-pilot",
        serviceId = "coordinatedopening-authority",
        syncTables = listOf(SyncTable("notes", syncKeyColumnName = "id")),
        transferTargets = OversqliteTransferTargets(
            maxPushRows = 8,
            maxPullBundles = 8,
            maxSnapshotChunkRows = 8,
        ),
    )

    private suspend fun issueToken(baseUrl: String, userId: String): String {
        val http = HttpClient(CIO)
        return try {
            val body = http.post("$baseUrl/dummy-signin") {
                contentType(ContentType.Application.Json)
                setBody("""{"user":"$userId","password":"anything","device":"jvm-pilot"}""")
            }.bodyAsText()
            Json.parseToJsonElement(body).jsonObject.getValue("token").jsonPrimitive.content
        } finally {
            http.close()
        }
    }

    private fun authorityHttp(baseUrl: String, token: String): HttpClient = HttpClient(CIO) {
        install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
        defaultRequest {
            url(baseUrl)
            header(HttpHeaders.Authorization, "Bearer $token")
        }
    }

    private suspend fun scalarLong(connection: SafeSQLiteConnection, sql: String): Long =
        connection.withPreparedStatement(sql) { statement ->
            check(statement.step()) { "scalar query returned no row" }
            statement.getLong(0)
        }

    private suspend fun scalarText(connection: SafeSQLiteConnection, sql: String): String =
        connection.withPreparedStatement(sql) { statement ->
            check(statement.step()) { "scalar query returned no row" }
            statement.getText(0)
        }
}
