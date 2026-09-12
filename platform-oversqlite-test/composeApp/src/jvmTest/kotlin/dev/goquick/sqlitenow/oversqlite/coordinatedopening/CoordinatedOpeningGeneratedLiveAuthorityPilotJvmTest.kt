package dev.goquick.sqlitenow.oversqlite.coordinatedopening

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.oversqlite.AttachResult
import dev.goquick.sqlitenow.oversqlite.MergeResult
import dev.goquick.sqlitenow.oversqlite.OversqliteOpening
import dev.goquick.sqlitenow.oversqlite.OversqliteOpeningOutcome
import dev.goquick.sqlitenow.oversqlite.OversqliteTransferTargets
import dev.goquick.sqlitenow.oversqlite.OversqliteUpgradePlan
import dev.goquick.sqlitenow.oversqlite.coordinatedopening.generationone.NoteQuery as GenerationOneNoteQuery
import dev.goquick.sqlitenow.oversqlite.coordinatedopening.generationone.CoordinatedOpeningGenerationOneDatabase
import dev.goquick.sqlitenow.oversqlite.coordinatedopening.generationone.VersionBasedDatabaseMigrations as GenerationOneMigrations
import dev.goquick.sqlitenow.oversqlite.coordinatedopening.generationtwo.NoteQuery as GenerationTwoNoteQuery
import dev.goquick.sqlitenow.oversqlite.coordinatedopening.generationtwo.CoordinatedOpeningGenerationTwoDatabase
import dev.goquick.sqlitenow.oversqlite.coordinatedopening.generationtwo.VersionBasedDatabaseMigrations as GenerationTwoMigrations
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
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

class CoordinatedOpeningGeneratedLiveAuthorityPilotJvmTest {
    @Test
    fun generatedTwoDeviceGenerationOneToTwoPilotUsesLiveAuthority() = runBlocking {
        val baseUrl = System.getenv("COORDINATED_OPENING_AUTHORITY_BASE_URL")?.trim().orEmpty()
        if (baseUrl.isEmpty()) {
            println("Skipping coordinated opening generated live-authority pilot; set COORDINATED_OPENING_AUTHORITY_BASE_URL")
            return@runBlocking
        }

        val userId = "coordinatedopening-generated-kotlin-${System.nanoTime()}"
        val token = issueToken(baseUrl, userId)
        val httpA = authorityHttp(baseUrl, token)
        val httpB = authorityHttp(baseUrl, token)
        val directory = Files.createTempDirectory("sqlitenow-coordinatedopening-generated-pilot")
        val pathA = directory.resolve("device-a.sqlite3").toString()
        val pathB = directory.resolve("device-b.sqlite3").toString()
        val generationOneA = CoordinatedOpeningGenerationOneDatabase(pathA, GenerationOneMigrations())
        val generationOneB = CoordinatedOpeningGenerationOneDatabase(pathB, GenerationOneMigrations())
        var openingA: OversqliteOpening<CoordinatedOpeningGenerationOneDatabase>? = null
        var openingB: OversqliteOpening<CoordinatedOpeningGenerationOneDatabase>? = null
        var targetOpening: OversqliteOpening<CoordinatedOpeningGenerationTwoDatabase>? = null

        try {
            val configA = generationOneA.buildOversqliteConfig(
                generationId = 1,
                databaseId = DATABASE_ID,
                serviceId = SERVICE_ID,
                transferTargets = OversqliteTransferTargets(
                    maxPushRows = 8,
                    maxPullBundles = 8,
                    maxSnapshotChunkRows = 8,
                ),
            )
            openingA = generationOneA.openForSync(configA, httpA)
            assertNull(openingA.readyOrNull())
            assertFalse(generationOneA.isOpen())
            assertEquals(OversqliteOpeningOutcome.READY, openingA.advance().outcome)
            val clientA = generationOneA.newOversqliteClient(
                generationId = 1,
                databaseId = DATABASE_ID,
                serviceId = SERVICE_ID,
                httpClient = httpA,
                transferTargets = OversqliteTransferTargets(
                    maxPushRows = 8,
                    maxPullBundles = 8,
                    maxSnapshotChunkRows = 8,
                ),
            )
            assertTrue(clientA.attach(userId).getOrThrow() is AttachResult.Connected)
            println("COORDINATED_OPENING-PILOT: generated Kotlin device A generation 1 ready")

            generationOneA.connection().execSQL(
                "CREATE TABLE drafts(id TEXT PRIMARY KEY NOT NULL, body BLOB NOT NULL)",
            )
            generationOneA.connection().execSQL(
                "INSERT INTO drafts(id,body) VALUES('draft-1',x'000102FF')",
            )
            generationOneA.note.insert(
                GenerationOneNoteQuery.Insert.Params(
                    id = CONFLICT_ID,
                    title = "base-conflict",
                ),
            )
            generationOneA.note.insert(
                GenerationOneNoteQuery.Insert.Params(
                    id = DELETE_ID,
                    title = "base-delete",
                ),
            )
            clientA.sync().getOrThrow()
            println("COORDINATED_OPENING-PILOT: generated Kotlin device A seed synchronized")

            val configB = generationOneB.buildOversqliteConfig(
                generationId = 1,
                databaseId = DATABASE_ID,
                serviceId = SERVICE_ID,
                transferTargets = OversqliteTransferTargets(
                    maxPushRows = 8,
                    maxPullBundles = 8,
                    maxSnapshotChunkRows = 8,
                ),
            )
            openingB = generationOneB.openForSync(configB, httpB)
            assertEquals(OversqliteOpeningOutcome.READY, openingB.advance().outcome)
            val clientB = generationOneB.newOversqliteClient(
                generationId = 1,
                databaseId = DATABASE_ID,
                serviceId = SERVICE_ID,
                httpClient = httpB,
                transferTargets = OversqliteTransferTargets(
                    maxPushRows = 8,
                    maxPullBundles = 8,
                    maxSnapshotChunkRows = 8,
                ),
            )
            assertTrue(clientB.attach(userId).getOrThrow() is AttachResult.Connected)
            assertEquals(2, generationOneB.note.selectAll.asList().size)
            println("COORDINATED_OPENING-PILOT: generated Kotlin device B generation 1 restored")

            generationOneB.note.updateTitle(
                GenerationOneNoteQuery.UpdateTitle.Params(
                    title = "server-wins",
                    id = CONFLICT_ID,
                ),
            )
            clientB.sync().getOrThrow()
            println("COORDINATED_OPENING-PILOT: generated Kotlin device B conflicting update synchronized")

            generationOneA.note.updateTitle(
                GenerationOneNoteQuery.UpdateTitle.Params(
                    title = "client-conflict",
                    id = CONFLICT_ID,
                ),
            )
            generationOneA.note.deleteById(GenerationOneNoteQuery.DeleteById.Params(DELETE_ID))
            generationOneA.note.insert(
                GenerationOneNoteQuery.Insert.Params(
                    id = INSERT_ID,
                    title = "client-insert",
                ),
            )
            openingA.close()
            openingA = null

            val callbackCalls = AtomicInteger()
            val generationTwoA = CoordinatedOpeningGenerationTwoDatabase(
                pathA,
                GenerationTwoMigrations { callbackCalls.incrementAndGet() },
            )
            val targetConfig = generationTwoA.buildOversqliteConfig(
                generationId = 2,
                databaseId = DATABASE_ID,
                serviceId = SERVICE_ID,
                transferTargets = OversqliteTransferTargets(
                    maxPushRows = 8,
                    maxPullBundles = 8,
                    maxSnapshotChunkRows = 8,
                ),
            )
            targetOpening = generationTwoA.openForSync(
                config = targetConfig,
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
            assertNull(targetOpening.readyOrNull())
            assertFalse(generationTwoA.isOpen())

            val paused = targetOpening.advance()
            println("COORDINATED_OPENING-PILOT: generated Kotlin target returned ${paused.phase}/${paused.outcome}")
            assertEquals(OversqliteOpeningOutcome.NEEDS_RESOLUTION, paused.outcome)
            val conflict = assertNotNull(targetOpening.pendingConflict().getOrThrow())
            assertEquals("notes", conflict.conflict.table)

            generationOneB.note.insert(
                GenerationOneNoteQuery.Insert.Params(
                    id = OLD_DEVICE_ID,
                    title = "old-device-live",
                ),
            )
            clientB.sync().getOrThrow()
            assertTrue(generationOneB.note.selectAll.asList().any { it.id == OLD_DEVICE_ID })
            println("COORDINATED_OPENING-PILOT: generated Kotlin old device remained writable during target pause")

            targetOpening.resolveConflict(conflict, MergeResult.AcceptServer).getOrThrow()
            val ready = targetOpening.advance()
            assertEquals(OversqliteOpeningOutcome.READY, ready.outcome)
            assertEquals("ready", ready.phase)
            val targetClient = generationTwoA.newOversqliteClient(
                generationId = 2,
                databaseId = DATABASE_ID,
                serviceId = SERVICE_ID,
                httpClient = httpA,
                transferTargets = OversqliteTransferTargets(
                    maxPushRows = 8,
                    maxPullBundles = 8,
                    maxSnapshotChunkRows = 8,
                ),
            )
            assertNotNull(targetClient)
            assertEquals(1, callbackCalls.get(), "generated migration callback must execute once")
            assertEquals(2L, scalarLong(generationTwoA.connection(), "PRAGMA user_version"))
            assertEquals(
                1L,
                scalarLong(
                    generationTwoA.connection(),
                    "SELECT count(*) FROM pragma_table_xinfo('notes') WHERE name='details' AND \"notnull\"=0",
                ),
            )
            val rows = generationTwoA.note.selectAll.asList().associateBy { it.id }
            assertEquals("server-wins", rows.getValue(CONFLICT_ID).title)
            assertFalse(DELETE_ID in rows)
            assertEquals("client-insert", rows.getValue(INSERT_ID).title)
            assertNull(rows.getValue(OLD_DEVICE_ID).details)
            assertEquals(1L, scalarLong(generationTwoA.connection(), "SELECT count(*) FROM drafts"))
            assertEquals(
                "000102FF",
                scalarText(generationTwoA.connection(), "SELECT hex(body) FROM drafts WHERE id='draft-1'"),
            )
            println("COORDINATED_OPENING-PILOT: generated Kotlin generation 2 ready with exact preservation")
        } finally {
            runCatching { targetOpening?.close() }
            runCatching { openingA?.close() }
            runCatching { openingB?.close() }
            httpA.close()
            httpB.close()
            directory.toFile().deleteRecursively()
        }
    }

    private suspend fun issueToken(baseUrl: String, userId: String): String {
        val http = HttpClient(CIO)
        return try {
            val body = http.post("$baseUrl/dummy-signin") {
                contentType(ContentType.Application.Json)
                setBody("""{"user":"$userId","password":"anything","device":"generated-kotlin-pilot"}""")
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
        connection.withExclusiveAccess {
            connection.prepare(sql).use { statement ->
                check(statement.step()) { "scalar query returned no row" }
                statement.getLong(0)
            }
        }

    private suspend fun scalarText(connection: SafeSQLiteConnection, sql: String): String =
        connection.withExclusiveAccess {
            connection.prepare(sql).use { statement ->
                check(statement.step()) { "scalar query returned no row" }
                statement.getText(0)
            }
        }

    private companion object {
        const val DATABASE_ID = "coordinatedopening-live-pilot"
        const val SERVICE_ID = "coordinatedopening-authority"
        const val CONFLICT_ID = "11111111-1111-4111-8111-111111111111"
        const val DELETE_ID = "22222222-2222-4222-8222-222222222222"
        const val INSERT_ID = "33333333-3333-4333-8333-333333333333"
        const val OLD_DEVICE_ID = "44444444-4444-4444-8444-444444444444"
    }
}
