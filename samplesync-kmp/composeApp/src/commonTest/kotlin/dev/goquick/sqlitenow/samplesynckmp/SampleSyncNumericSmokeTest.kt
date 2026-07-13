package dev.goquick.sqlitenow.samplesynckmp

import dev.goquick.sqlitenow.oversqlite.AttachOutcome
import dev.goquick.sqlitenow.oversqlite.AttachResult
import dev.goquick.sqlitenow.oversqlite.OversqliteClient
import dev.goquick.sqlitenow.oversqlite.PushOutcome
import dev.goquick.sqlitenow.samplesynckmp.db.NowSampleSyncDatabase
import dev.goquick.sqlitenow.samplesynckmp.db.PersonQuery
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.HttpHeaders
import io.ktor.http.ContentType
import io.ktor.http.contentType
import io.ktor.serialization.kotlinx.json.json
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlin.test.assertEquals
import kotlin.test.assertTrue

internal object SampleSyncNumericSmokeSupport {
    suspend fun run(baseUrl: String, platform: String) {
        val user = "phase4a-$platform-${kotlin.random.Random.nextInt(1_000_000)}"
        val expectedSsn = 9_007_199_254_740_993L
        val expectedScore = 6.57111473696007

        val seedDb = newSampleSyncDatabase(":memory:")
        val restoreDb = newSampleSyncDatabase(":memory:")
        var seedHttp: HttpClient? = null
        var restoreHttp: HttpClient? = null
        var seedClient: OversqliteClient? = null
        var restoreClient: OversqliteClient? = null
        try {
            seedDb.open()
            restoreDb.open()
            val seedSource = bootstrapSource(seedDb, baseUrl)
            val restoreSource = bootstrapSource(restoreDb, baseUrl)
            seedHttp = authenticatedHttp(baseUrl, user, seedSource)
            restoreHttp = authenticatedHttp(baseUrl, user, restoreSource)
            seedClient = seedDb.newOversqliteClient(schema = "business", httpClient = seedHttp)
            restoreClient = restoreDb.newOversqliteClient(schema = "business", httpClient = restoreHttp)

            seedClient.open().getOrThrow()
            assertEquals(AttachOutcome.STARTED_EMPTY, (seedClient.attach(user).getOrThrow() as AttachResult.Connected).outcome)
            seedDb.person.add(
                PersonQuery.Add.Params(
                    firstName = "Phase4A",
                    lastName = platform,
                    email = "phase4a-$platform-${kotlin.random.Random.nextInt(1_000_000)}@example.com",
                    phone = "555-0404",
                    birthDate = null,
                    ssn = expectedSsn,
                    score = expectedScore,
                    isActive = true,
                    notes = "deterministic numeric smoke",
                ),
            )
            assertEquals(PushOutcome.COMMITTED, seedClient.pushPending().getOrThrow().outcome)

            restoreClient.open().getOrThrow()
            val attach = (restoreClient.attach(user).getOrThrow() as AttachResult.Connected).outcome
            assertTrue(attach == AttachOutcome.USED_REMOTE_STATE || attach == AttachOutcome.STARTED_EMPTY)
            restoreClient.rebuild().getOrThrow()
            val rows = restoreDb.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            val person = rows.single { it.myFirstName == "Phase4A" && it.myLastName == platform }
            assertEquals(expectedSsn, person.ssn)
            assertEquals(expectedScore, person.score)
            assertTrue(person.isActive)
        } finally {
            seedClient?.close()
            restoreClient?.close()
            seedHttp?.close()
            restoreHttp?.close()
            seedDb.close()
            restoreDb.close()
        }
    }

    private suspend fun bootstrapSource(database: NowSampleSyncDatabase, baseUrl: String): String {
        val http = HttpClient { defaultRequest { url(baseUrl) } }
        val client = database.newOversqliteClient(schema = "business", httpClient = http)
        return try {
            client.open().getOrThrow()
            client.sourceInfo().getOrThrow().currentSourceId
        } finally {
            client.close()
            http.close()
        }
    }

    private suspend fun authenticatedHttp(baseUrl: String, user: String, sourceId: String): HttpClient {
        val jsonFormat = Json { ignoreUnknownKeys = true }
        val signin = HttpClient {
            install(ContentNegotiation) { json(jsonFormat) }
            defaultRequest { url(baseUrl) }
        }
        val token = try {
            signin.post("/dummy-signin") {
                contentType(ContentType.Application.Json)
                setBody(SigninRequest(user, "phase4a"))
            }.body<SigninResponse>().token
        } finally {
            signin.close()
        }
        return HttpClient {
            install(ContentNegotiation) { json(jsonFormat) }
            defaultRequest {
                url(baseUrl)
                header(HttpHeaders.Authorization, "Bearer $token")
                header("Oversync-Source-ID", sourceId)
            }
        }
    }

    @Serializable private data class SigninRequest(val user: String, val password: String)
    @Serializable private data class SigninResponse(val token: String)
}
