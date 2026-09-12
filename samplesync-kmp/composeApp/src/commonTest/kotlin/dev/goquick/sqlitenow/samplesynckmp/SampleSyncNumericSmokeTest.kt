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
        val user = "numeric_smoke-$platform-${kotlin.random.Random.nextInt(1_000_000)}"
        val expectedSsn = 9_007_199_254_740_993L
        val expectedScore = 6.57111473696007

        val seedDb = newSampleSyncDatabase(":memory:")
        val restoreDb = newSampleSyncDatabase(":memory:")
        var seedHttp: HttpClient? = null
        var restoreHttp: HttpClient? = null
        var seedClient: OversqliteClient? = null
        var restoreClient: OversqliteClient? = null
        try {
            val seedAuth = MutableTestAuth()
            val restoreAuth = MutableTestAuth()
            seedHttp = testHttp(baseUrl, seedAuth)
            restoreHttp = testHttp(baseUrl, restoreAuth)
            val seedOpening = seedDb.openForSync(
                config = seedDb.buildOversqliteConfig(1L, "sample-sync", sampleSyncServerAppName),
                httpClient = seedHttp,
            )
            val restoreOpening = restoreDb.openForSync(
                config = restoreDb.buildOversqliteConfig(1L, "sample-sync", sampleSyncServerAppName),
                httpClient = restoreHttp,
            )
            seedOpening.advance()
            restoreOpening.advance()
            seedClient = seedDb.newOversqliteClient(generationId = 1L, databaseId = "sample-sync", serviceId = sampleSyncServerAppName, httpClient = seedHttp)
            restoreClient = restoreDb.newOversqliteClient(generationId = 1L, databaseId = "sample-sync", serviceId = sampleSyncServerAppName, httpClient = restoreHttp)
            authenticate(seedHttp, seedAuth, user, seedClient.sourceInfo().getOrThrow().currentSourceId)
            authenticate(restoreHttp, restoreAuth, user, restoreClient.sourceInfo().getOrThrow().currentSourceId)

            assertEquals(AttachOutcome.STARTED_EMPTY, (seedClient.attach(user).getOrThrow() as AttachResult.Connected).outcome)
            seedDb.person.add(
                PersonQuery.Add.Params(
                    firstName = "NumericSmoke",
                    lastName = platform,
                    email = "numeric_smoke-$platform-${kotlin.random.Random.nextInt(1_000_000)}@example.com",
                    phone = "555-0404",
                    birthDate = null,
                    ssn = expectedSsn,
                    score = expectedScore,
                    isActive = true,
                    notes = "deterministic numeric smoke",
                ),
            )
            assertEquals(PushOutcome.COMMITTED, seedClient.pushPending().getOrThrow().outcome)

            val attach = (restoreClient.attach(user).getOrThrow() as AttachResult.Connected).outcome
            assertTrue(attach == AttachOutcome.USED_REMOTE_STATE || attach == AttachOutcome.STARTED_EMPTY)
            restoreClient.rebuild().getOrThrow()
            val rows = restoreDb.person.selectAll(PersonQuery.SelectAll.Params(limit = 10, offset = 0)).asList()
            val person = rows.single { it.myFirstName == "NumericSmoke" && it.myLastName == platform }
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

    private class MutableTestAuth(var token: String? = null, var sourceId: String? = null)

    private fun testHttp(baseUrl: String, auth: MutableTestAuth): HttpClient {
        val jsonFormat = Json { ignoreUnknownKeys = true }
        return HttpClient {
            install(ContentNegotiation) { json(jsonFormat) }
            defaultRequest {
                url(baseUrl)
                auth.token?.let { header(HttpHeaders.Authorization, "Bearer $it") }
                auth.sourceId?.let { header("Oversync-Source-ID", it) }
            }
        }
    }

    private suspend fun authenticate(http: HttpClient, auth: MutableTestAuth, user: String, sourceId: String) {
        auth.sourceId = sourceId
        auth.token = http.post("/dummy-signin") {
            contentType(ContentType.Application.Json)
            setBody(SigninRequest(user, "numeric_smoke"))
        }.body<SigninResponse>().token
    }

    @Serializable private data class SigninRequest(val user: String, val password: String)
    @Serializable private data class SigninResponse(val token: String)
}
