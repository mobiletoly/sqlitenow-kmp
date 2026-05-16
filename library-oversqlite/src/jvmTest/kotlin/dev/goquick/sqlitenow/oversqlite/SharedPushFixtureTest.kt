package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals

class SharedPushFixtureTest {
    private val contractJson = Json {
        ignoreUnknownKeys = true
        explicitNulls = true
    }
    private val fixtureFile = oversqliteContractFixture("push/basic-insert.json")

    @Test
    fun kmpSharedPushFixturesDecodeAgainstModels() {
        val spec = contractJson.decodeFromString<PushFixtureSpec>(fixtureFile.readText())
        assertEquals(1, spec.formatVersion)
        for (case in spec.cases) {
            case.pushSessionCreateRequest?.let {
                val request = contractJson.decodeFromString(
                    PushSessionCreateRequest.serializer(),
                    it.toString(),
                )
                assertEquals(1, request.sourceBundleId, case.name)
            }
            case.pushSessionCreateResponse?.let {
                val response = contractJson.decodeFromString(
                    PushSessionCreateResponse.serializer(),
                    it.toString(),
                )
                assertEquals("staging", response.status, case.name)
            }
            case.pushChunkRequest?.let {
                val request = contractJson.decodeFromString(
                    PushSessionChunkRequest.serializer(),
                    it.toString(),
                )
                assertEquals(1, request.rows.size, case.name)
                assertEquals("INSERT", request.rows.single().op, case.name)
            }
            case.pushChunkResponse?.let {
                val response = contractJson.decodeFromString(
                    PushSessionChunkResponse.serializer(),
                    it.toString(),
                )
                assertEquals(1L, response.nextExpectedRowOrdinal, case.name)
            }
            case.pushCommitResponse?.let {
                val response = contractJson.decodeFromString(
                    PushSessionCommitResponse.serializer(),
                    it.toString(),
                )
                assertEquals(1L, response.bundleSeq, case.name)
            }
            case.committedBundleRowsResponse?.let {
                val response = contractJson.decodeFromString(
                    CommittedBundleRowsResponse.serializer(),
                    it.toString(),
                )
                assertEquals(1, response.rows.size, case.name)
                assertEquals(1L, response.rows.single().rowVersion, case.name)
            }
            case.pushConflictResponse?.let {
                val response = contractJson.decodeFromString(
                    PushConflictResponse.serializer(),
                    it.toString(),
                )
                assertEquals("push_conflict", response.error, case.name)
                assertEquals(2L, response.conflict?.serverRowVersion, case.name)
            }
        }
    }

    @Serializable
    private data class PushFixtureSpec(
        val formatVersion: Int,
        val cases: List<PushFixtureCase>,
    )

    @Serializable
    private data class PushFixtureCase(
        val name: String,
        val pushSessionCreateRequest: JsonObject? = null,
        val pushSessionCreateResponse: JsonObject? = null,
        val pushChunkRequest: JsonObject? = null,
        val pushChunkResponse: JsonObject? = null,
        val pushCommitResponse: JsonObject? = null,
        val committedBundleRowsResponse: JsonObject? = null,
        val pushConflictResponse: JsonObject? = null,
        val expectedFinalState: JsonObject,
    )
}
