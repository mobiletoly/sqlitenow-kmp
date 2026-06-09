package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SharedPullSnapshotFixtureTest {
    private val contractJson = Json {
        ignoreUnknownKeys = true
        explicitNulls = true
    }
    private val fixtureFile = oversqliteContractFixture("pull-snapshot/basic.json")

    @Test
    fun kmpSharedPullSnapshotFixturesDecodeAgainstModels() {
        val spec = contractJson.decodeFromString<PullSnapshotFixtureSpec>(fixtureFile.readText())
        assertEquals(1, spec.formatVersion)
        for (case in spec.cases) {
            case.pullResponse?.let {
                val result = runCatching {
                    val response = contractJson.decodeFromString(
                        PullResponse.serializer(),
                        it.toString(),
                    )
                    validatePullResponse(response, case.afterBundleSeq)
                    response
                }
                assertValidationResult(case.name, case.expectedPullErrorContains, result)
                result.getOrNull()?.let { response ->
                    case.expectedFinalState?.get("lastBundleSeqSeen")?.let { expected ->
                        assertEquals(expected.toString(), response.stableBundleSeq.toString(), case.name)
                    }
                }
            }
            case.historyPrunedResponse?.let {
                val error = contractJson.decodeFromString(
                    ErrorResponse.serializer(),
                    it.body.toString(),
                )
                assertEquals(409, it.status, case.name)
                assertEquals("history_pruned", error.error, case.name)
            }
            case.snapshotSessionCreateRequest?.let {
                val request = contractJson.decodeFromString(
                    SnapshotSessionCreateRequest.serializer(),
                    it.toString(),
                )
                assertEquals("source_retired", request.sourceReplacement?.reason, case.name)
            }
            case.snapshotSession?.let { sessionJson ->
                val sessionResult = runCatching {
                    val session = contractJson.decodeFromString(
                        SnapshotSession.serializer(),
                        sessionJson.toString(),
                    )
                    validateSnapshotSession(session)
                    session
                }
                assertValidationResult(case.name, case.expectedSnapshotSessionErrorContains, sessionResult)
                sessionResult.getOrNull()?.let { session ->
                    case.snapshotChunkResponse?.let { chunkJson ->
                        val chunkResult = runCatching {
                            val chunk = contractJson.decodeFromString(
                                SnapshotChunkResponse.serializer(),
                                chunkJson.toString(),
                            )
                            validateSnapshotChunkResponse(
                                chunk,
                                snapshotId = session.snapshotId,
                                snapshotBundleSeq = session.snapshotBundleSeq,
                                afterRowOrdinal = case.snapshotChunkAfterRowOrdinal,
                            )
                            chunk
                        }
                        assertValidationResult(case.name, case.expectedSnapshotChunkErrorContains, chunkResult)
                    }
                }
            }
            case.sourceReplacementInvalidResponse?.let {
                val error = contractJson.decodeFromString(
                    ErrorResponse.serializer(),
                    it.body.toString(),
                )
                assertEquals(409, it.status, case.name)
                assertEquals("source_replacement_invalid", error.error, case.name)
            }
        }
    }

    private fun assertValidationResult(
        caseName: String,
        expectedMessage: String?,
        result: Result<*>,
    ) {
        if (expectedMessage == null) {
            assertNull(result.exceptionOrNull(), "$caseName: expected validation success")
            return
        }
        val error = assertNotNull(result.exceptionOrNull(), "$caseName: expected validation error")
        assertTrue(
            error.message?.contains(expectedMessage) == true,
            "$caseName: expected validation error containing '$expectedMessage' but was '${error.message}'",
        )
    }

    @Serializable
    private data class PullSnapshotFixtureSpec(
        val formatVersion: Int,
        val cases: List<PullSnapshotFixtureCase>,
    )

    @Serializable
    private data class PullSnapshotFixtureCase(
        val name: String,
        val afterBundleSeq: Long = 0,
        val pullResponse: JsonObject? = null,
        val expectedPullErrorContains: String? = null,
        val historyPrunedResponse: FixtureHttpResponse? = null,
        val snapshotSessionCreateRequest: JsonObject? = null,
        val snapshotSession: JsonObject? = null,
        val snapshotChunkResponse: JsonObject? = null,
        val snapshotChunkAfterRowOrdinal: Long = 0,
        val expectedSnapshotSessionErrorContains: String? = null,
        val expectedSnapshotChunkErrorContains: String? = null,
        val sourceReplacementInvalidResponse: FixtureHttpResponse? = null,
        val expectedFinalState: JsonObject? = null,
    )

    @Serializable
    private data class FixtureHttpResponse(
        val status: Int,
        val body: JsonObject,
    )
}
