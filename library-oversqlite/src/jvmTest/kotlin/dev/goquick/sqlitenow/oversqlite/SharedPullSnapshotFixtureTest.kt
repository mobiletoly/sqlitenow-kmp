package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.exists
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals

class SharedPullSnapshotFixtureTest {
    private val contractJson = Json {
        ignoreUnknownKeys = true
        explicitNulls = true
    }
    private val fixtureFile = findRepoRoot().resolve("oversqlite-contracts/pull-snapshot/basic.json")

    @Test
    fun kmpSharedPullSnapshotFixturesDecodeAgainstModels() {
        val spec = contractJson.decodeFromString<PullSnapshotFixtureSpec>(fixtureFile.readText())
        assertEquals(1, spec.formatVersion)
        for (case in spec.cases) {
            case.pullResponse?.let {
                val response = contractJson.decodeFromString(
                    PullResponse.serializer(),
                    it.toString(),
                )
                validatePullResponse(response, case.afterBundleSeq)
                assertEquals(
                    case.expectedFinalState["lastBundleSeqSeen"].toString(),
                    response.stableBundleSeq.toString(),
                    case.name,
                )
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
                val session = contractJson.decodeFromString(
                    SnapshotSession.serializer(),
                    sessionJson.toString(),
                )
                validateSnapshotSession(session)
                case.snapshotChunkResponse?.let { chunkJson ->
                    val chunk = contractJson.decodeFromString(
                        SnapshotChunkResponse.serializer(),
                        chunkJson.toString(),
                    )
                    validateSnapshotChunkResponse(
                        chunk,
                        snapshotId = session.snapshotId,
                        snapshotBundleSeq = session.snapshotBundleSeq,
                        afterRowOrdinal = 0,
                    )
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

    private fun findRepoRoot(): Path {
        var current = Paths.get("").toAbsolutePath()
        while (true) {
            if (current.resolve("settings.gradle.kts").exists()) {
                return current
            }
            current = current.parent
                ?: error("could not locate repository root from ${Paths.get("").toAbsolutePath()}")
        }
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
        val historyPrunedResponse: FixtureHttpResponse? = null,
        val snapshotSessionCreateRequest: JsonObject? = null,
        val snapshotSession: JsonObject? = null,
        val snapshotChunkResponse: JsonObject? = null,
        val sourceReplacementInvalidResponse: FixtureHttpResponse? = null,
        val expectedFinalState: JsonObject,
    )

    @Serializable
    private data class FixtureHttpResponse(
        val status: Int,
        val body: JsonObject,
    )
}
