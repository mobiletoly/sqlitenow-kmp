package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonPrimitive
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class SharedBundleChangeWatchFixtureTest {
    private val contractJson = Json {
        ignoreUnknownKeys = true
        explicitNulls = true
    }
    private val fixtureFile = oversqliteContractFixture("watch/basic.json")

    @Test
    fun kmpWatchCapabilityFixturesDecodeAgainstModels() {
        val spec = contractJson.decodeFromString(WatchSpec.serializer(), fixtureFile.readText())
        assertEquals(1, spec.formatVersion)
        for (case in spec.capabilitiesCases) {
            val capabilities = contractJson.decodeFromString(
                CapabilitiesResponse.serializer(),
                case.response.body.toString(),
            )
            assertEquals(
                case.expectedBundleChangeWatchSupported,
                capabilities.bundleChangeWatchSupported,
                case.name,
            )
        }
    }

    @Test
    fun kmpWatchSseFixturesParseAgainstModels() {
        val spec = contractJson.decodeFromString(WatchSpec.serializer(), fixtureFile.readText())
        for (case in spec.sseCases) {
            if (case.expectError) {
                assertFailsWith<IllegalArgumentException>(case.name) {
                    parseBundleChangeEventLines(case.lines, contractJson)
                }
            } else {
                assertEquals(
                    case.expectedEvents,
                    parseBundleChangeEventLines(case.lines, contractJson),
                    case.name,
                )
            }
        }
    }

    @Test
    fun kmpWatchSetupErrorFixturesRemainStructured() {
        val spec = contractJson.decodeFromString(WatchSpec.serializer(), fixtureFile.readText())
        assertEquals(
            listOf(400, 401, 403, 409, 409, 503, 500),
            spec.setupErrorResponses.map { it.status },
        )
        for (response in spec.setupErrorResponses) {
            assertEquals(
                response.expectedError,
                response.body["error"]?.jsonPrimitive?.contentOrNull,
                response.status.toString(),
            )
        }
    }

    @Test
    fun kmpWatchRuntimeFixturesRemainStructured() {
        val spec = contractJson.decodeFromString(WatchSpec.serializer(), fixtureFile.readText())
        assertEquals(
            listOf("non-ok-watch-response-closes-before-fallback"),
            spec.runtimeCases.map { it.name },
        )
        for (case in spec.runtimeCases) {
            assertEquals(1, case.expectedCloseCount, case.name)
            assertEquals(1, case.expectedFallbackPullsAtLeast, case.name)
            assertEquals(
                "bundle_change_watch_disabled",
                case.response.body["error"]?.jsonPrimitive?.contentOrNull,
                case.name,
            )
        }
    }


    @Serializable
    private data class WatchSpec(
        val formatVersion: Int,
        val capabilitiesCases: List<CapabilityCase>,
        val sseCases: List<SseCase>,
        val setupErrorResponses: List<SetupErrorResponse>,
        val runtimeCases: List<RuntimeCase>,
    )

    @Serializable
    private data class CapabilityCase(
        val name: String,
        val response: FixtureHttpResponse,
        val expectedBundleChangeWatchSupported: Boolean,
    )

    @Serializable
    private data class SseCase(
        val name: String,
        val lines: List<String>,
        val expectedEvents: List<BundleChangeEvent> = emptyList(),
        val expectError: Boolean = false,
    )

    @Serializable
    private data class SetupErrorResponse(
        val status: Int,
        val body: JsonObject,
        val expectedError: String,
    )

    @Serializable
    private data class RuntimeCase(
        val name: String,
        val response: FixtureHttpResponse,
        val expectedCloseCount: Int,
        val expectedFallbackPullsAtLeast: Int,
    )

    @Serializable
    private data class FixtureHttpResponse(
        val status: Int,
        val body: JsonObject,
    )
}
