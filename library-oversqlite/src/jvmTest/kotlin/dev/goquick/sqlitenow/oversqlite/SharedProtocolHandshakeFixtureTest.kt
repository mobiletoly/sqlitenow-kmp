package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.exists
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class SharedProtocolHandshakeFixtureTest {
    private val contractJson = Json {
        ignoreUnknownKeys = true
        explicitNulls = true
    }
    private val fixtureFile = findRepoRoot().resolve("oversqlite-contracts/protocol-handshake/connect.json")

    @Test
    fun kmpSharedProtocolHandshakeFixturesDecodeAgainstModels() {
        val spec = contractJson.decodeFromString<ProtocolHandshakeSpec>(fixtureFile.readText())
        assertEquals(1, spec.formatVersion)
        for (case in spec.cases) {
            val capabilities = contractJson.decodeFromString(
                CapabilitiesResponse.serializer(),
                case.capabilities.body.toString(),
            )
            assertEquals(
                case.expected.connectLifecycleSupported,
                capabilities.features.getOrElse("connect_lifecycle") { false },
                case.name,
            )

            val connect = case.connect
            if (case.expected.connectCalled) {
                assertNotNull(connect, case.name)
                val requestJson = contractJson.encodeToString(
                    ConnectRequest.serializer(),
                    ConnectRequest(hasLocalPendingRows = case.local.hasLocalPendingRows),
                )
                val request = contractJson.parseToJsonElement(requestJson).jsonObject
                assertEquals(
                    JsonPrimitive(case.local.hasLocalPendingRows),
                    request["has_local_pending_rows"],
                    case.name,
                )

                val response = contractJson.decodeFromString(
                    ConnectResponse.serializer(),
                    connect.body.toString(),
                )
                assertEquals(connect.body["resolution"], JsonPrimitive(response.resolution), case.name)
                when (case.expected.attachKind) {
                    "retryLater" -> assertEquals(case.expected.retryAfterSeconds, response.retryAfterSeconds, case.name)
                    "connected" -> assertEquals(case.expected.pendingInitializationId.orEmpty(), response.initializationId, case.name)
                    "deferredDataTransfer" -> assertEquals("remote_authoritative", response.resolution, case.name)
                }
            } else {
                assertNull(connect, case.name)
                assertEquals("unsupportedCapability", case.expected.attachKind, case.name)
            }
        }
    }

    private fun findRepoRoot(): Path {
        var current = Paths.get("").toAbsolutePath()
        while (true) {
            if (current.resolve("settings.gradle.kts").exists()) {
                return current
            }
            current = current.parent ?: error("could not locate repository root from ${Paths.get("").toAbsolutePath()}")
        }
    }

    @Serializable
    private data class ProtocolHandshakeSpec(
        val formatVersion: Int,
        val cases: List<ProtocolHandshakeCase>,
    )

    @Serializable
    private data class ProtocolHandshakeCase(
        val name: String,
        val capabilities: FixtureHttpResponse,
        val connect: FixtureHttpResponse? = null,
        val local: FixtureLocal,
        val expected: FixtureExpected,
    )

    @Serializable
    private data class FixtureHttpResponse(
        val status: Int,
        val body: JsonObject,
    )

    @Serializable
    private data class FixtureLocal(
        val hasLocalPendingRows: Boolean,
    )

    @Serializable
    private data class FixtureExpected(
        val connectLifecycleSupported: Boolean,
        val connectCalled: Boolean,
        val attachKind: String,
        val attachOutcome: String? = null,
        val retryAfterSeconds: Int? = null,
        val bindingState: String? = null,
        val pendingInitializationId: String? = null,
        val durableOperationKind: String? = null,
    )
}
