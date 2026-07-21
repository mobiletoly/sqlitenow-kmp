package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class SharedProtocolHandshakeFixtureTest {
    private val contractJson = Json {
        ignoreUnknownKeys = true
        explicitNulls = true
    }
    private val fixtureFile = oversqliteContractFixture("protocol-handshake/connect.json")

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

    @Test
    fun kmpSharedProtocolHandshakeTableContractCases() {
        val root = contractJson.parseToJsonElement(fixtureFile.readText()).jsonObject
        for (case in root.getValue("contractCases").jsonArray.map(JsonElement::jsonObject)) {
            val name = case.getValue("name").jsonPrimitive.content
            val localSpecs = contractJson.decodeFromJsonElement(
                ListSerializer(RegisteredTableSpec.serializer()),
                case.getValue("localSpecs"),
            )
            val base = contractJson.encodeToJsonElement(
                CapabilitiesResponse.serializer(),
                CapabilitiesResponse(
                    protocolVersion = "v1",
                    schemaVersion = 1,
                    registeredTableSpecs = emptyList(),
                    features = mapOf("connect_lifecycle" to true),
                    bundleLimits = testBundleCapabilitiesLimits(),
                ),
            ).jsonObject
            val capabilitiesJson = if (case.containsKey("advertised")) {
                JsonObject(base + ("registered_table_specs" to case.getValue("advertised")))
            } else {
                JsonObject(base - "registered_table_specs")
            }
            val expected = case.getValue("expected").jsonObject
            val kind = expected.getValue("kind").jsonPrimitive.content

            val decoded = runCatching {
                contractJson.decodeFromJsonElement(
                    CapabilitiesResponse.serializer(),
                    capabilitiesJson,
                )
            }
            if (kind == "invalid" && (case["advertised"] == null || case["advertised"] is JsonNull)) {
                assertFails(name) { decoded.getOrThrow() }
                continue
            }
            val capabilities = decoded.getOrThrow()
            val validated = ValidatedConfig(
                schema = localSpecs.first().schema,
                tables = localSpecs.map { spec ->
                    ValidatedSyncTable(spec.table, spec.syncKeyColumns.single())
                },
                pkByTable = emptyMap(),
                keyByTable = emptyMap(),
                tableOrder = emptyMap(),
                tableInfoByName = emptyMap(),
            )
            when (kind) {
                "match" -> capabilities.requireMatchingSyncTableContract(validated)
                "invalid" -> assertFailsWith<RemoteResponseSemanticException>(name) {
                    capabilities.requireMatchingSyncTableContract(validated)
                }
                "mismatch" -> {
                    val mismatch = assertFailsWith<SyncTableContractMismatchException>(name) {
                        capabilities.requireMatchingSyncTableContract(validated)
                    }
                    assertEquals(expected.stringList("serverOnlyTables"), mismatch.serverOnlyTables, name)
                    assertEquals(expected.stringList("clientOnlyTables"), mismatch.clientOnlyTables, name)
                    val expectedKeys = expected["syncKeyMismatches"]?.jsonArray.orEmpty()
                    assertEquals(expectedKeys.size, mismatch.syncKeyMismatches.size, name)
                    expectedKeys.zip(mismatch.syncKeyMismatches).forEach { (raw, actual) ->
                        val expectedKey = raw.jsonObject
                        assertEquals(
                            expectedKey.getValue("qualifiedTable").jsonPrimitive.content,
                            actual.qualifiedTable,
                            name,
                        )
                        assertEquals(expectedKey.stringList("clientSyncKeyColumns"), actual.clientSyncKeyColumns, name)
                        assertEquals(expectedKey.stringList("serverSyncKeyColumns"), actual.serverSyncKeyColumns, name)
                    }
                }
                else -> error("unknown table contract fixture kind $kind")
            }
        }
    }

    private fun JsonObject.stringList(name: String): List<String> =
        get(name)?.jsonArray?.map { it.jsonPrimitive.content }.orEmpty()

    @Serializable
    private data class ProtocolHandshakeSpec(
        val formatVersion: Int,
        val contractCases: List<JsonObject> = emptyList(),
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
