package dev.goquick.sqlitenow.oversqlite

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.header
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import io.ktor.serialization.kotlinx.json.json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class GenerationDiscoveryHttpTest {
    private val descriptor = Json.encodeToString(
        GenerationDiscovery.serializer(),
        GenerationDiscovery("v1", testGeneration(), testWireLimits(), watchEnabled = true),
    )

    @Test
    fun discoveryPreservesBasePathAuthenticationAndExactSourceWhileOmittingProof() = runTest {
        var requests = 0
        val http = HttpClient(MockEngine { request ->
            requests++
            assertEquals("/api/sync/generations/1", request.url.encodedPath)
            assertEquals("Bearer test-token", request.headers[HttpHeaders.Authorization])
            assertEquals("source-a", request.headers["Oversync-Source-ID"])
            assertNull(request.headers["Oversync-Schema-Generation-ID"])
            assertNull(request.headers["Oversync-Schema-Generation-Fingerprint"])
            respond(descriptor, headers = headersOf(HttpHeaders.ContentType, "application/json"))
        }) {
            defaultRequest {
                url("https://sync.test/api/")
                header(HttpHeaders.Authorization, "Bearer test-token")
                header("Oversync-Source-ID", "source-a")
            }
        }
        try {
            val api = OversqliteRemoteApi(http, Json, log = {}, generation = { testGeneration() })
            assertEquals("1", api.fetchGeneration("source-a", 1).generation.id)
            assertFails { api.fetchGeneration("", 1) }
            assertFails { api.fetchGeneration("source-a", 0) }
            assertEquals(1, requests)
        } finally {
            http.close()
        }
    }

    @Test
    fun discoveryRejectsConflictingOrDuplicateEffectiveIdentityHeadersBeforeSending() = runTest {
        val scenarios = listOf(
            "conflicting source" to listOf("Oversync-Source-ID" to "source-b"),
            "duplicate exact source" to listOf(
                "Oversync-Source-ID" to "source-a",
                "Oversync-Source-ID" to "source-a",
            ),
            "generation id on discovery" to listOf("Oversync-Schema-Generation-ID" to "1"),
            "generation fingerprint on discovery" to listOf(
                "Oversync-Schema-Generation-Fingerprint" to testGeneration().fingerprint,
            ),
        )
        for ((name, headers) in scenarios) {
            var requests = 0
            val http = HttpClient(MockEngine {
                requests++
                respond(descriptor, headers = headersOf(HttpHeaders.ContentType, "application/json"))
            }) {
                defaultRequest {
                    url("https://sync.test/")
                    for ((headerName, value) in headers) header(headerName, value)
                }
            }
            try {
                assertFailsWith<OversqliteRequestIdentityHeaderException>(name) {
                    OversqliteRemoteApi(http, Json, log = {}, generation = { testGeneration() })
                        .fetchGeneration("source-a", 1)
                }
                assertEquals(0, requests, name)
            } finally {
                http.close()
            }
        }
    }

    @Test
    fun dataPlaneAcceptsOneExactDefaultIdentityAndRejectsDuplicates() = runTest {
        val generation = testGeneration()
        val connected = Json.encodeToString(ConnectResponse.serializer(), ConnectResponse("initialize_empty"))
        var exactRequests = 0
        val exact = HttpClient(MockEngine {
            exactRequests++
            respond(connected, headers = headersOf(HttpHeaders.ContentType, "application/json"))
        }) {
            defaultRequest {
                url("https://sync.test/")
                header("Oversync-Source-ID", "source-a")
                header("Oversync-Schema-Generation-ID", generation.id)
                header("Oversync-Schema-Generation-Fingerprint", generation.fingerprint)
            }
        }
        try {
            OversqliteRemoteApi(exact, Json, log = {}, generation = { generation })
                .connect("source-a", hasLocalPendingRows = false)
            assertEquals(1, exactRequests)
        } finally {
            exact.close()
        }

        var duplicateRequests = 0
        val duplicate = HttpClient(MockEngine {
            duplicateRequests++
            respond(connected, headers = headersOf(HttpHeaders.ContentType, "application/json"))
        }) {
            defaultRequest {
                url("https://sync.test/")
                header("Oversync-Schema-Generation-ID", generation.id)
                header("Oversync-Schema-Generation-ID", generation.id)
            }
        }
        try {
            assertFailsWith<OversqliteRequestIdentityHeaderException> {
                OversqliteRemoteApi(duplicate, Json, log = {}, generation = { generation })
                    .connect("source-a", hasLocalPendingRows = false)
            }
            assertEquals(0, duplicateRequests)
        } finally {
            duplicate.close()
        }
    }

    @Test
    fun ordinaryJsonAndWatchPathsPreserveStructuredGenerationErrors() = runTest {
        data class Scenario(
            val name: String,
            val status: HttpStatusCode,
            val body: String,
            val invoke: suspend (OversqliteRemoteApi) -> Unit,
        )
        val scenarios = listOf(
            Scenario(
                name = "ordinary JSON",
                status = HttpStatusCode.Conflict,
                body = """{"error":"source_generation_conflict","message":"rotate","retryable":false,"remediation":"rotate_source"}""",
                invoke = { it.connect("source-a", hasLocalPendingRows = false) },
            ),
            Scenario(
                name = "watch",
                status = HttpStatusCode.ServiceUnavailable,
                body = """{"error":"history_projection_capacity_exceeded","message":"retry","retryable":true,"remediation":"retry"}""",
                invoke = { it.watchBundleChanges("source-a", 0) {} },
            ),
        )
        for (scenario in scenarios) {
            val http = HttpClient(MockEngine {
                respond(
                    scenario.body,
                    status = scenario.status,
                    headers = headersOf(HttpHeaders.ContentType, "application/json"),
                )
            }) {
                install(ContentNegotiation) { json(Json) }
                defaultRequest { url("https://sync.test/") }
            }
            try {
                val error = assertFailsWith<OversqliteRemoteContractException>(scenario.name) {
                    scenario.invoke(OversqliteRemoteApi(http, Json, log = {}, generation = { testGeneration() }))
                }
                assertEquals(scenario.status.value, error.httpStatus, scenario.name)
                assertEquals(scenario.name == "watch", error.retryable, scenario.name)
            } finally {
                http.close()
            }
        }
    }

    @Test
    fun discoveryBoundsDecodedResponseAndCanReadTheNextResponse() = runTest {
        var calls = 0
        val responses = listOf(
            descriptor.padEnd(generationControlBodyLimit.toInt(), ' '),
            descriptor.padEnd(generationControlBodyLimit.toInt() + 1, ' '),
            descriptor,
        )
        val http = HttpClient(MockEngine {
            respond(responses[calls++], headers = headersOf(HttpHeaders.ContentType, "application/json"))
        }) { defaultRequest { url("https://sync.test/") } }
        try {
            val api = OversqliteRemoteApi(http, Json, log = {}, generation = { testGeneration() })
            assertEquals("1", api.fetchGeneration("source-a", 1).generation.id)
            assertFails { api.fetchGeneration("source-a", 1) }
            assertEquals("1", api.fetchGeneration("source-a", 1).generation.id)
            assertEquals(3, calls)
        } finally {
            http.close()
        }
    }
}
