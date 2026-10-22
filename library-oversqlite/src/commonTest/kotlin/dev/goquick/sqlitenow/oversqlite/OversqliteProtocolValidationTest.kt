package dev.goquick.sqlitenow.oversqlite

import io.ktor.http.HttpStatusCode
import kotlinx.serialization.json.*
import kotlinx.serialization.decodeFromString
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

class OversqliteProtocolValidationTest {
    private val canonicalToken = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

    @Test
    fun committedBundleRejectsOlderAndFutureRowVersionsForEveryOperation() {
        for (operation in listOf("INSERT", "UPDATE", "DELETE")) {
            for (version in listOf(1L, 3L)) {
                val row = BundleRow(
                    "main", "users", mapOf("id" to "P"), operation, version,
                    if (operation == "DELETE") null else Json.parseToJsonElement("""{"id":"P","name":"parent"}"""),
                )
                val bundle = Bundle(2, "peer", 1, canonicalCommittedBundleHash(listOf(row)), listOf(row))
                val error = assertFailsWith<IllegalArgumentException>("$operation row-version $version in bundle2") {
                    validateBundle(bundle)
                }
                assertEquals("invalid bundle row 0: bundle row row_version $version does not match bundle_seq 2", error.message)
            }
        }
    }

    @Test
    fun committedBundleAcceptsMatchingVersionsForEveryOperation() {
        val rows = listOf("INSERT", "UPDATE", "DELETE").mapIndexed { index, operation ->
            BundleRow(
                "main", "users", mapOf("id" to index.toString()), operation, 2,
                if (operation == "DELETE") null else Json.parseToJsonElement("""{"id":"$index","name":"parent"}"""),
            )
        }
        validateBundle(Bundle(2, "peer", 1, canonicalCommittedBundleHash(rows), rows))
    }

    @Test
    fun conflictDecoderDistinguishesMissingFromExplicitNullServerRow() {
        val missing = """{"error":"push_conflict","message":"x","conflict":{"schema":"main","table":"users","key":{"id":"1"},"op":"DELETE","base_row_version":1,"server_row_version":2,"server_row_deleted":true}}"""
        assertFailsWith<IllegalArgumentException> {
            decodeHistoricalPushConflictFixtureOrNull(HttpStatusCode.Conflict, missing)
        }
        val explicitNull = """{"error":"push_conflict","message":"x","conflict":{"schema":"main","table":"users","key":{"id":"1"},"op":"DELETE","base_row_version":1,"server_row_version":2,"server_row_deleted":true,"server_row":null}}"""
        assertEquals(2L, decodeHistoricalPushConflictFixtureOrNull(HttpStatusCode.Conflict, explicitNull)?.conflict?.serverRowVersion)
    }

    @Test
    fun sessionToken_acceptsOnlyCanonicalLowercaseDashedUuid() {
        assertEquals(canonicalToken, requireValidOversqliteSessionToken(canonicalToken))

        val invalid = listOf(
            "",
            " $canonicalToken",
            "$canonicalToken ",
            "11111111-1111-4111-8111-11111111111A",
            "11111111111141118111111111111111",
            "{$canonicalToken}",
            "$canonicalToken\n",
            "１１１１１１１１-１１１１-４１１１-８１１１-１１１１１１１１１１１１",
            "not-a-uuid",
        )
        invalid.forEach { raw ->
            val error = assertFailsWith<IllegalArgumentException> {
                requireValidOversqliteSessionToken(raw)
            }
            if (raw.isNotEmpty()) {
                assertFalse(error.message.orEmpty().contains(raw))
            }
        }
    }

    @Test
    fun optionalSessionToken_allowsOnlyEmptyOrCanonical() {
        assertEquals("", requireValidOptionalOversqliteSessionToken(""))
        assertEquals(canonicalToken, requireValidOptionalOversqliteSessionToken(canonicalToken))
        assertFailsWith<IllegalArgumentException> { requireValidOptionalOversqliteSessionToken(" ") }
        assertFailsWith<IllegalArgumentException> {
            requireValidOptionalOversqliteSessionToken("{$canonicalToken}")
        }
    }

    @Test
    fun connectResponse_requiresCanonicalInitializationLeaseOnlyForInitializeLocal() {
        validateConnectResponse(
            ConnectResponse(
                resolution = "initialize_local",
                initializationId = canonicalToken,
                leaseExpiresAt = "2099-01-01T00:00:00Z",
            ),
        )
        assertFailsWith<IllegalArgumentException> {
            validateConnectResponse(
                ConnectResponse(resolution = "initialize_local", initializationId = " $canonicalToken "),
            )
        }
        assertFailsWith<IllegalArgumentException> {
            validateConnectResponse(
                ConnectResponse(resolution = "initialize_empty", initializationId = canonicalToken),
            )
        }
    }

    @Test
    fun connectResponseWire_requiresStrictFieldsForEveryResolution() {
        val valid = listOf(
            """{"resolution":"remote_authoritative"}""",
            """{"resolution":"initialize_empty"}""",
            """{"resolution":"initialize_local","initialization_id":"$canonicalToken","lease_expires_at":"2099-01-01T00:00:00Z"}""",
            """{"resolution":"retry_later","lease_expires_at":"2099-01-01T00:00:00Z","retry_after_seconds":3}""",
        )
        valid.forEach(::validateConnectResponseWire)

        val invalid = listOf(
            "{}",
            """{"resolution":"unknown"}""",
            """{"resolution":"remote_authoritative","retry_after_seconds":1}""",
            """{"resolution":"initialize_empty","initialization_id":"$canonicalToken"}""",
            """{"resolution":"initialize_local","initialization_id":"$canonicalToken"}""",
            """{"resolution":"initialize_local","initialization_id":"$canonicalToken","lease_expires_at":"2099-01-01T00:00:00Z","retry_after_seconds":0}""",
            """{"resolution":"retry_later","lease_expires_at":"2099-01-01T00:00:00Z","retry_after_seconds":0}""",
            """{"resolution":"retry_later","lease_expires_at":"2099-01-01T00:00:00Z","retry_after_seconds":-1}""",
            """{"resolution":"retry_later","lease_expires_at":"2099-01-01T00:00:00Z","retry_after_seconds":"3"}""",
            """{"resolution":"retry_later","lease_expires_at":"2099-01-01T00:00:00Z","retry_after_seconds":3.0}""",
            """{"resolution":"retry_later","lease_expires_at":"2099-01-01T00:00:00Z","retry_after_seconds":3,"extra":true}""",
            """{"resolution":"remote_authoritative","resolution":"initialize_empty"}""",
        )
        invalid.forEach { raw ->
            assertFailsWith<RemoteResponseSemanticException> { validateConnectResponseWire(raw) }
        }
    }
}

// Historical fixture input only, deliberately absent from production transport.
private val historicalConflictJson=Json {ignoreUnknownKeys=false}
private fun decodeHistoricalPushConflictFixtureOrNull(
    status: HttpStatusCode,
    rawBody: String,
): PushConflictException? {
    if (status != HttpStatusCode.Conflict) return null
    val root = runCatching { historicalConflictJson.parseToJsonElement(rawBody) as JsonObject }.getOrNull() ?: return null
    if ((root["error"] as? JsonPrimitive)?.content != "push_conflict") return null
    require(root.keys == setOf("error", "message", "conflict")) {
        "push conflict response fields are invalid"
    }
    val conflictObject = root["conflict"] as? JsonObject
        ?: throw RemoteResponseSemanticException("push conflict response")
    val required = setOf(
        "schema", "table", "key", "op", "base_row_version",
        "server_row_version", "server_row_deleted", "server_row",
    )
    require(conflictObject.keys == required) {
        "push conflict response fields are invalid"
    }
    val response = runCatching { historicalConflictJson.decodeFromString<PushConflictResponse>(rawBody) }
        .getOrElse { throw RemoteResponseDecodeException("push conflict response") }
    if (response.error != "push_conflict" || response.conflict == null) return null
    validatePushConflictDetails(response.conflict)
    return PushConflictException(rawBody = "", response = response.copy(message = ""))
}
