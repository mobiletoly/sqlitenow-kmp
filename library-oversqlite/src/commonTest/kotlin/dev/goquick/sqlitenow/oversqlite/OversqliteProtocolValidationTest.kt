package dev.goquick.sqlitenow.oversqlite

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

class OversqliteProtocolValidationTest {
    private val canonicalToken = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

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
            ConnectResponse(resolution = "initialize_local", initializationId = canonicalToken),
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
}
