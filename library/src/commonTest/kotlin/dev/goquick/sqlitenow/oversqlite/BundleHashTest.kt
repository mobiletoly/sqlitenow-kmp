package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals

class BundleHashTest {
    @Test
    fun sha256_knownVectors() {
        assertEquals(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            sha256Hex(byteArrayOf()),
        )
        assertEquals(
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            sha256Hex("abc".encodeToByteArray()),
        )
        assertEquals(
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            sha256Hex("hello".encodeToByteArray()),
        )
        assertEquals(
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0",
            sha256Hex(ByteArray(1_000_000) { 'a'.code.toByte() }),
        )
    }

    @Test
    fun canonicalizeJsonElement_normalizesEquivalentNumberLexemes() {
        val json = Json

        assertEquals(
            canonicalizeJsonElement(json.parseToJsonElement("""{"n":1}""")),
            canonicalizeJsonElement(json.parseToJsonElement("""{"n":1.0}""")),
        )
        assertEquals(
            canonicalizeJsonElement(json.parseToJsonElement("""{"n":1}""")),
            canonicalizeJsonElement(json.parseToJsonElement("""{"n":1e0}""")),
        )
        assertEquals(
            """{"n":0.000001}""",
            canonicalizeJsonElement(json.parseToJsonElement("""{"n":1e-6}""")),
        )
        assertEquals(
            """{"n":1e-7}""",
            canonicalizeJsonElement(json.parseToJsonElement("""{"n":0.0000001}""")),
        )
        assertEquals(
            """{"n":1e+21}""",
            canonicalizeJsonElement(json.parseToJsonElement("""{"n":1000000000000000000000}""")),
        )
    }

    @Test
    fun canonicalizeJsonElement_sortsKeys_andNormalizesDeleteStyleNullPayload() {
        val json = Json

        assertEquals(
            """{"key":{"a":1,"b":2},"payload":null}""",
            canonicalizeJsonElement(
                json.parseToJsonElement(
                    """{"payload":null,"key":{"b":2,"a":1}}"""
                )
            ),
        )
    }
}
