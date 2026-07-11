package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

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
    fun canonicalizeJsonElement_matchesRfc8785AppendixBFiniteBoundaries() {
        val json = Json
        val vectors = listOf(
            "0" to "0",
            "-0" to "0",
            "5e-324" to "5e-324",
            "-5e-324" to "-5e-324",
            "1.7976931348623157e308" to "1.7976931348623157e+308",
            "-1.7976931348623157e308" to "-1.7976931348623157e+308",
            "9007199254740992" to "9007199254740992",
            "-9007199254740992" to "-9007199254740992",
            "295147905179352830000" to "295147905179352830000",
            "9.999999999999997e22" to "9.999999999999997e+22",
            "1e23" to "1e+23",
            "1.0000000000000001e23" to "1.0000000000000001e+23",
            "999999999999999700000" to "999999999999999700000",
            "999999999999999900000" to "999999999999999900000",
            "1e21" to "1e+21",
            "9.999999999999997e-7" to "9.999999999999997e-7",
            "0.000001" to "0.000001",
            "333333333.3333332" to "333333333.3333332",
            "333333333.33333325" to "333333333.33333325",
            "333333333.3333333" to "333333333.3333333",
            "333333333.3333334" to "333333333.3333334",
            "333333333.33333343" to "333333333.33333343",
            "-0.0000033333333333333333" to "-0.0000033333333333333333",
            "1424953923781206.2" to "1424953923781206.2",
        )
        for ((input, expected) in vectors) {
            assertEquals(expected, canonicalizeJsonElement(json.parseToJsonElement(input)), input)
        }
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

    @Test
    fun c1Expected_typedExactNumbersUtf16OrderAndHashes() {
        val json = Json
        assertEquals(
            """{"d":"1234567890.123456789","n":"9007199254740993"}""",
            canonicalizeJsonElement(json.parseToJsonElement("""{"n":"9007199254740993","d":"1234567890.123456789"}""")),
        )
        assertEquals(
            """{"𐀀":"supplementary","":"bmp"}""",
            canonicalizeJsonElement(json.parseToJsonElement("""{"":"bmp","𐀀":"supplementary"}""")),
        )
        assertEquals("\"1e700\"", canonicalizeJsonElement(json.parseToJsonElement("\"1e700\"")))

        val left = sha256Hex(canonicalizeJsonElement(json.parseToJsonElement("""[{"n":"9007199254740992"}]""")).encodeToByteArray())
        val right = sha256Hex(canonicalizeJsonElement(json.parseToJsonElement("""[{"n":"9007199254740993"}]""")).encodeToByteArray())
        assertNotEquals(left, right)
    }
}
