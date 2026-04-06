package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class OversqlitePayloadCodecTest {
    @Test
    fun normalizeLocalUuidAsCanonicalWire_acceptsHexAndCanonicalUuid() {
        val canonical = "00112233-4455-6677-8899-aabbccddeeff"

        assertEquals(canonical, normalizeLocalUuidAsCanonicalWire("00112233445566778899aabbccddeeff"))
        assertEquals(canonical, normalizeLocalUuidAsCanonicalWire(canonical.uppercase()))
    }

    @Test
    fun decodeWireUuidBytes_rejectsNonCanonicalValues() {
        assertFailsWith<IllegalArgumentException> {
            decodeWireUuidBytes("00112233445566778899aabbccddeeff")
        }
        assertFailsWith<IllegalArgumentException> {
            decodeWireUuidBytes("00112233-4455-6677-8899-AABBCCDDEEFF")
        }
    }

    @Test
    fun decodeLocalBlobBytes_acceptsHexAndBase64() {
        val expected = byteArrayOf(0x01, 0x02, 0x03)

        assertContentEquals(expected, decodeLocalBlobBytes("010203"))
        assertContentEquals(expected, decodeLocalBlobBytes("AQID"))
    }

    @Test
    fun decodeLocalBlobBytes_prefersHexForAmbiguousSixteenByteValues() {
        assertContentEquals(
            byteArrayOf(
                0x00,
                0x11,
                0x22,
                0x33,
                0x44,
                0x55,
                0x66,
                0x77,
                0x88.toByte(),
                0x99.toByte(),
                0xaa.toByte(),
                0xbb.toByte(),
                0xcc.toByte(),
                0xdd.toByte(),
                0xee.toByte(),
                0xff.toByte(),
            ),
            decodeLocalBlobBytes("00112233445566778899aabbccddeeff"),
        )
    }

    @Test
    fun canonicalizeFiniteJsonNumber_normalizesEquivalentForms() {
        assertEquals("1", canonicalizeFiniteJsonNumber("1.00e+0"))
        assertEquals("10", canonicalizeFiniteJsonNumber("1.0e1"))
        assertEquals("-0.125", canonicalizeFiniteJsonNumber("-0.1250"))
        assertEquals("0", canonicalizeFiniteJsonNumber("-0.0"))
    }

    @Test
    fun integerPayloads_acceptBooleanPrimitivesAsOneAndZero() {
        val column = ColumnInfo(
            name = "enabled_flag",
            declaredType = "INTEGER",
            isPrimaryKey = false,
            notNull = true,
            defaultValue = null,
        )

        assertEquals(
            TypedValue.Integer(1),
            OversqliteValueCodec.decodePayloadValue(column, JsonPrimitive(true), PayloadSource.LOCAL_STATE),
        )
        assertEquals(
            TypedValue.Integer(0),
            OversqliteValueCodec.decodePayloadValue(column, JsonPrimitive(false), PayloadSource.AUTHORITATIVE_WIRE),
        )
        assertEquals(
            JsonPrimitive(1),
            OversqliteValueCodec.encodeWirePayloadValue(column, JsonPrimitive(true)),
        )
        assertFailsWith<IllegalStateException> {
            OversqliteValueCodec.decodePayloadValue(column, JsonPrimitive("false"), PayloadSource.AUTHORITATIVE_WIRE)
        }
    }
}
