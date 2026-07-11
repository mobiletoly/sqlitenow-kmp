package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class OversqlitePayloadCodecTest {
    @Test
    fun typedExactValuesBindToOrdinaryIntegerAndTextStorage() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        try {
            db.execSQL("CREATE TABLE exact_values(id TEXT PRIMARY KEY, min_value INTEGER NOT NULL, max_value INTEGER NOT NULL, amount TEXT NOT NULL)")
            val minColumn = ColumnInfo("min_value", "INTEGER", false, true, null, ColumnKind.EXACT_INT64)
            val maxColumn = ColumnInfo("max_value", "INTEGER", false, true, null, ColumnKind.EXACT_INT64)
            val amountColumn = ColumnInfo("amount", "TEXT", false, true, null, ColumnKind.EXACT_DECIMAL)
            db.withPreparedStatement("INSERT INTO exact_values VALUES('n-1', ?, ?, ?)") { statement ->
                OversqliteValueCodec.bindPayloadValue(statement, 1, minColumn, JsonPrimitive("-9223372036854775808"), PayloadSource.AUTHORITATIVE_WIRE)
                OversqliteValueCodec.bindPayloadValue(statement, 2, maxColumn, JsonPrimitive("9223372036854775807"), PayloadSource.AUTHORITATIVE_WIRE)
                OversqliteValueCodec.bindPayloadValue(statement, 3, amountColumn, JsonPrimitive("1234567890.123456789"), PayloadSource.AUTHORITATIVE_WIRE)
                statement.step()
            }
            db.withPreparedStatement("SELECT CAST(min_value AS TEXT), CAST(max_value AS TEXT), amount, typeof(min_value), typeof(max_value), typeof(amount) FROM exact_values") { statement ->
                assertEquals(true, statement.step())
                assertEquals("-9223372036854775808", statement.getText(0))
                assertEquals("9223372036854775807", statement.getText(1))
                assertEquals("1234567890.123456789", statement.getText(2))
                assertEquals("integer", statement.getText(3))
                assertEquals("integer", statement.getText(4))
                assertEquals("text", statement.getText(5))
            }
        } finally {
            db.close()
        }
    }

    @Test
    fun exactDecimalRejectsEveryNegativeZeroSpelling() {
        val column = ColumnInfo("amount", "TEXT", false, true, null, ColumnKind.EXACT_DECIMAL)
        listOf("-0", "-0.0", "-0e10", "-0.000E-9").forEach { value ->
            assertFailsWith<IllegalArgumentException>(value) {
                OversqliteValueCodec.decodePayloadValue(column, JsonPrimitive(value), PayloadSource.AUTHORITATIVE_WIRE)
            }
        }
    }

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
