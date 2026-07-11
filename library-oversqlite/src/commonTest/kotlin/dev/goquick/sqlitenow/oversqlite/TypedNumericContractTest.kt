package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertIs

class TypedNumericContractTest {
    @Test
    fun exactInt64UsesStringsAndEnforcesSignedRange() {
        val column = ColumnInfo(
            name = "value",
            declaredType = "INTEGER",
            isPrimaryKey = false,
            notNull = true,
            defaultValue = null,
            kind = ColumnKind.EXACT_INT64,
        )

        val minimum = OversqliteValueCodec.decodePayloadValue(
            column,
            JsonPrimitive("-9223372036854775808"),
            PayloadSource.AUTHORITATIVE_WIRE,
        )
        val maximum = OversqliteValueCodec.decodePayloadValue(
            column,
            JsonPrimitive("9223372036854775807"),
            PayloadSource.AUTHORITATIVE_WIRE,
        )
        assertEquals(Long.MIN_VALUE, assertIs<TypedValue.Integer>(minimum).value)
        assertEquals(Long.MAX_VALUE, assertIs<TypedValue.Integer>(maximum).value)
        assertFails {
            OversqliteValueCodec.decodePayloadValue(
                column,
                JsonPrimitive("9223372036854775808"),
                PayloadSource.AUTHORITATIVE_WIRE,
            )
        }
        assertFails {
            OversqliteValueCodec.decodePayloadValue(
                column,
                JsonPrimitive(9007199254740992L),
                PayloadSource.AUTHORITATIVE_WIRE,
            )
        }
    }

    @Test
    fun exactDecimalUsesTextWithoutNormalization() {
        val column = ColumnInfo(
            name = "amount",
            declaredType = "TEXT",
            isPrimaryKey = false,
            notNull = true,
            defaultValue = null,
            kind = ColumnKind.EXACT_DECIMAL,
        )
        val value = OversqliteValueCodec.decodePayloadValue(
            column,
            JsonPrimitive("1234567890.123456789"),
            PayloadSource.AUTHORITATIVE_WIRE,
        )
        assertEquals("1234567890.123456789", assertIs<TypedValue.ExactDecimal>(value).value)
        assertFails {
            OversqliteValueCodec.decodePayloadValue(
                column,
                JsonPrimitive(1.25),
                PayloadSource.AUTHORITATIVE_WIRE,
            )
        }
    }
}
