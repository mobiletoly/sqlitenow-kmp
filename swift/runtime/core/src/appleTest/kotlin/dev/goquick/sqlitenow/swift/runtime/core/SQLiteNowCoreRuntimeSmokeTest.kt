package dev.goquick.sqlitenow.swift.runtime.core

import kotlin.test.Test
import kotlin.test.assertEquals

class SQLiteNowCoreRuntimeSmokeTest {
    @Test
    fun bindValuesRetainTheirSwiftRuntimeRepresentations() {
        val text = SQLiteNowCoreRuntimeBindValue("hello")
        val int64 = SQLiteNowCoreRuntimeBindValue(42L)
        val bool = SQLiteNowCoreRuntimeBindValue(true)

        assertEquals("text", text.kind)
        assertEquals("hello", text.textValue)
        assertEquals("int64", int64.kind)
        assertEquals(42L, int64.int64Value)
        assertEquals("bool", bool.kind)
        assertEquals(true, bool.boolValue)
    }
}
