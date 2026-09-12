/*
 * Copyright 2026 Toly Pochkin
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertFailsWith

class RequestGzipAvailabilityTest {
    @Test
    fun missingCompressionStreamRejectsEligibleRequest() = runTest {
        val global: dynamic = js("globalThis")
        val original = global.CompressionStream
        try {
            global.CompressionStream = js("undefined")
            assertFailsWith<OversqliteRequestCompressionUnavailableException> {
                encodeOversqliteRequestBody(ByteArray(2_048) { 'a'.code.toByte() }, 2_048, 2_048)
            }
        } finally {
            global.CompressionStream = original
        }
    }
}
