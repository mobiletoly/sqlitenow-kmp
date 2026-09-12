/*
 * Copyright 2026 Toly Pochkin
 * SPDX-License-Identifier: Apache-2.0
 */
@file:OptIn(kotlin.js.ExperimentalWasmJsInterop::class)

package dev.goquick.sqlitenow.oversqlite

import kotlin.JsFun
import kotlin.js.JsAny
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertFailsWith

@JsFun("() => globalThis.CompressionStream")
private external fun originalCompressionStream(): JsAny

@JsFun("() => { globalThis.CompressionStream = undefined; }")
private external fun hideCompressionStream()

@JsFun("(value) => { globalThis.CompressionStream = value; }")
private external fun restoreCompressionStream(value: JsAny)

class RequestGzipAvailabilityTest {
    @Test
    fun missingCompressionStreamRejectsEligibleRequest() = runTest {
        val original = originalCompressionStream()
        try {
            hideCompressionStream()
            assertFailsWith<OversqliteRequestCompressionUnavailableException> {
                encodeOversqliteRequestBody(ByteArray(2_048) { 'a'.code.toByte() }, 2_048, 2_048)
            }
        } finally {
            restoreCompressionStream(original)
        }
    }
}
