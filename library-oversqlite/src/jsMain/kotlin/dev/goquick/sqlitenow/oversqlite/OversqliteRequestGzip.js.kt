/*
 * Copyright 2026 Toly Pochkin
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.async
import kotlinx.coroutines.await
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlin.js.Promise
import org.khronos.webgl.Uint8Array

internal actual suspend fun compressOversqliteRequestGzip(decoded: ByteArray, outputCap: Int): ByteArray? = coroutineScope {
    currentCoroutineContext().ensureActive()
    if (!(js("typeof globalThis.CompressionStream === 'function'") as Boolean)) {
        throw OversqliteRequestCompressionUnavailableException("CompressionStream is unavailable")
    }
    val input = Uint8Array(decoded.size)
    val values = input.asDynamic()
    for (index in decoded.indices) values[index] = decoded[index].toInt() and 0xff
    val stream: dynamic = js("new globalThis.CompressionStream('gzip')")
    val writer: dynamic = stream.writable.getWriter()
    val reader: dynamic = stream.readable.getReader()
    val output = ByteArray(outputCap)
    val feeding = async {
        var offset = 0
        while (offset < decoded.size) {
            currentCoroutineContext().ensureActive()
            val end = minOf(offset + 8 * 1024, decoded.size)
            writer.write(input.subarray(offset, end)).unsafeCast<Promise<dynamic>>().await()
            offset = end
        }
        writer.close().unsafeCast<Promise<dynamic>>().await()
    }
    var complete = false
    try {
        var count = 0
        while (true) {
            currentCoroutineContext().ensureActive()
            val next = reader.read().unsafeCast<Promise<dynamic>>().await()
            if (next.done as Boolean) break
            val chunk: dynamic = next.value
            val length = chunk.length as Int
            if (length > outputCap - count) return@coroutineScope null
            for (index in 0 until length) output[count + index] = (chunk[index] as Number).toByte()
            count += length
        }
        feeding.await()
        currentCoroutineContext().ensureActive()
        complete = true
        output.copyOf(count)
    } finally {
        if (!complete) {
            feeding.cancel()
            withContext(NonCancellable) {
                val aborting = async { runCatching { writer.abort().unsafeCast<Promise<dynamic>>().await() } }
                val cancelling = async { runCatching { reader.cancel().unsafeCast<Promise<dynamic>>().await() } }
                aborting.await()
                cancelling.await()
            }
        }
        runCatching { writer.releaseLock() }
        runCatching { reader.releaseLock() }
    }
}
