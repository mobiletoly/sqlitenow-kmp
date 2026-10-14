/*
 * Copyright 2026 Toly Pochkin
 * SPDX-License-Identifier: Apache-2.0
 */
@file:OptIn(kotlin.js.ExperimentalWasmJsInterop::class)
package dev.goquick.sqlitenow.oversqlite

import kotlin.JsFun
import kotlin.js.JsAny
import kotlin.js.Promise
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.await
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext

@JsFun("() => typeof globalThis.DecompressionStream === 'function'")
private external fun hasDecompressionStream(): Boolean

@JsFun("() => new globalThis.DecompressionStream('gzip')")
private external fun newDecompressionStream(): JsAny

@JsFun("(length) => new Uint8Array(length)")
private external fun newBytes(length: Int): JsAny

@JsFun("(bytes, index, value) => { bytes[index] = value; }")
private external fun setByte(bytes: JsAny, index: Int, value: Int)

@JsFun("(bytes, start, end) => bytes.subarray(start, end)")
private external fun subarray(bytes: JsAny, start: Int, end: Int): JsAny

@JsFun("(stream) => stream.writable.getWriter()")
private external fun getWriter(stream: JsAny): JsAny

@JsFun("(stream) => stream.readable.getReader()")
private external fun getReader(stream: JsAny): JsAny

@JsFun("(writer, bytes) => writer.write(bytes)")
private external fun writeBytes(writer: JsAny, bytes: JsAny): Promise<JsAny?>

@JsFun("(writer) => writer.close()")
private external fun closeWriter(writer: JsAny): Promise<JsAny?>

@JsFun("(writer) => writer.abort()")
private external fun abortWriter(writer: JsAny): Promise<JsAny?>

@JsFun("(reader) => reader.read()")
private external fun readBytes(reader: JsAny): Promise<JsAny>

@JsFun("(reader) => reader.cancel()")
private external fun cancelReader(reader: JsAny): Promise<JsAny?>

@JsFun("(reader) => reader.releaseLock()")
private external fun releaseReader(reader: JsAny)

@JsFun("(writer) => writer.releaseLock()")
private external fun releaseWriter(writer: JsAny)

@JsFun("(result) => result.done")
private external fun isDone(result: JsAny): Boolean

@JsFun("(result) => result.value")
private external fun resultBytes(result: JsAny): JsAny

@JsFun("(bytes) => bytes.length")
private external fun byteCount(bytes: JsAny): Int

@JsFun("(bytes, index) => bytes[index]")
private external fun getByte(bytes: JsAny, index: Int): Int

internal actual suspend fun decodeRequestGzipForTest(encoded: ByteArray, decodedLimit: Int): ByteArray = coroutineScope {
    currentCoroutineContext().ensureActive()
    if (!hasDecompressionStream()) {
        throw OversqliteRequestCompressionUnavailableException("DecompressionStream is unavailable")
    }
    val input = newBytes(encoded.size)
    encoded.forEachIndexed { index, byte -> setByte(input, index, byte.toInt() and 0xff) }
    val stream = newDecompressionStream()
    val writer = getWriter(stream)
    val reader = getReader(stream)
    val output = ByteArray(decodedLimit)
    val feeding = async {
        var offset = 0
        while (offset < encoded.size) {
            currentCoroutineContext().ensureActive()
            val end = minOf(offset + 8 * 1024, encoded.size)
            writeBytes(writer, subarray(input, offset, end)).await()
            offset = end
        }
        closeWriter(writer).await()
    }
    var complete = false
    try {
        var count = 0
        while (true) {
            currentCoroutineContext().ensureActive()
            val next = readBytes(reader).await()
            if (isDone(next)) break
            val chunk = resultBytes(next)
            val length = byteCount(chunk)
            require(length <= decodedLimit - count) { "mock request exceeds encoded body limit" }
            for (index in 0 until length) output[count + index] = getByte(chunk, index).toByte()
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
                val aborting = async { runCatching { abortWriter(writer).await() } }
                val cancelling = async { runCatching { cancelReader(reader).await() } }
                aborting.await()
                cancelling.await()
            }
        }
        runCatching { releaseWriter(writer) }
        runCatching { releaseReader(reader) }
    }
}
