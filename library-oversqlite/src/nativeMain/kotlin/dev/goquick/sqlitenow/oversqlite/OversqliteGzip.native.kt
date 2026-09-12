/*
 * Copyright 2026 Toly Pochkin
 * SPDX-License-Identifier: Apache-2.0
 */
@file:OptIn(kotlinx.cinterop.ExperimentalForeignApi::class)

package dev.goquick.sqlitenow.oversqlite

import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.convert
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.usePinned
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import platform.zlib.Z_DEFAULT_COMPRESSION
import platform.zlib.Z_DEFAULT_STRATEGY
import platform.zlib.Z_DEFLATED
import platform.zlib.Z_FINISH
import platform.zlib.Z_NO_FLUSH
import platform.zlib.Z_OK
import platform.zlib.Z_STREAM_END
import platform.zlib.deflate
import platform.zlib.deflateEnd
import platform.zlib.deflateInit2
import platform.zlib.inflate
import platform.zlib.inflateEnd
import platform.zlib.inflateInit2
import platform.zlib.z_stream

internal actual val oversqliteFetchManagesResponseCompression: Boolean = false

internal actual suspend fun compressOversqliteRequestGzip(decoded: ByteArray, outputCap: Int): ByteArray? {
    val context = currentCoroutineContext()
    val retained = ByteArray(outputCap)
    val scratch = ByteArray(8 * 1024)
    return memScoped {
        val stream = alloc<z_stream>()
        stream.zalloc = null
        stream.zfree = null
        stream.opaque = null
        check(deflateInit2(stream.ptr, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY) == Z_OK) {
            "request gzip deflater initialization failed"
        }
        try {
            decoded.usePinned { input ->
                scratch.usePinned { output ->
                    var consumed = 0
                    var written = 0
                    var finished = false
                    while (!finished) {
                        context.ensureActive()
                        val inputCount = minOf(8 * 1024, decoded.size - consumed)
                        if (inputCount > 0) {
                            stream.next_in = input.addressOf(consumed).reinterpret()
                            stream.avail_in = inputCount.convert()
                        } else {
                            stream.next_in = null
                            stream.avail_in = 0u
                        }
                        val flush = if (consumed + inputCount == decoded.size) Z_FINISH else Z_NO_FLUSH
                        do {
                            context.ensureActive()
                            val available = minOf(scratch.size, outputCap - written + 1)
                            stream.next_out = output.addressOf(0).reinterpret()
                            stream.avail_out = available.convert()
                            val beforeInput = stream.avail_in.toInt()
                            val result = deflate(stream.ptr, flush)
                            check(result == Z_OK || result == Z_STREAM_END) { "request gzip deflate failed: $result" }
                            val produced = available - stream.avail_out.toInt()
                            if (produced > outputCap - written) return@memScoped null
                            scratch.copyInto(retained, written, 0, produced)
                            written += produced
                            consumed += beforeInput - stream.avail_in.toInt()
                            if (result == Z_STREAM_END) finished = true
                            check(finished || beforeInput != stream.avail_in.toInt() || produced > 0) {
                                "request gzip deflater made no progress"
                            }
                        } while (!finished && (stream.avail_in.toInt() > 0 || (flush == Z_FINISH && stream.avail_out.toInt() == 0)))
                    }
                    context.ensureActive()
                    retained.copyOf(written)
                }
            }
        } finally {
            deflateEnd(stream.ptr)
        }
    }
}

internal actual fun decodeOversqliteSingleMemberGzip(encoded: ByteArray, decodedLimit: Long): ByteArray {
    require(decodedLimit in 0 until Int.MAX_VALUE.toLong()) { "decoded gzip limit is invalid" }
    require(encoded.isNotEmpty()) { "gzip body is empty" }
    val output = ByteArray(decodedLimit.toInt() + 1)
    return memScoped {
        val stream = alloc<z_stream>()
        stream.zalloc = null
        stream.zfree = null
        stream.opaque = null
        val initialized = inflateInit2(stream.ptr, 15 + 16)
        require(initialized == 0) { "gzip inflater initialization failed" }
        try {
            encoded.usePinned { inputPinned ->
                output.usePinned { outputPinned ->
                    stream.next_in = inputPinned.addressOf(0).reinterpret()
                    stream.avail_in = encoded.size.convert()
                    stream.next_out = outputPinned.addressOf(0).reinterpret()
                    stream.avail_out = output.size.convert()
                    val result = inflate(stream.ptr, Z_FINISH)
                    if (result != Z_STREAM_END && stream.total_out.toLong() > decodedLimit) {
                        throw OversqliteDecodedGzipTooLargeException()
                    }
                    require(result == Z_STREAM_END) { "invalid or truncated gzip body" }
                    require(stream.total_in.toLong() == encoded.size.toLong()) {
                        "gzip must contain exactly one member and no trailing bytes"
                    }
                    if (stream.total_out.toLong() > decodedLimit) throw OversqliteDecodedGzipTooLargeException()
                    output.copyOf(stream.total_out.toInt())
                }
            }
        } finally {
            inflateEnd(stream.ptr)
        }
    }
}
