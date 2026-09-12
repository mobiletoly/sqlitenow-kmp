/*
 * Copyright 2026 Toly Pochkin
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.goquick.sqlitenow.oversqlite

import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.util.zip.CRC32
import java.util.zip.DataFormatException
import java.util.zip.GZIPOutputStream
import java.util.zip.Inflater
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive

internal actual val oversqliteFetchManagesResponseCompression: Boolean = false

internal actual suspend fun compressOversqliteRequestGzip(decoded: ByteArray, outputCap: Int): ByteArray? {
    val context = currentCoroutineContext()
    val output = CappedGzipOutput(outputCap)
    return try {
        GZIPOutputStream(output, 8 * 1024).use { gzip ->
            var offset = 0
            while (offset < decoded.size) {
                context.ensureActive()
                val count = minOf(8 * 1024, decoded.size - offset)
                gzip.write(decoded, offset, count)
                offset += count
            }
            context.ensureActive()
        }
        context.ensureActive()
        output.bytes()
    } catch (_: GzipCapExceeded) {
        context.ensureActive()
        null
    }
}

private class GzipCapExceeded : RuntimeException()

private class CappedGzipOutput(private val cap: Int) : OutputStream() {
    private val buffer = ByteArray(cap)
    private var size = 0

    override fun write(value: Int) {
        if (size == cap) throw GzipCapExceeded()
        buffer[size++] = value.toByte()
    }

    override fun write(bytes: ByteArray, offset: Int, length: Int) {
        if (length > cap - size) throw GzipCapExceeded()
        bytes.copyInto(buffer, size, offset, offset + length)
        size += length
    }

    fun bytes(): ByteArray = buffer.copyOf(size)
}

internal actual fun decodeOversqliteSingleMemberGzip(encoded: ByteArray, decodedLimit: Long): ByteArray {
    require(decodedLimit in 0 until Int.MAX_VALUE.toLong()) { "decoded gzip limit is invalid" }
    try {
        val payloadStart = androidGzipPayloadStart(encoded)
        val inflater = Inflater(true)
        try {
            inflater.setInput(encoded, payloadStart, encoded.size - payloadStart)
            val output = ByteArrayOutputStream(minOf(decodedLimit.toInt(), 16 * 1024))
            val buffer = ByteArray(8 * 1024)
            while (!inflater.finished()) {
                val count = inflater.inflate(buffer)
                if (count == 0) {
                    require(!inflater.needsDictionary() && !inflater.needsInput()) { "invalid gzip DEFLATE stream" }
                    error("truncated gzip DEFLATE stream")
                }
                if (output.size().toLong() + count > decodedLimit) throw OversqliteDecodedGzipTooLargeException()
                output.write(buffer, 0, count)
            }
            val trailerStart = payloadStart + inflater.totalIn
            require(trailerStart + 8 == encoded.size) { "gzip must contain exactly one member and no trailing bytes" }
            val decoded = output.toByteArray()
            val expectedCrc = androidLittleEndianInt(encoded, trailerStart)
            val expectedSize = androidLittleEndianInt(encoded, trailerStart + 4)
            val crc = CRC32().also { it.update(decoded) }.value.toInt()
            require(crc == expectedCrc && decoded.size == expectedSize) { "invalid gzip trailer" }
            return decoded
        } finally {
            inflater.end()
        }
    } catch (error: DataFormatException) {
        throw IllegalArgumentException("invalid gzip body", error)
    } catch (error: IndexOutOfBoundsException) {
        throw IllegalArgumentException("truncated gzip body", error)
    }
}

private fun androidGzipPayloadStart(bytes: ByteArray): Int {
    require(bytes.size >= 18) { "truncated gzip body" }
    require((bytes[0].toInt() and 0xff) == 0x1f && (bytes[1].toInt() and 0xff) == 0x8b) { "invalid gzip header" }
    require((bytes[2].toInt() and 0xff) == 8) { "unsupported gzip compression method" }
    val flags = bytes[3].toInt() and 0xff
    require(flags and 0xe0 == 0) { "invalid gzip flags" }
    var cursor = 10
    if (flags and 0x04 != 0) {
        val length = (bytes[cursor].toInt() and 0xff) or ((bytes[cursor + 1].toInt() and 0xff) shl 8)
        cursor += 2 + length
    }
    if (flags and 0x08 != 0) cursor = androidSkipZeroTerminated(bytes, cursor)
    if (flags and 0x10 != 0) cursor = androidSkipZeroTerminated(bytes, cursor)
    if (flags and 0x02 != 0) cursor += 2
    require(cursor <= bytes.size - 8) { "truncated gzip header" }
    return cursor
}

private fun androidSkipZeroTerminated(bytes: ByteArray, start: Int): Int {
    var cursor = start
    while (cursor < bytes.size && bytes[cursor].toInt() != 0) cursor++
    require(cursor < bytes.size) { "truncated gzip header" }
    return cursor + 1
}

private fun androidLittleEndianInt(bytes: ByteArray, offset: Int): Int =
    (bytes[offset].toInt() and 0xff) or
        ((bytes[offset + 1].toInt() and 0xff) shl 8) or
        ((bytes[offset + 2].toInt() and 0xff) shl 16) or
        ((bytes[offset + 3].toInt() and 0xff) shl 24)
