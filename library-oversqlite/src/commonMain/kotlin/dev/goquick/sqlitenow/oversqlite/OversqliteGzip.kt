/*
 * Copyright 2026 Toly Pochkin
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive

internal const val oversqliteGzipThresholdBytes = 2_048

internal data class OversqliteEncodedBody(
    val bytes: ByteArray,
    val contentEncoding: String?,
    val decodedByteCount: Long,
)

internal fun interface OversqliteRequestGzipEngine {
    /** Returns null only when the next output byte exceeds the inclusive cap. */
    suspend fun compress(decoded: ByteArray, outputCap: Int): ByteArray?
}

internal class OversqliteRequestCompressionException(message: String, cause: Throwable? = null) :
    IllegalStateException(message, cause)

internal class OversqliteRequestCompressionUnavailableException(message: String) :
    IllegalStateException(message)

/** Selects a complete bounded gzip member only when it is smaller than identity. */
internal suspend fun encodeOversqliteRequestBody(
    decoded: ByteArray,
    decodedLimit: Long,
    encodedHardLimit: Long,
    engine: OversqliteRequestGzipEngine = OversqliteRequestGzipEngine(::compressOversqliteRequestGzip),
): OversqliteEncodedBody {
    require(decodedLimit >= 0L && encodedHardLimit >= 0L) { "request body limits must be nonnegative" }
    require(decoded.size.toLong() <= decodedLimit) { "decoded request body exceeds its admitted limit" }
    currentCoroutineContext().ensureActive()
    if (decoded.size < oversqliteGzipThresholdBytes) {
        require(decoded.size.toLong() <= encodedHardLimit) { "encoded request body exceeds its admitted limit" }
        return OversqliteEncodedBody(decoded, null, decoded.size.toLong())
    }
    val cap = minOf(decoded.size.toLong() - 1L, encodedHardLimit).toInt()
    val gzip = try {
        engine.compress(decoded, cap)
    } catch (error: CancellationException) {
        throw error
    } catch (error: OversqliteRequestCompressionUnavailableException) {
        throw error
    } catch (error: Throwable) {
        throw OversqliteRequestCompressionException("request gzip compression failed", error)
    }
    currentCoroutineContext().ensureActive()
    if (gzip != null && gzip.size > cap) {
        throw OversqliteRequestCompressionException("request gzip engine exceeded its output cap")
    }
    if (gzip != null && gzip.size < decoded.size) {
        return OversqliteEncodedBody(gzip, "gzip", decoded.size.toLong())
    }
    require(decoded.size.toLong() <= encodedHardLimit) { "encoded request body exceeds its admitted limit" }
    return OversqliteEncodedBody(decoded, null, decoded.size.toLong())
}

internal expect suspend fun compressOversqliteRequestGzip(decoded: ByteArray, outputCap: Int): ByteArray?

internal expect val oversqliteFetchManagesResponseCompression: Boolean

/** Raw transports decode exactly one gzip member and enforce the decoded limit while inflating. */
internal expect fun decodeOversqliteSingleMemberGzip(encoded: ByteArray, decodedLimit: Long): ByteArray

internal class OversqliteDecodedGzipTooLargeException : RuntimeException()
