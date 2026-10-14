package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFails

internal class RequestGzipFixtureDecoderTest {
    private val decoded = "complete compressed mock request ".repeat(100).encodeToByteArray()

    @Test fun compressedMockRequestRetainsEveryDecodedByte() = runTest {
        val encoded = checkNotNull(compressOversqliteRequestGzip(decoded, decoded.size - 1))
        assertContentEquals(decoded, decodeRequestGzipForTest(encoded, decoded.size))
    }

    @Test fun compressedMockRequestCannotExceedDecodedLimit() = runTest {
        val encoded = checkNotNull(compressOversqliteRequestGzip(decoded, decoded.size - 1))
        assertFails { decodeRequestGzipForTest(encoded, decoded.size - 1) }
    }

    @Test fun corruptMockRequestTrailerIsRejected() = runTest {
        val encoded = checkNotNull(compressOversqliteRequestGzip(decoded, decoded.size - 1))
        encoded[encoded.size - 8] = (encoded[encoded.size - 8].toInt() xor 1).toByte()
        assertFails { decodeRequestGzipForTest(encoded, decoded.size) }
    }
}
