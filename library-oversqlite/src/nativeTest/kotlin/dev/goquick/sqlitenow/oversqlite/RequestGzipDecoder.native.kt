package dev.goquick.sqlitenow.oversqlite

internal actual suspend fun decodeRequestGzipForTest(encoded: ByteArray, decodedLimit: Int): ByteArray =
    decodeOversqliteSingleMemberGzip(encoded, decodedLimit.toLong())
