package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertNull

class SnapshotProtocolTest {
    private val generation = testGeneration(testRegisteredTableSpecs("users", "posts"))

    @Test
    fun transferTargetsAreOneImmutableConfigurationSurface() {
        val defaults = OversqliteTransferTargets()
        assertEquals(200, defaults.maxPushRows)
        assertEquals(1L * 1024L * 1024L, defaults.maxPushDecodedBytes)
        assertEquals(1_000, defaults.maxPullBundles)
        assertEquals(1_000, defaults.maxSnapshotChunkRows)
        assertEquals(4L * 1024L * 1024L, defaults.maxSnapshotChunkDecodedBytes)
    }

    @Test
    fun compactPushRoundTripExpandsBeforeSemanticUse() {
        val rows = listOf(
            PushRequestRow(
                "main", "users", mapOf("id" to "u1"), "INSERT", 0,
                buildJsonObject { put("id", JsonPrimitive("u1")); put("name", JsonPrimitive("Ada")) },
            ),
            PushRequestRow(
                "main", "users", mapOf("id" to "u2"), "DELETE", 4, null,
            ),
            PushRequestRow(
                "main", "posts", mapOf("id" to "p1"), "UPDATE", 7,
                buildJsonObject {
                    put("id", JsonPrimitive("p1"))
                    put("user_id", JsonPrimitive("u1"))
                    put("title", JsonPrimitive("Post"))
                },
            ),
        )

        val compact = OversqliteCompactCodec.encodePushRows(generation, rows)
        val expanded = OversqliteCompactCodec.decodeSemanticRows(generation, compact)
        assertEquals(listOf("INSERT", "DELETE", "UPDATE"), expanded.map { it.op })
        assertEquals(listOf(0L, 4L, 7L), expanded.map { it.version })
        assertEquals(JsonPrimitive("u1"), expanded.first().key.getValue("id"))
    }

    @Test
    fun compactRowsRejectMalformedShapesBeforeReturningAnyRows() {
        val valid = OversqliteCompactCodec.encodePushRows(
            generation,
            listOf(PushRequestRow(
                "main", "users", mapOf("id" to "u1"), "INSERT", 0,
                buildJsonObject { put("id", JsonPrimitive("u1")); put("name", JsonPrimitive("Ada")) },
            )),
        )
        val tuple = valid[0].let { it as JsonArray }[2].let { it as JsonArray }[0].let { it as JsonArray }
        fun group(tableIndex: Int, ordinal: Int, rows: List<JsonArray>): JsonArray = JsonArray(
            listOf(JsonArray(listOf(
                JsonPrimitive(tableIndex),
                JsonPrimitive(ordinal),
                JsonArray(rows),
            ))),
        )
        val malformed = listOf(
            JsonArray(emptyList()),
            group(0, 1, listOf(tuple)),
            group(99, 0, listOf(tuple)),
            group(0, 0, emptyList()),
            group(0, 0, listOf(JsonArray(tuple + JsonNull))),
            group(0, 0, listOf(tuple, tuple)),
        )
        malformed.forEach { encoded ->
            assertFails(encoded.toString()) {
                OversqliteCompactCodec.decodeSemanticRows(generation, encoded)
            }
        }
    }

    @Test
    fun snapshotCompactGroupsUseOneBasedRequestedOrdinalAndLogicalBytes() {
        val rows = listOf(BundleRow(
            "main", "users", mapOf("id" to "u1"), "INSERT", 3,
            buildJsonObject { put("id", JsonPrimitive("u1")); put("name", JsonPrimitive("Ada")) },
        ))
        val encoded = OversqliteCompactCodec.encodeBundleRows(
            generation, rows, firstOrdinal = 8, snapshot = true,
        )
        assertEquals(8L, (encoded[0] as JsonArray)[1].let { (it as JsonPrimitive).content.toLong() })
        assertEquals(rows, OversqliteCompactCodec.decodeBundleRows(
            generation, encoded, expectedFirstOrdinal = 8, snapshot = true,
        ))
        assertEquals(
            canonicalSnapshotLogicalBytes(rows),
            OversqliteCompactCodec.snapshotLogicalByteCount(generation, rows),
        )
    }

    @Test
    fun snapshotSessionAndChunkUseLogicalCounts() {
        val session = SnapshotSession("snapshot", 7, 1, 24, "2099-01-01T00:00:00Z")
        validateSnapshotSession(session)
        assertFails { validateSnapshotSession(session.copy(rowCount = 0)) }
        assertFails { validateSnapshotSession(session.copy(logicalByteCount = 0)) }

        val row = SnapshotRow(
            "main", "users", mapOf("id" to "u1"), 7,
            buildJsonObject { put("id", JsonPrimitive("u1")); put("name", JsonPrimitive("Ada")) },
        )
        val chunk = SnapshotChunkResponse(listOf(row), 1, hasMore = false, logicalByteCount = 24)
        validateSnapshotChunkResponse(chunk, "snapshot", 7, 0, 1, 1024)
        assertFails { validateSnapshotChunkResponse(chunk.copy(nextRowOrdinal = 2), "snapshot", 7, 0, 1, 1024) }
        assertFails { validateSnapshotChunkResponse(chunk.copy(logicalByteCount = 0), "snapshot", 7, 0, 1, 1024) }
    }

    @Test
    fun exactGzipSelectionRuleUsesIdentityBelowThresholdAndOnlyKeepsSmallerGzip() = runTest {
        val below = ByteArray(oversqliteGzipThresholdBytes - 1) { 'a'.code.toByte() }
        val threshold = ByteArray(oversqliteGzipThresholdBytes) { 'a'.code.toByte() }
        val noisy = Random(1).nextBytes(oversqliteGzipThresholdBytes)

        assertNull(encodeOversqliteRequestBody(below, below.size.toLong(), below.size.toLong()).contentEncoding)
        assertEquals("gzip", encodeOversqliteRequestBody(threshold, threshold.size.toLong(), threshold.size.toLong()).contentEncoding)
        assertNull(encodeOversqliteRequestBody(noisy, noisy.size.toLong(), noisy.size.toLong()).contentEncoding)
    }

    @Test
    fun rawGzipDecoderAcceptsExactlyOneValidMemberWithinTheDecodedCeiling() = runTest {
        val decoded = ByteArray(8_192) { (it % 17).toByte() }
        val gzip = requireNotNull(compressOversqliteRequestGzip(decoded, decoded.size - 1))
        if (oversqliteFetchManagesResponseCompression) {
            assertFails { decodeOversqliteSingleMemberGzip(gzip, decoded.size.toLong()) }
            return@runTest
        }
        assertContentEquals(decoded, decodeOversqliteSingleMemberGzip(gzip, decoded.size.toLong()))
        assertFails { decodeOversqliteSingleMemberGzip(gzip, decoded.size - 1L) }
        assertFails { decodeOversqliteSingleMemberGzip(gzip + gzip, decoded.size.toLong() * 2L) }
        assertFails { decodeOversqliteSingleMemberGzip(gzip + byteArrayOf(0), decoded.size.toLong()) }
        assertFails {
            val badCrc = gzip.copyOf().also { it[it.lastIndex - 7] = (it[it.lastIndex - 7].toInt() xor 1).toByte() }
            decodeOversqliteSingleMemberGzip(badCrc, decoded.size.toLong())
        }
    }

    private fun canonicalSnapshotLogicalBytes(rows: List<BundleRow>): Long = rows.sumOf { row ->
        val tableIndex = generation.tables.indexOfFirst { it.schema == row.schema && it.table == row.table }
        val table = generation.tables[tableIndex]
        canonicalizeJsonElement(JsonArray(listOf(
            JsonPrimitive("i"),
            JsonPrimitive(tableIndex),
            JsonArray(table.syncKeyColumns.map { JsonPrimitive(row.key.getValue(it)) }),
            JsonPrimitive(row.rowVersion),
            row.payload ?: JsonNull,
        ))).encodeToByteArray().size.toLong()
    }
}
