package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive

/** Local receipt identity. This encoding is independent of the wire-v1 digest. */
internal data class BootstrapProjectionIdentity(
    val snapshotId: String,
    val generationId: String,
    val generationFingerprint: String,
    val snapshotBundleSeq: Long,
    val descriptorSignature: String,
) {
    init {
        require(snapshotId.isNotEmpty() && generationId.matches(Regex("[1-9][0-9]*")))
        generationId.toLong()
        require(snapshotBundleSeq >= 0 && isCanonicalSha256(generationFingerprint) &&
            isCanonicalSha256(descriptorSignature))
    }

    internal fun header(domain: String, range: LongRange? = null): String = bootstrapCanonicalStrings(
        listOf(domain, snapshotId, generationId, generationFingerprint,
            snapshotBundleSeq.toString(), descriptorSignature) +
            (range?.let { listOf(it.first.toString(), it.last.toString()) } ?: emptyList()),
    )
}

/**
 * One admitted writer owns this streaming state. Inputs and encoding copies remain
 * charged to its page; neither values nor frames are retained by the digest.
 * Any failed append poisons the writer, including a provider failure after mutation.
 */
internal class BootstrapProjectionHashWriter(
    private val owner: Any,
    private val identity: BootstrapProjectionIdentity,
    private val range: LongRange? = null,
    factory: () -> SnapshotDigest = ::newSnapshotDigest,
) {
    private var digest: SnapshotDigest? = factory()
    private var nextOrdinal = range?.first ?: 1L
    private var rows = 0L
    private var logicalBytes = 0L

    init {
        require(range == null || range.first > 0 && range.last >= range.first)
        guarded(owner) {
            frame(identity.header(if (range == null) "oversqlite.bootstrap.values.v1"
                else "oversqlite.bootstrap.page.values.v1", range).encodeToByteArray())
        }
    }

    fun requireOwner(candidate: Any) {
        check(digest != null && owner === candidate) { "bootstrap digest belongs to a different or expired writer" }
    }

    fun add(
        candidate: Any,
        ordinal: Long,
        generationTableIndex: Int,
        rowVersion: Long,
        keyJson: String,
        values: List<TypedValue>,
        logicalWireBytes: Long,
    ) = guarded(candidate) {
        require(ordinal == nextOrdinal && ordinal > 0 && generationTableIndex >= 0 && rowVersion > 0)
        require(range == null || ordinal <= range.last)
        require(logicalWireBytes >= 0)
        // Validate all lengths and scalar conversions before the first digest update.
        bootstrapProjectionRowBytes(keyJson, values)
        val nextRows = bootstrapCheckedAdd(rows, 1)
        val nextBytes = bootstrapCheckedAdd(logicalBytes, logicalWireBytes)
        appendInteger(ordinal, 8)
        appendInteger(generationTableIndex.toLong(), 4)
        appendInteger(rowVersion, 8)
        frame(keyJson.encodeToByteArray())
        appendInteger(values.size.toLong(), 4)
        for (value in values) when (value) {
            TypedValue.Null -> tag(0)
            is TypedValue.Integer -> { tag(1); appendInteger(value.canonicalText.toLong(), 8) }
            is TypedValue.Real -> {
                tag(2)
                val number = value.canonicalText.toDouble()
                appendInteger((if (number == 0.0) 0.0 else number).toBits(), 8)
            }
            is TypedValue.Text -> { tag(3); frame(value.value.encodeToByteArray()) }
            is TypedValue.Blob -> { tag(4); frame(value.bytes) }
        }
        rows = nextRows
        logicalBytes = nextBytes
        // Long.MAX_VALUE is a valid final ordinal; no wrapped next ordinal is reused.
        nextOrdinal = if (ordinal == Long.MAX_VALUE) 0 else ordinal + 1
    }

    fun finish(candidate: Any, rowCount: Long, logicalWireBytes: Long): String = guarded(candidate) {
        require(rowCount == rows && logicalWireBytes == logicalBytes)
        require(range == null || rows == range.last - range.first + 1)
        frame(bootstrapCanonicalStrings(listOf("end", rows.toString(), logicalBytes.toString())).encodeToByteArray())
        checkNotNull(digest).finishHex().also { digest = null }
    }

    fun close() { digest = null }

    private inline fun <T> guarded(candidate: Any, block: () -> T): T {
        requireOwner(candidate)
        return try { block() } catch (failure: Throwable) { digest = null; throw failure }
    }

    private fun tag(value: Int) = checkNotNull(digest).appendAscii(value.toChar())
    private fun appendInteger(value: Long, width: Int) = checkNotNull(digest).append(bootstrapBigEndian(value, width))
    private fun frame(bytes: ByteArray) {
        appendInteger(bytes.size.toLong(), 8)
        checkNotNull(digest).append(bytes)
    }
}

/** Exact Section 5.2 row body length, without allocating a second encoded row. */
internal fun bootstrapProjectionRowBytes(keyJson: String, values: List<TypedValue>): Long {
    var bytes = bootstrapCheckedAdd(32, utf8Size(keyJson)) // ordinal/index/version/F(key)/column count
    for (value in values) {
        bytes = bootstrapCheckedAdd(bytes, 1)
        bytes = bootstrapCheckedAdd(bytes, when (value) {
            TypedValue.Null -> 0
            is TypedValue.Integer -> { value.canonicalText.toLong(); 8 }
            is TypedValue.Real -> { require(value.canonicalText.toDouble().isFinite()); 8 }
            is TypedValue.Text -> bootstrapCheckedAdd(8, utf8Size(value.value))
            is TypedValue.Blob -> bootstrapCheckedAdd(8, value.bytes.size.toLong())
        })
    }
    return bytes
}

internal fun bootstrapPageProof(
    identity: BootstrapProjectionIdentity,
    pageOrdinal: Long,
    firstOrdinal: Long,
    lastOrdinal: Long,
    rowCount: Long,
    logicalBytes: Long,
    wirePageHash: String,
    typedPageHash: String,
): String {
    require(pageOrdinal > 0 && firstOrdinal > 0 && lastOrdinal >= firstOrdinal &&
        rowCount == lastOrdinal - firstOrdinal + 1 && logicalBytes >= 0)
    require(isCanonicalSha256(wirePageHash) && isCanonicalSha256(typedPageHash))
    return bootstrapFramedHash(bootstrapCanonicalStrings(listOf(
        "oversqlite.bootstrap.page.proof.v1", identity.snapshotId, identity.descriptorSignature,
        pageOrdinal.toString(), firstOrdinal.toString(), lastOrdinal.toString(), rowCount.toString(),
        logicalBytes.toString(), wirePageHash, typedPageHash,
    )).encodeToByteArray())
}

internal fun bootstrapPageChainSeed(identity: BootstrapProjectionIdentity): String = bootstrapFramedHash(
    bootstrapCanonicalStrings(listOf("oversqlite.bootstrap.page.chain.v1", identity.snapshotId,
        identity.descriptorSignature)).encodeToByteArray(),
)

internal fun bootstrapPageChainExtend(previous: String, proof: String): String {
    require(isCanonicalSha256(previous) && isCanonicalSha256(proof))
    fun bytes(hex: String) = ByteArray(32) { hex.substring(2 * it, 2 * it + 2).toInt(16).toByte() }
    val digest = newSnapshotDigest()
    for (value in listOf(previous, proof)) {
        digest.append(bootstrapBigEndian(32, 8))
        digest.append(bytes(value))
    }
    return digest.finishHex()
}

internal fun bootstrapCheckedAdd(left: Long, right: Long): Long {
    require(left >= 0 && right >= 0 && left <= Long.MAX_VALUE - right) { "bootstrap length overflow" }
    return left + right
}

internal fun bootstrapBigEndian(value: Long, width: Int): ByteArray {
    require(width == 4 || width == 8)
    return ByteArray(width) { (value ushr ((width - 1 - it) * 8)).toByte() }
}

internal fun bootstrapCanonicalStrings(values: List<String>): String =
    canonicalizeJsonElement(JsonArray(values.map(::JsonPrimitive)))

internal fun bootstrapFramedHash(bytes: ByteArray): String = newSnapshotDigest().let {
    it.append(bootstrapBigEndian(bytes.size.toLong(), 8)); it.append(bytes); it.finishHex()
}

/** Independent received wire-page proof; never finalizes the ordinary snapshot digest. */
internal class BootstrapWirePageHashWriter(
    private val owner: Any,
    identity: BootstrapProjectionIdentity,
    private val range: LongRange,
) {
    private var digest: SnapshotDigest? = newSnapshotDigest()
    private var rows = 0L
    private var bytes = 0L

    init {
        require(range.first > 0 && range.last >= range.first)
        frame(identity.header("oversqlite.bootstrap.page.wire.v1", range))
    }

    fun add(candidate: Any, ordinal: Long, completeFrame: ByteArray, logicalBytes: Long) {
        check(owner === candidate && digest != null) { "wire page belongs to a different or expired writer" }
        try {
            require(ordinal == range.first + rows && ordinal <= range.last && logicalBytes >= 0)
            requireSnapshotFrameLength(completeFrame)
            bytes = bootstrapCheckedAdd(bytes, logicalBytes)
            rows = bootstrapCheckedAdd(rows, 1)
            checkNotNull(digest).append(completeFrame)
        } catch (failure: Throwable) { close(); throw failure }
    }

    fun finish(candidate: Any, rowCount: Long, logicalBytes: Long): String {
        check(owner === candidate && digest != null) { "wire page belongs to a different or expired writer" }
        try {
            require(rowCount == rows && rows == range.last - range.first + 1 && logicalBytes == bytes)
            frame(bootstrapCanonicalStrings(listOf("end", rows.toString(), bytes.toString())))
            return checkNotNull(digest).finishHex().also { close() }
        } catch (failure: Throwable) { close(); throw failure }
    }

    fun close() { digest = null }
    private fun frame(value: String) {
        val encoded = value.encodeToByteArray()
        checkNotNull(digest).append(bootstrapBigEndian(encoded.size.toLong(), 8))
        checkNotNull(digest).append(encoded)
    }
}
