package dev.goquick.sqlitenow.oversqlite

/** One writer, bounded streaming state, and exactly one finalization attempt. */
internal abstract class SnapshotDigest {
    private var finished = false
    fun append(bytes: ByteArray) {
        check(!finished) { "snapshot digest is finalized" }
        updateBytes(bytes)
    }
    fun appendAscii(value: Char) {
        check(!finished) { "snapshot digest is finalized" }
        require(value.code <= 127) { "ASCII byte required" }
        updateAscii(value)
    }
    fun finishHex(): String {
        check(!finished) { "snapshot digest is finalized" }
        finished = true
        return finishDigestHex()
    }
    protected abstract fun updateBytes(bytes: ByteArray)
    protected abstract fun updateAscii(value: Char)
    protected abstract fun finishDigestHex(): String
}

internal expect fun newSnapshotDigest(): SnapshotDigest

/** Native/web fallback; other integrity hash callers keep their existing accumulator. */
internal class PortableSnapshotDigest : SnapshotDigest() {
    private var accumulator: Sha256Accumulator? = Sha256Accumulator()
    override fun updateBytes(bytes: ByteArray) = checkNotNull(accumulator).append(bytes)
    override fun updateAscii(value: Char) = checkNotNull(accumulator).appendAscii(value)
    override fun finishDigestHex(): String = try { checkNotNull(accumulator).finishHex() }
        finally { accumulator = null }
}
