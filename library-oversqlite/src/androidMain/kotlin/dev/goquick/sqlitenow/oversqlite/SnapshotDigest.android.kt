package dev.goquick.sqlitenow.oversqlite

import java.security.MessageDigest

internal actual fun newSnapshotDigest(): SnapshotDigest = object : SnapshotDigest() {
    private var digest: MessageDigest? = MessageDigest.getInstance("SHA-256")
    override fun updateBytes(bytes: ByteArray) { checkNotNull(digest).update(bytes) }
    override fun updateAscii(value: Char) { checkNotNull(digest).update(value.code.toByte()) }
    override fun finishDigestHex(): String = try { bytesToHexLower(checkNotNull(digest).digest()) }
        finally { digest = null }
}
