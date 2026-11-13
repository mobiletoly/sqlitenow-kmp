package dev.goquick.sqlitenow.oversqlite

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.convert
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.sizeOf
import kotlinx.cinterop.usePinned
import platform.CoreCrypto.CC_SHA256_CTX
import platform.CoreCrypto.CC_SHA256_DIGEST_LENGTH
import platform.CoreCrypto.CC_SHA256_Final
import platform.CoreCrypto.CC_SHA256_Init
import platform.CoreCrypto.CC_SHA256_Update

internal actual fun newSnapshotDigest(): SnapshotDigest = CommonCryptoSnapshotDigest()

/**
 * SHA-256 by the platform's CommonCrypto, which uses the CPU's SHA instructions.
 *
 * The hashing state is a plain C struct with no pointers into itself. It is kept in a Kotlin byte
 * array and pinned only for the length of each call, so an abandoned digest is ordinary garbage
 * and nothing has to be freed. Small appends are gathered in a buffer first, because the restore
 * path appends a few bytes at a time and each platform call has a fixed cost.
 */
@OptIn(ExperimentalForeignApi::class)
private class CommonCryptoSnapshotDigest : SnapshotDigest() {
    private var state: ByteArray? = ByteArray(sizeOf<CC_SHA256_CTX>().toInt()).also { bytes ->
        bytes.usePinned { CC_SHA256_Init(it.addressOf(0).reinterpret()) }
    }
    private val pending = ByteArray(pendingCapacity)
    private var pendingSize = 0

    override fun updateBytes(bytes: ByteArray) {
        if (bytes.size >= pendingCapacity) {
            flushPending()
            update(bytes, bytes.size)
            return
        }
        if (bytes.size > pendingCapacity - pendingSize) flushPending()
        bytes.copyInto(pending, pendingSize)
        pendingSize += bytes.size
    }

    override fun updateAscii(value: Char) {
        if (pendingSize == pendingCapacity) flushPending()
        pending[pendingSize++] = value.code.toByte()
    }

    override fun finishDigestHex(): String = try {
        flushPending()
        val digest = ByteArray(CC_SHA256_DIGEST_LENGTH)
        checkNotNull(state).usePinned { context ->
            digest.usePinned { CC_SHA256_Final(it.addressOf(0).reinterpret(), context.addressOf(0).reinterpret()) }
        }
        bytesToHexLower(digest)
    } finally {
        state = null
    }

    private fun flushPending() {
        if (pendingSize == 0) return
        update(pending, pendingSize)
        pendingSize = 0
    }

    private fun update(bytes: ByteArray, size: Int) {
        if (size == 0) return
        checkNotNull(state).usePinned { context ->
            bytes.usePinned { CC_SHA256_Update(context.addressOf(0).reinterpret(), it.addressOf(0), size.convert()) }
        }
    }

    private companion object {
        const val pendingCapacity = 4096
    }
}
