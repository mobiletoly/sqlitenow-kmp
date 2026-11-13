package dev.goquick.sqlitenow.oversqlite

import kotlin.test.*

internal class SnapshotDigestTest {
    @Test fun independentVectorsSegmentedUpdatesAndPortableParity() {
        // Fixed answers generated independently with Python hashlib/OpenSSL.
        val vectors = mapOf(
            0 to "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            1 to "6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d",
            55 to "463eb28e72f82e0a96c0a4cc53690c571281131f672aa229e0d45ae59b598b59",
            56 to "da2ae4d6b36748f2a318f23e7ab1dfdf45acdc9d049bd80e59de82a60895f562",
            63 to "29af2686fd53374a36b0846694cc342177e428d1647515f078784d69cdb9e488",
            64 to "fdeab9acf3710362bd2658cdc9a29e8f9c757fcf9811603a8c447cd1d9151108",
            65 to "4bfd2c8b6f1eec7a2afeb48b934ee4b2694182027e6d0fc075074f2fabb31781",
            4097 to "a16560d668b843fb3be99ace41dbd18471f342bd3255a1d21204b35e43f74436",
            1000000 to "2c030d49ec131bfbbb446ad21e7a2f12cdb4f2f4f3fda3ac709dd2e68a4646c7"
        )
        for ((size, expected) in vectors) for (factory in listOf(::newSnapshotDigest, ::PortableSnapshotDigest)) {
            val bytes = ByteArray(size) { (it % 251).toByte() }
            val whole = factory(); whole.append(bytes); assertEquals(expected, whole.finishHex())
            for (segment in listOf(1, 55, 64, 997)) {
                val digest = factory()
                var offset = 0
                while (offset < bytes.size) {
                    val end = minOf(bytes.size, offset + segment)
                    digest.append(bytes.copyOfRange(offset, end)); offset = end
                }
                assertEquals(expected, digest.finishHex(), "size=$size segment=$segment")
            }
        }
    }
    @Test fun mixedSmallAndLargeAppendsMatchThePortableDigest() {
        // The platform digests may gather small appends before hashing; every way of cutting the
        // same input, across and around any internal buffer size, must give the portable result.
        val bytes = ByteArray(40_000) { (it * 31 % 127).toByte() }
        val expected = PortableSnapshotDigest().also { it.append(bytes) }.finishHex()
        for (segment in listOf(4095, 4096, 4097, 8191, 8192, 8193, 12_289)) {
            val digest = newSnapshotDigest()
            var offset = 0
            while (offset < bytes.size) {
                val end = minOf(bytes.size, offset + segment)
                digest.append(bytes.copyOfRange(offset, end)); offset = end
            }
            assertEquals(expected, digest.finishHex(), "segment=$segment")
        }
        var seed = 0x2545F491L
        repeat(40) { round ->
            val digest = newSnapshotDigest()
            var offset = 0
            while (offset < bytes.size) {
                seed = seed * 6364136223846793005L + 1442695040888963407L
                val pick = (seed ushr 33).toInt()
                when (pick.mod(4)) {
                    0 -> { digest.appendAscii(bytes[offset].toInt().toChar()); offset++ }
                    1 -> { digest.append(byteArrayOf()); val end = minOf(bytes.size, offset + 1 + pick.mod(40)); digest.append(bytes.copyOfRange(offset, end)); offset = end }
                    2 -> { val end = minOf(bytes.size, offset + 1 + pick.mod(700)); digest.append(bytes.copyOfRange(offset, end)); offset = end }
                    else -> { val end = minOf(bytes.size, offset + 3_000 + pick.mod(6_000)); digest.append(bytes.copyOfRange(offset, end)); offset = end }
                }
            }
            assertEquals(expected, digest.finishHex(), "round=$round")
        }
    }
    @Test fun singleByteAppendsAcrossABufferBoundaryMatchThePortableDigest() {
        // Sizes chosen around 4096, a typical gathering buffer: finishing with it exactly full,
        // an ASCII append arriving when it is full, and long runs of ASCII appends alone.
        fun digestOf(factory: () -> SnapshotDigest, bytes: Int, asciiAfter: Int, viaAscii: Boolean): String {
            val digest = factory()
            repeat(bytes) { index ->
                val value = index % 90 + 32 // printable ASCII
                if (viaAscii) digest.appendAscii(value.toChar()) else digest.append(byteArrayOf(value.toByte()))
            }
            repeat(asciiAfter) { digest.appendAscii(']') }
            return digest.finishHex()
        }
        for (bytes in listOf(4095, 4096, 4097, 8192)) for (asciiAfter in listOf(0, 1, 2)) for (viaAscii in listOf(false, true)) {
            assertEquals(
                digestOf(::PortableSnapshotDigest, bytes, asciiAfter, viaAscii),
                digestOf(::newSnapshotDigest, bytes, asciiAfter, viaAscii),
                "bytes=$bytes asciiAfter=$asciiAfter viaAscii=$viaAscii",
            )
        }
        assertEquals(digestOf(::PortableSnapshotDigest, 5000, 5000, true), digestOf(::newSnapshotDigest, 5000, 5000, true))
    }
    @Test fun asciiFinalizationAndIndependentWriters() {
        for (factory in listOf(::newSnapshotDigest, ::PortableSnapshotDigest)) {
            val digest = factory(); val other = factory()
            digest.appendAscii('a'); digest.append("bc".encodeToByteArray())
            assertEquals("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", digest.finishHex())
            assertFailsWith<IllegalStateException> { digest.finishHex() }
            assertFailsWith<IllegalStateException> { digest.append(byteArrayOf()) }
            assertFailsWith<IllegalStateException> { digest.appendAscii('a') }
            assertFailsWith<IllegalArgumentException> { other.appendAscii('é') }
            assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", other.finishHex())
        }
    }
    @Test fun providerFailuresPropagateAndFinalizationFailureIsTerminal() {
        val failure = IllegalStateException("provider failure")
        val digest = object : SnapshotDigest() {
            override fun updateBytes(bytes: ByteArray): Unit = throw failure
            override fun updateAscii(value: Char): Unit = throw failure
            override fun finishDigestHex(): String = throw failure
        }
        assertSame(failure, assertFails { digest.append(byteArrayOf(1)) })
        assertSame(failure, assertFails { digest.appendAscii('a') })
        assertSame(failure, assertFails { digest.finishHex() })
        assertNotSame(failure, assertFails { digest.finishHex() })
        assertNotSame(failure, assertFails { digest.append(byteArrayOf()) })
    }
}
