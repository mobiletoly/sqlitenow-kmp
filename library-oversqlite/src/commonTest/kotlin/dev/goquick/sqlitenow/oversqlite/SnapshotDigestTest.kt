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
