package dev.goquick.sqlitenow.oversqlite

import java.security.MessageDigest
import kotlin.test.*

/** Run each method in a fresh instrumentation process. Fixed generic bytes, no application data. */
internal class SnapshotDigestTimingTest {
    @Test fun portable() = witness("portable", ::PortableSnapshotDigest)
    @Test fun platform() = witness("platform", ::newSnapshotDigest)

    private fun witness(label: String, factory: () -> SnapshotDigest) {
        val block = ByteArray(65536) { (it % 251).toByte() }
        fun run(blocks: Int): String {
            val digest = factory()
            repeat(blocks) { digest.append(block) }
            return digest.finishHex()
        }
        repeat(2) { run(16) }
        val expected = MessageDigest.getInstance("SHA-256").let { digest ->
            repeat(128) { digest.update(block) }; bytesToHexLower(digest.digest())
        }
        val elapsed = List(3) {
            val start = System.nanoTime(); val result = run(128); val ns = System.nanoTime() - start
            assertEquals(expected, result); ns
        }
        println("DIGEST_TIMING implementation=$label bytes=8388608 samples_ns=${elapsed.joinToString(",")} provider=${MessageDigest.getInstance("SHA-256").provider.name}")
    }
}
