package dev.goquick.sqlitenow.oversqlite

import kotlin.math.pow
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.fail

/**
 * Production renders a finite binary64 value with integer arithmetic. It must produce exactly the text
 * of the reference string-arithmetic renderer for every value: a single differing character changes a
 * bundle hash shared with the server and the other clients.
 */
class CanonicalBinary64RenderingTest {
    private val maxFiniteBits = 0x7fefffffffffffffL

    private fun assertMatchesReference(bitPatterns: Sequence<Long>) {
        val mismatches = mutableListOf<String>()
        var checked = 0
        for (bits in bitPatterns.filter { it in 1L..maxFiniteBits }.distinct()) {
            for (value in listOf(Double.fromBits(bits), -Double.fromBits(bits))) {
                val expected = referenceCanonicalBinary64(value)
                val actual = canonicalizeBinary64(value)
                checked++
                if (actual != expected && mismatches.size < 10) {
                    mismatches += "bits=0x${value.toRawBits().toULong().toString(16)} expected=$expected actual=$actual"
                }
            }
        }
        if (mismatches.isNotEmpty()) fail("rendering differs from the reference in $checked values checked:\n" + mismatches.joinToString("\n"))
        check(checked > 0)
    }

    private fun around(bits: Long, radius: Int = 1) = (-radius..radius).asSequence().map { bits + it }

    @Test
    fun everyBinadeBoundaryAndItsNeighboursMatch() {
        // Exponent changes are where the lower neighbour is closer than the upper one.
        assertMatchesReference((0L..2046L).asSequence().flatMap { exponent -> around(exponent shl 52) })
    }

    @Test
    fun everyPowerOfTenAndItsNeighboursMatch() {
        assertMatchesReference((-323..308).asSequence().flatMap { power -> around("1e$power".toDouble().toBits(), 2) })
    }

    @Test
    fun extremesMatch() {
        assertMatchesReference((1L..64L).asSequence() + around(maxFiniteBits - 3, 3) +
            sequenceOf(0x0010000000000000L, 0x000fffffffffffffL, 1.0.toBits(), 0.1.toBits(), (2.0 / 3.0).toBits()))
    }

    @Test
    fun randomBitPatternsMatch() {
        val random = Random(20261112)
        assertMatchesReference(generateSequence { random.nextLong() and Long.MAX_VALUE }.take(1500))
    }

    @Test
    fun randomSubnormalsMatch() {
        val random = Random(1074)
        assertMatchesReference(generateSequence { random.nextLong(1L, 1L shl 52) }.take(300))
    }

    @Test
    fun everydayMagnitudesMatch() {
        // Values like prices, ratios and measurements, where real payloads concentrate.
        val random = Random(8785)
        assertMatchesReference(generateSequence {
            (random.nextDouble() * 10.0.pow(random.nextInt(-8, 12))).toBits()
        }.take(1500))
    }

    @Test
    fun rendersRfc8785AppendixBValues() {
        // Spot checks on the public entry point; BundleHashTest holds the full appendix B list.
        assertEquals("5e-324", canonicalizeBinary64(Double.fromBits(1)))
        assertEquals("1.7976931348623157e+308", canonicalizeBinary64(Double.fromBits(maxFiniteBits)))
        assertEquals("-0.0000033333333333333333", canonicalizeBinary64(-0.0000033333333333333333))
        assertEquals("0", canonicalizeBinary64(-0.0))
        assertEquals("1e+21", canonicalizeBinary64(1e21))
    }
}
