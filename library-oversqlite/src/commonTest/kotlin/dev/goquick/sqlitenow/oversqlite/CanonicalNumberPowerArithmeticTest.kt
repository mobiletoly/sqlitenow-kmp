package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.Json
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The reference binary64 renderer ([referenceCanonicalBinary64]), the oracle for production rendering,
 * expands a value exactly as significand × 2^e or significand × 5^e. These tests hold that expansion to
 * digit-by-digit decimal arithmetic across every exponent a finite binary64 value can produce, so the
 * oracle itself stays exact.
 */
class CanonicalNumberPowerArithmeticTest {
    private fun decimalStringTimes(value: String, multiplier: Int): String {
        val out = StringBuilder(value.length + 1)
        var carry = 0
        for (index in value.indices.reversed()) {
            val product = (value[index] - '0') * multiplier + carry
            out.append(('0'.code + product % 10).toChar())
            carry = product / 10
        }
        while (carry > 0) {
            out.append(('0'.code + carry % 10).toChar())
            carry /= 10
        }
        return out.reverse().toString()
    }

    private fun decimalStringTimesPower(initial: String, multiplier: Int, exponent: Int): String {
        var result = initial
        repeat(exponent) { result = decimalStringTimes(result, multiplier) }
        return result
    }

    // 2^e reaches 971 for the largest finite value; 5^e reaches 1074 for subnormals.
    private val largestExponent = mapOf(2 to 971, 5 to 1074)
    private val smallestSignificand = "1"
    private val largestSignificand = "9007199254740991" // 2^53 - 1

    @Test
    fun everyExponentMatchesDecimalStringArithmetic() {
        for ((multiplier, last) in largestExponent) {
            for (significand in listOf(smallestSignificand, largestSignificand)) {
                var expected = significand
                for (exponent in 0..last) {
                    assertEquals(expected, referenceMultiplyPower(significand, multiplier, exponent), "$significand × $multiplier^$exponent")
                    expected = decimalStringTimes(expected, multiplier)
                }
            }
        }
    }

    @Test
    fun randomSignificandsMatchDecimalStringArithmetic() {
        val random = Random(20261111)
        repeat(400) { case ->
            val multiplier = if (random.nextBoolean()) 2 else 5
            val last = largestExponent.getValue(multiplier)
            // Most real values sit near exponent 50; every tenth case covers the whole range.
            val exponent = if (case % 10 == 0) random.nextInt(last + 1) else random.nextInt(minOf(last, 140) + 1)
            val significand = random.nextLong(1L, 1L shl 53).toString()
            assertEquals(
                decimalStringTimesPower(significand, multiplier, exponent),
                referenceMultiplyPower(significand, multiplier, exponent),
                "$significand × $multiplier^$exponent",
            )
        }
    }

    @Test
    fun significandsOfEveryDecimalLengthMatch() {
        // Limb grouping must not depend on how many digits the significand has.
        for (length in 1..largestSignificand.length) {
            val significand = largestSignificand.take(length)
            for (multiplier in listOf(2, 5)) for (exponent in listOf(0, 1, 6, 7, 8, 16, 17, 18, 50, 51, 52, 53)) {
                assertEquals(
                    decimalStringTimesPower(significand, multiplier, exponent),
                    referenceMultiplyPower(significand, multiplier, exponent),
                    "$significand × $multiplier^$exponent",
                )
            }
        }
    }

    @Test
    fun everydayValuesKeepTheirCanonicalSpelling() {
        val vectors = listOf(
            "4.25" to "4.25", "1.5" to "1.5", "0.1" to "0.1", "-0.1" to "-0.1", "0.30000000000000004" to "0.30000000000000004",
            "37.7749" to "37.7749", "-122.4194" to "-122.4194", "19.99" to "19.99", "123456.789" to "123456.789",
            "4.0" to "4", "2.50" to "2.5", "1e-10" to "1e-10", "6.02214076e23" to "6.02214076e+23",
            "2.2250738585072014e-308" to "2.2250738585072014e-308", "4.9e-324" to "5e-324",
            "3.141592653589793" to "3.141592653589793", "0.000001234" to "0.000001234", "1234567890.125" to "1234567890.125",
        )
        for ((input, expected) in vectors) {
            assertEquals("{\"n\":$expected}", canonicalizeJsonElement(Json.parseToJsonElement("{\"n\":$input}")), input)
        }
    }
}
