/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.oversqlite

import kotlin.math.abs
import kotlin.math.floor
import kotlin.math.log10
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

internal fun canonicalizeJsonElement(value: JsonElement): String = when (value) {
    is JsonObject -> value.entries
        .sortedBy { it.key }
        .joinToString(separator = ",", prefix = "{", postfix = "}") { (key, element) ->
            "${canonicalizeJsonString(key)}:${canonicalizeJsonElement(element)}"
        }

    is JsonArray -> value.joinToString(separator = ",", prefix = "[", postfix = "]") { element ->
        canonicalizeJsonElement(element)
    }

    is JsonPrimitive -> canonicalizeJsonPrimitive(value)
}

private fun canonicalizeJsonPrimitive(value: JsonPrimitive): String {
    if (value is JsonNull) return "null"
    if (value.isString) {
        return canonicalizeJsonString(value.content)
    }
    return when (value.content) {
        "true", "false" -> value.content
        else -> canonicalizeJsonNumber(value.content)
    }
}

internal fun requireUnicodeScalarString(value: String) {
    var index = 0
    while (index < value.length) {
        val ch = value[index]
        if (ch in '\uDC00'..'\uDFFF') {
            require(index > 0 && value[index - 1] in '\uD800'..'\uDBFF') { "invalid unpaired low surrogate" }
        }
        if (ch in '\uD800'..'\uDBFF') {
            require(index + 1 < value.length && value[index + 1] in '\uDC00'..'\uDFFF') {
                "invalid unpaired high surrogate"
            }
        }
        index++
    }
}

private fun canonicalizeJsonString(value: String): String {
    requireUnicodeScalarString(value)
    val out = StringBuilder(value.length + 2)
    out.append('"')
    var index = 0
    while (index < value.length) {
        val ch = value[index]
        when (ch) {
            '\\' -> out.append("\\\\")
            '"' -> out.append("\\\"")
            '\b' -> out.append("\\b")
            '\u000c' -> out.append("\\f")
            '\n' -> out.append("\\n")
            '\r' -> out.append("\\r")
            '\t' -> out.append("\\t")
            else -> {
                if (ch.code < 0x20) {
                    out.append("\\u")
                    out.append(ch.code.toString(16).padStart(4, '0'))
                } else {
                    out.append(ch)
                }
            }
        }
        index++
    }
    out.append('"')
    return out.toString()
}

internal fun sha256Hex(bytes: ByteArray): String {
    val padded = padSha256(bytes)
    var h0 = 0x6a09e667
    var h1 = -0x4498517b
    var h2 = 0x3c6ef372
    var h3 = -0x5ab00ac6
    var h4 = 0x510e527f
    var h5 = -0x64fa9774
    var h6 = 0x1f83d9ab
    var h7 = 0x5be0cd19

    val w = IntArray(64)
    var offset = 0
    while (offset < padded.size) {
        var index = 0
        while (index < 16) {
            val base = offset + index * 4
            w[index] = ((padded[base].toInt() and 0xff) shl 24) or
                ((padded[base + 1].toInt() and 0xff) shl 16) or
                ((padded[base + 2].toInt() and 0xff) shl 8) or
                (padded[base + 3].toInt() and 0xff)
            index++
        }
        while (index < 64) {
            val s0 = w[index - 15].rotateRight(7) xor w[index - 15].rotateRight(18) xor (w[index - 15] ushr 3)
            val s1 = w[index - 2].rotateRight(17) xor w[index - 2].rotateRight(19) xor (w[index - 2] ushr 10)
            w[index] = w[index - 16] + s0 + w[index - 7] + s1
            index++
        }

        var a = h0
        var b = h1
        var c = h2
        var d = h3
        var e = h4
        var f = h5
        var g = h6
        var h = h7

        index = 0
        while (index < 64) {
            val s1 = e.rotateRight(6) xor e.rotateRight(11) xor e.rotateRight(25)
            val ch = (e and f) xor (e.inv() and g)
            val temp1 = h + s1 + ch + SHA256_K[index] + w[index]
            val s0 = a.rotateRight(2) xor a.rotateRight(13) xor a.rotateRight(22)
            val maj = (a and b) xor (a and c) xor (b and c)
            val temp2 = s0 + maj

            h = g
            g = f
            f = e
            e = d + temp1
            d = c
            c = b
            b = a
            a = temp1 + temp2
            index++
        }

        h0 += a
        h1 += b
        h2 += c
        h3 += d
        h4 += e
        h5 += f
        h6 += g
        h7 += h
        offset += 64
    }

    val digest = ByteArray(32)
    writeIntBigEndian(digest, 0, h0)
    writeIntBigEndian(digest, 4, h1)
    writeIntBigEndian(digest, 8, h2)
    writeIntBigEndian(digest, 12, h3)
    writeIntBigEndian(digest, 16, h4)
    writeIntBigEndian(digest, 20, h5)
    writeIntBigEndian(digest, 24, h6)
    writeIntBigEndian(digest, 28, h7)
    return bytesToHexLower(digest)
}

internal class Sha256Accumulator {
    private val block = ByteArray(64)
    private val words = IntArray(64)
    private var used = 0
    private var byteCount = 0L
    private var h0 = 0x6a09e667
    private var h1 = -0x4498517b
    private var h2 = 0x3c6ef372
    private var h3 = -0x5ab00ac6
    private var h4 = 0x510e527f
    private var h5 = -0x64fa9774
    private var h6 = 0x1f83d9ab
    private var h7 = 0x5be0cd19

    fun appendAscii(value: Char) {
        require(value.code < 0x80)
        appendByte(value.code.toByte())
    }

    fun append(bytes: ByteArray) {
        var offset = 0
        while (offset < bytes.size) {
            val count = minOf(block.size - used, bytes.size - offset)
            bytes.copyInto(block, used, offset, offset + count)
            used += count; offset += count; byteCount += count
            if (used == block.size) { compress(); used = 0 }
        }
    }

    private fun appendByte(value: Byte) {
        block[used++] = value
        byteCount++
        if (used == block.size) {
            compress()
            used = 0
        }
    }

    fun finishHex(): String {
        val bitCount = byteCount * 8L
        block[used++] = 0x80.toByte()
        if (used > 56) {
            while (used < block.size) block[used++] = 0
            compress()
            used = 0
        }
        while (used < 56) block[used++] = 0
        for (shift in 56 downTo 0 step 8) block[used++] = (bitCount ushr shift).toByte()
        compress()
        val digest = ByteArray(32)
        writeIntBigEndian(digest, 0, h0)
        writeIntBigEndian(digest, 4, h1)
        writeIntBigEndian(digest, 8, h2)
        writeIntBigEndian(digest, 12, h3)
        writeIntBigEndian(digest, 16, h4)
        writeIntBigEndian(digest, 20, h5)
        writeIntBigEndian(digest, 24, h6)
        writeIntBigEndian(digest, 28, h7)
        return bytesToHexLower(digest)
    }

    private fun compress() {
        for (index in 0 until 16) {
            val offset = index * 4
            words[index] = ((block[offset].toInt() and 0xff) shl 24) or
                ((block[offset + 1].toInt() and 0xff) shl 16) or
                ((block[offset + 2].toInt() and 0xff) shl 8) or
                (block[offset + 3].toInt() and 0xff)
        }
        for (index in 16 until 64) {
            val s0 = words[index - 15].rotateRight(7) xor words[index - 15].rotateRight(18) xor
                (words[index - 15] ushr 3)
            val s1 = words[index - 2].rotateRight(17) xor words[index - 2].rotateRight(19) xor
                (words[index - 2] ushr 10)
            words[index] = words[index - 16] + s0 + words[index - 7] + s1
        }
        var a = h0
        var b = h1
        var c = h2
        var d = h3
        var e = h4
        var f = h5
        var g = h6
        var h = h7
        for (index in 0 until 64) {
            val s1 = e.rotateRight(6) xor e.rotateRight(11) xor e.rotateRight(25)
            val choice = (e and f) xor (e.inv() and g)
            val temp1 = h + s1 + choice + SHA256_K[index] + words[index]
            val s0 = a.rotateRight(2) xor a.rotateRight(13) xor a.rotateRight(22)
            val majority = (a and b) xor (a and c) xor (b and c)
            val temp2 = s0 + majority
            h = g
            g = f
            f = e
            e = d + temp1
            d = c
            c = b
            b = a
            a = temp1 + temp2
        }
        h0 += a
        h1 += b
        h2 += c
        h3 += d
        h4 += e
        h5 += f
        h6 += g
        h7 += h
    }
}

private fun padSha256(bytes: ByteArray): ByteArray {
    val bitLength = bytes.size.toLong() * 8L
    val finalLength = ((bytes.size + 9 + 63) / 64) * 64
    val out = ByteArray(finalLength)
    bytes.copyInto(out, 0, 0, bytes.size)
    out[bytes.size] = 0x80.toByte()
    var index = out.size - 8
    while (index < out.size) {
        val shift = (out.size - 1 - index) * 8
        out[index] = ((bitLength ushr shift) and 0xff).toByte()
        index++
    }
    return out
}

private fun writeIntBigEndian(target: ByteArray, offset: Int, value: Int) {
    target[offset] = (value ushr 24).toByte()
    target[offset + 1] = (value ushr 16).toByte()
    target[offset + 2] = (value ushr 8).toByte()
    target[offset + 3] = value.toByte()
}

private fun Int.rotateRight(distance: Int): Int =
    (this ushr distance) or (this shl (32 - distance))

private const val MAX_SAFE_JSON_INTEGER = 9_007_199_254_740_991L

internal fun canonicalizeJsonNumber(raw: String): String {
    // Exact integers below2^53 already have their ECMAScript decimal spelling.
    // Equality excludes alternate lexemes; their rounding and signed-zero rules stay on the general path.
    if (raw.length <= 17) {
        val integer = raw.toLongOrNull()
        if (integer != null && integer in -MAX_SAFE_JSON_INTEGER..MAX_SAFE_JSON_INTEGER && integer.toString() == raw) {
            return raw
        }
    }
    val number = raw.toDoubleOrNull()
        ?: throw IllegalArgumentException("invalid JSON number: $raw")
    require(number.isFinite()) { "JSON number must be finite: $raw" }
    return canonicalizeBinary64(number)
}

private data class ExactDecimal(val digits: String, val scale: Int)

private const val BINARY64_FRACTION_MASK = 0x000fffffffffffffL
private const val BINARY64_MAX_FINITE_BITS = 0x7fefffffffffffffL

/** RFC 8785 rendering (ECMAScript Number::toString) of a finite binary64 value. */
internal fun canonicalizeBinary64(value: Double): String {
    require(value.isFinite()) { "JSON number must be finite: $value" }
    if (value == 0.0) return "0"
    return renderEcmaScriptBinary64(value, abs(value))
}

private fun binary64Significand(bits: Long): Long =
    if (((bits ushr 52) and 0x7ffL) == 0L) bits and BINARY64_FRACTION_MASK else (bits and BINARY64_FRACTION_MASK) or (1L shl 52)

private fun binary64Exponent(bits: Long): Int {
    val exponentBits = ((bits ushr 52) and 0x7ffL).toInt()
    return if (exponentBits == 0) -1074 else exponentBits - 1075
}

/**
 * Shortest decimal that reads back as [value], choosing the closest such decimal and, on a tie, the
 * even one. Every quantity is an exact integer, so no decimal string is built until the result.
 *
 * The rounding interval is bounded by the midpoints to the neighbouring doubles (inclusive when the
 * significand is even). For each precision from 1 to 17 the value's round-half-even digits, then one
 * below and one above, are tried; the first strictly inside the interval is the result.
 */
private fun renderEcmaScriptBinary64(value: Double, magnitude: Double): String {
    val bits = magnitude.toBits()
    val previous = bits - 1
    val hasNext = bits != BINARY64_MAX_FINITE_BITS
    // Express the value and both neighbours as integers in units of 2^unit; the extra halving step
    // keeps the midpoints integral.
    var unit = minOf(binary64Exponent(previous), binary64Exponent(bits))
    if (hasNext) unit = minOf(unit, binary64Exponent(bits + 1))
    unit -= 1
    fun scaled(of: Long) = ExactInteger().also { it.setLong(binary64Significand(of)); it.shiftLeft(binary64Exponent(of) - unit) }
    val exact = scaled(bits)
    val lower = scaled(previous).also { it.add(exact); it.shiftRightOne() }
    val upper = if (hasNext) {
        scaled(bits + 1).also { it.add(exact); it.shiftRightOne() }
    } else {
        ExactInteger().also { it.copyFrom(exact); it.subtract(scaled(previous)); it.shiftRightOne(); it.add(exact) }
    }
    // A quantity q stands for q / denominator; both sides are integers.
    val up = maxOf(unit, 0)
    val denominator = ExactInteger().also { it.setLong(1); it.shiftLeft(maxOf(-unit, 0)) }
    exact.shiftLeft(up); lower.shiftLeft(up); upper.shiftLeft(up)
    val boundariesInclusive = binary64Significand(bits) and 1L == 0L

    // Scientific exponent: 10^e <= value < 10^(e+1).
    val left = ExactInteger(); val right = ExactInteger()
    fun compareToPowerOfTen(power: Int): Int {
        left.copyFrom(exact); left.multiplyPowerOfTen(maxOf(-power, 0))
        right.copyFrom(denominator); right.multiplyPowerOfTen(maxOf(power, 0))
        return left.compareTo(right)
    }
    var scientificExponent = floor(log10(magnitude)).toInt()
    while (compareToPowerOfTen(scientificExponent) < 0) scientificExponent--
    while (compareToPowerOfTen(scientificExponent + 1) >= 0) scientificExponent++

    // The first 18 significant digits of the exact value and whether any further digit is non-zero.
    val digits = IntArray(18)
    val remainder = ExactInteger().also { it.copyFrom(exact); it.multiplyPowerOfTen(maxOf(-scientificExponent, 0)) }
    val divisor = ExactInteger().also { it.copyFrom(denominator); it.multiplyPowerOfTen(maxOf(scientificExponent, 0)) }
    for (index in digits.indices) {
        var digit = 0
        while (remainder >= divisor) { remainder.subtract(divisor); digit++ }
        check(digit <= 9 && (index > 0 || digit > 0)) { "binary64 digit generation failed" }
        digits[index] = digit
        remainder.multiplySmall(10)
    }
    val sticky = !remainder.isZero()

    val candidateValue = ExactInteger(); val low = ExactInteger(); val high = ExactInteger()
    var prefix = 0L
    for (precision in 1..17) {
        prefix = prefix * 10 + digits[precision - 1]
        val next = digits[precision]
        var remainingNonZero = sticky
        for (index in precision + 1 until digits.size) if (digits[index] != 0) remainingNonZero = true
        val roundUp = next > 5 || (next == 5 && (remainingNonZero || prefix and 1L == 1L))
        val nearest = prefix + if (roundUp) 1L else 0L
        val power = scientificExponent - precision + 1
        low.copyFrom(lower); low.multiplyPowerOfTen(maxOf(-power, 0))
        high.copyFrom(upper); high.multiplyPowerOfTen(maxOf(-power, 0))
        for (candidate in longArrayOf(nearest, nearest - 1, nearest + 1)) {
            if (candidate == 0L) continue
            candidateValue.setLong(candidate); candidateValue.multiplyPowerOfTen(maxOf(power, 0))
            candidateValue.multiplyBy(denominator)
            val lowerComparison = candidateValue.compareTo(low)
            val upperComparison = candidateValue.compareTo(high)
            val inside = (lowerComparison > 0 || boundariesInclusive && lowerComparison == 0) &&
                (upperComparison < 0 || boundariesInclusive && upperComparison == 0)
            if (inside) {
                var significand = candidate
                var scale = -power
                while (significand % 10 == 0L) { significand /= 10; scale-- }
                return renderJcsDecimal(ExactDecimal(significand.toString(), scale), value < 0.0)
            }
        }
    }
    error("unable to render finite binary64 value $value")
}

private fun renderJcsDecimal(value: ExactDecimal, negative: Boolean): String {
    val exponent = value.digits.length - value.scale - 1
    val sign = if (negative) "-" else ""
    if (exponent in -6..20) {
        val decimalIndex = value.digits.length - value.scale
        return sign + when {
            decimalIndex <= 0 -> "0." + "0".repeat(-decimalIndex) + value.digits
            decimalIndex >= value.digits.length -> value.digits + "0".repeat(decimalIndex - value.digits.length)
            else -> value.digits.substring(0, decimalIndex) + "." + value.digits.substring(decimalIndex)
        }
    }
    val mantissa = if (value.digits.length == 1) value.digits else value.digits.first() + "." + value.digits.substring(1)
    return "$sign$mantissa" + "e${if (exponent >= 0) "+" else ""}$exponent"
}

private const val WORD_MASK = 0xffffffffL
private val SMALL_POWERS_OF_TEN = intArrayOf(1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000)

/** Non-negative integer in little-endian base-2^32 words, mutated in place to keep rendering allocation small. */
private class ExactInteger {
    private var words = IntArray(4)
    private var size = 0

    fun setLong(value: Long) {
        require(value >= 0)
        words[0] = value.toInt(); words[1] = (value ushr 32).toInt(); size = 2; trim()
    }

    fun copyFrom(other: ExactInteger) {
        ensure(other.size); other.words.copyInto(words, 0, 0, other.size); size = other.size
    }

    fun isZero(): Boolean = size == 0

    fun shiftLeft(bits: Int) {
        require(bits >= 0)
        if (size == 0 || bits == 0) return
        val wordShift = bits ushr 5
        val bitShift = bits and 31
        ensure(size + wordShift + 1)
        if (bitShift == 0) {
            for (index in size - 1 downTo 0) words[index + wordShift] = words[index]
            words[size + wordShift] = 0
        } else {
            words[size + wordShift] = words[size - 1] ushr (32 - bitShift)
            for (index in size - 1 downTo 1) words[index + wordShift] = (words[index] shl bitShift) or (words[index - 1] ushr (32 - bitShift))
            words[wordShift] = words[0] shl bitShift
        }
        for (index in 0 until wordShift) words[index] = 0
        size += wordShift + 1
        trim()
    }

    fun shiftRightOne() {
        for (index in 0 until size) {
            val high = if (index + 1 < size) words[index + 1] shl 31 else 0
            words[index] = (words[index] ushr 1) or high
        }
        trim()
    }

    /** Multiplies by 1..10^9. */
    fun multiplySmall(factor: Int) {
        require(factor in 1..1_000_000_000)
        var carry = 0L
        for (index in 0 until size) {
            val product = (words[index].toLong() and WORD_MASK) * factor + carry
            words[index] = product.toInt()
            carry = product ushr 32
        }
        if (carry != 0L) { ensure(size + 1); words[size++] = carry.toInt() }
    }

    fun multiplyPowerOfTen(power: Int) {
        require(power >= 0)
        var remaining = power
        while (remaining >= 9) { multiplySmall(1_000_000_000); remaining -= 9 }
        if (remaining > 0) multiplySmall(SMALL_POWERS_OF_TEN[remaining])
    }

    /** Multiplies by a power of two held as an [ExactInteger]. */
    fun multiplyBy(powerOfTwo: ExactInteger) {
        val top = powerOfTwo.size - 1
        check(top >= 0 && (0 until top).all { powerOfTwo.words[it] == 0 } && powerOfTwo.words[top].countOneBits() == 1)
        shiftLeft(top * 32 + powerOfTwo.words[top].countTrailingZeroBits())
    }

    fun add(other: ExactInteger) {
        val length = maxOf(size, other.size)
        ensure(length + 1)
        var carry = 0L
        for (index in 0 until length) {
            val sum = (if (index < size) words[index].toLong() and WORD_MASK else 0L) +
                (if (index < other.size) other.words[index].toLong() and WORD_MASK else 0L) + carry
            words[index] = sum.toInt()
            carry = sum ushr 32
        }
        size = length
        if (carry != 0L) words[size++] = carry.toInt()
    }

    /** Subtracts a value no larger than this one. */
    fun subtract(other: ExactInteger) {
        require(this >= other)
        var borrow = 0L
        for (index in 0 until size) {
            var difference = (words[index].toLong() and WORD_MASK) - borrow -
                (if (index < other.size) other.words[index].toLong() and WORD_MASK else 0L)
            borrow = if (difference < 0) { difference += 1L shl 32; 1L } else 0L
            words[index] = difference.toInt()
        }
        check(borrow == 0L)
        trim()
    }

    operator fun compareTo(other: ExactInteger): Int {
        if (size != other.size) return size.compareTo(other.size)
        for (index in size - 1 downTo 0) {
            val mine = words[index].toLong() and WORD_MASK
            val theirs = other.words[index].toLong() and WORD_MASK
            if (mine != theirs) return mine.compareTo(theirs)
        }
        return 0
    }

    private fun ensure(capacity: Int) {
        if (words.size < capacity) words = words.copyOf(maxOf(capacity, words.size * 2))
    }

    private fun trim() {
        while (size > 0 && words[size - 1] == 0) size--
    }
}

private val SHA256_K = intArrayOf(
    0x428a2f98,
    0x71374491,
    -0x4a3f0431,
    -0x164a245b,
    0x3956c25b,
    0x59f111f1,
    -0x6dc07d5c,
    -0x54e3a12b,
    -0x27f85568,
    0x12835b01,
    0x243185be,
    0x550c7dc3,
    0x72be5d74,
    -0x7f214e02,
    -0x6423f959,
    -0x3e640e8c,
    -0x1b64963f,
    -0x1041b87a,
    0x0fc19dc6,
    0x240ca1cc,
    0x2de92c6f,
    0x4a7484aa,
    0x5cb0a9dc,
    0x76f988da,
    -0x67c1aeae,
    -0x57ce3993,
    -0x4ffcd838,
    -0x40a68039,
    -0x391ff40d,
    -0x2a586eb9,
    0x06ca6351,
    0x14292967,
    0x27b70a85,
    0x2e1b2138,
    0x4d2c6dfc,
    0x53380d13,
    0x650a7354,
    0x766a0abb,
    -0x7e3d36d2,
    -0x6d8dd37b,
    -0x5d40175f,
    -0x57e599b5,
    -0x3db47490,
    -0x3893ae5d,
    -0x2e6d17e7,
    -0x2966f9dc,
    -0xbf1ca7b,
    0x106aa070,
    0x19a4c116,
    0x1e376c08,
    0x2748774c,
    0x34b0bcb5,
    0x391c0cb3,
    0x4ed8aa4a,
    0x5b9cca4f,
    0x682e6ff3,
    0x748f82ee,
    0x78a5636f,
    -0x7b3787ec,
    -0x7338fdf8,
    -0x6f410006,
    -0x5baf9315,
    -0x41065c09,
    -0x398e870e,
)
