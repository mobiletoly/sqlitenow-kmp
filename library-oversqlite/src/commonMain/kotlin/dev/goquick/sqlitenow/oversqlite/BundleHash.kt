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

private fun canonicalizeJsonString(value: String): String {
    val out = StringBuilder(value.length + 2)
    out.append('"')
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

private fun canonicalizeJsonNumber(raw: String): String {
    val number = raw.toDoubleOrNull()
        ?: throw IllegalArgumentException("invalid JSON number: $raw")
    require(number.isFinite()) { "JSON number must be finite: $raw" }
    if (number == 0.0) return "0"

    val absNumber = abs(number)
    return renderEcmaScriptBinary64(number, absNumber)
}

private data class ExactDecimal(val digits: String, val scale: Int)

private fun renderEcmaScriptBinary64(value: Double, magnitude: Double): String {
    val bits = magnitude.toBits()
    val exact = exactDecimal(bits)
    val previous = exactDecimal(bits - 1)
    val lower = half(addDecimal(previous, exact))
    val upper = if (bits == 0x7fefffffffffffffL) {
        addDecimal(exact, half(subtractDecimal(exact, previous)))
    } else {
        half(addDecimal(exact, exactDecimal(bits + 1)))
    }
    val significand = if (((bits ushr 52) and 0x7ffL) == 0L) {
        bits and 0x000fffffffffffffL
    } else {
        (bits and 0x000fffffffffffffL) or (1L shl 52)
    }
    val boundariesInclusive = significand and 1L == 0L
    val scientificExponent = exact.digits.length - exact.scale - 1

    for (precision in 1..17) {
        val sourceDigits = exact.digits.padEnd(precision + 1, '0')
        val prefix = sourceDigits.substring(0, precision).toULong()
        val next = sourceDigits[precision]
        val remainingNonZero = sourceDigits.drop(precision + 1).any { it != '0' }
        val roundUp = next > '5' || (next == '5' && (remainingNonZero || prefix and 1uL == 1uL))
        val nearest = prefix + if (roundUp) 1uL else 0uL
        for (candidate in listOf(nearest, nearest - 1uL, nearest + 1uL).distinct()) {
            if (candidate == 0uL) continue
            val decimal = decimalFromSignificand(candidate.toString(), scientificExponent - precision + 1)
            val lowerComparison = compareDecimal(decimal, lower)
            val upperComparison = compareDecimal(decimal, upper)
            val inside = (lowerComparison > 0 || boundariesInclusive && lowerComparison == 0) &&
                (upperComparison < 0 || boundariesInclusive && upperComparison == 0)
            if (inside) {
                return renderJcsDecimal(decimal, value < 0.0)
            }
        }
    }
    error("unable to render finite binary64 value $value")
}

private fun exactDecimal(bits: Long): ExactDecimal {
    if (bits == 0L) return ExactDecimal("0", 0)
    val exponentBits = ((bits ushr 52) and 0x7ffL).toInt()
    val fraction = bits and 0x000fffffffffffffL
    val significand: Long
    val exponent: Int
    if (exponentBits == 0) {
        significand = fraction
        exponent = 1 - 1023 - 52
    } else {
        significand = fraction or (1L shl 52)
        exponent = exponentBits - 1023 - 52
    }
    return if (exponent >= 0) {
        normalizeDecimal(ExactDecimal(multiplyPower(significand.toString(), 2, exponent), 0))
    } else {
        normalizeDecimal(ExactDecimal(multiplyPower(significand.toString(), 5, -exponent), -exponent))
    }
}

private fun decimalFromSignificand(significand: String, power10: Int): ExactDecimal =
    normalizeDecimal(if (power10 >= 0) ExactDecimal(significand + "0".repeat(power10), 0) else ExactDecimal(significand, -power10))

private fun normalizeDecimal(value: ExactDecimal): ExactDecimal {
    var digits = value.digits.trimStart('0').ifEmpty { "0" }
    var scale = value.scale
    while (digits.length > 1 && digits.endsWith('0')) {
        digits = digits.dropLast(1)
        scale--
    }
    return ExactDecimal(digits, scale)
}

private fun addDecimal(left: ExactDecimal, right: ExactDecimal): ExactDecimal {
    val scale = maxOf(left.scale, right.scale)
    return normalizeDecimal(ExactDecimal(addUnsigned(left.digits + "0".repeat(scale - left.scale), right.digits + "0".repeat(scale - right.scale)), scale))
}

private fun subtractDecimal(left: ExactDecimal, right: ExactDecimal): ExactDecimal {
    val scale = maxOf(left.scale, right.scale)
    return normalizeDecimal(ExactDecimal(subtractUnsigned(left.digits + "0".repeat(scale - left.scale), right.digits + "0".repeat(scale - right.scale)), scale))
}

private fun half(value: ExactDecimal): ExactDecimal = if ((value.digits.last() - '0') % 2 == 0) {
    normalizeDecimal(ExactDecimal(divideByTwo(value.digits), value.scale))
} else {
    normalizeDecimal(ExactDecimal(multiplySmall(value.digits, 5), value.scale + 1))
}

private fun compareDecimal(left: ExactDecimal, right: ExactDecimal): Int {
    val scale = maxOf(left.scale, right.scale)
    val leftDigits = (left.digits + "0".repeat(scale - left.scale)).trimStart('0').ifEmpty { "0" }
    val rightDigits = (right.digits + "0".repeat(scale - right.scale)).trimStart('0').ifEmpty { "0" }
    return leftDigits.length.compareTo(rightDigits.length).takeIf { it != 0 } ?: leftDigits.compareTo(rightDigits)
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

private fun multiplyPower(initial: String, multiplier: Int, exponent: Int): String {
    var result = initial
    repeat(exponent) { result = multiplySmall(result, multiplier) }
    return result
}

private fun multiplySmall(value: String, multiplier: Int): String {
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

private fun addUnsigned(left: String, right: String): String {
    val out = StringBuilder(maxOf(left.length, right.length) + 1)
    var carry = 0
    var li = left.lastIndex
    var ri = right.lastIndex
    while (li >= 0 || ri >= 0 || carry != 0) {
        val sum = (if (li >= 0) left[li--] - '0' else 0) + (if (ri >= 0) right[ri--] - '0' else 0) + carry
        out.append(('0'.code + sum % 10).toChar())
        carry = sum / 10
    }
    return out.reverse().toString()
}

private fun subtractUnsigned(left: String, right: String): String {
    require(left.length > right.length || left.length == right.length && left >= right)
    val paddedRight = right.padStart(left.length, '0')
    val out = StringBuilder(left.length)
    var borrow = 0
    for (index in left.indices.reversed()) {
        var digit = (left[index] - '0') - (paddedRight[index] - '0') - borrow
        if (digit < 0) { digit += 10; borrow = 1 } else borrow = 0
        out.append(('0'.code + digit).toChar())
    }
    return out.reverse().toString().trimStart('0').ifEmpty { "0" }
}

private fun divideByTwo(value: String): String {
    val out = StringBuilder(value.length)
    var carry = 0
    for (char in value) {
        val current = carry * 10 + (char - '0')
        out.append(('0'.code + current / 2).toChar())
        carry = current % 2
    }
    require(carry == 0)
    return out.toString().trimStart('0').ifEmpty { "0" }
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
