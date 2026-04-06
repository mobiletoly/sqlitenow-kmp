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
    for (ch in value) {
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
    return digest.toHexLower()
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

private fun ByteArray.toHexLower(): String {
    val out = StringBuilder(size * 2)
    for (byte in this) {
        val value = byte.toInt() and 0xff
        out.append("0123456789abcdef"[value ushr 4])
        out.append("0123456789abcdef"[value and 0x0f])
    }
    return out.toString()
}

private fun Int.rotateRight(distance: Int): Int =
    (this ushr distance) or (this shl (32 - distance))

private fun canonicalizeJsonNumber(raw: String): String {
    val number = raw.toDoubleOrNull()
        ?: throw IllegalArgumentException("invalid JSON number: $raw")
    require(number.isFinite()) { "JSON number must be finite: $raw" }
    if (number == 0.0) return "0"

    val absNumber = abs(number)
    val rendered = number.toString().lowercase()
    return if (absNumber >= 1e-6 && absNumber < 1e21) {
        normalizePlainNumber(scientificToPlain(rendered))
    } else {
        normalizeScientificNumber(rendered)
    }
}

private fun scientificToPlain(raw: String): String {
    if (!raw.contains('e')) {
        return raw
    }
    val sign = if (raw.startsWith('-')) "-" else ""
    val unsigned = if (sign.isEmpty()) raw else raw.substring(1)
    val parts = unsigned.split('e')
    require(parts.size == 2) { "invalid scientific notation: $raw" }

    val mantissa = parts[0]
    val exponent = parts[1].toInt()
    val dotIndex = mantissa.indexOf('.')
    val digits = mantissa.replace(".", "")
    val fractionalDigits = if (dotIndex >= 0) mantissa.length - dotIndex - 1 else 0
    val decimalIndex = digits.length + exponent - fractionalDigits

    return buildString(digits.length + abs(exponent) + 4) {
        append(sign)
        when {
            decimalIndex <= 0 -> {
                append("0.")
                repeat(-decimalIndex) { append('0') }
                append(digits)
            }

            decimalIndex >= digits.length -> {
                append(digits)
                repeat(decimalIndex - digits.length) { append('0') }
            }

            else -> {
                append(digits.substring(0, decimalIndex))
                append('.')
                append(digits.substring(decimalIndex))
            }
        }
    }
}

private fun normalizePlainNumber(raw: String): String {
    val sign = if (raw.startsWith('-')) "-" else ""
    val unsigned = if (sign.isEmpty()) raw else raw.substring(1)
    val parts = unsigned.split('.', limit = 2)
    val integerPart = parts[0].trimStart('0').ifEmpty { "0" }
    val fractionalPart = parts.getOrElse(1) { "" }.trimEnd('0')
    if (integerPart == "0" && fractionalPart.isEmpty()) {
        return "0"
    }
    return buildString(raw.length) {
        append(sign)
        append(integerPart)
        if (fractionalPart.isNotEmpty()) {
            append('.')
            append(fractionalPart)
        }
    }
}

private fun normalizeScientificNumber(raw: String): String {
    if (!raw.contains('e')) {
        return plainToScientific(normalizePlainNumber(raw))
    }
    val sign = if (raw.startsWith('-')) "-" else ""
    val unsigned = if (sign.isEmpty()) raw else raw.substring(1)
    val parts = unsigned.split('e')
    require(parts.size == 2) { "invalid scientific notation: $raw" }

    val mantissa = normalizePlainNumber(parts[0])
    val exponentValue = parts[1].toInt()
    return buildString(raw.length) {
        append(sign)
        append(mantissa)
        append('e')
        if (exponentValue >= 0) {
            append('+')
        }
        append(exponentValue)
    }
}

private fun plainToScientific(raw: String): String {
    val sign = if (raw.startsWith('-')) "-" else ""
    val unsigned = if (sign.isEmpty()) raw else raw.substring(1)
    val parts = unsigned.split('.', limit = 2)
    val integerPart = parts[0]
    val fractionalPart = parts.getOrElse(1) { "" }

    val firstNonZeroIndex = integerPart.indexOfFirst { it != '0' }
    return if (firstNonZeroIndex >= 0) {
        val digits = (integerPart + fractionalPart).trimStart('0')
        val exponent = integerPart.length - 1
        val mantissa = digits.first() + digits.drop(1).let { tail ->
            if (tail.isEmpty()) "" else ".$tail"
        }
        "$sign$mantissa" + "e+$exponent"
    } else {
        val fractionIndex = fractionalPart.indexOfFirst { it != '0' }
        require(fractionIndex >= 0) { "plainToScientific requires a non-zero number: $raw" }
        val digits = fractionalPart.substring(fractionIndex)
        val exponent = -(fractionIndex + 1)
        val mantissa = digits.first() + digits.drop(1).let { tail ->
            if (tail.isEmpty()) "" else ".$tail"
        }
        "$sign$mantissa" + "e$exponent"
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
