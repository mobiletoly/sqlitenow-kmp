package dev.goquick.sqlitenow.oversqlite

import kotlin.math.abs

/**
 * The string-arithmetic binary64 renderer that production used before integer rendering, kept verbatim
 * (names prefixed) as an independent oracle: the production renderer must produce the same canonical
 * text for every finite value.
 */
internal fun referenceCanonicalBinary64(value: Double): String {
    require(value.isFinite())
    if (value == 0.0) return "0"
    return referenceRenderEcmaScriptBinary64(value, abs(value))
}

private data class ReferenceExactDecimal(val digits: String, val scale: Int)

private fun referenceRenderEcmaScriptBinary64(value: Double, magnitude: Double): String {
    val bits = magnitude.toBits()
    val exact = referenceExactDecimal(bits)
    val previous = referenceExactDecimal(bits - 1)
    val lower = referenceHalf(referenceAddDecimal(previous, exact))
    val upper = if (bits == 0x7fefffffffffffffL) {
        referenceAddDecimal(exact, referenceHalf(referenceSubtractDecimal(exact, previous)))
    } else {
        referenceHalf(referenceAddDecimal(exact, referenceExactDecimal(bits + 1)))
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
            val decimal = referenceDecimalFromSignificand(candidate.toString(), scientificExponent - precision + 1)
            val lowerComparison = referenceCompareDecimal(decimal, lower)
            val upperComparison = referenceCompareDecimal(decimal, upper)
            val inside = (lowerComparison > 0 || boundariesInclusive && lowerComparison == 0) &&
                (upperComparison < 0 || boundariesInclusive && upperComparison == 0)
            if (inside) {
                return referenceRenderJcsDecimal(decimal, value < 0.0)
            }
        }
    }
    error("unable to render finite binary64 value $value")
}

private fun referenceExactDecimal(bits: Long): ReferenceExactDecimal {
    if (bits == 0L) return ReferenceExactDecimal("0", 0)
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
        referenceNormalizeDecimal(ReferenceExactDecimal(referenceMultiplyPower(significand.toString(), 2, exponent), 0))
    } else {
        referenceNormalizeDecimal(ReferenceExactDecimal(referenceMultiplyPower(significand.toString(), 5, -exponent), -exponent))
    }
}

private fun referenceDecimalFromSignificand(significand: String, power10: Int): ReferenceExactDecimal =
    referenceNormalizeDecimal(if (power10 >= 0) ReferenceExactDecimal(significand + "0".repeat(power10), 0) else ReferenceExactDecimal(significand, -power10))

private fun referenceNormalizeDecimal(value: ReferenceExactDecimal): ReferenceExactDecimal {
    var digits = value.digits.trimStart('0').ifEmpty { "0" }
    var scale = value.scale
    while (digits.length > 1 && digits.endsWith('0')) {
        digits = digits.dropLast(1)
        scale--
    }
    return ReferenceExactDecimal(digits, scale)
}

private fun referenceAddDecimal(left: ReferenceExactDecimal, right: ReferenceExactDecimal): ReferenceExactDecimal {
    val scale = maxOf(left.scale, right.scale)
    return referenceNormalizeDecimal(ReferenceExactDecimal(referenceAddUnsigned(left.digits + "0".repeat(scale - left.scale), right.digits + "0".repeat(scale - right.scale)), scale))
}

private fun referenceSubtractDecimal(left: ReferenceExactDecimal, right: ReferenceExactDecimal): ReferenceExactDecimal {
    val scale = maxOf(left.scale, right.scale)
    return referenceNormalizeDecimal(ReferenceExactDecimal(referenceSubtractUnsigned(left.digits + "0".repeat(scale - left.scale), right.digits + "0".repeat(scale - right.scale)), scale))
}

private fun referenceHalf(value: ReferenceExactDecimal): ReferenceExactDecimal = if ((value.digits.last() - '0') % 2 == 0) {
    referenceNormalizeDecimal(ReferenceExactDecimal(referenceDivideByTwo(value.digits), value.scale))
} else {
    referenceNormalizeDecimal(ReferenceExactDecimal(referenceMultiplySmall(value.digits, 5), value.scale + 1))
}

private fun referenceCompareDecimal(left: ReferenceExactDecimal, right: ReferenceExactDecimal): Int {
    val scale = maxOf(left.scale, right.scale)
    val leftDigits = (left.digits + "0".repeat(scale - left.scale)).trimStart('0').ifEmpty { "0" }
    val rightDigits = (right.digits + "0".repeat(scale - right.scale)).trimStart('0').ifEmpty { "0" }
    return leftDigits.length.compareTo(rightDigits.length).takeIf { it != 0 } ?: leftDigits.compareTo(rightDigits)
}

private fun referenceRenderJcsDecimal(value: ReferenceExactDecimal, negative: Boolean): String {
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

private const val REFERENCE_POWER_LIMB_BASE = 10_000
private const val REFERENCE_POWER_LIMB_DIGITS = 4
// A limb times a factor of at most this, plus the carry, stays below 2^31.
private const val REFERENCE_POWER_FACTOR_LIMIT = 200_000

/**
 * Decimal digits of `initial × multiplier^exponent`, where [initial] is a non-negative decimal
 * integer without leading zeros and [multiplier] is 2..10.
 *
 * Every finite binary64 number is expanded through this function three times per rendering, with
 * exponents near 50 for everyday values and up to 1074 for subnormals. The digits are held as
 * base-10^4 limbs and multiplied by several powers at once, in plain Int arithmetic so the cost is
 * the same on every target. The result is exactly that of digit-by-digit decimal multiplication.
 */
internal fun referenceMultiplyPower(initial: String, multiplier: Int, exponent: Int): String {
    if (exponent == 0) return initial
    require(exponent > 0 && multiplier in 2..10) { "unsupported power $multiplier^$exponent" }

    // Each multiplication by at most 10 adds at most one decimal digit.
    val limbs = IntArray((initial.length + exponent) / REFERENCE_POWER_LIMB_DIGITS + 2)
    var used = 0
    var end = initial.length
    while (end > 0) {
        val start = maxOf(0, end - REFERENCE_POWER_LIMB_DIGITS)
        var limb = 0
        for (index in start until end) {
            val digit = initial[index] - '0'
            require(digit in 0..9) { "decimal digits required" }
            limb = limb * 10 + digit
        }
        limbs[used++] = limb
        end = start
    }

    var powersPerPass = 1
    var passFactor = multiplier
    while (passFactor * multiplier <= REFERENCE_POWER_FACTOR_LIMIT) {
        passFactor *= multiplier
        powersPerPass++
    }
    var remaining = exponent
    while (remaining > 0) {
        var factor = passFactor
        if (remaining < powersPerPass) {
            factor = 1
            repeat(remaining) { factor *= multiplier }
            remaining = 0
        } else {
            remaining -= powersPerPass
        }
        var carry = 0
        for (index in 0 until used) {
            val product = limbs[index] * factor + carry
            limbs[index] = product % REFERENCE_POWER_LIMB_BASE
            carry = product / REFERENCE_POWER_LIMB_BASE
        }
        while (carry > 0) {
            limbs[used++] = carry % REFERENCE_POWER_LIMB_BASE
            carry /= REFERENCE_POWER_LIMB_BASE
        }
    }

    val out = StringBuilder(used * REFERENCE_POWER_LIMB_DIGITS)
    out.append(limbs[used - 1])
    for (index in used - 2 downTo 0) {
        val limb = limbs[index]
        if (limb < 1000) out.append('0')
        if (limb < 100) out.append('0')
        if (limb < 10) out.append('0')
        out.append(limb)
    }
    return out.toString()
}

private fun referenceMultiplySmall(value: String, multiplier: Int): String {
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

private fun referenceAddUnsigned(left: String, right: String): String {
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

private fun referenceSubtractUnsigned(left: String, right: String): String {
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

private fun referenceDivideByTwo(value: String): String {
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
