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
package dev.goquick.sqlitenow.core.util

/**
 * Creates a compact preview string for values being bound to SQLite.
 * - String: prints up to 60 chars, wraps in quotes; shows (len=N) when truncated
 * - Arrays: prints up to 16 items, shows (size=N) when truncated
 * - Other types: uses toString()
 */
fun sqliteNowPreview(value: Any?): String = when (value) {
    null -> "null"
    is String -> previewString(value)
    is ByteArray -> previewByteArrayHex(value)
    is IntArray -> previewPrimitiveArray(value.toList(), value.size)
    is LongArray -> previewPrimitiveArray(value.toList(), value.size)
    is ShortArray -> previewPrimitiveArray(value.toList(), value.size)
    is CharArray -> previewPrimitiveArray(value.toList(), value.size)
    is FloatArray -> previewPrimitiveArray(value.toList(), value.size)
    is DoubleArray -> previewPrimitiveArray(value.toList(), value.size)
    is BooleanArray -> previewPrimitiveArray(value.toList(), value.size)
    is Array<*> -> previewObjectArray(value.asList(), value.size)
    else -> value.toString()
}

private fun previewString(s: String): String {
    val max = 120
    return if (s.length > max) {
        val shown = s.substring(0, max) + "…"
        '"' + shown + '"' + "(len=" + s.length + ")"
    } else {
        '"' + s + '"'
    }
}

private fun <T> previewPrimitiveArray(list: List<T>, size: Int): String {
    val max = 16
    val shown = list.take(max).joinToString(", ") { it.toString() }
    return if (size > max) "[$shown, …](size=$size)" else "[$shown]"
}

private fun previewObjectArray(list: List<*>, size: Int): String {
    val max = 16
    val shown = list.take(max).joinToString(", ") { it?.toString() ?: "null" }
    return if (size > max) "[$shown, …](size=$size)" else "[$shown]"
}

private const val HEX_DIGITS: String = "0123456789ABCDEF"

private fun byteToHex(b: Byte): String {
    val v = b.toInt() and 0xFF
    val hi = HEX_DIGITS[v ushr 4]
    val lo = HEX_DIGITS[v and 0x0F]
    return "$hi$lo"
}

private fun previewByteArrayHex(bytes: ByteArray): String {
    val max = 16
    val n = bytes.size
    val shown = bytes.take(max).joinToString(" ") { byteToHex(it) }
    return if (n > max) "0x[$shown, …](size=$n)" else "[$shown]"
}
