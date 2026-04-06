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

import dev.goquick.sqlitenow.core.sqlite.SqliteStatement
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

internal enum class PayloadSource {
    LOCAL_STATE,
    AUTHORITATIVE_WIRE,
}

internal sealed interface TypedValue {
    data object Null : TypedValue

    data class Text(val value: String) : TypedValue

    data class Integer(val value: Long) : TypedValue

    data class Real(
        val canonicalText: String,
    ) : TypedValue

    data class Blob(val bytes: ByteArray) : TypedValue
}

private const val HEX_DIGITS = "0123456789abcdef"
private val payloadJson = Json
private val JSON_NUMBER_REGEX = Regex("^-?(0|[1-9]\\d*)(\\.\\d+)?([eE][+-]?\\d+)?$")

internal fun bytesToHexLower(bytes: ByteArray): String {
    val out = StringBuilder(bytes.size * 2)
    for (byte in bytes) {
        val value = byte.toInt() and 0xff
        out.append(HEX_DIGITS[value ushr 4])
        out.append(HEX_DIGITS[value and 0x0f])
    }
    return out.toString()
}

private fun String.isHexValue(): Boolean {
    return all { it.isDigit() || it.lowercaseChar() in 'a'..'f' }
}

private fun decodeHexBytes(value: String): ByteArray {
    require(value.length % 2 == 0) { "hex value must have even length" }
    require(value.isHexValue()) { "invalid hex value" }

    val out = ByteArray(value.length / 2)
    var index = 0
    while (index < value.length) {
        out[index / 2] = value.substring(index, index + 2).toInt(16).toByte()
        index += 2
    }
    return out
}

@OptIn(ExperimentalEncodingApi::class)
private fun tryDecodeBase64Exact(value: String): ByteArray? {
    return runCatching { Base64.decode(value) }
        .getOrNull()
        ?.takeIf { Base64.encode(it) == value }
}

// Tolerant local/internal parser. Remote payloads must use the canonical wire decoders below.
@OptIn(ExperimentalUuidApi::class, ExperimentalEncodingApi::class)
internal fun decodeLocalBlobBytes(value: String): ByteArray {
    if (value.isEmpty()) return ByteArray(0)

    val clean = value.trim().removePrefix("\\x").removePrefix("\\X")
    if (clean.length % 2 == 0 && clean.isHexValue()) {
        return decodeHexBytes(clean)
    }

    tryDecodeBase64Exact(value)?.let { return it }

    runCatching { return Uuid.parse(value).toByteArray() }
    error("invalid blob encoding")
}

@OptIn(ExperimentalEncodingApi::class)
internal fun decodeWireBlobBytes(value: String): ByteArray {
    if (value.isEmpty()) return ByteArray(0)
    return tryDecodeBase64Exact(value) ?: error("invalid canonical wire blob encoding")
}

// Tolerant local/internal parser. Remote UUID-valued keys must use decodeWireUuidBytes.
@OptIn(ExperimentalUuidApi::class, ExperimentalEncodingApi::class)
internal fun decodeLocalUuidBytes(value: String): ByteArray {
    runCatching { return Uuid.parse(value).toByteArray() }

    tryDecodeBase64Exact(value)?.let {
        require(it.size == 16) { "base64 decoded length is ${it.size}, want 16" }
        return it
    }

    val clean = value.trim()
    if (clean.length % 2 == 0 && clean.isHexValue()) {
        val bytes = decodeHexBytes(clean)
        require(bytes.size == 16) { "hex decoded length is ${bytes.size}, want 16" }
        return bytes
    }

    error("not a UUID encoding")
}

@OptIn(ExperimentalUuidApi::class)
internal fun decodeWireUuidBytes(value: String): ByteArray {
    require(value.trim() == value) { "invalid canonical wire UUID encoding" }
    require(value.length == 36) { "invalid canonical wire UUID encoding" }

    val parsed = Uuid.parse(value)
    require(parsed.toString() == value) { "invalid canonical wire UUID encoding" }
    return parsed.toByteArray()
}

@OptIn(ExperimentalUuidApi::class)
internal fun normalizeLocalUuidAsCanonicalWire(value: String): String {
    val bytes = decodeLocalUuidBytes(value)
    require(bytes.size == 16) { "UUID byte length is ${bytes.size}, want 16" }
    return Uuid.parseHex(bytesToHexLower(bytes)).toString()
}

internal fun canonicalizeFiniteJsonNumber(value: String): String {
    require(JSON_NUMBER_REGEX.matches(value)) { "invalid JSON number: $value" }

    val negative = value.startsWith('-')
    val unsigned = if (negative) value.substring(1) else value
    val exponentIndex = unsigned.indexOfFirst { it == 'e' || it == 'E' }
    val mantissa = if (exponentIndex >= 0) unsigned.substring(0, exponentIndex) else unsigned
    val exponent = if (exponentIndex >= 0) unsigned.substring(exponentIndex + 1).toInt() else 0

    val dotIndex = mantissa.indexOf('.')
    val integerPart = if (dotIndex >= 0) mantissa.substring(0, dotIndex) else mantissa
    val fractionPart = if (dotIndex >= 0) mantissa.substring(dotIndex + 1) else ""
    val rawDigits = integerPart + fractionPart
    val leadingZeroCount = rawDigits.indexOfFirst { it != '0' }.let { if (it < 0) rawDigits.length else it }
    val digits = rawDigits.drop(leadingZeroCount)
    if (digits.isEmpty()) {
        return "0"
    }

    val shiftedDecimalIndex = integerPart.length + exponent - leadingZeroCount
    val plain = when {
        shiftedDecimalIndex <= 0 -> "0." + "0".repeat(-shiftedDecimalIndex) + digits
        shiftedDecimalIndex >= digits.length -> digits + "0".repeat(shiftedDecimalIndex - digits.length)
        else -> digits.substring(0, shiftedDecimalIndex) + "." + digits.substring(shiftedDecimalIndex)
    }

    val normalized = if ('.' in plain) {
        val parts = plain.split('.', limit = 2)
        val normalizedInteger = parts[0].trimStart('0').ifEmpty { "0" }
        val normalizedFraction = parts[1].trimEnd('0')
        if (normalizedFraction.isEmpty()) normalizedInteger else "$normalizedInteger.$normalizedFraction"
    } else {
        plain.trimStart('0').ifEmpty { "0" }
    }

    return if (negative && normalized != "0") "-$normalized" else normalized
}

private fun decodeIntegerPayloadValue(
    column: ColumnInfo,
    value: JsonElement,
): Long {
    value.jsonPrimitive.content.toLongOrNull()?.let { return it }
    return when (value.toString()) {
        "true" -> 1L
        "false" -> 0L
        else -> error("expected integer for ${column.name}, got ${value.jsonPrimitive.content}")
    }
}

internal object OversqliteValueCodec {
    fun readLocalPayloadValue(
        statement: SqliteStatement,
        index: Int,
        column: ColumnInfo,
    ): JsonElement {
        if (statement.isNull(index)) {
            return JsonNull
        }
        return when (column.kind) {
            ColumnKind.TEXT -> JsonPrimitive(statement.getText(index))
            ColumnKind.INTEGER -> JsonPrimitive(statement.getLong(index))
            ColumnKind.REAL -> payloadJson.parseToJsonElement(statement.getText(index))
            ColumnKind.BLOB, ColumnKind.UUID_BLOB -> JsonPrimitive(bytesToHexLower(statement.getBlob(index)))
        }
    }

    @OptIn(ExperimentalUuidApi::class, ExperimentalEncodingApi::class)
    fun encodeWirePayloadValue(
        column: ColumnInfo,
        value: JsonElement,
    ): JsonElement {
        return when (val typed = decodePayloadValue(column, value, PayloadSource.LOCAL_STATE)) {
            TypedValue.Null -> JsonNull
            is TypedValue.Text -> JsonPrimitive(typed.value)
            is TypedValue.Integer -> JsonPrimitive(typed.value)
            is TypedValue.Real -> payloadJson.parseToJsonElement(typed.canonicalText)
            is TypedValue.Blob -> {
                if (column.kind == ColumnKind.UUID_BLOB) {
                    JsonPrimitive(Uuid.parseHex(bytesToHexLower(typed.bytes)).toString())
                } else {
                    JsonPrimitive(Base64.encode(typed.bytes))
                }
            }
        }
    }

    fun decodePayloadValue(
        column: ColumnInfo,
        value: JsonElement,
        payloadSource: PayloadSource,
    ): TypedValue {
        if (value is JsonNull) {
            return TypedValue.Null
        }
        val primitive = value.jsonPrimitive
        return when (column.kind) {
            ColumnKind.TEXT -> TypedValue.Text(primitive.content)
            ColumnKind.INTEGER -> TypedValue.Integer(decodeIntegerPayloadValue(column, value))
            ColumnKind.REAL -> {
                val content = primitive.content
                TypedValue.Real(canonicalText = canonicalizeFiniteJsonNumber(content))
            }
            ColumnKind.BLOB -> TypedValue.Blob(
                when (payloadSource) {
                    PayloadSource.LOCAL_STATE -> decodeLocalBlobBytes(primitive.content)
                    PayloadSource.AUTHORITATIVE_WIRE -> decodeWireBlobBytes(primitive.content)
                },
            )
            ColumnKind.UUID_BLOB -> TypedValue.Blob(
                when (payloadSource) {
                    PayloadSource.LOCAL_STATE -> decodeLocalUuidBytes(primitive.content)
                    PayloadSource.AUTHORITATIVE_WIRE -> decodeWireUuidBytes(primitive.content)
                },
            )
        }
    }

    fun bindPayloadValue(
        statement: SqliteStatement,
        index: Int,
        column: ColumnInfo,
        value: JsonElement,
        payloadSource: PayloadSource,
    ) {
        when (val typed = decodePayloadValue(column, value, payloadSource)) {
            TypedValue.Null -> statement.bindNull(index)
            is TypedValue.Text -> statement.bindText(index, typed.value)
            is TypedValue.Integer -> statement.bindLong(index, typed.value)
            is TypedValue.Real -> {
                val numericValue = typed.canonicalText.toDouble()
                require(numericValue.isFinite()) { "expected finite real for ${column.name}, got ${typed.canonicalText}" }
                statement.bindDouble(index, numericValue)
            }
            is TypedValue.Blob -> statement.bindBlob(index, typed.bytes)
        }
    }

    fun equivalentPayloadTexts(
        tableInfo: TableInfo,
        left: String?,
        right: String?,
        leftSource: PayloadSource,
        rightSource: PayloadSource,
        json: Json,
    ): Boolean {
        if (left == null && right == null) return true
        if (left == null || right == null) return false
        val leftObject = json.parseToJsonElement(left) as? JsonObject ?: return false
        val rightObject = json.parseToJsonElement(right) as? JsonObject ?: return false
        return equivalentPayloadObjects(tableInfo, leftObject, rightObject, leftSource, rightSource)
    }

    fun equivalentPayloadObjects(
        tableInfo: TableInfo,
        left: JsonObject,
        right: JsonObject,
        leftSource: PayloadSource,
        rightSource: PayloadSource,
    ): Boolean {
        if (left.size != tableInfo.columns.size || right.size != tableInfo.columns.size) {
            return false
        }
        return tableInfo.columns.all { column ->
            val key = column.name.lowercase()
            val leftValue = left[key] ?: left[column.name] ?: return false
            val rightValue = right[key] ?: right[column.name] ?: return false
            equivalent(column, leftValue, rightValue, leftSource, rightSource)
        }
    }

    fun equivalent(
        column: ColumnInfo,
        left: JsonElement,
        right: JsonElement,
        leftSource: PayloadSource,
        rightSource: PayloadSource,
    ): Boolean {
        val leftValue = decodePayloadValue(column, left, leftSource)
        val rightValue = decodePayloadValue(column, right, rightSource)
        return when {
            leftValue is TypedValue.Null && rightValue is TypedValue.Null -> true
            leftValue is TypedValue.Text && rightValue is TypedValue.Text -> leftValue.value == rightValue.value
            leftValue is TypedValue.Integer && rightValue is TypedValue.Integer -> leftValue.value == rightValue.value
            leftValue is TypedValue.Real && rightValue is TypedValue.Real -> leftValue.canonicalText == rightValue.canonicalText
            leftValue is TypedValue.Blob && rightValue is TypedValue.Blob -> leftValue.bytes.contentEquals(rightValue.bytes)
            else -> false
        }
    }

    fun encodeLocalPayloadObject(
        tableInfo: TableInfo,
        statement: SqliteStatement,
    ): JsonObject {
        return buildJsonObject {
            tableInfo.columns.forEachIndexed { index, column ->
                put(column.name.lowercase(), readLocalPayloadValue(statement, index, column))
            }
        }
    }
}
