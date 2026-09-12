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

import androidx.sqlite.SQLiteStatement
import androidx.sqlite.SQLITE_DATA_BLOB
import androidx.sqlite.SQLITE_DATA_FLOAT
import androidx.sqlite.SQLITE_DATA_INTEGER
import androidx.sqlite.SQLITE_DATA_NULL
import androidx.sqlite.SQLITE_DATA_TEXT
import kotlinx.serialization.json.JsonArray
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

    data class Integer(val canonicalText: String) : TypedValue

	data class Real(
		val canonicalText: String,
	) : TypedValue

    data class Blob(val bytes: ByteArray) : TypedValue
}

private const val HEX_DIGITS = "0123456789abcdef"
private val payloadJson = Json
private val JSON_NUMBER_REGEX = Regex("^-?(0|[1-9]\\d*)(\\.\\d+)?([eE][+-]?\\d+)?$")
private val EXACT_INT64_REGEX = Regex("^-?(0|[1-9][0-9]*)$")

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
	return canonicalizeJsonElement(payloadJson.parseToJsonElement(value))
}

private fun requireSignedInt64(value: String): String {
	require(EXACT_INT64_REGEX.matches(value) && value != "-0") { "invalid canonical signed-64 string: $value" }
	val negative = value.startsWith('-')
	val magnitude = if (negative) value.substring(1) else value
	val limit = if (negative) "9223372036854775808" else "9223372036854775807"
	require(magnitude.length < limit.length || magnitude.length == limit.length && magnitude <= limit) {
		"signed-64 value is outside range: $value"
	}
	return value
}

private fun capturedPayloadValue(column: ColumnInfo, value: JsonElement): JsonElement {
    val capture = value as? JsonObject ?: error("${column.name}: expected typed SQLite capture")
    require(capture.keys == setOf("storage", "value")) { "invalid typed SQLite capture" }
    val storage = capture.getValue("storage").jsonPrimitive
    require(storage.isString) { "invalid SQLite storage tag" }
    val captured = capture.getValue("value")
    if (storage.content == "null") {
        require(captured == JsonNull) { "null capture must have a null value" }
        return JsonNull
    }
    require(storage.content in setOf("text", "integer", "real", "blob") && captured.jsonPrimitive.isString) { "invalid captured SQLite value" }
    require(storage.content.uppercase() == sqliteAffinity(column.declaredType)) { "${column.name}: actual SQLite storage ${storage.content} does not match its declared affinity" }
    return captured
}

private fun requirePermittedText(value: String): String {
    require('\u0000' !in value) { "U+0000 is not permitted in synced text" }
    requireUnicodeScalarString(value)
    return value
}

private fun requireDecimal(value: String): String {
    require(Regex("-?(0|[1-9][0-9]*)(\\.[0-9]+)?").matches(value)) { "invalid exact decimal" }
    require(value.count { it in '0'..'9' } <= 1000) { "decimal exceeds 1000 coefficient digits" }
    require(!value.startsWith('-') || value.any { it in '1'..'9' }) { "negative decimal zero is not canonical" }
    return value
}

private fun canonicalGenerationJson(raw: String): String {
    requireUniqueSnapshotJsonObjectMembers(raw)
    val value = payloadJson.parseToJsonElement(raw)
    requireJsonTextDomain(value)
    return canonicalizeJsonElement(value)
}

private fun requireJsonTextDomain(value: JsonElement, parentDepth: Int = 0) {
    if (value is JsonObject || value is JsonArray) require(parentDepth <= 64) { "JSON nesting exceeds 64" }
    when (value) {
        is JsonObject -> value.forEach { (key, child) -> requirePermittedText(key); requireJsonTextDomain(child, parentDepth + 1) }
        is JsonArray -> value.forEach { requireJsonTextDomain(it, parentDepth + 1) }
        is JsonPrimitive -> if (value.isString) requirePermittedText(value.content)
    }
}

internal object OversqliteValueCodec {
    fun readLocalPayloadValue(
        statement: SQLiteStatement,
        index: Int,
        column: ColumnInfo,
    ): JsonElement {
        val (storage, value) = when (statement.getColumnType(index)) {
            SQLITE_DATA_NULL -> "null" to JsonNull
            SQLITE_DATA_INTEGER -> "integer" to JsonPrimitive(statement.getLong(index).toString())
            SQLITE_DATA_FLOAT -> "real" to JsonPrimitive(statement.getDouble(index).toString())
            SQLITE_DATA_TEXT -> "text" to JsonPrimitive(statement.getText(index))
            SQLITE_DATA_BLOB -> "blob" to JsonPrimitive(bytesToHexLower(statement.getBlob(index)))
            else -> error("unknown SQLite storage for ${column.name}")
        }
        return buildJsonObject { put("storage", JsonPrimitive(storage)); put("value", value) }
    }

    @OptIn(ExperimentalUuidApi::class, ExperimentalEncodingApi::class)
    fun encodeWirePayloadValue(
        column: ColumnInfo,
        value: JsonElement,
    ): JsonElement {
        return when (val typed = decodePayloadValue(column, value, PayloadSource.LOCAL_STATE)) {
            TypedValue.Null -> JsonNull
            is TypedValue.Text -> JsonPrimitive(typed.value)
            is TypedValue.Integer -> if (column.wireType == "boolean") {
                JsonPrimitive(typed.canonicalText == "1")
            } else {
                JsonPrimitive(typed.canonicalText)
            }
            is TypedValue.Real -> JsonPrimitive(typed.canonicalText)
            is TypedValue.Blob -> {
                if (column.wireType == "uuid") {
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
        val wireType = checkNotNull(column.wireType) { "contract is unconfirmed for ${column.name}" }
        val scalar = if (payloadSource == PayloadSource.LOCAL_STATE) capturedPayloadValue(column, value) else value
        if (scalar == JsonNull) {
            require(!column.notNull && !column.isPrimaryKey) { "${column.name} must not be null" }
            return TypedValue.Null
        }
        val primitive = scalar.jsonPrimitive
        return when (wireType) {
            "boolean" -> {
                if (payloadSource == PayloadSource.AUTHORITATIVE_WIRE) {
                    require(!primitive.isString && primitive.content in setOf("true", "false")) {
                        "Boolean wire value required"
                    }
                    TypedValue.Integer(if (primitive.content == "true") "1" else "0")
                } else {
                    require(primitive.isString && primitive.content in setOf("0", "1")) {
                        "Boolean SQLite value must be 0 or 1"
                    }
                    TypedValue.Integer(primitive.content)
                }
            }
            "int64" -> {
                require(primitive.isString) {
                    "int64 ${if (payloadSource == PayloadSource.LOCAL_STATE) "SQLite value must be a JSON string" else "wire value must be a canonical JSON string"}"
                }
                TypedValue.Integer(requireSignedInt64(primitive.content))
            }
            "float64" -> {
                require(primitive.isString) {
                    "float64 ${if (payloadSource == PayloadSource.LOCAL_STATE) "SQLite value must be a JSON string" else "wire value must be a canonical JSON string"}"
                }
                val parsed = primitive.content.toDoubleOrNull()
                require(parsed != null && parsed.isFinite()) { "finite binary64 value required" }
                val canonical = canonicalizeFiniteJsonNumber(parsed.toString())
                require(payloadSource == PayloadSource.LOCAL_STATE || canonical == primitive.content) {
                    "canonical binary64 wire value required"
                }
                TypedValue.Real(canonical)
            }
            "decimal" -> TypedValue.Text(requireDecimal(requireCanonicalString(primitive, column.name)))
            "text" -> TypedValue.Text(requirePermittedText(requireCanonicalString(primitive, column.name)))
            "json" -> {
                val text = requireCanonicalString(primitive, column.name)
                val canonical = canonicalGenerationJson(text)
                require(payloadSource == PayloadSource.LOCAL_STATE || canonical == text) { "canonical JSON wire document required" }
                TypedValue.Text(canonical)
            }
            "bytes" -> TypedValue.Blob(
                if (payloadSource == PayloadSource.LOCAL_STATE) {
                    decodeHexBytes(requireCanonicalString(primitive, column.name))
                } else {
                    decodeWireBlobBytes(requireCanonicalString(primitive, column.name))
                },
            )
            "uuid" -> {
                val text = requireCanonicalString(primitive, column.name)
                if (column.kind.isBlobKind()) {
                    val bytes = if (payloadSource == PayloadSource.LOCAL_STATE) decodeHexBytes(text) else decodeWireUuidBytes(text)
                    require(bytes.size == 16) { "UUID BLOB must contain 16 bytes" }
                    TypedValue.Blob(bytes)
                } else {
                    decodeWireUuidBytes(text)
                    TypedValue.Text(text)
                }
            }
            else -> error("unsupported wire type $wireType")
        }
    }

    private fun requireCanonicalString(primitive: JsonPrimitive, columnName: String): String {
        require(primitive.isString) { "canonical JSON string required for $columnName" }
        return primitive.content
    }

    fun bindPayloadValue(
        statement: SQLiteStatement,
        index: Int,
        column: ColumnInfo,
        value: JsonElement,
        payloadSource: PayloadSource,
    ) {
        when (val typed = decodePayloadValue(column, value, payloadSource)) {
            TypedValue.Null -> statement.bindNull(index)
            is TypedValue.Text -> statement.bindText(index, typed.value)
            is TypedValue.Integer -> statement.bindText(index, typed.canonicalText)
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
            leftValue is TypedValue.Integer && rightValue is TypedValue.Integer -> leftValue.canonicalText == rightValue.canonicalText
			leftValue is TypedValue.Real && rightValue is TypedValue.Real -> leftValue.canonicalText == rightValue.canonicalText
            leftValue is TypedValue.Blob && rightValue is TypedValue.Blob -> leftValue.bytes.contentEquals(rightValue.bytes)
            else -> false
        }
    }

    fun encodeLocalPayloadObject(
        tableInfo: TableInfo,
        statement: SQLiteStatement,
    ): JsonObject {
        return buildJsonObject {
            tableInfo.columns.forEachIndexed { index, column ->
                put(column.name.lowercase(), readLocalPayloadValue(statement, index, column))
            }
        }
    }
}
