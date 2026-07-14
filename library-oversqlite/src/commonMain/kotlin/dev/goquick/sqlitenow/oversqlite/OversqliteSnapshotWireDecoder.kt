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

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.JsonUnquotedLiteral

internal fun decodeSnapshotChunkResponse(raw: String): SnapshotChunkResponse =
    SnapshotChunkJsonDecoder(raw).decode()

private class SnapshotChunkJsonDecoder(
    private val raw: String,
) {
    private companion object {
        const val MAX_NESTING_DEPTH = 128

        const val SNAPSHOT_ID = 1 shl 0
        const val SNAPSHOT_BUNDLE_SEQ = 1 shl 1
        const val ROWS = 1 shl 2
        const val NEXT_ROW_ORDINAL = 1 shl 3
        const val HAS_MORE = 1 shl 4
        const val BYTE_COUNT = 1 shl 5
        const val ALL_CHUNK_MEMBERS = (1 shl 6) - 1

        const val SCHEMA = 1 shl 0
        const val TABLE = 1 shl 1
        const val KEY = 1 shl 2
        const val ROW_VERSION = 1 shl 3
        const val PAYLOAD = 1 shl 4
        const val ALL_ROW_MEMBERS = (1 shl 5) - 1
    }

    private var index = 0

    fun decode(): SnapshotChunkResponse {
        skipWhitespace()
        val chunk = readChunk(depth = 0)
        skipWhitespace()
        if (index != raw.length) syntaxError()
        return chunk
    }

    private fun readChunk(depth: Int): SnapshotChunkResponse {
        requireDepth(depth)
        expect('{')
        skipWhitespace()

        var seen = 0
        var snapshotId: String? = null
        var snapshotBundleSeq = 0L
        var rows: List<SnapshotRow>? = null
        var nextRowOrdinal = 0L
        var hasMore = false
        var byteCount = 0L

        if (!consume('}')) {
            while (true) {
                val name = readMemberName()
                expectMemberValue()
                when (name) {
                    "snapshot_id" -> {
                        seen = claimMember(seen, SNAPSHOT_ID)
                        snapshotId = readStringValue(depth + 1)
                    }
                    "snapshot_bundle_seq" -> {
                        seen = claimMember(seen, SNAPSHOT_BUNDLE_SEQ)
                        snapshotBundleSeq = readLongValue(depth + 1)
                    }
                    "rows" -> {
                        seen = claimMember(seen, ROWS)
                        rows = readRows(depth + 1)
                    }
                    "next_row_ordinal" -> {
                        seen = claimMember(seen, NEXT_ROW_ORDINAL)
                        nextRowOrdinal = readLongValue(depth + 1)
                    }
                    "has_more" -> {
                        seen = claimMember(seen, HAS_MORE)
                        hasMore = readBooleanValue(depth + 1)
                    }
                    "byte_count" -> {
                        seen = claimMember(seen, BYTE_COUNT)
                        byteCount = readLongValue(depth + 1)
                    }
                    else -> syntaxError()
                }
                skipWhitespace()
                if (consume('}')) break
                expect(',')
            }
        }

        if (seen != ALL_CHUNK_MEMBERS) {
            if (seen == (ALL_CHUNK_MEMBERS and BYTE_COUNT.inv())) {
                throw SnapshotSemanticException(SnapshotSemanticFailure.INVALID_CHUNK)
            }
            syntaxError()
        }
        return SnapshotChunkResponse(
            snapshotId = snapshotId ?: syntaxError(),
            snapshotBundleSeq = snapshotBundleSeq,
            rows = rows ?: syntaxError(),
            nextRowOrdinal = nextRowOrdinal,
            hasMore = hasMore,
            byteCount = byteCount,
        )
    }

    private fun readRows(depth: Int): List<SnapshotRow> {
        requireDepth(depth)
        skipWhitespace()
        expect('[')
        skipWhitespace()
        val rows = mutableListOf<SnapshotRow>()
        if (consume(']')) return rows
        while (true) {
            rows += readRow(depth + 1)
            skipWhitespace()
            if (consume(']')) return rows
            expect(',')
        }
    }

    private fun readRow(depth: Int): SnapshotRow {
        requireDepth(depth)
        skipWhitespace()
        expect('{')
        skipWhitespace()

        var seen = 0
        var schema: String? = null
        var table: String? = null
        var key: Map<String, String>? = null
        var rowVersion = 0L
        var payload: JsonObject? = null

        if (!consume('}')) {
            while (true) {
                val name = readMemberName()
                expectMemberValue()
                when (name) {
                    "schema" -> {
                        seen = claimMember(seen, SCHEMA)
                        schema = readStringValue(depth + 1)
                    }
                    "table" -> {
                        seen = claimMember(seen, TABLE)
                        table = readStringValue(depth + 1)
                    }
                    "key" -> {
                        seen = claimMember(seen, KEY)
                        key = readStringMap(depth + 1)
                    }
                    "row_version" -> {
                        seen = claimMember(seen, ROW_VERSION)
                        rowVersion = readLongValue(depth + 1)
                    }
                    "payload" -> {
                        seen = claimMember(seen, PAYLOAD)
                        payload = readJsonObject(depth + 1)
                    }
                    else -> syntaxError()
                }
                skipWhitespace()
                if (consume('}')) break
                expect(',')
            }
        }

        if (seen != ALL_ROW_MEMBERS) syntaxError()
        return SnapshotRow(
            schema = schema ?: syntaxError(),
            table = table ?: syntaxError(),
            key = key ?: syntaxError(),
            rowVersion = rowVersion,
            payload = payload ?: syntaxError(),
        )
    }

    private fun readStringMap(depth: Int): Map<String, String> {
        requireDepth(depth)
        skipWhitespace()
        expect('{')
        skipWhitespace()
        val entries = linkedMapOf<String, String>()
        if (consume('}')) return entries
        while (true) {
            val name = readMemberName()
            rejectDuplicateMember(entries, name)
            expectMemberValue()
            entries[name] = readStringValue(depth + 1)
            skipWhitespace()
            if (consume('}')) return entries
            expect(',')
        }
    }

    private fun readJsonObject(depth: Int): JsonObject {
        requireDepth(depth)
        skipWhitespace()
        expect('{')
        skipWhitespace()
        val entries = linkedMapOf<String, JsonElement>()
        if (consume('}')) return JsonObject(entries)
        while (true) {
            val name = readMemberName()
            rejectDuplicateMember(entries, name)
            expectMemberValue()
            entries[name] = readJsonValue(depth + 1)
            skipWhitespace()
            if (consume('}')) return JsonObject(entries)
            expect(',')
        }
    }

    private fun readJsonArray(depth: Int): JsonArray {
        requireDepth(depth)
        skipWhitespace()
        expect('[')
        skipWhitespace()
        val values = mutableListOf<JsonElement>()
        if (consume(']')) return JsonArray(values)
        while (true) {
            values += readJsonValue(depth + 1)
            skipWhitespace()
            if (consume(']')) return JsonArray(values)
            expect(',')
        }
    }

    @OptIn(ExperimentalSerializationApi::class)
    private fun readJsonValue(depth: Int): JsonElement {
        requireDepth(depth)
        skipWhitespace()
        return when (peek()) {
            '{' -> readJsonObject(depth)
            '[' -> readJsonArray(depth)
            '"' -> JsonPrimitive(readString())
            't' -> {
                expectLiteral("true")
                JsonPrimitive(true)
            }
            'f' -> {
                expectLiteral("false")
                JsonPrimitive(false)
            }
            'n' -> {
                expectLiteral("null")
                JsonNull
            }
            '-', in '0'..'9' -> JsonUnquotedLiteral(readNumberToken())
            else -> syntaxError()
        }
    }

    private fun readStringValue(depth: Int): String {
        requireDepth(depth)
        skipWhitespace()
        if (peek() != '"') syntaxError()
        return readString()
    }

    private fun readLongValue(depth: Int): Long {
        requireDepth(depth)
        skipWhitespace()
        val token = readNumberToken()
        if (token.any { it == '.' || it == 'e' || it == 'E' }) syntaxError()
        return token.toLongOrNull() ?: syntaxError()
    }

    private fun readBooleanValue(depth: Int): Boolean {
        requireDepth(depth)
        skipWhitespace()
        return when (peek()) {
            't' -> {
                expectLiteral("true")
                true
            }
            'f' -> {
                expectLiteral("false")
                false
            }
            else -> syntaxError()
        }
    }

    private fun readMemberName(): String {
        skipWhitespace()
        if (peek() != '"') syntaxError()
        return readString()
    }

    private fun expectMemberValue() {
        skipWhitespace()
        expect(':')
    }

    private fun readString(): String {
        expect('"')
        val start = index
        var decoded: StringBuilder? = null
        while (index < raw.length) {
            val currentIndex = index
            val current = raw[index++]
            when {
                current == '"' -> return decoded?.toString() ?: raw.substring(start, currentIndex)
                current == '\\' -> {
                    val output = decoded ?: StringBuilder().also {
                        it.append(raw.substring(start, currentIndex))
                        decoded = it
                    }
                    readEscape(output)
                }
                current < ' ' -> syntaxError()
                decoded != null -> decoded.append(current)
            }
        }
        syntaxError()
    }

    private fun readEscape(decoded: StringBuilder) {
        val escaped = peek() ?: syntaxError()
        index++
        when (escaped) {
            '"', '\\', '/' -> decoded.append(escaped)
            'b' -> decoded.append('\b')
            'f' -> decoded.append('\u000c')
            'n' -> decoded.append('\n')
            'r' -> decoded.append('\r')
            't' -> decoded.append('\t')
            'u' -> decoded.append(readUnicodeEscape())
            else -> syntaxError()
        }
    }

    private fun readUnicodeEscape(): Char {
        if (index + 4 > raw.length) syntaxError()
        var value = 0
        repeat(4) {
            value = (value shl 4) or hexValue(raw[index++])
        }
        return value.toChar()
    }

    private fun readNumberToken(): String {
        val start = index
        consume('-')
        when (peek()) {
            '0' -> {
                index++
                if (peek() in '0'..'9') syntaxError()
            }
            in '1'..'9' -> while (peek() in '0'..'9') index++
            else -> syntaxError()
        }
        if (consume('.')) {
            if (peek() !in '0'..'9') syntaxError()
            while (peek() in '0'..'9') index++
        }
        if (peek() == 'e' || peek() == 'E') {
            index++
            if (peek() == '+' || peek() == '-') index++
            if (peek() !in '0'..'9') syntaxError()
            while (peek() in '0'..'9') index++
        }
        return raw.substring(start, index)
    }

    private fun expectLiteral(expected: String) {
        if (!raw.startsWith(expected, index)) syntaxError()
        index += expected.length
    }

    private fun claimMember(seen: Int, member: Int): Int {
        if (seen and member != 0) duplicateMember()
        return seen or member
    }

    private fun rejectDuplicateMember(entries: Map<String, *>, name: String) {
        if (entries.containsKey(name)) duplicateMember()
    }

    private fun requireDepth(depth: Int) {
        if (depth > MAX_NESTING_DEPTH) {
            throw SnapshotSemanticException(SnapshotSemanticFailure.EXCESSIVE_NESTING)
        }
    }

    private fun skipWhitespace() {
        while (peek() == ' ' || peek() == '\t' || peek() == '\n' || peek() == '\r') index++
    }

    private fun consume(expected: Char): Boolean {
        if (peek() != expected) return false
        index++
        return true
    }

    private fun expect(expected: Char) {
        if (!consume(expected)) syntaxError()
    }

    private fun peek(): Char? = raw.getOrNull(index)

    private fun hexValue(value: Char): Int = when (value) {
        in '0'..'9' -> value - '0'
        in 'a'..'f' -> value - 'a' + 10
        in 'A'..'F' -> value - 'A' + 10
        else -> syntaxError()
    }

    private fun duplicateMember(): Nothing =
        throw SnapshotSemanticException(SnapshotSemanticFailure.DUPLICATE_OBJECT_MEMBER)

    private fun syntaxError(): Nothing = throw SnapshotWireSyntaxException()
}

/** Recursive duplicate/depth defense retained only for bounded snapshot control bodies. */
internal fun requireUniqueSnapshotJsonObjectMembers(raw: String) {
    SnapshotJsonMemberScanner(raw).validate()
}

private class SnapshotWireSyntaxException : IllegalArgumentException("snapshot response is invalid JSON")

private class SnapshotJsonMemberScanner(
    private val raw: String,
) {
    private companion object {
        const val MAX_NESTING_DEPTH = 128
    }

    private var index = 0

    fun validate() {
        skipWhitespace()
        readValue(depth = 0)
        skipWhitespace()
        if (index != raw.length) syntaxError()
    }

    private fun readValue(depth: Int) {
        if (depth > MAX_NESTING_DEPTH) {
            throw SnapshotSemanticException(SnapshotSemanticFailure.EXCESSIVE_NESTING)
        }
        skipWhitespace()
        when (peek()) {
            '{' -> readObject(depth)
            '[' -> readArray(depth)
            '"' -> readString(decode = false)
            null -> syntaxError()
            else -> readScalar()
        }
    }

    private fun readObject(depth: Int) {
        expect('{')
        skipWhitespace()
        if (consume('}')) return

        val members = mutableSetOf<String>()
        while (true) {
            skipWhitespace()
            val member = readString(decode = true)
            if (!members.add(member)) {
                throw SnapshotSemanticException(SnapshotSemanticFailure.DUPLICATE_OBJECT_MEMBER)
            }
            skipWhitespace()
            expect(':')
            readValue(depth + 1)
            skipWhitespace()
            if (consume('}')) return
            expect(',')
        }
    }

    private fun readArray(depth: Int) {
        expect('[')
        skipWhitespace()
        if (consume(']')) return
        while (true) {
            readValue(depth + 1)
            skipWhitespace()
            if (consume(']')) return
            expect(',')
        }
    }

    private fun readString(decode: Boolean): String {
        expect('"')
        val decoded = if (decode) StringBuilder() else null
        while (index < raw.length) {
            val current = raw[index++]
            when {
                current == '"' -> return decoded?.toString().orEmpty()
                current == '\\' -> readEscape(decoded)
                current < ' ' -> syntaxError()
                else -> decoded?.append(current)
            }
        }
        syntaxError()
    }

    private fun readEscape(decoded: StringBuilder?) {
        val escaped = peek() ?: syntaxError()
        index++
        when (escaped) {
            '"', '\\', '/' -> decoded?.append(escaped)
            'b' -> decoded?.append('\b')
            'f' -> decoded?.append('\u000c')
            'n' -> decoded?.append('\n')
            'r' -> decoded?.append('\r')
            't' -> decoded?.append('\t')
            'u' -> decoded?.append(readUnicodeEscape())
            else -> syntaxError()
        }
    }

    private fun readUnicodeEscape(): Char {
        if (index + 4 > raw.length) syntaxError()
        var value = 0
        repeat(4) {
            value = (value shl 4) or hexValue(raw[index++])
        }
        return value.toChar()
    }

    private fun readScalar() {
        val start = index
        while (index < raw.length) {
            val current = raw[index]
            if (current.isWhitespace() || current == ',' || current == ']' || current == '}') break
            index++
        }
        if (index == start) syntaxError()
    }

    private fun skipWhitespace() {
        while (index < raw.length && raw[index].isWhitespace()) index++
    }

    private fun consume(expected: Char): Boolean {
        if (peek() != expected) return false
        index++
        return true
    }

    private fun expect(expected: Char) {
        if (!consume(expected)) syntaxError()
    }

    private fun peek(): Char? = raw.getOrNull(index)

    private fun hexValue(value: Char): Int = when (value) {
        in '0'..'9' -> value - '0'
        in 'a'..'f' -> value - 'a' + 10
        in 'A'..'F' -> value - 'A' + 10
        else -> syntaxError()
    }

    private fun syntaxError(): Nothing = throw SnapshotWireSyntaxException()
}
