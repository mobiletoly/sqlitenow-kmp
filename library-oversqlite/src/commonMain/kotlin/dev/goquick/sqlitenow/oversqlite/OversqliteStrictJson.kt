/*
 * Copyright 2026 Toly Pochkin
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.goquick.sqlitenow.oversqlite

internal fun requireUniqueSnapshotJsonObjectMembers(raw: String) {
    StrictJsonMemberScanner(raw).validate()
}

private class StrictJsonSyntaxException : IllegalArgumentException("response is invalid JSON")

private class StrictJsonMemberScanner(private val raw: String) {
    private companion object { const val MAX_NESTING_DEPTH = 128 }
    private var index = 0

    fun validate() {
        skipWhitespace()
        readValue(0)
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
            '"' -> readString(false)
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
            if (!members.add(readString(true))) {
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
        repeat(4) { value = (value shl 4) or hexValue(raw[index++]) }
        return value.toChar()
    }

    private fun readScalar() {
        when (peek()) {
            't' -> readLiteral("true")
            'f' -> readLiteral("false")
            'n' -> readLiteral("null")
            else -> readNumber()
        }
    }

    private fun readLiteral(literal: String) {
        literal.forEach(::expect)
        requireScalarEnd()
    }

    private fun readNumber() {
        consume('-')
        val firstDigit = peek()
        when {
            firstDigit == '0' -> {
                index++
                if (isDecimalDigit(peek())) syntaxError()
            }
            firstDigit != null && firstDigit in '1'..'9' -> {
                index++
                while (isDecimalDigit(peek())) index++
            }
            else -> syntaxError()
        }
        if (consume('.')) {
            if (!isDecimalDigit(peek())) syntaxError()
            while (isDecimalDigit(peek())) index++
        }
        if (consume('e') || consume('E')) {
            if (!consume('+')) consume('-')
            if (!isDecimalDigit(peek())) syntaxError()
            while (isDecimalDigit(peek())) index++
        }
        requireScalarEnd()
    }

    private fun requireScalarEnd() {
        val next = peek()
        if (next != null && !next.isWhitespace() && next != ',' && next != ']' && next != '}') syntaxError()
    }

    private fun isDecimalDigit(value: Char?): Boolean = value != null && value in '0'..'9'

    private fun skipWhitespace() { while (index < raw.length && raw[index].isWhitespace()) index++ }
    private fun consume(expected: Char): Boolean = if (peek() == expected) { index++; true } else false
    private fun expect(expected: Char) { if (!consume(expected)) syntaxError() }
    private fun peek(): Char? = raw.getOrNull(index)
    private fun hexValue(value: Char): Int = when (value) {
        in '0'..'9' -> value - '0'
        in 'a'..'f' -> value - 'a' + 10
        in 'A'..'F' -> value - 'A' + 10
        else -> syntaxError()
    }
    private fun syntaxError(): Nothing = throw StrictJsonSyntaxException()
}
