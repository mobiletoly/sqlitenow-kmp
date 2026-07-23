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
package dev.goquick.sqlitenow.core.sqlite

internal class SqliteTransactionState {
    private var explicitTransactionActive = false
    private val savepoints = mutableListOf<String>()

    fun observeSuccessfulStatement(normalizedSql: String) {
        val tokens = TransactionControlTokens(normalizedSql)
        when (tokens.nextKeyword()) {
            "BEGIN" -> {
                explicitTransactionActive = true
                savepoints.clear()
            }

            "COMMIT", "END" -> finishTransaction()
            "ROLLBACK" -> observeRollback(tokens)
            "SAVEPOINT" -> tokens.nextIdentifier()?.let { savepoints += it.canonicalSavepointName() }
            "RELEASE" -> observeRelease(tokens)
        }
    }

    fun inTransaction(): Boolean = explicitTransactionActive || savepoints.isNotEmpty()

    fun reset() {
        finishTransaction()
    }

    private fun observeRollback(tokens: TransactionControlTokens) {
        var token = tokens.nextKeyword()
        if (token == "TRANSACTION") {
            token = tokens.nextKeyword()
        }
        if (token != "TO") {
            finishTransaction()
            return
        }

        if (tokens.peekKeyword() == "SAVEPOINT") {
            tokens.nextKeyword()
        }
        val savepoint = tokens.nextIdentifier()?.canonicalSavepointName() ?: return
        val index = savepoints.indexOfLast { it == savepoint }
        if (index >= 0) {
            savepoints.subList(index + 1, savepoints.size).clear()
        }
    }

    private fun observeRelease(tokens: TransactionControlTokens) {
        if (tokens.peekKeyword() == "SAVEPOINT") {
            tokens.nextKeyword()
        }
        val savepoint = tokens.nextIdentifier()?.canonicalSavepointName() ?: return
        val index = savepoints.indexOfLast { it == savepoint }
        if (index >= 0) {
            savepoints.subList(index, savepoints.size).clear()
        }
    }

    private fun finishTransaction() {
        explicitTransactionActive = false
        savepoints.clear()
    }
}

private class TransactionControlTokens(
    private val sql: String,
) {
    private var offset = 0
    private var peeked: String? = null

    fun nextKeyword(): String? = nextIdentifier()?.uppercase()

    fun peekKeyword(): String? {
        if (peeked == null) {
            peeked = readIdentifier()
        }
        return peeked?.uppercase()
    }

    fun nextIdentifier(): String? {
        val buffered = peeked
        if (buffered != null) {
            peeked = null
            return buffered
        }
        return readIdentifier()
    }

    private fun readIdentifier(): String? {
        skipSeparators()
        if (offset >= sql.length) return null

        return when (val opening = sql[offset]) {
            '"', '\'', '`' -> readQuotedIdentifier(opening)
            '[' -> readBracketedIdentifier()
            else -> readUnquotedIdentifier()
        }
    }

    private fun skipSeparators() {
        while (offset < sql.length && (sql[offset].isWhitespace() || sql[offset] == ';')) {
            offset++
        }
    }

    private fun readQuotedIdentifier(quote: Char): String {
        offset++
        val result = StringBuilder()
        while (offset < sql.length) {
            val current = sql[offset++]
            if (current != quote) {
                result.append(current)
                continue
            }
            if (offset < sql.length && sql[offset] == quote) {
                result.append(quote)
                offset++
                continue
            }
            break
        }
        return result.toString()
    }

    private fun readBracketedIdentifier(): String {
        offset++
        val start = offset
        while (offset < sql.length && sql[offset] != ']') {
            offset++
        }
        val result = sql.substring(start, offset)
        if (offset < sql.length) {
            offset++
        }
        return result
    }

    private fun readUnquotedIdentifier(): String {
        val start = offset
        while (
            offset < sql.length &&
            !sql[offset].isWhitespace() &&
            sql[offset] != ';' &&
            sql[offset] != '"' &&
            sql[offset] != '\'' &&
            sql[offset] != '`' &&
            sql[offset] != '['
        ) {
            offset++
        }
        return sql.substring(start, offset)
    }
}

private fun String.canonicalSavepointName(): String = buildString(length) {
    for (character in this@canonicalSavepointName) {
        append(if (character in 'A'..'Z') character.lowercaseChar() else character)
    }
}
