/*
 * Copyright 2025 Anatoliy Pochkin
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
package dev.goquick.sqlitenow.gradle.sqlite

private enum class State { OUTSIDE, LINE_COMMENT, BLOCK_COMMENT, SINGLE_QUOTE, DOUBLE_QUOTE }

/**
 * Splits the given SQL script into individual statements, correctly handling semicolons
 * that appear inside quoted strings or comments, and extracts both leading and inner comments.
 * This implementation preserves the original formatting by slicing the input string,
 * and associates comments that follow a semicolon with the next statement.
 *
 * @param sql the full SQL script containing one or more statements; must not be {@code null}
 * @return an ordered list of SqlSingleStatement objects containing SQL statements and their comments
 */
internal fun parseSqlStatements(sql: String): List<SqlSingleStatement> {
    val results = mutableListOf<SqlSingleStatement>()
    val topComments = mutableListOf<String>()
    val innerComments = mutableListOf<String>()
    val pendingTopComments = mutableListOf<String>() // Comments between statements that belong to the next statement

    var stmtStart: Int? = null               // index where current statement begins
    val commentBuf = StringBuilder()
    var readingStmt = false               // whether we've seen non-comment SQL content
    var state = State.OUTSIDE

    fun flushStatement(endIdx: Int) {
        val rawSql = sql.substring(stmtStart!!, endIdx)

        // Add any pending top comments from previous statement
        if (pendingTopComments.isNotEmpty()) {
            topComments.addAll(0, pendingTopComments)
            pendingTopComments.clear()
        }

        results += SqlSingleStatement(
            topComments.toList(),
            innerComments.toList(),
            rawSql,
        )
        topComments.clear()
        innerComments.clear()
        stmtStart = null
        readingStmt = false
    }

    var i = 0
    while (i < sql.length) {
        val c = sql[i]
        val c2 = sql.getOrNull(i + 1)
        when (state) {
            State.OUTSIDE -> when {
                // start line comment
                c == '-' && c2 == '-' -> {
                    state = State.LINE_COMMENT
                    commentBuf.clear()
                    i += 2
                }
                // start block comment
                c == '/' && c2 == '*' -> {
                    state = State.BLOCK_COMMENT
                    commentBuf.clear()
                    i += 2
                }
                // single-quoted literal
                c == '\'' -> {
                    if (stmtStart == null) {
                        stmtStart = i
                    }
                    readingStmt = true
                    state = State.SINGLE_QUOTE
                    i++
                }
                // double-quoted literal/identifier
                c == '"' -> {
                    if (stmtStart == null) {
                        stmtStart = i
                    }
                    readingStmt = true
                    state = State.DOUBLE_QUOTE
                    i++
                }
                // statement terminator
                c == ';' -> {
                    if (stmtStart == null) {
                        stmtStart = i
                    }
                    // include semicolon
                    val endIdx = i + 1
                    flushStatement(endIdx)
                    i = endIdx
                }
                // skip leading whitespace
                c.isWhitespace() && !readingStmt -> i++
                // normal SQL content
                else -> {
                    if (stmtStart == null) {
                        stmtStart = i
                    }
                    readingStmt = true
                    i++
                }
            }

            State.LINE_COMMENT -> if (c == '\n' || c == '\r') {
                val comment = "--$commentBuf"
                if (stmtStart == null) {
                    // This is a comment between statements, save it for the next statement
                    pendingTopComments += comment
                    // Don't set stmtStart here, wait for actual SQL content
                } else if (!readingStmt) {
                    // This is a top comment for the current statement
                    topComments += comment
                } else {
                    // This is an inner comment within the current statement
                    innerComments += comment
                }
                state = State.OUTSIDE
                i++
            } else {
                commentBuf.append(c); i++
            }

            State.BLOCK_COMMENT -> if (c == '*' && c2 == '/') {
                val comment = "/*$commentBuf*/"
                if (stmtStart == null) {
                    // This is a comment between statements, save it for the next statement
                    pendingTopComments += comment
                    // Don't set stmtStart here, wait for actual SQL content
                } else if (!readingStmt) {
                    // This is a top comment for the current statement
                    topComments += comment
                } else {
                    // This is an inner comment within the current statement
                    innerComments += comment
                }
                state = State.OUTSIDE; i += 2
            } else {
                commentBuf.append(c); i++
            }

            State.SINGLE_QUOTE -> if (c == '\'' && c2 == '\'') {
                i += 2
            } else if (c == '\'') {
                state = State.OUTSIDE; i++
            } else {
                i++
            }

            State.DOUBLE_QUOTE -> if (c == '"' && c2 == '"') {
                i += 2
            } else if (c == '"') {
                state = State.OUTSIDE; i++
            } else {
                i++
            }
        }
    }

    // unterminated line comment at EOF
    if (state == State.LINE_COMMENT) {
        val comment = "--$commentBuf"
        if (stmtStart == null) {
            // This is a comment at the end of the file with no following statement
            // We'll add it to the last statement if there is one
            if (results.isNotEmpty()) {
                val lastStatement = results.last()
                val updatedComments = lastStatement.innerComments + comment
                results[results.size - 1] = lastStatement.copy(innerComments = updatedComments)
            }
        } else if (!readingStmt) {
            topComments += comment
        } else {
            innerComments += comment
        }
    }

    // flush any trailing SQL (including queries without semicolon)
    if (stmtStart != null) {
        flushStatement(sql.length)
    }

    return results
}
