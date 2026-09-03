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
package dev.goquick.sqlitenow.core

import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.sqlite.use

/** Application code invoked after one numbered migration boundary has been reached. */
typealias SqliteNowMigrationStepCallback = suspend (SqliteNowMigrationScope) -> Unit

/**
 * The version boundary currently being migrated.
 *
 * [connection] is deliberately narrower than [SafeSQLiteConnection]. It cannot close the
 * connection or begin, commit, or roll back a transaction, and it rejects lifecycle-owned SQL.
 */
class SqliteNowMigrationScope internal constructor(
    rawConnection: SafeSQLiteConnection,
    private val ownerAccess: MigrationOwnerAccess,
    val originalVersion: Int,
    val fromVersion: Int,
    val toVersion: Int,
    val targetVersion: Int,
) {
    val connection: SqliteNowMigrationConnection = SqliteNowMigrationConnection(rawConnection, ownerAccess)

    internal fun expire() {
        ownerAccess.expire()
    }

    internal suspend fun drain(
        cancelOperations: Boolean,
        propagateOperationFailure: Boolean,
    ) {
        ownerAccess.expireAndDrain(cancelOperations, propagateOperationFailure)
    }
}

/** Runtime entry point used by generated migration implementations. */
object SqliteNowMigrationStepRunner {
    suspend fun run(
        rawConnection: SafeSQLiteConnection,
        originalVersion: Int,
        fromVersion: Int,
        toVersion: Int,
        targetVersion: Int,
        callback: SqliteNowMigrationStepCallback,
    ) {
        val ownerAccess = rawConnection.captureMigrationOwnerAccess()
        val scope = SqliteNowMigrationScope(
            rawConnection = rawConnection,
            ownerAccess = ownerAccess,
            originalVersion = originalVersion,
            fromVersion = fromVersion,
            toVersion = toVersion,
            targetVersion = targetVersion,
        )
        var callbackFailure: Throwable? = null
        try {
            callback(scope)
        } catch (failure: Throwable) {
            callbackFailure = failure
            throw failure
        } finally {
            scope.expire()
            scope.drain(
                cancelOperations = callbackFailure != null,
                propagateOperationFailure = callbackFailure == null,
            )
        }
    }
}

/** Low-level SQL capability available only for the duration of a migration-step callback. */
class SqliteNowMigrationConnection internal constructor(
    private val connection: SafeSQLiteConnection,
    private val ownerAccess: MigrationOwnerAccess,
) {
    suspend fun execSQL(sql: String) {
        requireMigrationSqlAllowed(sql)
        connection.withMigrationOwnerAccess(ownerAccess) {
            connection.execSQL(sql)
        }
    }

    suspend fun <T> usePrepared(
        sql: String,
        block: suspend (SQLiteStatement) -> T,
    ): T {
        requireMigrationSqlAllowed(sql)
        return connection.withMigrationOwnerAccess(ownerAccess) {
            connection.prepare(sql).use { statement -> block(statement) }
        }
    }
}

private val forbiddenMigrationStatementKeywords = setOf(
    "BEGIN",
    "COMMIT",
    "END",
    "ROLLBACK",
    "SAVEPOINT",
    "RELEASE",
)

internal fun requireMigrationSqlAllowed(sql: String) {
    val sanitized = stripMigrationSqlCommentsAndStrings(sql)
    for ((statementStart, statementEnd) in migrationSqlStatementRanges(sanitized)) {
        val sanitizedStatement = sanitized.substring(statementStart, statementEnd)
        val originalStatement = sql.substring(statementStart, statementEnd)
        val keywordMatch = Regex("[A-Za-z_]+").find(sanitizedStatement) ?: continue
        val keyword = keywordMatch.value.uppercase()
        require(keyword !in forbiddenMigrationStatementKeywords) {
            "$keyword is not allowed from a migration-step callback"
        }

        if (keyword == "PRAGMA") {
            val pragmaName = readMigrationPragmaName(
                originalStatement,
                keywordMatch.range.last + 1,
            )
            require(!pragmaName.equals("user_version", ignoreCase = true)) {
                "PRAGMA user_version is owned by SQLiteNow during migration"
            }
        }
    }
}

private fun migrationSqlStatementRanges(sql: String): List<Pair<Int, Int>> {
    val ranges = mutableListOf<Pair<Int, Int>>()
    var statementStart = 0
    var index = 0
    var trigger = false
    var triggerBody = false
    var triggerEndSeen = false
    var caseDepth = 0
    val leadingWords = mutableListOf<String>()

    fun resetStatement() {
        trigger = false
        triggerBody = false
        triggerEndSeen = false
        caseDepth = 0
        leadingWords.clear()
    }

    while (index < sql.length) {
        if (sql[index].isLetter() || sql[index] == '_') {
            val wordStart = index
            index++
            while (index < sql.length && (sql[index].isLetterOrDigit() || sql[index] == '_')) index++
            val word = sql.substring(wordStart, index).uppercase()
            if (!triggerBody) {
                if (leadingWords.size < 4) leadingWords += word
                trigger = trigger || leadingWords == listOf("CREATE", "TRIGGER") ||
                    leadingWords == listOf("CREATE", "TEMP", "TRIGGER") ||
                    leadingWords == listOf("CREATE", "TEMPORARY", "TRIGGER")
                if (trigger && word == "BEGIN") triggerBody = true
            } else {
                when (word) {
                    "CASE" -> caseDepth++
                    "END" -> {
                        if (caseDepth > 0) caseDepth-- else triggerEndSeen = true
                    }
                }
            }
            continue
        }

        if (sql[index] == ';' && (!triggerBody || triggerEndSeen)) {
            ranges += statementStart to index
            statementStart = index + 1
            resetStatement()
        }
        index++
    }
    if (statementStart < sql.length) ranges += statementStart to sql.length
    return ranges
}

private fun readMigrationPragmaName(statement: String, startIndex: Int): String? {
    var index = skipMigrationSqlTrivia(statement, startIndex)
    val first = readMigrationSqlIdentifier(statement, index) ?: return null
    index = skipMigrationSqlTrivia(statement, first.second)
    if (statement.getOrNull(index) != '.') return first.first
    index = skipMigrationSqlTrivia(statement, index + 1)
    return readMigrationSqlIdentifier(statement, index)?.first
}

private fun skipMigrationSqlTrivia(sql: String, startIndex: Int): Int {
    var index = startIndex
    while (index < sql.length) {
        when {
            sql[index].isWhitespace() -> index++
            sql[index] == '-' && sql.getOrNull(index + 1) == '-' -> {
                index += 2
                while (index < sql.length && sql[index] != '\n') index++
            }
            sql[index] == '/' && sql.getOrNull(index + 1) == '*' -> {
                index += 2
                while (index < sql.length) {
                    if (sql[index] == '*' && sql.getOrNull(index + 1) == '/') {
                        index += 2
                        break
                    }
                    index++
                }
            }
            else -> return index
        }
    }
    return index
}

private fun readMigrationSqlIdentifier(sql: String, startIndex: Int): Pair<String, Int>? {
    if (startIndex >= sql.length) return null
    val opening = sql[startIndex]
    if (opening == '\'' || opening == '"' || opening == '`' || opening == '[') {
        val closing = if (opening == '[') ']' else opening
        val value = StringBuilder()
        var index = startIndex + 1
        while (index < sql.length) {
            if (sql[index] == closing) {
                if (sql.getOrNull(index + 1) == closing) {
                    value.append(closing)
                    index += 2
                    continue
                }
                return value.toString() to (index + 1)
            }
            value.append(sql[index])
            index++
        }
        return value.toString() to index
    }

    var index = startIndex
    while (index < sql.length && (sql[index].isLetterOrDigit() || sql[index] == '_')) index++
    if (index == startIndex) return null
    return sql.substring(startIndex, index) to index
}

private fun stripMigrationSqlCommentsAndStrings(sql: String): String {
    val result = StringBuilder(sql.length)
    var index = 0
    while (index < sql.length) {
        when {
            sql[index] == '\'' || sql[index] == '"' || sql[index] == '`' || sql[index] == '[' -> {
                val opening = sql[index]
                val closing = if (opening == '[') ']' else opening
                result.append(' ')
                index++
                while (index < sql.length) {
                    if (sql[index] == closing && sql.getOrNull(index + 1) == closing) {
                        result.append("  ")
                        index += 2
                    } else if (sql[index] == closing) {
                        result.append(' ')
                        index++
                        break
                    } else {
                        result.append(if (sql[index] == '\n') '\n' else ' ')
                        index++
                    }
                }
            }
            sql[index] == '-' && sql.getOrNull(index + 1) == '-' -> {
                result.append("  ")
                index += 2
                while (index < sql.length && sql[index] != '\n') {
                    result.append(' ')
                    index++
                }
            }
            sql[index] == '/' && sql.getOrNull(index + 1) == '*' -> {
                result.append("  ")
                index += 2
                while (index < sql.length) {
                    if (sql[index] == '*' && sql.getOrNull(index + 1) == '/') {
                        result.append("  ")
                        index += 2
                        break
                    }
                    result.append(if (sql[index] == '\n') '\n' else ' ')
                    index++
                }
            }
            else -> {
                result.append(sql[index])
                index++
            }
        }
    }
    return result.toString()
}
